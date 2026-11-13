"""Headless PixelForge quality gate.

CPU stills (ONNX SPAN, and the bundled NCNN weights through the NCNN Python
package) run anywhere those packages import. The shipping Vulkan executables,
NVIDIA RTX VSR, and libvmaf do not run in a CPU-only environment. This script
records that gap instead of inventing those numbers.

Exit codes:
  0  the requested scope passed
  2  a model ran and was not visibly different from Lanczos
  3  the default Real-ESRGAN weights could not be executed
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pixelforge.metrics import aligned_scores, mean_abs_diff, psnr, score_pair, ssim, tenengrad
from pixelforge.ncnn_infer import upscale_rgb
from pixelforge.presets import get_profile_preset
from pixelforge.reference_scenes import degrade, lanczos_upscale, make_detail_scene, product_still_crop


VISIBLE_SHARPNESS_RATIO = 1.25
VISIBLE_MEAN_ABS_DIFF = 2.0
DEFAULT_MODEL = str(get_profile_preset("balanced", "live", "enhanced")["model"])


def _ffmpeg_has_filter(name: str) -> bool:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return False
    completed = subprocess.run(
        [ffmpeg, "-hide_banner", "-filters"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return f" {name} " in completed.stdout or completed.stdout.strip().endswith(name)


def alignment_self_test() -> dict[str, object]:
    """A two-frame shift must be found, and the aligned PSNR must recover."""
    frames = []
    for index in range(8):
        image = Image.new("RGB", (96, 64), (index * 24, 40, 180 - index * 12))
        draw_band = 8 + index * 6
        for x in range(draw_band, draw_band + 10):
            for y in range(8, 56):
                image.putpixel((min(95, x), y), (250, 220, 40))
        frames.append(image)
    shifted = frames[2:] + frames[:2]
    aligned = aligned_scores(frames, shifted, radius=4)
    unaligned_gap = float(aligned["psnr"]) - float(aligned["unaligned_psnr"])
    passed = int(aligned["offset"]) == -2 and unaligned_gap > 5.0 and float(aligned["psnr"]) > 40.0
    return {
        "passed": passed,
        "expected_offset": -2,
        "measured_offset": aligned["offset"],
        "aligned_psnr": aligned["psnr"],
        "unaligned_psnr": aligned["unaligned_psnr"],
        "note": "Offset 0 is the v1.0.20-v1.0.22 failure mode: metrics compared different frames.",
    }


def _scenes() -> list[dict[str, object]]:
    detail = make_detail_scene()
    scenes = [{"name": "synthetic_detail", "truth": detail, "scale": 4, "jpeg_quality": 40}]
    product = product_still_crop(ROOT)
    if product is not None:
        scenes.append({"name": "product_still_crop", "truth": product, "scale": 4, "jpeg_quality": 40})
    return scenes


def _onnx_span(source: Image.Image) -> Image.Image:
    from pixelforge.onnx_upscale import OnnxUpscaler

    model = ROOT / "onnx-models" / "4xSPANkendata_fp32.onnx"
    runner = OnnxUpscaler(model, 4, tile_size=128, context=8)
    return runner.upscale_image(source)


def _available_engines() -> list[dict[str, object]]:
    engines: list[dict[str, object]] = []
    span = ROOT / "onnx-models" / "4xSPANkendata_fp32.onnx"
    try:
        import onnxruntime  # noqa: F401

        if span.is_file():
            engines.append({
                "name": "span-photo-x4",
                "kind": "onnx-cpu",
                "role": "fast-preview",
                "run": _onnx_span,
            })
    except ImportError:
        pass
    ncnn_models = (
        ("realesrgan-x4plus", ROOT / "models" / "realesrgan-x4plus.param", ROOT / "models" / "realesrgan-x4plus.bin", "default-enhanced"),
        ("realsr-df2k", ROOT / "realsr-models" / "models-DF2K" / "x4.param", ROOT / "realsr-models" / "models-DF2K" / "x4.bin", "faithful"),
    )
    try:
        import ncnn  # noqa: F401
    except ImportError:
        ncnn = None
    if ncnn is not None:
        for name, param, weights, role in ncnn_models:
            if param.is_file() and weights.is_file():
                engines.append({
                    "name": name,
                    "kind": "ncnn-python-cpu",
                    "role": role,
                    "run": lambda image, param=param, weights=weights: upscale_rgb(param, weights, image),
                })
    return engines


def _vulkan_exe_status() -> dict[str, str]:
    expected = {
        "realesrgan-ncnn-vulkan.exe": ROOT / "realesrgan-ncnn-vulkan.exe",
        "realsr-ncnn-vulkan.exe": ROOT / "realsr-ncnn-vulkan.exe",
        "realcugan-ncnn-vulkan.exe": ROOT / "realcugan-ncnn-vulkan.exe",
        "srmd-ncnn-vulkan.exe": ROOT / "srmd-ncnn-vulkan.exe",
        "rife-ncnn-vulkan.exe": ROOT / "rife-ncnn-vulkan.exe",
    }
    status: dict[str, str] = {}
    for name, path in expected.items():
        if not path.is_file():
            status[name] = "missing"
            continue
        if path.read_bytes()[:2] == b"MZ" and platform.system() != "Windows":
            status[name] = "windows-executable-not-run-on-this-host"
            continue
        try:
            completed = subprocess.run(
                [str(path), "-h"],
                check=False,
                capture_output=True,
                timeout=8,
            )
            status[name] = "runs" if completed.returncode == 0 or completed.stderr or completed.stdout else "not-executed"
        except OSError as exc:
            status[name] = f"cannot-start: {exc.__class__.__name__}"
    return status


def _score_against_lanczos(truth: Image.Image, candidate: Image.Image, lanczos: Image.Image) -> dict[str, object]:
    paired = score_pair(truth, candidate)
    lanczos_scores = score_pair(truth, lanczos)
    sharp_ratio = float(paired["sharpness_tenengrad"]) / max(1e-9, float(lanczos_scores["sharpness_tenengrad"]))
    difference = mean_abs_diff(candidate, lanczos)
    visible = sharp_ratio >= VISIBLE_SHARPNESS_RATIO and difference >= VISIBLE_MEAN_ABS_DIFF
    return {
        **paired,
        "lanczos_psnr": lanczos_scores["psnr"],
        "lanczos_ssim": lanczos_scores["ssim"],
        "lanczos_sharpness_tenengrad": lanczos_scores["sharpness_tenengrad"],
        "sharpness_ratio_vs_lanczos": round(sharp_ratio, 4),
        "mean_abs_diff_vs_lanczos": round(difference, 4),
        "closer_psnr_than_lanczos": float(paired["psnr"]) > float(lanczos_scores["psnr"]),
        "closer_ssim_than_lanczos": float(paired["ssim"]) > float(lanczos_scores["ssim"]),
        "visibly_different_from_lanczos": visible,
        "lpips_status": "computed" if paired["lpips"] is not None else "not_installed",
    }


def _same_resolution_roundtrip(low_res: Image.Image, upscaled: Image.Image) -> dict[str, float]:
    returned = upscaled.resize(low_res.size, Image.Resampling.LANCZOS)
    return {
        "mean_abs_diff_vs_input": round(mean_abs_diff(low_res, returned), 4),
        "psnr_vs_input": round(psnr(low_res, returned), 4),
        "ssim_vs_input": round(ssim(low_res, returned), 6),
    }


def run_gate(output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    alignment = alignment_self_test()
    engines = _available_engines()
    scene_reports: list[dict[str, object]] = []
    for scene in _scenes():
        truth: Image.Image = scene["truth"]  # type: ignore[assignment]
        low_res = degrade(truth, scale=int(scene["scale"]), jpeg_quality=int(scene["jpeg_quality"]))
        lanczos = lanczos_upscale(low_res, truth.width, truth.height)
        scene_dir = output_dir / str(scene["name"])
        scene_dir.mkdir(parents=True, exist_ok=True)
        truth.save(scene_dir / "truth.png")
        low_res.save(scene_dir / "low_res.jpg", quality=90)
        lanczos.save(scene_dir / "lanczos.png")
        engine_reports = []
        for engine in engines:
            timer = time.perf_counter()
            try:
                enhanced = engine["run"](low_res)
            except Exception as exc:
                engine_reports.append({
                    "engine": engine["name"],
                    "kind": engine["kind"],
                    "role": engine["role"],
                    "error": str(exc),
                })
                continue
            if enhanced.size != truth.size:
                enhanced = enhanced.resize(truth.size, Image.Resampling.LANCZOS)
            enhanced.save(scene_dir / f"{engine['name']}.png")
            report = _score_against_lanczos(truth, enhanced, lanczos)
            report.update({
                "engine": engine["name"],
                "kind": engine["kind"],
                "role": engine["role"],
                "seconds": round(time.perf_counter() - timer, 3),
                "same_resolution_roundtrip": _same_resolution_roundtrip(low_res, enhanced),
            })
            engine_reports.append(report)
        scene_reports.append({
            "name": scene["name"],
            "truth_size": [truth.width, truth.height],
            "low_res_size": [low_res.width, low_res.height],
            "lanczos": {
                "psnr": round(psnr(truth, lanczos), 4),
                "ssim": round(ssim(truth, lanczos), 6),
                "sharpness_tenengrad": round(tenengrad(lanczos), 4),
            },
            "engines": engine_reports,
        })

    default_rows = [
        engine
        for scene in scene_reports
        for engine in scene["engines"]
        if engine.get("engine") == DEFAULT_MODEL and "error" not in engine
    ]
    default_visible = bool(default_rows) and all(bool(row.get("visibly_different_from_lanczos")) for row in default_rows)
    vulkan = _vulkan_exe_status()
    vulkan_ran = any(state == "runs" for state in vulkan.values())
    owner_must_run = [
        "RUN_QUALITY_GATE.vbs on Windows with a Vulkan driver, so the shipping realesrgan-ncnn-vulkan.exe path is scored. Python NCNN on CPU is the same weights, not that executable.",
        "An NVIDIA RTX GPU for the nvidia-rtx-vsr profile. This host did not run it.",
        "ffmpeg built with libvmaf, if a VMAF number is required. This gate does not emit VMAF.",
    ]
    if not any(engine["name"] == DEFAULT_MODEL for engine in engines):
        owner_must_run.insert(0, "Install the ncnn Python package or run the Vulkan executable so Real-ESRGAN x4plus can be scored.")
    cpu_passed = bool(alignment["passed"]) and default_visible
    report: dict[str, object] = {
        "version": "1.0.23",
        "default_model": DEFAULT_MODEL,
        "default_fidelity": "enhanced",
        "faithful_model": str(get_profile_preset("balanced", "live", "faithful")["model"]),
        "host": {
            "system": platform.system(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "visibility_rule": {
            "sharpness_ratio_vs_lanczos_at_least": VISIBLE_SHARPNESS_RATIO,
            "mean_abs_diff_vs_lanczos_at_least": VISIBLE_MEAN_ABS_DIFF,
            "psnr_ssim_lpips": "Reported against the ground-truth still. Beating Lanczos on PSNR is not required and is not claimed unless closer_psnr_than_lanczos is true.",
        },
        "alignment_self_test": alignment,
        "lpips_status": next(
            (
                engine.get("lpips_status")
                for scene in scene_reports
                for engine in scene["engines"]
                if engine.get("lpips_status")
            ),
            "not_installed",
        ),
        "ffmpeg_libvmaf": _ffmpeg_has_filter("libvmaf"),
        "vulkan_executables": vulkan,
        "scenes": scene_reports,
        "cpu_visibility_passed": cpu_passed,
        "vulkan_executable_ran": vulkan_ran,
        "quality_gate_done": False,
        "owner_must_run": owner_must_run,
        "withdrawn_history": "v1.0.20 through v1.0.22 representative-video VMAF/SSIM/PSNR numbers are withdrawn. They were computed without a frame-offset search.",
        "seconds": round(time.perf_counter() - started, 3),
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the PixelForge still-image quality gate")
    parser.add_argument("--output", type=Path, default=ROOT / "_benchmarks" / "quality-gate")
    parser.add_argument("--full", action="store_true", help="Also require the shipping Vulkan executable before writing quality-gate.done")
    args = parser.parse_args()
    output = args.output.resolve()
    report = run_gate(output)
    cpu_passed = bool(report["cpu_visibility_passed"])
    vulkan_ran = bool(report["vulkan_executable_ran"])
    report["quality_gate_done"] = bool(cpu_passed and vulkan_ran)
    destination = output / "GATE_RESULTS.json"
    destination.write_text(json.dumps(report, indent=2), encoding="utf-8")
    cpu_marker = output / "quality-gate.cpu.done"
    full_marker = output / "quality-gate.done"
    if cpu_passed:
        cpu_marker.write_text(destination.name + "\n", encoding="utf-8")
    elif cpu_marker.exists():
        cpu_marker.unlink()
    if report["quality_gate_done"]:
        full_marker.write_text(destination.name + "\n", encoding="utf-8")
    elif full_marker.exists():
        full_marker.unlink()
    print(f"Wrote {destination}")
    print(
        f"default={report['default_model']} cpu_passed={cpu_passed} "
        f"vulkan_ran={vulkan_ran} quality_gate_done={report['quality_gate_done']}"
    )
    if not any(
        engine.get("engine") == DEFAULT_MODEL and "error" not in engine
        for scene in report["scenes"]
        for engine in scene["engines"]
    ):
        return 3
    if not cpu_passed:
        return 2
    if args.full and not vulkan_ran:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
