"""Reproducible PixelForge preset comparison on a known high-resolution frame.

The script degrades a source frame, reconstructs it with each local engine, and
records full-reference metrics plus labeled contact sheets. It never uploads the
source image.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pixelforge.onnx_upscale import OnnxUpscaler


def run(command: list[str]) -> float:
    started = time.perf_counter()
    subprocess.run(command, check=True, creationflags=0x08000000)
    return time.perf_counter() - started


def ffmpeg_image(source: Path, output: Path, vf: str) -> None:
    run(["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", str(source), "-vf", vf, str(output)])


def metrics(reference_path: Path, candidate_path: Path) -> dict[str, float]:
    reference = np.asarray(Image.open(reference_path).convert("RGB"), dtype=np.float32) / 255.0
    candidate_image = Image.open(candidate_path).convert("RGB").resize((reference.shape[1], reference.shape[0]), Image.Resampling.LANCZOS)
    candidate = np.asarray(candidate_image, dtype=np.float32) / 255.0
    mse = float(np.mean((reference - candidate) ** 2))
    psnr = 99.0 if mse <= 1e-12 else float(10.0 * np.log10(1.0 / mse))
    ref_y = (reference[..., 0] * 0.2126) + (reference[..., 1] * 0.7152) + (reference[..., 2] * 0.0722)
    out_y = (candidate[..., 0] * 0.2126) + (candidate[..., 1] * 0.7152) + (candidate[..., 2] * 0.0722)
    c1, c2 = 0.01**2, 0.03**2
    mu_x, mu_y = float(ref_y.mean()), float(out_y.mean())
    var_x, var_y = float(ref_y.var()), float(out_y.var())
    covariance = float(((ref_y - mu_x) * (out_y - mu_y)).mean())
    ssim = ((2 * mu_x * mu_y + c1) * (2 * covariance + c2)) / ((mu_x**2 + mu_y**2 + c1) * (var_x + var_y + c2))
    ref_edges = np.abs(np.diff(ref_y, axis=0)).mean() + np.abs(np.diff(ref_y, axis=1)).mean()
    out_edges = np.abs(np.diff(out_y, axis=0)).mean() + np.abs(np.diff(out_y, axis=1)).mean()
    edge_error = float(abs(ref_edges - out_edges))
    score = psnr + (8.0 * ssim) - (80.0 * edge_error)
    return {"psnr": round(psnr, 4), "ssim_global": round(float(ssim), 6), "edge_error": round(edge_error, 6), "score": round(score, 4)}


def ncnn(source: Path, output: Path, model: str) -> float:
    if model == "realesrgan-x4plus":
        command = [str(ROOT / "realesrgan-ncnn-vulkan.exe"), "-i", str(source), "-o", str(output), "-m", str(ROOT / "models"), "-n", model, "-s", "4", "-f", "png", "-j", "4:4:4"]
    elif model == "realsr-df2k":
        command = [str(ROOT / "realsr-ncnn-vulkan.exe"), "-i", str(source), "-o", str(output), "-m", str(ROOT / "realsr-models" / "models-DF2K"), "-s", "4", "-f", "png", "-j", "4:4:4"]
    elif model == "realsr-df2k-jpeg":
        command = [str(ROOT / "realsr-ncnn-vulkan.exe"), "-i", str(source), "-o", str(output), "-m", str(ROOT / "realsr-models" / "models-DF2K_JPEG"), "-s", "4", "-f", "png", "-j", "4:4:4"]
    elif model.startswith("srmd-"):
        noise = model.rsplit("n", 1)[1]
        command = [str(ROOT / "srmd-ncnn-vulkan.exe"), "-i", str(source), "-o", str(output), "-m", str(ROOT / "models-srmd"), "-n", noise, "-s", "4", "-f", "png", "-j", "4:4:4"]
    elif model.startswith("realcugan-"):
        noise = model.rsplit("n", 1)[1]
        command = [str(ROOT / "realcugan-ncnn-vulkan.exe"), "-i", str(source), "-o", str(output), "-m", str(ROOT / "realcugan-models" / "models-se"), "-n", noise, "-s", "2", "-f", "png", "-j", "4:4:4"]
    else:
        raise ValueError(model)
    return run(command)


def onnx(source: Path, output: Path, model_name: str, scale: int) -> tuple[float, str]:
    runner = OnnxUpscaler(ROOT / "onnx-models" / model_name, scale, tile_size=256, context=16)
    started = time.perf_counter()
    with Image.open(source) as image:
        result = runner.upscale_image(image)
    result.save(output, format="PNG", compress_level=1)
    return time.perf_counter() - started, runner.provider


def contact_sheet(reference: Path, low_res: Path, candidates: list[tuple[str, Path]], destination: Path) -> None:
    items = [("Degraded input", low_res), ("Reference", reference), *candidates]
    tile_size = (480, 270)
    canvas = Image.new("RGB", (tile_size[0] * 2, (tile_size[1] + 36) * ((len(items) + 1) // 2)), "#08101d")
    draw = ImageDraw.Draw(canvas)
    for index, (label, path) in enumerate(items):
        x = (index % 2) * tile_size[0]
        y = (index // 2) * (tile_size[1] + 36)
        image = Image.open(path).convert("RGB").resize(tile_size, Image.Resampling.LANCZOS)
        canvas.paste(image, (x, y + 36))
        draw.text((x + 10, y + 10), label, fill="#d8f7ff")
    canvas.save(destination, format="PNG", compress_level=1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path, help="High-resolution source image")
    parser.add_argument("--output", type=Path, default=ROOT / "_benchmarks" / "preset-study")
    args = parser.parse_args()
    output = args.output
    output.mkdir(parents=True, exist_ok=True)

    truth = output / "truth.png"
    ffmpeg_image(args.source, truth, "crop=1024:576:(iw-1024)/2:(ih-576)/2")
    live_lr = output / "live_lr.png"
    animation_lr = output / "animation_lr.png"
    restore_lr = output / "restore_lr.jpg"
    ffmpeg_image(truth, live_lr, "scale=256:144:flags=lanczos")
    ffmpeg_image(truth, animation_lr, "scale=512:288:flags=lanczos")
    run(["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", str(truth), "-vf", "scale=256:144:flags=bicubic,noise=alls=7:allf=t", "-q:v", "12", str(restore_lr)])

    studies = {
        "live": (live_lr, [("realesrgan-x4plus", "ncnn"), ("realsr-df2k", "ncnn"), ("span-photo-x4", "onnx4")]),
        "animation": (animation_lr, [("realcugan-se-x2-n0", "ncnn"), ("realcugan-se-x2-n1", "ncnn"), ("realcugan-se-x2-n3", "ncnn"), ("span-modern-animation-x2", "onnx2")]),
        "restore": (restore_lr, [("srmd-x4-n3", "ncnn"), ("srmd-x4-n5", "ncnn"), ("srmd-x4-n8", "ncnn"), ("realsr-df2k-jpeg", "ncnn"), ("span-photo-x4", "onnx4")]),
    }
    report: dict[str, object] = {}
    for study, (low_res, engines) in studies.items():
        entries: list[dict[str, object]] = []
        sheet_items: list[tuple[str, Path]] = []
        for name, kind in engines:
            destination = output / f"{study}_{name}.png"
            if kind == "ncnn":
                elapsed = ncnn(low_res, destination, name)
                provider = "NCNN Vulkan"
            elif kind == "onnx4":
                elapsed, provider = onnx(low_res, destination, "4xSPANkendata_fp32.onnx", 4)
            else:
                elapsed, provider = onnx(low_res, destination, "2x_ModernSpanimationV1_fp32_op17.onnx", 2)
            result = {"engine": name, "seconds": round(elapsed, 3), "provider": provider, **metrics(truth, destination)}
            entries.append(result)
            sheet_items.append((f"{name} | score {result['score']}", destination))

            for cas in (0.08, 0.14, 0.20):
                filtered = output / f"{study}_{name}_cas{cas:.2f}.png"
                ffmpeg_image(destination, filtered, f"cas={cas}")
                filtered_result = {"engine": f"{name}+CAS {cas:.2f}", "seconds": None, "provider": provider, **metrics(truth, filtered)}
                entries.append(filtered_result)
        entries.sort(key=lambda item: float(item["score"]), reverse=True)
        report[study] = entries
        contact_sheet(truth, low_res, sheet_items, output / f"{study}_contact-sheet.png")

    (output / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({study: values[:5] for study, values in report.items()}, indent=2))


if __name__ == "__main__":
    main()
