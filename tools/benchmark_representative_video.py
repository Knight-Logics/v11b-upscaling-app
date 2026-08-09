"""Run matched-source PixelForge video benchmarks on a prepared corpus.

Every candidate receives the exact same degraded input.  Optional Topaz outputs
can be dropped into ``<corpus>/topaz/<case>.mp4`` and are measured without any
special treatment.  Reference footage is intentionally not downloaded by this
script so licensing/attribution remains an explicit preparation step.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue

from benchmark_presets import contact_sheet


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "process_full_video_ultimate.py"
SPEC = importlib.util.spec_from_file_location("pixelforge_benchmark_app", APP_PATH)
assert SPEC and SPEC.loader
APP = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = APP
SPEC.loader.exec_module(APP)


@dataclass(frozen=True)
class Candidate:
    speed: str
    content: str
    smooth_60: bool = False
    nvidia_mode: str = ""

    @property
    def name(self) -> str:
        suffix = "_60fps" if self.smooth_60 else ""
        mode_suffix = f"_{self.nvidia_mode}" if self.nvidia_mode else ""
        return f"pixelforge_{self.speed}_{self.content}{suffix}{mode_suffix}"


@dataclass(frozen=True)
class Case:
    name: str
    source: str
    reference: str
    candidates: tuple[Candidate, ...]
    baseline_filter: str = ""


CASES = (
    Case(
        "live_faces_2x",
        "live_faces_2x.mp4",
        "live_faces.mkv",
        (
            Candidate("balanced", "live"), Candidate("quality", "live"), Candidate("nvidia", "live"),
            Candidate("nvidia", "live", nvidia_mode="clean-ultra"),
        ),
    ),
    Case(
        "live_restore_4x",
        "live_restore_4x.mp4",
        "live_detail.mkv",
        (
            Candidate("balanced", "restore"), Candidate("quality", "restore"), Candidate("nvidia", "restore"),
            Candidate("nvidia", "restore", nvidia_mode="clean-ultra"),
        ),
    ),
    Case(
        "animation_faces_2x",
        "animation_faces_2x.mp4",
        "animation_faces.mkv",
        (
            Candidate("balanced", "animation"), Candidate("quality", "animation"), Candidate("nvidia", "animation"),
        ),
    ),
    Case(
        "animation_arch_2x",
        "animation_arch_2x.mp4",
        "animation_arch.mkv",
        (
            Candidate("balanced", "animation"), Candidate("quality", "animation"), Candidate("nvidia", "animation"),
        ),
    ),
    Case(
        "motion_city_30_to_60",
        "motion_city_30fps_2x.mp4",
        "motion_city_60fps.mkv",
        (Candidate("balanced", "live", True), Candidate("nvidia", "live", True)),
        "minterpolate=fps=60:mi_mode=mci:mc_mode=aobmc:me_mode=bidir:vsbmc=1",
    ),
    Case(
        "interlaced_city_30i",
        "motion_city_30i_2x.mp4",
        "motion_city_30fps.mkv",
        (Candidate("balanced", "live"), Candidate("nvidia", "live")),
        "bwdif=mode=send_frame:parity=auto:deint=all",
    ),
)


def run(command: list[str], *, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=True,
        capture_output=capture,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def baseline(source: Path, output: Path, width: int, height: int, extra_filter: str) -> float:
    filters = [extra_filter] if extra_filter else []
    filters.append(f"scale={width}:{height}:flags=lanczos")
    started = time.perf_counter()
    run(
        [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", str(source),
            "-vf", ",".join(filters), "-an", "-c:v", "libx264", "-preset", "slow",
            "-crf", "16", "-pix_fmt", "yuv420p", str(output),
        ]
    )
    return time.perf_counter() - started


def settings_for(case: Case, candidate: Candidate, source: Path, output: Path, reference: Path):
    preset = APP.get_profile_preset(candidate.speed, candidate.content)
    reference_media = APP.probe_media(reference)
    return APP.PipelineSettings(
        input_video=source,
        output_video=output,
        model=str(preset["model"]),
        scale=int(preset["scale"]),
        image_format=str(preset["image_format"]),
        threads="4:4:4",
        start_time=0.0,
        clip_duration=0.0,
        denoise=float(preset["denoise"]),
        enable_color=bool(preset["enable_color"]),
        vibrance=float(preset["vibrance"]),
        contrast=float(preset["contrast"]),
        brightness=float(preset["brightness"]),
        saturation=float(preset["saturation"]),
        gamma=float(preset["gamma"]),
        enable_sharpen=bool(preset["enable_sharpen"]),
        cas_strength=float(preset["cas_strength"]),
        unsharp1=float(preset["unsharp1"]),
        unsharp2=float(preset["unsharp2"]),
        enable_interpolation=candidate.smooth_60,
        target_fps=60 if candidate.smooth_60 else int(round(APP.PipelineRunner.get_fps(source))),
        interp_engine=APP.INTERP_ENGINE_RIFE,
        rife_model=str(preset["rife_model"]),
        apply_final_scale=True,
        preserve_aspect_ratio=False,
        target_width=reference_media.width,
        target_height=reference_media.height,
        crf=int(preset["crf"]),
        encode_preset=str(preset["encode_preset"]),
        video_codec="H.264",
        prefer_hardware_encode=False,
        preserve_media=True,
        chunk_size=60,
        resume_job=False,
        include_audio=False,
        keep_intermediate=False,
        auto_deinterlace=True,
        nvidia_vsr_mode=candidate.nvidia_mode or str(preset.get("nvidia_vsr_mode", "auto-standard")),
    )


def process_candidate(case: Case, candidate: Candidate, source: Path, reference: Path, output: Path) -> tuple[float, str]:
    messages: Queue[str] = Queue()
    started = time.perf_counter()
    APP.PipelineRunner(settings_for(case, candidate, source, output, reference), messages, threading.Event()).run()
    elapsed = time.perf_counter() - started
    lines: list[str] = []
    while True:
        try:
            lines.append(messages.get_nowait())
        except Empty:
            break
    return elapsed, "\n".join(lines)


def metric(reference: Path, candidate: Path, kind: str, report_dir: Path) -> float:
    shared = (
        "[0:v]setpts=PTS-STARTPTS,format=yuv420p[dist];"
        "[1:v]setpts=PTS-STARTPTS,format=yuv420p[ref];"
    )
    if kind == "vmaf":
        log_path = (report_dir / f"vmaf-{candidate.stem}.json").as_posix().replace(":", "\\:")
        graph = shared + f"[dist][ref]libvmaf=log_fmt=json:log_path='{log_path}':n_threads=8"
        run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", str(candidate), "-i", str(reference), "-lavfi", graph, "-f", "null", "NUL"])
        payload = json.loads((report_dir / f"vmaf-{candidate.stem}.json").read_text(encoding="utf-8"))
        return float(payload["pooled_metrics"]["vmaf"]["mean"])
    graph = shared + f"[dist][ref]{kind}"
    completed = run(
        ["ffmpeg", "-hide_banner", "-i", str(candidate), "-i", str(reference), "-lavfi", graph, "-f", "null", "NUL"],
        capture=True,
    )
    pattern = r"All:([0-9.]+)" if kind == "ssim" else r"average:([0-9.]+)"
    match = re.search(pattern, completed.stderr)
    if not match:
        raise RuntimeError(f"Unable to parse {kind} output for {candidate}")
    return float(match.group(1))


def evaluate(reference: Path, candidate: Path, report_dir: Path) -> dict[str, float]:
    return {
        "vmaf": round(metric(reference, candidate, "vmaf", report_dir), 4),
        "ssim": round(metric(reference, candidate, "ssim", report_dir), 6),
        "psnr": round(metric(reference, candidate, "psnr", report_dir), 4),
    }


def extract_frame(video: Path, destination: Path) -> None:
    run(
        [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-ss", "1",
            "-i", str(video), "-frames:v", "1", str(destination),
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", type=Path, default=ROOT / "_benchmarks" / "representative-v1020")
    parser.add_argument("--case", action="append", choices=[case.name for case in CASES])
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    corpus = args.corpus.resolve()
    degraded_dir = corpus / "degraded"
    reference_dir = corpus / "reference"
    output_dir = corpus / "outputs"
    report_dir = corpus / "report"
    topaz_dir = corpus / "topaz"
    for directory in (output_dir, report_dir, topaz_dir):
        directory.mkdir(parents=True, exist_ok=True)

    report: dict[str, object] = {
        "method": "identical degraded video input with full-reference VMAF/SSIM/PSNR",
        "limitations": [
            "Topaz results are included only when matching files exist in the topaz directory.",
            "Objective metrics do not replace blind review for hallucination, identity, or temporal artifacts.",
            "Software H.264 output is used to make codec behavior reproducible across machines.",
        ],
        "cases": {},
    }
    selected_cases = [case for case in CASES if not args.case or case.name in args.case]
    for case in selected_cases:
        source = degraded_dir / case.source
        reference = reference_dir / case.reference
        if not source.is_file() or not reference.is_file():
            raise FileNotFoundError(f"Missing corpus input for {case.name}: {source} / {reference}")
        ref_media = APP.probe_media(reference)
        entries: list[dict[str, object]] = []
        output_paths: dict[str, Path] = {}
        baseline_path = output_dir / f"{case.name}__baseline.mp4"
        baseline_seconds = 0.0
        if args.force or not baseline_path.is_file():
            baseline_seconds = baseline(source, baseline_path, ref_media.width, ref_media.height, case.baseline_filter)
        entries.append({"candidate": "baseline_lanczos", "seconds": round(baseline_seconds, 3), **evaluate(reference, baseline_path, report_dir)})
        output_paths["baseline_lanczos"] = baseline_path

        for candidate in case.candidates:
            destination = output_dir / f"{case.name}__{candidate.name}.mp4"
            elapsed = 0.0
            log_text = ""
            if args.force or not destination.is_file():
                elapsed, log_text = process_candidate(case, candidate, source, reference, destination)
                (report_dir / f"{destination.stem}.log").write_text(log_text, encoding="utf-8")
            entries.append({"candidate": candidate.name, "seconds": round(elapsed, 3), **evaluate(reference, destination, report_dir)})
            output_paths[candidate.name] = destination

        topaz_path = topaz_dir / f"{case.name}.mp4"
        if topaz_path.is_file():
            entries.append({"candidate": "topaz", "seconds": None, **evaluate(reference, topaz_path, report_dir)})
            output_paths["topaz"] = topaz_path
        entries.sort(key=lambda item: float(item["vmaf"]), reverse=True)
        frame_dir = report_dir / "frames" / case.name
        frame_dir.mkdir(parents=True, exist_ok=True)
        source_frame = frame_dir / "source.png"
        reference_frame = frame_dir / "reference.png"
        extract_frame(source, source_frame)
        extract_frame(reference, reference_frame)
        sheet_items: list[tuple[str, Path]] = []
        for entry in entries:
            name = str(entry["candidate"])
            candidate_frame = frame_dir / f"{name}.png"
            extract_frame(output_paths[name], candidate_frame)
            sheet_items.append((f"{name} | VMAF {entry['vmaf']}", candidate_frame))
        contact_sheet(reference_frame, source_frame, sheet_items, report_dir / f"{case.name}-comparison.png")
        report["cases"][case.name] = {
            "source": str(source.relative_to(corpus)),
            "source_sha256": sha256(source),
            "reference": str(reference.relative_to(corpus)),
            "reference_sha256": sha256(reference),
            "width": ref_media.width,
            "height": ref_media.height,
            "fps": APP.PipelineRunner.get_fps(reference),
            "results": entries,
        }
        print(f"{case.name}: {entries[0]['candidate']} VMAF={entries[0]['vmaf']}", flush=True)

    destination = report_dir / "results.json"
    destination.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
