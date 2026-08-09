"""Run a tiny end-to-end NVIDIA RTX VSR media-preservation smoke test."""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
import threading
from pathlib import Path
from queue import Queue


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "process_full_video_ultimate.py"
SPEC = importlib.util.spec_from_file_location("pixelforge_app_nvidia_smoke", MODULE_PATH)
assert SPEC and SPEC.loader
APP = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = APP
SPEC.loader.exec_module(APP)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", type=Path, default=ROOT / ".venv-nvidia" / "pipeline-smoke")
    parser.add_argument("--smooth-60", action="store_true", help="Also exercise the NVIDIA + RIFE 60 FPS path")
    args = parser.parse_args()
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    source = work_dir / "source.mp4"
    output = work_dir / "output.mp4"
    source_fps = 30 if args.smooth_60 else 2
    source_duration = 0.5 if args.smooth_60 else 2.0
    subprocess.run(
        [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", f"testsrc2=size=640x360:rate={source_fps}:duration={source_duration}",
            "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate=48000:duration={source_duration}",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", str(source),
        ],
        check=True,
    )
    settings = APP.PipelineSettings(
        input_video=source,
        output_video=output,
        model=APP.NVIDIA_MODEL_KEY,
        scale=4,
        image_format="png",
        threads="2:2:2",
        start_time=0.0,
        clip_duration=0.0,
        denoise=0.0,
        enable_color=False,
        vibrance=0.0,
        contrast=1.0,
        brightness=0.0,
        saturation=1.0,
        gamma=1.0,
        enable_sharpen=False,
        cas_strength=0.0,
        unsharp1=0.0,
        unsharp2=0.0,
        enable_interpolation=args.smooth_60,
        target_fps=60 if args.smooth_60 else source_fps,
        interp_engine=APP.INTERP_ENGINE_RIFE,
        rife_model="rife-v4.25",
        apply_final_scale=True,
        preserve_aspect_ratio=False,
        target_width=1280,
        target_height=720,
        crf=18,
        encode_preset="medium",
        video_codec="H.264",
        prefer_hardware_encode=True,
        preserve_media=True,
        chunk_size=30,
        resume_job=False,
        include_audio=True,
        keep_intermediate=False,
        auto_deinterlace=True,
        nvidia_vsr_mode="auto-standard",
    )
    messages: Queue[str] = Queue()
    APP.PipelineRunner(settings, messages, threading.Event()).run()
    properties = APP.probe_media(output)
    duration = APP.PipelineRunner.get_video_duration(output)
    fps = APP.PipelineRunner.get_fps(output)
    if (properties.width, properties.height) != (1280, 720):
        raise RuntimeError(f"Unexpected output dimensions: {properties.width}x{properties.height}")
    if properties.audio_streams != 1:
        raise RuntimeError(f"Audio preservation failed: {properties.audio_streams} stream(s)")
    if abs(duration - source_duration) > 0.15:
        raise RuntimeError(f"Unexpected duration: {duration:.3f}s")
    expected_fps = 60.0 if args.smooth_60 else float(source_fps)
    if abs(fps - expected_fps) > 0.25:
        raise RuntimeError(f"Unexpected frame rate: {fps:.3f} FPS")
    print(
        f"NVIDIA pipeline smoke passed: {properties.width}x{properties.height}, "
        f"{fps:.3f} FPS, {duration:.3f}s, audio={properties.audio_streams}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
