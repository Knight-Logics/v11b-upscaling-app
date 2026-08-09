"""Crash-isolated NVIDIA VFX streaming worker.

Binary protocol for normal operation:
  stdin:  repeated packed RGB24 frames at --width x --height
  stdout: repeated packed RGB24 frames at --output-width x --output-height

NVIDIA's 0.1.0.1 Python binding can hang while closing its context on some
Blackwell Windows systems even after successful inference.  The worker exits
at process scope after flushing output so the OS performs reliable cleanup.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path


QUALITY_NAMES = {
    "low": "LOW",
    "medium": "MEDIUM",
    "high": "HIGH",
    "ultra": "ULTRA",
    "denoise-low": "DENOISE_LOW",
    "denoise-medium": "DENOISE_MEDIUM",
    "denoise-high": "DENOISE_HIGH",
    "denoise-ultra": "DENOISE_ULTRA",
    "deblur-low": "DEBLUR_LOW",
    "deblur-medium": "DEBLUR_MEDIUM",
    "deblur-high": "DEBLUR_HIGH",
    "deblur-ultra": "DEBLUR_ULTRA",
    "clean-low": "HIGHBITRATE_LOW",
    "clean-medium": "HIGHBITRATE_MEDIUM",
    "clean-high": "HIGHBITRATE_HIGH",
    "clean-ultra": "HIGHBITRATE_ULTRA",
}


def _read_exact(stream, byte_count: int) -> bytes:
    chunks: list[bytes] = []
    remaining = int(byte_count)
    while remaining > 0:
        chunk = stream.read(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _prepare_runtime():
    cache_override = str(os.environ.get("PIXELFORGE_NVIDIA_CACHE", "")).strip()
    cache_root = (
        Path(cache_override)
        if cache_override
        else Path(os.environ.get("LOCALAPPDATA") or tempfile.gettempdir()) / "KnightLogics" / "PixelForgeAI" / "nvidia-cache"
    )
    cache_root.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("CUPY_CACHE_DIR", str(cache_root))
    import cupy as cp
    import numpy as np
    from nvvfx import VideoSuperRes

    return cp, np, VideoSuperRes


def _safe_process_exit(code: int) -> None:
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    finally:
        os._exit(int(code))


def _probe() -> None:
    cp, np, VideoSuperRes = _prepare_runtime()
    properties = cp.cuda.runtime.getDeviceProperties(0)
    source = cp.asarray(np.zeros((3, 360, 640), dtype=np.float32))
    started = time.perf_counter()
    effect = VideoSuperRes(VideoSuperRes.QualityLevel.HIGH)
    effect.output_width = 1280
    effect.output_height = 720
    effect.load()
    result = cp.from_dlpack(effect.run(source).image).get()
    payload = {
        "ok": True,
        "device": properties["name"].decode(errors="replace"),
        "output_shape": list(result.shape),
        "seconds": round(time.perf_counter() - started, 3),
    }
    print(json.dumps(payload), flush=True)
    _safe_process_exit(0)


def _stream(args: argparse.Namespace) -> None:
    cp, np, VideoSuperRes = _prepare_runtime()
    quality_member = QUALITY_NAMES.get(args.quality)
    if not quality_member:
        raise ValueError(f"Unknown NVIDIA quality mode: {args.quality}")
    quality = getattr(VideoSuperRes.QualityLevel, quality_member)
    effect = VideoSuperRes(quality)
    effect.output_width = int(args.output_width)
    effect.output_height = int(args.output_height)
    effect.load()

    input_bytes = int(args.width) * int(args.height) * 3
    output_bytes = int(args.output_width) * int(args.output_height) * 3
    source_stream = sys.stdin.buffer
    destination_stream = sys.stdout.buffer
    frames = 0
    while True:
        raw = _read_exact(source_stream, input_bytes)
        if not raw:
            break
        if len(raw) != input_bytes:
            raise RuntimeError(f"partial RGB frame: {len(raw)} of {input_bytes} bytes")
        host = np.frombuffer(raw, dtype=np.uint8).reshape(args.height, args.width, 3)
        normalized = np.ascontiguousarray(host.transpose(2, 0, 1), dtype=np.float32) / 255.0
        source = cp.asarray(normalized)
        restored = cp.from_dlpack(effect.run(source).image).get()
        output = np.clip(restored.transpose(1, 2, 0) * 255.0 + 0.5, 0.0, 255.0).astype(np.uint8)
        payload = output.tobytes(order="C")
        if len(payload) != output_bytes:
            raise RuntimeError(f"unexpected output frame: {len(payload)} of {output_bytes} bytes")
        destination_stream.write(payload)
        destination_stream.flush()
        frames += 1
    print(f"PixelForge NVIDIA worker completed {frames} frame(s)", file=sys.stderr, flush=True)
    _safe_process_exit(0)


def _image(args: argparse.Namespace) -> None:
    from PIL import Image

    cp, np, VideoSuperRes = _prepare_runtime()
    quality_member = QUALITY_NAMES.get(args.quality)
    if not quality_member:
        raise ValueError(f"Unknown NVIDIA quality mode: {args.quality}")
    with Image.open(args.input_image) as source_image:
        source_image = source_image.convert("RGB")
        width, height = source_image.size
        host = np.asarray(source_image, dtype=np.uint8)
    output_width = int(args.output_width or width * 2)
    output_height = int(args.output_height or height * 2)
    normalized = np.ascontiguousarray(host.transpose(2, 0, 1), dtype=np.float32) / 255.0
    source = cp.asarray(normalized)
    effect = VideoSuperRes(getattr(VideoSuperRes.QualityLevel, quality_member))
    effect.output_width = output_width
    effect.output_height = output_height
    effect.load()
    restored = cp.from_dlpack(effect.run(source).image).get()
    output = np.clip(restored.transpose(1, 2, 0) * 255.0 + 0.5, 0.0, 255.0).astype(np.uint8)
    Image.fromarray(output, mode="RGB").save(args.output_image, format="PNG", compress_level=1)
    _safe_process_exit(0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--width", type=int, default=0)
    parser.add_argument("--height", type=int, default=0)
    parser.add_argument("--output-width", type=int, default=0)
    parser.add_argument("--output-height", type=int, default=0)
    parser.add_argument("--quality", choices=sorted(QUALITY_NAMES), default="ultra")
    parser.add_argument("--input-image")
    parser.add_argument("--output-image")
    args = parser.parse_args()
    try:
        if args.probe:
            _probe()
        if args.input_image or args.output_image:
            if not args.input_image or not args.output_image:
                raise ValueError("--input-image and --output-image must be used together")
            _image(args)
        if min(args.width, args.height, args.output_width, args.output_height) < 1:
            raise ValueError("positive input and output dimensions are required")
        _stream(args)
    except Exception as exc:
        print(f"PixelForge NVIDIA worker failed: {exc}", file=sys.stderr, flush=True)
        _safe_process_exit(1)


if __name__ == "__main__":
    main()
