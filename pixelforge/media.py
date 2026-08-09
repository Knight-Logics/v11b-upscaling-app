"""Media planning helpers for the PixelForge video pipeline."""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


VIDEO_CODEC_OPTIONS: tuple[str, ...] = (
    "Auto (best local)",
    "H.264",
    "HEVC 10-bit",
    "AV1 10-bit",
    "ProRes 422 HQ",
    "Image sequence (PNG)",
)


@dataclass(frozen=True)
class MediaProperties:
    width: int
    height: int
    pix_fmt: str
    color_space: str
    color_transfer: str
    color_primaries: str
    video_codec: str
    audio_streams: int
    subtitle_streams: int
    attachment_streams: int
    chapters: int
    field_order: str = ""

    @property
    def is_hdr(self) -> bool:
        return self.color_transfer.lower() in {"smpte2084", "arib-std-b67"}

    @property
    def is_high_bit_depth(self) -> bool:
        text = self.pix_fmt.lower()
        return any(marker in text for marker in ("10", "12", "14", "16", "p010", "p016"))

    @property
    def is_interlaced(self) -> bool:
        return self.field_order.lower() not in {"", "unknown", "progressive"}


def probe_media(path: Path) -> MediaProperties:
    command = [
        "ffprobe", "-v", "error", "-print_format", "json",
        "-show_streams", "-show_chapters", str(path),
    ]
    payload = json.loads(subprocess.check_output(command, text=True, encoding="utf-8", errors="replace"))
    streams = payload.get("streams", [])
    video = next((stream for stream in streams if stream.get("codec_type") == "video"), {})
    count = lambda kind: sum(1 for stream in streams if stream.get("codec_type") == kind)
    return MediaProperties(
        width=int(video.get("width") or 0),
        height=int(video.get("height") or 0),
        pix_fmt=str(video.get("pix_fmt") or ""),
        color_space=str(video.get("color_space") or ""),
        color_transfer=str(video.get("color_transfer") or ""),
        color_primaries=str(video.get("color_primaries") or ""),
        video_codec=str(video.get("codec_name") or ""),
        audio_streams=count("audio"),
        subtitle_streams=count("subtitle"),
        attachment_streams=count("attachment"),
        chapters=len(payload.get("chapters", [])),
        field_order=str(video.get("field_order") or ""),
    )


def deinterlace_filter(source: MediaProperties, enabled: bool) -> str | None:
    """Return a conservative same-frame-rate deinterlace filter when needed.

    ``send_frame`` avoids silently doubling duration estimates or frame counts.
    BWDIF still reconstructs each progressive frame from both source fields.
    """
    if not enabled or not source.is_interlaced:
        return None
    return "bwdif=mode=send_frame:parity=auto:deint=all"


def estimate_workspace_bytes(
    source_width: int,
    source_height: int,
    frames: int,
    engine_scale: int,
    *,
    image_format: str,
    post_enabled: bool,
    interpolation_factor: int,
) -> int:
    """Conservative estimate for the current decoded-frame workspace."""
    compression = 0.28 if image_format.lower() == "jpg" else 1.35
    source_pixels = max(1, source_width * source_height)
    output_pixels = source_pixels * max(1, engine_scale) ** 2
    per_frame = (source_pixels * compression) + (output_pixels * compression)
    if post_enabled:
        per_frame += output_pixels * compression
    if interpolation_factor > 1:
        per_frame += output_pixels * 1.35 * interpolation_factor
    return int(per_frame * max(1, frames) * 1.30) + (2 * 1024**3)


def ensure_workspace_capacity(path: Path, required_bytes: int) -> tuple[int, int]:
    usage = shutil.disk_usage(path)
    if usage.free < required_bytes:
        required_gib = required_bytes / 1024**3
        free_gib = usage.free / 1024**3
        raise RuntimeError(
            f"Not enough free workspace for this job: approximately {required_gib:.1f} GiB required, "
            f"{free_gib:.1f} GiB available on {path.anchor or path}. Choose a shorter clip, lower target, "
            "or free disk space before starting."
        )
    return required_bytes, usage.free


def available_ffmpeg_encoders() -> set[str]:
    output = subprocess.check_output(
        ["ffmpeg", "-hide_banner", "-encoders"],
        text=True,
        encoding="utf-8",
        errors="replace",
        stderr=subprocess.STDOUT,
    )
    return {line.split()[1] for line in output.splitlines() if len(line.split()) >= 2 and line.startswith(" ")}


def video_encode_args(
    codec_choice: str,
    *,
    prefer_hardware: bool,
    crf: int,
    encode_preset: str,
    source: MediaProperties,
    encoders: set[str],
) -> tuple[list[str], str]:
    """Return FFmpeg video args and a user-facing encoder label."""
    choice = codec_choice
    if choice == "Auto (best local)":
        choice = "HEVC 10-bit" if source.is_hdr or source.is_high_bit_depth else "H.264"

    if choice == "H.264":
        if prefer_hardware and "h264_nvenc" in encoders:
            return (["-c:v", "h264_nvenc", "-preset", "p6", "-tune", "hq", "-rc", "vbr", "-cq", str(crf), "-b:v", "0", "-pix_fmt", "yuv420p"], "H.264 NVENC")
        return (["-c:v", "libx264", "-crf", str(crf), "-preset", encode_preset, "-pix_fmt", "yuv420p"], "H.264 software")

    if choice == "HEVC 10-bit":
        if prefer_hardware and "hevc_nvenc" in encoders:
            return (["-c:v", "hevc_nvenc", "-preset", "p6", "-tune", "hq", "-rc", "vbr", "-cq", str(crf), "-b:v", "0", "-profile:v", "main10", "-pix_fmt", "p010le"], "HEVC Main10 NVENC")
        return (["-c:v", "libx265", "-crf", str(crf), "-preset", encode_preset, "-pix_fmt", "yuv420p10le"], "HEVC Main10 software")

    if choice == "AV1 10-bit":
        if prefer_hardware and "av1_nvenc" in encoders:
            return (["-c:v", "av1_nvenc", "-preset", "p6", "-tune", "hq", "-rc", "vbr", "-cq", str(crf), "-b:v", "0", "-pix_fmt", "p010le"], "AV1 10-bit NVENC")
        return (["-c:v", "libaom-av1", "-crf", str(crf), "-b:v", "0", "-cpu-used", "4", "-pix_fmt", "yuv420p10le"], "AV1 10-bit software")

    if choice == "ProRes 422 HQ":
        return (["-c:v", "prores_ks", "-profile:v", "3", "-pix_fmt", "yuv422p10le"], "ProRes 422 HQ")

    raise ValueError(f"Unsupported video codec choice: {codec_choice}")


def color_metadata_args(source: MediaProperties) -> list[str]:
    args: list[str] = []
    for flag, value in (
        ("-colorspace", source.color_space),
        ("-color_trc", source.color_transfer),
        ("-color_primaries", source.color_primaries),
    ):
        if value and value.lower() not in {"unknown", "reserved"}:
            args.extend([flag, value])
    return args
