"""Honest before/after compare helpers for PixelForge AI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class CompareTimestampPlan:
    primary_ts: float
    filmstrip_pcts: tuple[float, ...]
    filmstrip_timestamps: tuple[float, ...]
    note: str


def clamp_pct(value: float) -> float:
    try:
        pct = float(value)
    except (TypeError, ValueError):
        pct = 50.0
    return max(0.0, min(100.0, pct))


def effective_clip_duration(total_duration: float, start_time: float, clip_duration: float) -> float:
    duration = max(0.1, float(total_duration))
    start = max(0.0, float(start_time))
    effective = max(0.1, duration - start)
    if float(clip_duration) > 0:
        effective = min(effective, float(clip_duration))
    return max(0.1, effective)


def timestamp_at_pct(start_time: float, effective_duration: float, frame_pct: float) -> float:
    pct = clamp_pct(frame_pct)
    return max(0.0, float(start_time) + effective_duration * (pct / 100.0))


def build_compare_timestamp_plan(
    total_duration: float,
    start_time: float = 0.0,
    clip_duration: float = 0.0,
    frame_pct: float = 50.0,
    filmstrip_pcts: Iterable[float] = (25.0, 50.0, 75.0),
) -> CompareTimestampPlan:
    """Honor the user-selected frame percentage exactly (no brightness hunting)."""
    effective = effective_clip_duration(total_duration, start_time, clip_duration)
    pct = clamp_pct(frame_pct)
    primary = timestamp_at_pct(start_time, effective, pct)
    strip = tuple(clamp_pct(p) for p in filmstrip_pcts)
    strip_ts = tuple(timestamp_at_pct(start_time, effective, p) for p in strip)
    note = (
        f"Preview of one frame at {pct:.0f}% through the clip — not the full video. "
        "Motion interpolation is not shown in still compare."
    )
    return CompareTimestampPlan(
        primary_ts=primary,
        filmstrip_pcts=strip,
        filmstrip_timestamps=strip_ts,
        note=note,
    )


def mean_luma(image) -> float:
    """Approximate perceived brightness 0-255 from a PIL image."""
    if image is None:
        return 0.0
    try:
        from PIL import ImageStat

        gray = image.convert("L")
        return float(ImageStat.Stat(gray).mean[0])
    except Exception:
        return 0.0


def pick_default_filmstrip_pct(
    brightness_by_pct: Mapping[float, float],
    candidates: Iterable[float] = (25.0, 50.0, 75.0),
    min_luma: float = 18.0,
    preferred: float = 50.0,
) -> float:
    """
    Choose a usable filmstrip frame for first compare.

    Prefer the preferred mid-frame when bright enough; otherwise the brightest
    candidate that clears min_luma; otherwise preferred anyway.
    """
    ordered = [clamp_pct(p) for p in candidates]
    if not ordered:
        return clamp_pct(preferred)

    preferred_pct = clamp_pct(preferred)
    preferred_luma = float(brightness_by_pct.get(preferred_pct, 0.0))
    if preferred_luma >= min_luma:
        return preferred_pct

    usable = [(pct, float(brightness_by_pct.get(pct, 0.0))) for pct in ordered if float(brightness_by_pct.get(pct, 0.0)) >= min_luma]
    if usable:
        usable.sort(key=lambda item: (-item[1], abs(item[0] - preferred_pct)))
        return usable[0][0]

    # All dark — still prefer mid, else brightest available.
    if preferred_pct in ordered:
        return preferred_pct
    ranked = sorted(ordered, key=lambda pct: (-float(brightness_by_pct.get(pct, 0.0)), abs(pct - preferred_pct)))
    return ranked[0]


def align_before_after_images(before, after, resample_filter):
    """
    Scale the before frame up to the after frame size so the separator is a true A/B
    at identical resolution (detail difference only).
    """
    if before is None or after is None:
        return before, after
    if before.size == after.size:
        return before, after
    return before.resize(after.size, resample_filter), after
