"""User-facing output targets for PixelForge AI."""

from __future__ import annotations


OUTPUT_TARGETS: tuple[str, ...] = ("Same resolution", "1080p", "1440p", "4K", "8K")

TARGET_BOXES: dict[str, tuple[int, int]] = {
    "1080p": (1920, 1080),
    "1440p": (2560, 1440),
    "4K": (3840, 2160),
    "8K": (7680, 4320),
}


def _even(value: float) -> int:
    rounded = max(2, int(round(value)))
    return rounded if rounded % 2 == 0 else rounded + 1


def target_dimensions(source_width: int, source_height: int, target: str) -> tuple[int, int]:
    """Fit the source aspect ratio into a standard target box without stretching."""
    width = int(source_width)
    height = int(source_height)
    if width <= 0 or height <= 0:
        raise ValueError("Source dimensions must be positive")
    if target == "Same resolution":
        return _even(width), _even(height)
    if target not in TARGET_BOXES:
        raise ValueError(f"Unknown output target: {target}")
    box_width, box_height = TARGET_BOXES[target]
    fit = min(box_width / width, box_height / height)
    return _even(width * fit), _even(height * fit)
