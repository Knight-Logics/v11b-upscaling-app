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


def suggested_output_target(source_width: int, source_height: int) -> str:
    """Pick a delivery size that keeps recovered detail on a typical low-resolution source.

    Same resolution runs the model and then scales the frame back to the source size, which
    throws away most of the pixels the network just created. The suggestion is the smallest
    standard target that enlarges the long edge by about 2x, and never past 4K.
    """
    width = int(source_width)
    height = int(source_height)
    if width <= 0 or height <= 0:
        return "Same resolution"
    chosen = "Same resolution"
    chosen_factor = 1.0
    for name in ("1080p", "1440p", "4K"):
        target_width, target_height = target_dimensions(width, height, name)
        factor = max(target_width / width, target_height / height)
        if factor < 1.5:
            continue
        if 1.8 <= factor <= 2.35:
            return name
        if factor > chosen_factor and factor <= 2.35:
            chosen = name
            chosen_factor = factor
    if chosen_factor >= 1.5:
        return chosen
    return "Same resolution"


def preview_delivery_size(
    crop_width: int,
    crop_height: int,
    source_width: int,
    source_height: int,
    target_width: int,
    target_height: int,
    apply_final_scale: bool,
) -> tuple[int, int] | None:
    """Scale a preview crop by the same factor as the full delivered frame.

    Returns None when the preview should stay at the model's native size.
    """
    if not apply_final_scale:
        return None
    if min(int(crop_width), int(crop_height), int(source_width), int(source_height), int(target_width), int(target_height)) <= 0:
        return None
    scale_x = int(target_width) / int(source_width)
    scale_y = int(target_height) / int(source_height)
    return max(2, int(round(crop_width * scale_x))), max(2, int(round(crop_height * scale_y)))


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
