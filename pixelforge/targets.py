"""User-facing output targets for PixelForge AI."""

from __future__ import annotations

from dataclasses import dataclass


OUTPUT_TARGETS: tuple[str, ...] = ("Same resolution", "1080p", "1440p", "4K", "8K")
AUTOMATIC_TARGETS: tuple[str, ...] = ("1080p", "1440p", "4K")

TARGET_BOXES: dict[str, tuple[int, int]] = {
    "1080p": (1920, 1080),
    "1440p": (2560, 1440),
    "4K": (3840, 2160),
    "8K": (7680, 4320),
}


def _even(value: float) -> int:
    rounded = max(2, int(round(value)))
    return rounded if rounded % 2 == 0 else rounded + 1


def oriented_pixel_size(
    width: int,
    height: int,
    rotation_degrees: float = 0.0,
    sample_aspect: float = 1.0,
) -> tuple[int, int]:
    """Coded pixels after EXIF/display rotation and sample aspect.

    FFmpeg autorotates during decode. Planning has to use this size, or the final
    scale is computed for a frame that is no longer the one being written.
    """
    coded_w = max(1, int(width))
    coded_h = max(1, int(height))
    aspect = float(sample_aspect) if sample_aspect and sample_aspect > 0 else 1.0
    display_w = max(1, int(round(coded_w * aspect)))
    display_h = coded_h
    quarter_turns = int(round(float(rotation_degrees) / 90.0)) % 4
    if quarter_turns % 2 == 1:
        display_w, display_h = display_h, display_w
    return display_w, display_h


def suggested_output_target(source_width: int, source_height: int) -> str:
    """Pick a delivery size that keeps recovered detail.

    Same resolution runs the model and then scales the frame back to the source size.
    The automatic choice is the smallest standard target that enlarges by at least 1.8x,
    and never past 4K. A tiny frame whose smallest standard target is already larger than
    2x still uses that target. A source that is already near 4K stays at its own size.
    """
    width = int(source_width)
    height = int(source_height)
    if width <= 0 or height <= 0:
        return "Same resolution"
    candidates: list[tuple[float, str]] = []
    for name in AUTOMATIC_TARGETS:
        target_width, target_height = target_dimensions(width, height, name)
        factor = max(target_width / width, target_height / height)
        if factor >= 1.5:
            candidates.append((factor, name))
    if not candidates:
        return "Same resolution"
    generous = [item for item in candidates if item[0] >= 1.8]
    pool = generous or candidates
    pool.sort(key=lambda item: (item[0], AUTOMATIC_TARGETS.index(item[1])))
    return pool[0][1]


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


@dataclass(frozen=True)
class DeliveryPlan:
    """Pixels the pipeline writes, shared by stills, video, and frame batches."""

    target_name: str
    source_width: int
    source_height: int
    model_scale: int
    model_width: int
    model_height: int
    output_width: int
    output_height: int
    enlargement: float
    apply_final_scale: bool
    explicit_same_resolution: bool
    automatic: bool

    @property
    def keeps_model_pixels(self) -> bool:
        return self.output_width >= self.model_width and self.output_height >= self.model_height


def plan_delivery(
    source_width: int,
    source_height: int,
    model_scale: int,
    *,
    target_name: str | None = None,
    apply_final_scale: bool = True,
    automatic: bool = False,
) -> DeliveryPlan:
    """Resolve the file size for one source and one model scale.

    ``target_name is None`` means the automatic suggestion. Same resolution is the
    only automatic result that puts the file back at the source size. An explicit
    smaller target (for example 1080p from a 4K master) is allowed and recorded
    by an enlargement below 1.
    """
    width = int(source_width)
    height = int(source_height)
    scale = max(1, int(model_scale))
    if width <= 0 or height <= 0:
        raise ValueError("Source dimensions must be positive")
    chosen = suggested_output_target(width, height) if target_name is None else target_name
    automatic_choice = target_name is None or automatic
    if not apply_final_scale:
        output_width = width * scale
        output_height = height * scale
        enlargement = float(scale)
        return DeliveryPlan(
            target_name=chosen,
            source_width=width,
            source_height=height,
            model_scale=scale,
            model_width=output_width,
            model_height=output_height,
            output_width=output_width,
            output_height=output_height,
            enlargement=enlargement,
            apply_final_scale=False,
            explicit_same_resolution=False,
            automatic=automatic_choice,
        )
    output_width, output_height = target_dimensions(width, height, chosen)
    enlargement = min(output_width / width, output_height / height)
    if automatic_choice and chosen == "8K":
        raise ValueError("Automatic delivery is capped at 4K")
    if automatic_choice and enlargement < 1.0:
        raise ValueError("Automatic delivery must not shrink the source")
    return DeliveryPlan(
        target_name=chosen,
        source_width=width,
        source_height=height,
        model_scale=scale,
        model_width=width * scale,
        model_height=height * scale,
        output_width=output_width,
        output_height=output_height,
        enlargement=enlargement,
        apply_final_scale=True,
        explicit_same_resolution=chosen == "Same resolution",
        automatic=automatic_choice,
    )


def final_scale_filter(plan: DeliveryPlan, *, preserve_aspect_ratio: bool = False) -> str | None:
    """FFmpeg filter that matches ``plan`` for a still, a video, or a frame batch."""
    if not plan.apply_final_scale:
        return None
    width = plan.output_width
    height = plan.output_height
    if preserve_aspect_ratio:
        return (
            f"scale={width}:{height}:force_original_aspect_ratio=decrease:flags=lanczos,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black"
        )
    return f"scale={width}:{height}:flags=lanczos"


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
