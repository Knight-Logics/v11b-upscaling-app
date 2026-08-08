"""Conservative quality policy for foolproof PixelForge defaults."""

from __future__ import annotations

from dataclasses import dataclass

from .media import MediaProperties


PRECISION_SAFE_MODELS = frozenset({"span-photo-x4", "span-modern-animation-x2"})


def precision_safe_model(content_name: str) -> tuple[str, int]:
    """Choose a bundled FP32/DirectML model that preserves 10/16-bit samples."""
    if content_name == "animation":
        return "span-modern-animation-x2", 2
    return "span-photo-x4", 4


def output_enlargement_factor(
    source_width: int,
    source_height: int,
    target_width: int,
    target_height: int,
) -> float:
    if min(source_width, source_height, target_width, target_height) <= 0:
        return 1.0
    return max(target_width / source_width, target_height / source_height)


@dataclass(frozen=True)
class QualityAssessment:
    precision_model: str | None
    precision_scale: int | None
    enlargement_factor: float
    notices: tuple[str, ...]
    requires_extreme_scale_confirmation: bool


def assess_quality(
    source: MediaProperties,
    *,
    model_key: str,
    content_name: str,
    target_width: int,
    target_height: int,
) -> QualityAssessment:
    notices: list[str] = []
    safe_model: str | None = None
    safe_scale: int | None = None

    if (source.is_hdr or source.is_high_bit_depth) and model_key not in PRECISION_SAFE_MODELS:
        safe_model, safe_scale = precision_safe_model(content_name)
        notices.append(
            "HDR/10-bit precision guard will use the FP32 DirectML path instead of an 8-bit NCNN engine."
        )
    if source.is_interlaced:
        notices.append("Interlaced source detected; automatic BWDIF correction is recommended.")

    factor = output_enlargement_factor(source.width, source.height, target_width, target_height)
    extreme = factor > 4.05
    if extreme:
        notices.append(
            f"The requested output is {factor:.1f}x larger per dimension. Above 4x, extra pixels are final scaling, "
            "not additional recovered AI detail."
        )

    return QualityAssessment(
        precision_model=safe_model,
        precision_scale=safe_scale,
        enlargement_factor=factor,
        notices=tuple(notices),
        requires_extreme_scale_confirmation=extreme,
    )
