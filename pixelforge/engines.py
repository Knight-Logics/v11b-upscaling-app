"""Engine/model catalogs for PixelForge AI (NCNN Vulkan backends)."""

from __future__ import annotations

# Real-CUGAN: -m models-{se|pro|nose} -s scale -n noise
# noise: -1=no denoise, 0..3 denoise strength; conservative weights use special mapping via -n/-s in exe.
REALCUGAN_MODEL_CONFIGS: dict[str, dict[str, object]] = {
    "realcugan-se-x2-n0": {"family": "se", "scale": 2, "noise": 0, "label": "Real-CUGAN SE 2x (conservative detail)"},
    "realcugan-se-x2-n1": {"family": "se", "scale": 2, "noise": 1, "label": "Real-CUGAN SE 2x (mild denoise)"},
    "realcugan-se-x2-n3": {"family": "se", "scale": 2, "noise": 3, "label": "Real-CUGAN SE 2x (strong denoise)"},
    "realcugan-se-x3-n3": {"family": "se", "scale": 3, "noise": 3, "label": "Real-CUGAN SE 3x (strong denoise)"},
    "realcugan-se-x4-n3": {"family": "se", "scale": 4, "noise": 3, "label": "Real-CUGAN SE 4x (strong denoise)"},
    "realcugan-pro-x2-n3": {"family": "pro", "scale": 2, "noise": 3, "label": "Real-CUGAN Pro 2x (strong denoise)"},
    "realcugan-pro-x3-n3": {"family": "pro", "scale": 3, "noise": 3, "label": "Real-CUGAN Pro 3x (strong denoise)"},
}

# SRMD: -s scale -n noise(-1..10); -1 uses no-denoise network
SRMD_MODEL_CONFIGS: dict[str, dict[str, object]] = {
    "srmd-x2-n3": {"scale": 2, "noise": 3, "label": "SRMD 2x (denoise 3)"},
    "srmd-x2-n5": {"scale": 2, "noise": 5, "label": "SRMD 2x (denoise 5)"},
    "srmd-x3-n5": {"scale": 3, "noise": 5, "label": "SRMD 3x (denoise 5)"},
    "srmd-x4-n5": {"scale": 4, "noise": 5, "label": "SRMD 4x (denoise 5)"},
    "srmd-x4-n8": {"scale": 4, "noise": 8, "label": "SRMD 4x (denoise 8 — heavy noise)"},
    "srmd-x4-nf": {"scale": 4, "noise": -1, "label": "SRMD 4x (no denoise)"},
}

RIFE_MODEL_DETAILS = [
    ("rife-v4.25", "rife-v4.25  (recommended quality)"),
    ("rife-v4.22-lite", "rife-v4.22-lite  (fast preview)"),
    ("rife-v4.12-lite", "rife-v4.12-lite  (fast draft)"),
    ("rife-v4.26", "rife-v4.26  (latest quality)"),
    ("rife-v4.6", "rife-v4.6  (legacy)"),
    ("rife-v4", "rife-v4  (legacy baseline)"),
]

# Rough relative cost multipliers vs realesrgan-x4plus @ 4x for ETA heuristics
ENGINE_COST_FACTORS: dict[str, float] = {
    "span-photo-x4": 0.22,
    "span-modern-animation-x2": 0.18,
    "realcugan-se-x2-n0": 0.55,
    "realcugan-se-x2-n1": 0.58,
    "realcugan-se-x2-n3": 0.62,
    "realcugan-se-x3-n3": 0.85,
    "realcugan-se-x4-n3": 1.15,
    "realcugan-pro-x2-n3": 0.70,
    "realcugan-pro-x3-n3": 0.95,
    "srmd-x2-n3": 0.45,
    "srmd-x2-n5": 0.48,
    "srmd-x3-n5": 0.70,
    "srmd-x4-n5": 0.95,
    "srmd-x4-n8": 1.00,
    "srmd-x4-nf": 0.90,
}
