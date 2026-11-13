"""Profile presets for PixelForge AI.

Live/natural footage uses Real-ESRGAN / RealSR / SRMD.
Anime uses Real-CUGAN (preferred) and animevideov3 fallbacks.
"""

from __future__ import annotations

MODEL_NATIVE_SCALES: dict[str, set[int]] = {
    "nvidia-rtx-vsr": {4},
    "span-photo-x4": {4},
    "span-modern-animation-x2": {2},
    "realesrgan-x4plus": {4},
    "realesrgan-x4plus-anime": {4},
    "realesr-animevideov3-x2": {2},
    "realesr-animevideov3-x3": {3},
    "realesr-animevideov3-x4": {4},
    "realsr-df2k": {4},
    "realsr-df2k-jpeg": {4},
    "waifu2x-cunet-noise3": {1, 2, 4},
    "waifu2x-cunet-noise1": {1, 2, 4},
    "waifu2x-anime-noise3": {1, 2, 4},
    "waifu2x-anime-noise1": {1, 2, 4},
    # Real-CUGAN
    "realcugan-se-x2-n0": {2},
    "realcugan-se-x2-n1": {2},
    "realcugan-se-x2-n3": {2},
    "realcugan-se-x3-n3": {3},
    "realcugan-se-x4-n3": {4},
    "realcugan-pro-x2-n3": {2},
    "realcugan-pro-x3-n3": {3},
    # SRMD
    "srmd-x2-n3": {2},
    "srmd-x2-n5": {2},
    "srmd-x3-n5": {3},
    "srmd-x4-n5": {4},
    "srmd-x4-n8": {4},
    "srmd-x4-nf": {4},
}

SPEED_LABELS = {
    "fast": "Fast Preview",
    "balanced": "Balanced",
    "quality": "Max Detail",
    "nvidia": "NVIDIA RTX",
}

CONTENT_LABELS = {
    "live": "People / camera",
    "animation": "Anime / game",
    "restore": "Old / noisy",
}

CONTENT_PICKER_COPY = {
    "live": "Phones, cameras, people, products, and real-world video.",
    "animation": "Animation, line art, anime, or game captures.",
    "restore": "Compressed, blocky, noisy, or older sources.",
}


FIDELITY_LABELS = {
    "enhanced": "Enhanced",
    "faithful": "Faithful",
}

# Enhanced is the default. It uses the full Real-ESRGAN output for camera footage.
# Faithful keeps the previous RealSR / mild-denoise stack, which stays closer to a resize.
_MODEL_TABLES: dict[str, dict[str, dict[str, tuple[str, int]]]] = {
    "enhanced": {
        "live": {
            "fast": ("span-photo-x4", 4),
            "balanced": ("realesrgan-x4plus", 4),
            "quality": ("realesrgan-x4plus", 4),
            "nvidia": ("nvidia-rtx-vsr", 4),
        },
        "animation": {
            "fast": ("span-modern-animation-x2", 2),
            "balanced": ("realcugan-se-x2-n0", 2),
            "quality": ("realcugan-se-x2-n0", 2),
            "nvidia": ("nvidia-rtx-vsr", 4),
        },
        "restore": {
            "fast": ("srmd-x2-n5", 2),
            "balanced": ("srmd-x4-n5", 4),
            "quality": ("realsr-df2k-jpeg", 4),
            "nvidia": ("nvidia-rtx-vsr", 4),
        },
    },
    "faithful": {
        "live": {
            "fast": ("span-photo-x4", 4),
            "balanced": ("realsr-df2k", 4),
            "quality": ("realsr-df2k", 4),
            "nvidia": ("nvidia-rtx-vsr", 4),
        },
        "animation": {
            "fast": ("span-modern-animation-x2", 2),
            "balanced": ("realcugan-se-x2-n1", 2),
            "quality": ("realcugan-se-x2-n0", 2),
            "nvidia": ("nvidia-rtx-vsr", 4),
        },
        "restore": {
            "fast": ("srmd-x2-n5", 2),
            "balanced": ("srmd-x4-n5", 4),
            "quality": ("realsr-df2k-jpeg", 4),
            "nvidia": ("nvidia-rtx-vsr", 4),
        },
    },
}


def get_profile_preset(speed_name: str, content_name: str, fidelity: str = "enhanced") -> dict[str, object]:
    """Return a tested, internally compatible preset without touching Tk state."""
    if speed_name not in {"fast", "balanced", "quality", "nvidia"}:
        raise ValueError(f"Unknown speed profile: {speed_name}")
    if content_name not in {"live", "animation", "restore"}:
        raise ValueError(f"Unknown content profile: {content_name}")
    if fidelity not in _MODEL_TABLES:
        raise ValueError(f"Unknown fidelity: {fidelity}")

    preset: dict[str, object] = {
        "denoise": 0.0,
        "enable_color": False,
        "vibrance": 0.0,
        "contrast": 1.0,
        "brightness": 0.0,
        "saturation": 1.0,
        "gamma": 1.0,
        "enable_sharpen": False,
        "cas_strength": 0.20,
        "unsharp1": 0.0,
        "unsharp2": 0.0,
        "enable_interpolation": False,
        "auto_deinterlace": True,
        "target_fps": 30,
        "apply_final_scale": False,
        "rife_model": "rife-v4.25",
    }

    if speed_name == "fast":
        preset.update(image_format="jpg", encode_preset="veryfast", crf=21)
    elif speed_name == "nvidia":
        preset.update(image_format="png", encode_preset="slow", crf=15)
    elif speed_name == "quality":
        preset.update(image_format="png", encode_preset="slow", crf=15)
    else:
        preset.update(image_format="png", encode_preset="medium", crf=16)

    # Real-CUGAN remains the anime stack. Enhanced turns denoise off so lines stay.
    # Restore stays on SRMD / RealSR JPEG for both looks; denoise is the point of that path.
    model, scale = _MODEL_TABLES[fidelity][content_name][speed_name]
    if scale not in MODEL_NATIVE_SCALES.get(model, {scale}):
        raise ValueError(f"Preset {speed_name}/{content_name} uses non-native scale {scale} for {model}")
    preset.update(model=model, scale=scale, fidelity=fidelity)
    if speed_name == "nvidia":
        preset["nvidia_vsr_mode"] = {
            "live": "auto-standard",
            "animation": "auto-standard",
            "restore": "auto-restore",
        }[content_name]
    if content_name == "restore" and speed_name == "balanced":
        preset.update(enable_sharpen=True, cas_strength=0.08, unsharp1=0.0, unsharp2=0.0)
    return preset
