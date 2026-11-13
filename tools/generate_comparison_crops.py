"""Write real Lanczos vs PixelForge crops for the site comparison slider.

The images come from the model path that can execute on this machine. Re-run this
script on the owner's Windows machine after RUN_QUALITY_GATE.vbs if the slider
should show the Vulkan executable instead of CPU NCNN. The manifest records which
runtime produced the files. It does not claim a result the gate did not measure.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from PIL import Image, ImageDraw

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pixelforge.metrics import mean_abs_diff, score_pair, tenengrad
from pixelforge.ncnn_infer import upscale_rgb
from pixelforge.presets import get_profile_preset
from pixelforge.reference_scenes import degrade, lanczos_upscale, make_detail_scene


def _zoom(image: Image.Image, box: tuple[int, int, int, int], factor: int = 3) -> Image.Image:
    crop = image.crop(box)
    return crop.resize((crop.width * factor, crop.height * factor), Image.Resampling.NEAREST)


def _label(image: Image.Image, text: str) -> Image.Image:
    canvas = Image.new("RGB", (image.width, image.height + 28), (8, 16, 29))
    canvas.paste(image, (0, 28))
    draw = ImageDraw.Draw(canvas)
    draw.text((8, 6), text, fill=(216, 247, 255))
    return canvas


def main() -> int:
    parser = argparse.ArgumentParser(description="Regenerate Lanczos vs PixelForge comparison crops")
    parser.add_argument("--output", type=Path, default=ROOT / "assets" / "comparisons")
    parser.add_argument("--artifacts", type=Path, default=Path("/opt/cursor/artifacts"))
    args = parser.parse_args()
    output = args.output
    output.mkdir(parents=True, exist_ok=True)

    preset = get_profile_preset("balanced", "live", "enhanced")
    model = str(preset["model"])
    if model != "realesrgan-x4plus":
        raise SystemExit(f"Refusing to label crops as the default model; preset is {model}")
    param = ROOT / "models" / "realesrgan-x4plus.param"
    weights = ROOT / "models" / "realesrgan-x4plus.bin"
    truth = make_detail_scene()
    low_res = degrade(truth, scale=4, jpeg_quality=40)
    lanczos = lanczos_upscale(low_res, truth.width, truth.height)
    enhanced = upscale_rgb(param, weights, low_res)
    if enhanced.size != truth.size:
        enhanced = enhanced.resize(truth.size, Image.Resampling.LANCZOS)

    # Text line and the fine grating are the regions a viewer can judge quickly.
    crops = {
        "text": (24, 22, 280, 70),
        "grating": (250, 176, 410, 230),
    }
    lanczos.save(output / "lanczos.png")
    enhanced.save(output / "pixelforge.png")
    truth.save(output / "ground_truth.png")
    low_res.save(output / "low_res.jpg", quality=90)
    for name, box in crops.items():
        _zoom(lanczos, box).save(output / f"zoom-{name}-lanczos.png")
        _zoom(enhanced, box).save(output / f"zoom-{name}-pixelforge.png")
        _zoom(truth, box).save(output / f"zoom-{name}-ground-truth.png")

    pair = Image.new("RGB", (truth.width * 2, truth.height), (0, 0, 0))
    pair.paste(lanczos, (0, 0))
    pair.paste(enhanced, (truth.width, 0))
    labeled = _label(pair, "Left: Lanczos 4x    Right: PixelForge Real-ESRGAN x4plus (CPU NCNN)")
    labeled.save(output / "side-by-side.png")

    scores = score_pair(truth, enhanced)
    lanczos_sharp = tenengrad(lanczos)
    manifest = {
        "engine": model,
        "runtime": "ncnn-python-cpu",
        "weights": "models/realesrgan-x4plus.param + .bin",
        "degrade": "Lanczos 4x downsample, JPEG quality 40",
        "scene": "synthetic_detail",
        "slider": {
            "before": "lanczos.png",
            "after": "pixelforge.png",
        },
        "zoomed_crops": {
            name: {
                "before": f"zoom-{name}-lanczos.png",
                "after": f"zoom-{name}-pixelforge.png",
                "ground_truth": f"zoom-{name}-ground-truth.png",
            }
            for name in crops
        },
        "metrics_vs_ground_truth": scores,
        "lanczos_sharpness_tenengrad": round(lanczos_sharp, 4),
        "sharpness_ratio_vs_lanczos": round(float(scores["sharpness_tenengrad"]) / max(1e-9, lanczos_sharp), 4),
        "mean_abs_diff_vs_lanczos": round(mean_abs_diff(enhanced, lanczos), 4),
        "lpips_status": "computed" if scores["lpips"] is not None else "not_installed",
        "not_claimed": [
            "These stills are not the withdrawn v1.0.20-v1.0.22 video benchmark.",
            "They are not the NVIDIA RTX path and not the Vulkan executable.",
            "Regenerate on the owner machine with this script after RUN_QUALITY_GATE.vbs for that path.",
        ],
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if args.artifacts:
        args.artifacts.mkdir(parents=True, exist_ok=True)
        for path in output.iterdir():
            if path.is_file():
                shutil.copy2(path, args.artifacts / path.name)
    print(json.dumps({
        "output": str(output),
        "sharpness_ratio_vs_lanczos": manifest["sharpness_ratio_vs_lanczos"],
        "mean_abs_diff_vs_lanczos": manifest["mean_abs_diff_vs_lanczos"],
        "psnr": scores["psnr"],
        "ssim": scores["ssim"],
        "lpips": scores["lpips"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
