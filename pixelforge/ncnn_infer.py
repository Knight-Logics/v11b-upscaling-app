"""CPU NCNN inference for the quality gate and comparison crops.

The Windows app still upscales with the bundled Vulkan executables. This module
runs the same ``.param`` / ``.bin`` weights through the NCNN Python package so a
machine without Vulkan can score a still. It is not the shipping video path.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np


def param_blob_names(param_path: Path) -> tuple[str, str]:
    """Read the network input blob and the final output blob from an NCNN param file."""
    input_name = "data"
    output_name = "output"
    for raw_line in Path(param_path).read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line[0].isdigit():
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        try:
            bottom_count = int(parts[2])
            top_count = int(parts[3])
        except ValueError:
            continue
        blobs_end = 4 + bottom_count + top_count
        if blobs_end > len(parts):
            continue
        tops = parts[4 + bottom_count:blobs_end]
        if parts[0] == "Input" and tops:
            input_name = tops[0]
        elif tops:
            output_name = tops[-1]
    return input_name, output_name


def upscale_rgb(param_path: Path, model_path: Path, image, threads: int = 4):
    """Run one RGB image through an NCNN model on CPU and return a PIL image."""
    try:
        import ncnn
    except ImportError as exc:
        raise RuntimeError(
            "The CPU quality gate needs the ncnn Python package to run bundled .param/.bin weights. "
            "On Windows, RUN_QUALITY_GATE.vbs can also use the Vulkan executables."
        ) from exc
    from PIL import Image

    rgb = image.convert("RGB")
    width, height = rgb.size
    net = ncnn.Net()
    net.opt.use_vulkan_compute = False
    net.opt.num_threads = max(1, int(threads))
    if net.load_param(str(param_path)) != 0:
        raise RuntimeError(f"Could not load NCNN param: {param_path}")
    if net.load_model(str(model_path)) != 0:
        raise RuntimeError(f"Could not load NCNN model: {model_path}")
    input_name, output_name = param_blob_names(param_path)
    mat = ncnn.Mat.from_pixels(rgb.tobytes(), ncnn.Mat.PixelType.PIXEL_RGB, width, height)
    mat.substract_mean_normalize([], [1.0 / 255.0, 1.0 / 255.0, 1.0 / 255.0])
    extractor = net.create_extractor()
    extractor.input(input_name, mat)
    result, output = extractor.extract(output_name)
    if result != 0:
        raise RuntimeError(f"NCNN extract failed for {param_path.name} (code {result})")
    array = np.array(output)
    if array.ndim != 3:
        raise RuntimeError(f"Unexpected NCNN output shape {array.shape} from {param_path.name}")
    if array.shape[0] in {1, 3} and array.shape[-1] != 3:
        array = np.transpose(array, (1, 2, 0))
    array = np.clip(array, 0.0, 1.0)
    return Image.fromarray((array * 255.0).round().astype(np.uint8), mode="RGB")
