"""Local ONNX/DirectML upscaling for modern, redistributable SPAN models.

The implementation deliberately keeps the model runner independent from Tk so it
can be benchmarked and tested.  DirectML is preferred on Windows and the CPU
provider remains a compatibility fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Callable


@dataclass(frozen=True)
class OnnxUpscaleModel:
    key: str
    filename: str
    scale: int
    label: str
    content: str
    license_name: str
    attribution: str
    source_url: str
    sha256: str


MODEL_CATALOG: dict[str, OnnxUpscaleModel] = {
    "span-modern-animation-x2": OnnxUpscaleModel(
        key="span-modern-animation-x2",
        filename="2x_ModernSpanimationV1_fp32_op17.onnx",
        scale=2,
        label="SPAN Modern Animation 2x (DirectML, 2024)",
        content="animation",
        license_name="MIT",
        attribution="ModernSpanimationV1 by TNTwise",
        source_url="https://github.com/TNTwise/Models/releases/tag/2x_ModernSpanimationV1",
        sha256="6cfe12c4c4fa68c60dcd0a33135a052e5a7ff451ea8c2921482661e2858f117c",
    ),
    "span-photo-x4": OnnxUpscaleModel(
        key="span-photo-x4",
        filename="4xSPANkendata_fp32.onnx",
        scale=4,
        label="SPANkendata Photo 4x (DirectML, 2024)",
        content="live",
        license_name="CC-BY-SA-4.0",
        attribution="SPANkendata by Crustaceous D / terrainer",
        source_url="https://github.com/terrainer/AI-Upscaling-Models/tree/main/4xSPANkendata",
        sha256="7478c40953a5902efd785ef1fe8e07a8bb7e7ee9b1977273550813ceefae6348",
    ),
}


def available_providers() -> list[str]:
    """Return installed ONNX Runtime providers without making import mandatory."""
    try:
        import onnxruntime as ort
    except ImportError:
        return []
    return list(ort.get_available_providers())


class OnnxUpscaler:
    """Tiled SPAN inference with context around every output tile."""

    def __init__(self, model_path: Path, scale: int, tile_size: int = 256, context: int = 16):
        try:
            import numpy as np
            import onnxruntime as ort
        except ImportError as exc:
            raise RuntimeError(
                "The DirectML engine is not installed. Reinstall PixelForge AI with its ONNX runtime."
            ) from exc

        if not model_path.is_file():
            raise FileNotFoundError(f"ONNX model not found: {model_path}")
        self.np = np
        self.ort = ort
        self.model_path = Path(model_path)
        self.scale = int(scale)
        self.tile_size = max(64, int(tile_size))
        self.context = max(8, int(context))

        options = ort.SessionOptions()
        # Required/recommended for DirectML sessions.
        options.enable_mem_pattern = False
        options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        installed = set(ort.get_available_providers())
        providers = ["DmlExecutionProvider", "CPUExecutionProvider"] if "DmlExecutionProvider" in installed else ["CPUExecutionProvider"]
        self.session = ort.InferenceSession(str(self.model_path), sess_options=options, providers=providers)
        self.input_name = self.session.get_inputs()[0].name
        self.provider = self.session.get_providers()[0]

    @staticmethod
    def _round_up(value: int, multiple: int = 8) -> int:
        return ((int(value) + multiple - 1) // multiple) * multiple

    def _infer_tile(self, tile, *, peak_value: float = 255.0, output_dtype=None):
        np = self.np
        height, width = tile.shape[:2]
        padded_h = self._round_up(height)
        padded_w = self._round_up(width)
        if padded_h != height or padded_w != width:
            tile = np.pad(tile, ((0, padded_h - height), (0, padded_w - width), (0, 0)), mode="reflect")
        tensor = np.ascontiguousarray(tile.transpose(2, 0, 1)[None, ...], dtype=np.float32) / peak_value
        result = self.session.run(None, {self.input_name: tensor})[0][0]
        dtype = output_dtype or np.uint8
        result = np.clip(result.transpose(1, 2, 0) * peak_value, 0.0, peak_value).round().astype(dtype)
        return result[: height * self.scale, : width * self.scale]

    def upscale_array(
        self,
        pixels,
        *,
        cancel_event: Event | None = None,
        progress: Callable[[int, int], None] | None = None,
    ):
        """Upscale an RGB uint8/uint16 array without reducing its sample precision.

        The ONNX models consume FP32 tensors.  A uint16 source is therefore
        normalized from 0..65535 directly, rather than being quantized through
        Pillow's 8-bit RGB conversion first.
        """
        np = self.np
        source = np.asarray(pixels)
        if source.ndim != 3 or source.shape[2] != 3:
            raise ValueError("Expected an HxWx3 RGB array")
        if source.dtype == np.uint8:
            peak_value = 255.0
            output_dtype = np.uint8
        elif source.dtype == np.uint16:
            peak_value = 65535.0
            output_dtype = np.uint16
        else:
            raise TypeError("Only uint8 and uint16 RGB arrays are supported")

        height, width = source.shape[:2]
        output = np.empty((height * self.scale, width * self.scale, 3), dtype=output_dtype)
        x_starts = list(range(0, width, self.tile_size))
        y_starts = list(range(0, height, self.tile_size))
        total = len(x_starts) * len(y_starts)
        completed = 0

        for y0 in y_starts:
            y1 = min(height, y0 + self.tile_size)
            for x0 in x_starts:
                if cancel_event and cancel_event.is_set():
                    raise RuntimeError("Preview canceled by user")
                x1 = min(width, x0 + self.tile_size)
                sx0 = max(0, x0 - self.context)
                sy0 = max(0, y0 - self.context)
                sx1 = min(width, x1 + self.context)
                sy1 = min(height, y1 + self.context)
                inferred = self._infer_tile(
                    source[sy0:sy1, sx0:sx1],
                    peak_value=peak_value,
                    output_dtype=output_dtype,
                )

                crop_x0 = (x0 - sx0) * self.scale
                crop_y0 = (y0 - sy0) * self.scale
                crop_x1 = crop_x0 + ((x1 - x0) * self.scale)
                crop_y1 = crop_y0 + ((y1 - y0) * self.scale)
                output[y0 * self.scale:y1 * self.scale, x0 * self.scale:x1 * self.scale] = inferred[
                    crop_y0:crop_y1, crop_x0:crop_x1
                ]
                completed += 1
                if progress:
                    progress(completed, total)
        return output

    def upscale_image(
        self,
        source,
        *,
        cancel_event: Event | None = None,
        progress: Callable[[int, int], None] | None = None,
    ):
        """Upscale a PIL RGB image while bounding GPU memory with contextual tiles."""
        from PIL import Image

        rgb = source.convert("RGB")
        pixels = self.np.asarray(rgb, dtype=self.np.uint8)
        output = self.upscale_array(pixels, cancel_event=cancel_event, progress=progress)
        return Image.fromarray(output, mode="RGB")


def upscale_file(
    input_path: Path,
    output_path: Path,
    model_path: Path,
    scale: int,
    *,
    tile_size: int = 256,
    cancel_event: Event | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> str:
    """Upscale one image and return the provider used."""
    from PIL import Image

    runner = OnnxUpscaler(model_path, scale, tile_size=tile_size)
    with Image.open(input_path) as source:
        enhanced = runner.upscale_image(source, cancel_event=cancel_event, progress=progress)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    enhanced.save(output_path, format="PNG", compress_level=1)
    return runner.provider
