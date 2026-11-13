"""Still-image orientation and alpha, kept off the video path.

FFmpeg autorotates video. Pillow does not autorotate stills unless asked.
The pipeline decodes stills through FFmpeg, so a rotated JPEG has to be
transposed before that decode or the delivery plan uses the wrong axis.
Alpha is not a model input. It is resized to the delivered RGB size and put back.
"""

from __future__ import annotations

from pathlib import Path


def still_display_size(path: Path) -> tuple[int, int]:
    from PIL import Image, ImageOps

    with Image.open(path) as image:
        oriented = ImageOps.exif_transpose(image)
        return oriented.size


def prepare_still(path: Path, destination: Path) -> tuple[Path, object | None]:
    """Write an upright RGB PNG and return a detached alpha channel when one exists.

    The returned path is ``path`` itself when the file is already upright RGB.
    """
    from PIL import Image, ImageOps

    with Image.open(path) as image:
        oriented = ImageOps.exif_transpose(image)
        rotated = oriented.size != image.size
        has_alpha = "A" in oriented.getbands() or (
            oriented.mode == "P" and "transparency" in oriented.info
        )
        if not rotated and not has_alpha:
            return path, None
        alpha = oriented.convert("RGBA").getchannel("A") if has_alpha else None
        destination.parent.mkdir(parents=True, exist_ok=True)
        oriented.convert("RGB").save(destination, format="PNG")
        return destination, alpha


def attach_alpha(rgb_path: Path, alpha, output_path: Path) -> None:
    """Resize the source alpha to the delivered RGB image and save it."""
    from PIL import Image

    with Image.open(rgb_path) as rgb:
        color = rgb.convert("RGBA")
    mask = alpha.resize(color.size, Image.Resampling.LANCZOS)
    color.putalpha(mask)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        color.convert("RGB").save(output_path, format="JPEG", quality=95, subsampling=0)
        return
    if suffix == ".webp":
        color.save(output_path, format="WEBP")
        return
    color.save(output_path, format="PNG")
