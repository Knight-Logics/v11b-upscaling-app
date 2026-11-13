"""Small reference stills for the headless quality gate.

The procedural scene is generated in-process so the gate does not download footage.
The product still is the bundled marketing graphic, which is already soft; it is a
second sample, not an independent photographic ground truth.
"""

from __future__ import annotations

import io
from pathlib import Path

from PIL import Image, ImageDraw


def make_detail_scene(width: int = 512, height: int = 288) -> Image.Image:
    """A sharp synthetic frame with text, edges, and fine texture."""
    image = Image.new("RGB", (width, height), (32, 40, 52))
    draw = ImageDraw.Draw(image)
    for x in range(0, width, 16):
        shade = 70 + (x * 140 // max(1, width))
        draw.rectangle((x, 0, x + 7, height), fill=(shade, shade // 3, 48))
    for y in range(24, height - 24, 4):
        draw.line((18, y, 220, y + 18), fill=(230, 214, 176), width=1)
    draw.rounded_rectangle((250, 28, 490, 168), radius=18, outline=(236, 236, 236), width=3, fill=(18, 28, 44))
    draw.ellipse((300, 48, 430, 176), fill=(196, 148, 118), outline=(92, 54, 42))
    draw.ellipse((338, 86, 358, 106), fill=(24, 24, 28))
    draw.ellipse((386, 86, 406, 106), fill=(24, 24, 28))
    draw.arc((332, 112, 412, 156), 15, 165, fill=(130, 48, 48), width=2)
    draw.rectangle((20, height - 48, width - 20, height - 8), fill=(12, 16, 24))
    font = None
    small = None
    try:
        from PIL import ImageFont

        for candidate in (
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            r"C:\Windows\Fonts\arialbd.ttf",
            r"C:\Windows\Fonts\segoeuib.ttf",
        ):
            if Path(candidate).is_file():
                font = ImageFont.truetype(candidate, 28)
                small = ImageFont.truetype(candidate, 18)
                break
    except Exception:
        font = None
        small = None
    draw.text((28, 28), "PixelForge detail", fill=(255, 236, 186), font=font)
    draw.text((28, height - 40), "LOW-RES INPUT 4x", fill=(186, 220, 255), font=small)
    for x in range(260, 490):
        if (x // 2) % 2 == 0:
            draw.line((x, 188, x, 214), fill=(250, 250, 250))
    return image


def degrade(image: Image.Image, scale: int = 4, jpeg_quality: int = 40) -> Image.Image:
    """Lanczos downsample plus JPEG, the usual 'typical low-res input' stand-in."""
    if scale < 2:
        raise ValueError("scale must be at least 2")
    width = max(2, image.width // scale)
    height = max(2, image.height // scale)
    small = image.resize((width, height), Image.Resampling.LANCZOS)
    buffer = io.BytesIO()
    small.save(buffer, format="JPEG", quality=int(jpeg_quality))
    buffer.seek(0)
    with Image.open(buffer) as compressed:
        return compressed.convert("RGB")


def lanczos_upscale(image: Image.Image, width: int, height: int) -> Image.Image:
    return image.resize((width, height), Image.Resampling.LANCZOS)


def product_still_crop(root: Path, width: int = 512, height: int = 288) -> Image.Image | None:
    """Center-left photographic area of the bundled product graphic, when it exists."""
    candidate = root / "assets" / "pxlForgeDefault.png"
    if not candidate.is_file():
        return None
    with Image.open(candidate) as source:
        rgb = source.convert("RGB")
    box = (40, 40, 40 + 720, 40 + 405)
    cropped = rgb.crop(box)
    return cropped.resize((width, height), Image.Resampling.LANCZOS)
