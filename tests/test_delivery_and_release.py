"""Delivery size, still orientation, Faithful behavior, and the v1.0.23 updater."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from PIL import Image, ImageDraw

from pixelforge.image_io import attach_alpha, prepare_still, still_display_size
from pixelforge.media import display_size_from_stream
from pixelforge.presets import get_profile_preset
from pixelforge.targets import (
    AUTOMATIC_TARGETS,
    final_scale_filter,
    oriented_pixel_size,
    plan_delivery,
    suggested_output_target,
)
from updater import _parse_version, _select_windows_asset


ROOT = Path(__file__).resolve().parents[1]
SPEEDS = ("fast", "balanced", "quality", "nvidia")
CONTENTS = ("live", "animation", "restore")
FIDELITIES = ("enhanced", "faithful")
SOURCES = (
    (2, 2),
    (16, 9),
    (320, 180),
    (640, 360),
    (854, 480),
    (1280, 720),
    (1920, 1080),
    (1440, 1080),
    (1080, 1920),
    (3840, 2160),
    (7680, 4320),
)


class DeliveryPlanTests(unittest.TestCase):
    def test_every_mode_keeps_the_expected_enlargement(self) -> None:
        for fidelity in FIDELITIES:
            for speed in SPEEDS:
                for content in CONTENTS:
                    preset = get_profile_preset(speed, content, fidelity)
                    scale = int(preset["scale"])
                    for width, height in SOURCES:
                        with self.subTest(fidelity=fidelity, speed=speed, content=content, size=(width, height)):
                            native = plan_delivery(width, height, scale, apply_final_scale=False)
                            self.assertGreaterEqual(native.output_width, width * scale)
                            self.assertGreaterEqual(native.output_height, height * scale)
                            self.assertFalse(native.explicit_same_resolution)

                            automatic = plan_delivery(width, height, scale)
                            self.assertIn(automatic.target_name, ("Same resolution",) + AUTOMATIC_TARGETS)
                            self.assertNotEqual(automatic.target_name, "8K")
                            self.assertGreaterEqual(automatic.output_width, width)
                            self.assertGreaterEqual(automatic.output_height, height)
                            self.assertGreaterEqual(automatic.enlargement, 1.0)
                            long_edge = max(width, height)
                            if long_edge <= 1280 and min(width, height) >= 2:
                                self.assertGreaterEqual(automatic.enlargement, 1.5)
                                self.assertNotEqual(automatic.target_name, "Same resolution")
                            if long_edge >= 3840 and width >= height:
                                self.assertEqual(automatic.target_name, "Same resolution")
                                self.assertLess(automatic.enlargement, 1.05)

                            same = plan_delivery(width, height, scale, target_name="Same resolution")
                            self.assertTrue(same.explicit_same_resolution)
                            self.assertLessEqual(abs(same.output_width - (width + width % 2)), 1)
                            self.assertGreaterEqual(same.enlargement, 1.0)
                            self.assertLess(same.enlargement, 1.05)

                            for name in ("1080p", "1440p", "4K", "8K"):
                                chosen = plan_delivery(width, height, scale, target_name=name)
                                box = {"1080p": (1920, 1080), "1440p": (2560, 1440), "4K": (3840, 2160), "8K": (7680, 4320)}[name]
                                self.assertLessEqual(chosen.output_width, box[0] + 1)
                                self.assertLessEqual(chosen.output_height, box[1] + 1)
                                self.assertGreaterEqual(chosen.output_width, 2)
                                self.assertGreaterEqual(chosen.output_height, 2)

    def test_four_k_cap_is_only_on_the_automatic_path(self) -> None:
        self.assertEqual(suggested_output_target(2, 2), "1080p")
        self.assertEqual(suggested_output_target(320, 180), "1080p")
        self.assertEqual(suggested_output_target(640, 360), "1080p")
        self.assertEqual(suggested_output_target(7680, 4320), "Same resolution")
        explicit = plan_delivery(1280, 720, 4, target_name="8K")
        self.assertEqual(explicit.output_width, 7680)
        self.assertGreater(explicit.enlargement, 4)

    def test_aspect_is_not_stretched(self) -> None:
        plan = plan_delivery(1440, 1080, 4, target_name="1080p")
        self.assertAlmostEqual(plan.output_width / plan.output_height, 1440 / 1080, places=2)
        self.assertLessEqual(plan.output_height, 1080)
        self.assertLessEqual(plan.output_width, 1920)

    def test_frame_batch_filter_matches_the_single_frame_plan(self) -> None:
        plan = plan_delivery(640, 360, 4)
        single = final_scale_filter(plan)
        batch = final_scale_filter(plan)
        self.assertEqual(single, batch)
        self.assertIn(f"scale={plan.output_width}:{plan.output_height}", single or "")
        preserved = final_scale_filter(plan, preserve_aspect_ratio=True)
        self.assertIn("force_original_aspect_ratio=decrease", preserved or "")
        self.assertIn(f"pad={plan.output_width}:{plan.output_height}", preserved or "")

    def test_faithful_keeps_realsr_and_the_same_delivery_size(self) -> None:
        enhanced = get_profile_preset("balanced", "live", "enhanced")
        faithful = get_profile_preset("balanced", "live", "faithful")
        self.assertEqual(enhanced["model"], "realesrgan-x4plus")
        self.assertEqual(faithful["model"], "realsr-df2k")
        self.assertFalse(faithful["enable_sharpen"])
        self.assertFalse(enhanced["enable_sharpen"])
        for width, height in ((640, 360), (1280, 720), (3840, 2160)):
            left = plan_delivery(width, height, int(enhanced["scale"]))
            right = plan_delivery(width, height, int(faithful["scale"]))
            self.assertEqual((left.output_width, left.output_height), (right.output_width, right.output_height))
            self.assertEqual(left.target_name, right.target_name)
        self.assertEqual(get_profile_preset("balanced", "animation", "faithful")["model"], "realcugan-se-x2-n1")
        self.assertEqual(get_profile_preset("balanced", "animation", "enhanced")["model"], "realcugan-se-x2-n0")

    def test_rotation_and_sample_aspect_swap_the_planned_axis(self) -> None:
        self.assertEqual(oriented_pixel_size(1920, 1080, 90), (1080, 1920))
        self.assertEqual(oriented_pixel_size(720, 480, 0, 8 / 9), (640, 480))
        self.assertEqual(
            display_size_from_stream({
                "width": 1920,
                "height": 1080,
                "side_data_list": [{"rotation": -90}],
            }),
            (1080, 1920),
        )
        upright = plan_delivery(*oriented_pixel_size(1920, 1080, 90), 4)
        sideways = plan_delivery(1920, 1080, 4)
        self.assertNotEqual((upright.output_width, upright.output_height), (sideways.output_width, sideways.output_height))


class StillImageTests(unittest.TestCase):
    def test_exif_orientation_and_alpha_survive_the_delivery_scale(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "rotated.png"
            image = Image.new("RGBA", (32, 16), (0, 0, 0, 0))
            draw = ImageDraw.Draw(image)
            draw.rectangle((2, 2, 14, 14), fill=(255, 0, 0, 255))
            image.save(source)
            exif = Image.Exif()
            exif[274] = 6  # orientation: rotate 90 CW
            jpeg = root / "turned.jpg"
            image.convert("RGB").save(jpeg, format="JPEG", exif=exif)
            self.assertEqual(still_display_size(jpeg), (16, 32))

            prepared_path, alpha = prepare_still(source, root / "upright.png")
            self.assertIsNotNone(alpha)
            with Image.open(prepared_path) as prepared:
                self.assertEqual(prepared.mode, "RGB")
                self.assertEqual(prepared.size, (32, 16))

            plan = plan_delivery(32, 16, 4)
            delivered = root / "scaled.png"
            subprocess.run(
                [
                    "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                    "-i", str(prepared_path),
                    "-vf", final_scale_filter(plan) or "scale=iw:ih",
                    str(delivered),
                ],
                check=True,
            )
            restored = root / "restored.png"
            attach_alpha(delivered, alpha, restored)
            with Image.open(restored) as result:
                self.assertEqual(result.size, (plan.output_width, plan.output_height))
                self.assertGreaterEqual(result.width, 32)
                self.assertGreaterEqual(result.height, 16)
                self.assertIn("A", result.getbands())
                self.assertLess(result.getpixel((0, 0))[3], 16)
                opaque = result.getpixel((result.width // 5, result.height // 5))
                self.assertGreater(opaque[3], 200)


class UpdaterCompatibilityTests(unittest.TestCase):
    def test_one_point_zero_point_twenty_three_is_newer_than_the_published_release(self) -> None:
        self.assertGreater(_parse_version("1.0.23"), _parse_version("v1.0.22"))
        self.assertFalse(_parse_version("v1.0.22") > _parse_version("1.0.23"))
        self.assertEqual(_parse_version("v1.0.23"), _parse_version("1.0.23"))

    def test_windows_zip_for_this_version_is_selected(self) -> None:
        payload = {
            "tag_name": "v1.0.23",
            "assets": [
                {"name": "notes.txt", "browser_download_url": "https://example.invalid/notes", "size": 10},
                {
                    "name": "PixelForge-AI_1.0.23_windows_x64.zip",
                    "browser_download_url": "https://example.invalid/app.zip",
                    "size": 80000000,
                    "digest": "sha256:" + ("ab" * 32),
                },
            ],
        }
        url, name, size, digest = _select_windows_asset(payload)
        self.assertEqual(name, "PixelForge-AI_1.0.23_windows_x64.zip")
        self.assertEqual(url, "https://example.invalid/app.zip")
        self.assertEqual(size, 80000000)
        self.assertEqual(digest, "ab" * 32)

    def test_app_version_matches_the_installer_metadata(self) -> None:
        source = (ROOT / "process_full_video_ultimate.py").read_text(encoding="utf-8")
        self.assertIn('APP_VERSION = "1.0.23"', source)
        version_info = (ROOT / "assets" / "version_info.txt").read_text(encoding="utf-8")
        self.assertIn("filevers=(1, 0, 23, 0)", version_info)
        self.assertIn("prodvers=(1, 0, 23, 0)", version_info)
        self.assertIn("1.0.23.0", version_info)
        for name in ("build_release.ps1", "build_nvidia_pack.ps1", "build_nvidia_edition.ps1"):
            text = (ROOT / name).read_text(encoding="utf-8")
            self.assertIn("1.0.23", text)
        notes = ROOT / "RELEASE_NOTES_v1.0.23.md"
        self.assertTrue(notes.is_file())


if __name__ == "__main__":
    unittest.main()
