from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pixelforge.media import MediaProperties, deinterlace_filter, estimate_workspace_bytes, video_encode_args
from pixelforge.onnx_upscale import MODEL_CATALOG, OnnxUpscaler
from pixelforge.quality import assess_quality, output_enlargement_factor
from pixelforge.targets import OUTPUT_TARGETS, target_dimensions


class OutputTargetTests(unittest.TestCase):
    def test_standard_16_by_9_targets(self) -> None:
        self.assertEqual(target_dimensions(1280, 720, "Same resolution"), (1280, 720))
        self.assertEqual(target_dimensions(1280, 720, "1080p"), (1920, 1080))
        self.assertEqual(target_dimensions(1280, 720, "1440p"), (2560, 1440))
        self.assertEqual(target_dimensions(1280, 720, "4K"), (3840, 2160))
        self.assertEqual(target_dimensions(1280, 720, "8K"), (7680, 4320))

    def test_nonstandard_aspect_fits_without_stretching(self) -> None:
        width, height = target_dimensions(3440, 1440, "4K")
        self.assertLessEqual(width, 3840)
        self.assertLessEqual(height, 2160)
        self.assertAlmostEqual(width / height, 3440 / 1440, places=2)

    def test_public_choices_are_deliverables_not_engine_scales(self) -> None:
        self.assertEqual(OUTPUT_TARGETS, ("Same resolution", "1080p", "1440p", "4K", "8K"))


class MediaPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sdr = MediaProperties(1920, 1080, "yuv420p", "bt709", "bt709", "bt709", "h264", 2, 1, 0, 3)
        self.hdr = MediaProperties(3840, 2160, "yuv420p10le", "bt2020nc", "smpte2084", "bt2020", "hevc", 2, 1, 1, 3)

    def test_auto_prefers_nvenc_h264_for_sdr(self) -> None:
        args, label = video_encode_args(
            "Auto (best local)", prefer_hardware=True, crf=17, encode_preset="medium",
            source=self.sdr, encoders={"h264_nvenc", "hevc_nvenc"},
        )
        self.assertIn("h264_nvenc", args)
        self.assertIn("NVENC", label)

    def test_auto_prefers_main10_hevc_for_hdr(self) -> None:
        args, label = video_encode_args(
            "Auto (best local)", prefer_hardware=True, crf=17, encode_preset="medium",
            source=self.hdr, encoders={"h264_nvenc", "hevc_nvenc"},
        )
        self.assertIn("hevc_nvenc", args)
        self.assertIn("p010le", args)
        self.assertIn("Main10", label)

    def test_workspace_estimate_accounts_for_interpolation(self) -> None:
        base = estimate_workspace_bytes(1920, 1080, 100, 4, image_format="png", post_enabled=False, interpolation_factor=1)
        interpolated = estimate_workspace_bytes(1920, 1080, 100, 4, image_format="png", post_enabled=True, interpolation_factor=2)
        self.assertGreater(interpolated, base)

    def test_interlaced_sources_receive_same_rate_bwdif(self) -> None:
        interlaced = MediaProperties(
            720, 480, "yuv420p", "smpte170m", "bt709", "smpte170m", "mpeg2video", 1, 0, 0, 0, "tt"
        )
        self.assertTrue(interlaced.is_interlaced)
        self.assertEqual(deinterlace_filter(interlaced, True), "bwdif=mode=send_frame:parity=auto:deint=all")
        self.assertIsNone(deinterlace_filter(interlaced, False))


class QualityGuardTests(unittest.TestCase):
    def test_hdr_ncnn_route_is_replaced_with_precision_safe_directml(self) -> None:
        source = MediaProperties(
            1920, 1080, "yuv420p10le", "bt2020nc", "smpte2084", "bt2020", "hevc", 1, 0, 0, 0
        )
        assessment = assess_quality(
            source,
            model_key="realsr-df2k",
            content_name="live",
            target_width=3840,
            target_height=2160,
        )
        self.assertEqual((assessment.precision_model, assessment.precision_scale), ("span-photo-x4", 4))
        self.assertFalse(assessment.requires_extreme_scale_confirmation)

    def test_animation_hdr_uses_animation_precision_model(self) -> None:
        source = MediaProperties(
            1280, 720, "yuv420p10le", "bt2020nc", "arib-std-b67", "bt2020", "hevc", 0, 0, 0, 0
        )
        assessment = assess_quality(
            source,
            model_key="realcugan-se-x2-n1",
            content_name="animation",
            target_width=2560,
            target_height=1440,
        )
        self.assertEqual((assessment.precision_model, assessment.precision_scale), ("span-modern-animation-x2", 2))

    def test_extreme_enlargement_requires_confirmation(self) -> None:
        source = MediaProperties(640, 360, "yuv420p", "bt709", "bt709", "bt709", "h264", 1, 0, 0, 0)
        assessment = assess_quality(
            source,
            model_key="span-photo-x4",
            content_name="live",
            target_width=3840,
            target_height=2160,
        )
        self.assertAlmostEqual(output_enlargement_factor(640, 360, 3840, 2160), 6.0)
        self.assertTrue(assessment.requires_extreme_scale_confirmation)


class ModernModelCatalogTests(unittest.TestCase):
    def test_models_record_commercial_license_and_hash(self) -> None:
        self.assertEqual(MODEL_CATALOG["span-modern-animation-x2"].license_name, "MIT")
        self.assertEqual(MODEL_CATALOG["span-photo-x4"].license_name, "CC-BY-SA-4.0")
        for model in MODEL_CATALOG.values():
            self.assertEqual(len(model.sha256), 64)

    def test_array_upscale_preserves_uint16_samples(self) -> None:
        import numpy as np

        runner = object.__new__(OnnxUpscaler)
        runner.np = np
        runner.scale = 2
        runner.tile_size = 2
        runner.context = 1

        def nearest(tile, *, peak_value=255.0, output_dtype=None):
            self.assertEqual(peak_value, 65535.0)
            self.assertEqual(output_dtype, np.uint16)
            return np.repeat(np.repeat(tile, 2, axis=0), 2, axis=1)

        runner._infer_tile = nearest
        source = np.array(
            [
                [[0, 1024, 65535], [4096, 8192, 16384], [32768, 40000, 50000]],
                [[65535, 50000, 40000], [30000, 20000, 10000], [9000, 8000, 7000]],
            ],
            dtype=np.uint16,
        )
        enhanced = runner.upscale_array(source)
        self.assertEqual(enhanced.dtype, np.uint16)
        self.assertEqual(enhanced.shape, (4, 6, 3))
        self.assertEqual(int(enhanced[0, 2, 1]), 8192)
        self.assertEqual(int(enhanced[-1, -1, 0]), 9000)


if __name__ == "__main__":
    unittest.main()
