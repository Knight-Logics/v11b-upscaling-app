"""Alignment, preset fidelity, and billing-unreachable behavior."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

from PIL import Image

from pixelforge.billing_client import UNREACHABLE_MESSAGE, BillingUnreachable, KnightAccountClient
from pixelforge.metrics import aligned_scores, psnr

_GATE_PATH = Path(__file__).resolve().parents[1] / "tools" / "quality_gate.py"
_SPEC = importlib.util.spec_from_file_location("pixelforge_quality_gate", _GATE_PATH)
assert _SPEC and _SPEC.loader
_GATE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_GATE)


class AlignmentTests(unittest.TestCase):
    def test_two_frame_shift_is_recovered(self) -> None:
        frames = []
        for index in range(6):
            image = Image.new("RGB", (64, 48), (20 + index * 30, 10, 80))
            for x in range(4 + index * 8, 12 + index * 8):
                for y in range(8, 40):
                    image.putpixel((min(63, x), y), (240, 200, 20))
            frames.append(image)
        shifted = frames[2:] + frames[:2]
        scores = aligned_scores(frames, shifted, radius=4)
        self.assertEqual(scores["offset"], -2)
        self.assertGreater(float(scores["psnr"]), float(scores["unaligned_psnr"]) + 5.0)
        self.assertGreater(psnr(frames[2], shifted[0]), 90.0)

    def test_gate_self_test_rejects_the_unaligned_comparison(self) -> None:
        result = _GATE.alignment_self_test()
        self.assertTrue(result["passed"])
        self.assertEqual(result["measured_offset"], -2)


class BillingUnreachableTests(unittest.TestCase):
    def test_telemetry_survives_a_closed_port(self) -> None:
        client = KnightAccountClient("https://127.0.0.1:9", "a" * 64, "1.0.23")
        result = client.record_event("app_open", {"outcome": "opened"})
        self.assertFalse(result["ok"])
        self.assertFalse(result["reachable"])
        self.assertEqual(result["error"], UNREACHABLE_MESSAGE)

    def test_status_raises_without_changing_a_balance(self) -> None:
        client = KnightAccountClient("https://127.0.0.1:9", "a" * 64, "1.0.23")
        with self.assertRaises(BillingUnreachable) as caught:
            client.get_status("v11b-testtoken123456")
        self.assertIn("not changed", str(caught.exception).lower())
        self.assertFalse(caught.exception.payload["reachable"])

    def test_checkout_does_not_invent_a_session_when_the_server_is_down(self) -> None:
        client = KnightAccountClient("https://127.0.0.1:9", "a" * 64, "1.0.23")
        with self.assertRaises(BillingUnreachable):
            client.create_checkout_session("v11b-testtoken123456", 12, package_name="starter_12")


if __name__ == "__main__":
    unittest.main()
