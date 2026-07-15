from __future__ import annotations

import ast
import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "process_full_video_ultimate.py"
SPEC = importlib.util.spec_from_file_location("pixelforge_app", MODULE_PATH)
assert SPEC and SPEC.loader
APP = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = APP
SPEC.loader.exec_module(APP)


class ProfilePresetTests(unittest.TestCase):
    def test_all_profile_combinations_use_native_model_scales(self) -> None:
        for speed in ("fast", "balanced", "quality"):
            for content in ("live", "animation", "restore"):
                with self.subTest(speed=speed, content=content):
                    preset = APP.get_profile_preset(speed, content)
                    self.assertIn(preset["scale"], APP.MODEL_NATIVE_SCALES[preset["model"]])

    def test_balanced_natural_is_neutral_safe_default(self) -> None:
        preset = APP.get_profile_preset("balanced", "live")
        self.assertEqual(preset["model"], "waifu2x-cunet-noise1")
        self.assertEqual(preset["scale"], 2)
        self.assertFalse(preset["enable_color"])
        self.assertFalse(preset["enable_sharpen"])
        self.assertFalse(preset["enable_interpolation"])
        self.assertFalse(preset["apply_final_scale"])

    def test_speed_profiles_never_turn_on_motion_interpolation(self) -> None:
        for speed in ("fast", "balanced", "quality"):
            for content in ("live", "animation", "restore"):
                with self.subTest(speed=speed, content=content):
                    self.assertFalse(APP.get_profile_preset(speed, content)["enable_interpolation"])

    def test_profile_validation_rejects_unknown_names(self) -> None:
        with self.assertRaises(ValueError):
            APP.get_profile_preset("turbo", "live")
        with self.assertRaises(ValueError):
            APP.get_profile_preset("balanced", "mystery")


class BillingRegressionTests(unittest.TestCase):
    def test_stop_handler_does_not_add_or_restore_credits(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
        stop_function = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "_stop_processing"
        )
        called_attributes = {
            node.func.attr
            for node in ast.walk(stop_function)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertNotIn("restore_credits", called_attributes)
        self.assertNotIn("add_credits", called_attributes)

    def test_local_ledger_charges_only_on_consume(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = APP.BillingStore(root / "tokens.json", root / "audit.jsonl")
            token = "test-token"
            claimed, balance = store.claim_free_trial(token, "device:test", 20)
            self.assertTrue(claimed)
            self.assertEqual(balance, 20)
            self.assertEqual(store.get_status(token)["credits"], 20)
            consumed, balance = store.consume_credits(token, 3)
            self.assertTrue(consumed)
            self.assertEqual(balance, 17)

    def test_embedded_reservation_charges_only_when_committed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = APP.BillingStore(root / "tokens.json", root / "audit.jsonl")
            token = "test-token"
            store.claim_free_trial(token, "device:test", 20)
            backend = APP.EmbeddedBillingBackend(store)

            reserved = backend.reserve_credits(token, 4)
            self.assertTrue(reserved["ok"])
            self.assertEqual(store.get_status(token)["credits"], 20)

            committed = backend.commit_reservation(token, reserved["reservation_id"])
            self.assertTrue(committed["ok"])
            self.assertEqual(store.get_status(token)["credits"], 16)

    def test_production_billing_rejects_plain_http(self) -> None:
        with self.assertRaises(RuntimeError):
            APP.RemoteBillingBackend("http://example.com", "a" * 64, APP.APP_VERSION)

    def test_checkout_packages_are_server_plan_ids(self) -> None:
        self.assertEqual(
            APP.RemoteBillingBackend.PLAN_BY_CREDITS,
            {32: "starter_32", 68: "creator_68", 144: "pro_144"},
        )

    def test_no_compiled_recovery_signing_secret(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(APP._get_recovery_hmac_secret())
            with self.assertRaises(RuntimeError):
                APP._build_recovery_token(100, "buyer@example.com")


if __name__ == "__main__":
    unittest.main()
