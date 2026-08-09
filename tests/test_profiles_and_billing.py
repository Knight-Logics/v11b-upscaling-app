from __future__ import annotations

import ast
import importlib.util
import hashlib
import os
import sys
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pixelforge.compare import align_before_after_images, build_compare_timestamp_plan
from pixelforge.engines import REALCUGAN_MODEL_CONFIGS, RIFE_MODEL_DETAILS, SRMD_MODEL_CONFIGS
from pixelforge.presets import MODEL_NATIVE_SCALES, get_profile_preset
from pixelforge.nvidia import (
    activate_nvidia_pack,
    download_nvidia_pack,
    frame_rate_choice,
    install_nvidia_pack,
    parse_nvidia_smi_row,
)

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
                    preset = get_profile_preset(speed, content)
                    self.assertIn(preset["scale"], MODEL_NATIVE_SCALES[preset["model"]])
                    self.assertTrue(preset["auto_deinterlace"])

    def test_balanced_natural_uses_benchmark_winning_realsr(self) -> None:
        preset = get_profile_preset("balanced", "live")
        self.assertEqual(preset["model"], "realsr-df2k")
        self.assertEqual(preset["scale"], 4)
        self.assertFalse(preset["enable_color"])
        self.assertFalse(preset["enable_sharpen"])
        self.assertFalse(preset["enable_interpolation"])
        self.assertFalse(preset["apply_final_scale"])
        self.assertFalse(str(preset["model"]).startswith("waifu2x"))
        self.assertFalse(str(preset["model"]).startswith("realesr-anime"))

    def test_live_fast_also_avoids_anime_models(self) -> None:
        preset = get_profile_preset("fast", "live")
        self.assertEqual(preset["model"], "span-photo-x4")

    def test_animation_presets_use_modern_span_then_conservative_realcugan(self) -> None:
        for speed, model in (
            ("fast", "span-modern-animation-x2"),
            ("balanced", "realcugan-se-x2-n1"),
            ("quality", "realcugan-se-x2-n0"),
        ):
            with self.subTest(speed=speed):
                preset = get_profile_preset(speed, "animation")
                self.assertEqual(preset["model"], model)
                if speed != "fast":
                    self.assertIn(model, REALCUGAN_MODEL_CONFIGS)

    def test_restore_presets_use_srmd_or_realsr(self) -> None:
        self.assertEqual(get_profile_preset("fast", "restore")["model"], "srmd-x2-n5")
        self.assertEqual(get_profile_preset("balanced", "restore")["model"], "srmd-x4-n5")
        self.assertEqual(get_profile_preset("quality", "restore")["model"], "realsr-df2k-jpeg")
        self.assertIn("srmd-x2-n5", SRMD_MODEL_CONFIGS)

    def test_presets_default_to_newer_rife(self) -> None:
        for speed in ("fast", "balanced", "quality"):
            for content in ("live", "animation", "restore"):
                with self.subTest(speed=speed, content=content):
                    self.assertEqual(get_profile_preset(speed, content)["rife_model"], "rife-v4.25")

    def test_speed_profiles_never_turn_on_motion_interpolation(self) -> None:
        for speed in ("fast", "balanced", "quality"):
            for content in ("live", "animation", "restore"):
                with self.subTest(speed=speed, content=content):
                    self.assertFalse(get_profile_preset(speed, content)["enable_interpolation"])

    def test_nvidia_profiles_use_official_rtx_vsr_with_content_modes(self) -> None:
        expected_modes = {
            "live": "auto-standard",
            "animation": "auto-clean",
            "restore": "auto-restore",
        }
        for content, mode in expected_modes.items():
            with self.subTest(content=content):
                preset = get_profile_preset("nvidia", content)
                self.assertEqual(preset["model"], "nvidia-rtx-vsr")
                self.assertEqual(preset["nvidia_vsr_mode"], mode)
                self.assertFalse(preset["enable_interpolation"])


class NvidiaCapabilityTests(unittest.TestCase):
    def test_supported_rtx_requires_current_driver_and_vram(self) -> None:
        detected = parse_nvidia_smi_row("NVIDIA GeForce RTX 5070 Ti, 610.62, 16303, 12.0")
        self.assertTrue(detected.supported)
        self.assertEqual(detected.vram_mb, 16303)

    def test_old_driver_and_non_tensor_gpu_fail_closed(self) -> None:
        self.assertFalse(parse_nvidia_smi_row("NVIDIA GeForce RTX 2080, 560.12, 8192, 7.5").supported)
        self.assertFalse(parse_nvidia_smi_row("NVIDIA GeForce GTX 1080, 610.62, 8192, 6.1").supported)

    def test_smooth_60_fps_is_target_driven(self) -> None:
        self.assertEqual(frame_rate_choice("Smooth 60 FPS", 23.976), (True, 60))
        self.assertEqual(frame_rate_choice("Keep source", 29.97), (False, 30))
        self.assertEqual(frame_rate_choice("Smooth 60 FPS", 60.0), (False, 60))

    def test_pack_download_verifies_hash_and_installs_safely(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "source.zip"
            with zipfile.ZipFile(source, "w") as bundle:
                bundle.writestr("PixelForge-NVIDIA-Worker.exe", b"worker")
                bundle.writestr("_internal/runtime.dat", b"runtime")
            expected = hashlib.sha256(source.read_bytes()).hexdigest()
            downloaded = download_nvidia_pack(
                root / "downloaded.zip",
                url=source.as_uri(),
                expected_sha256=expected,
            )
            worker = install_nvidia_pack(downloaded, root / "installed")
            self.assertEqual(worker.read_bytes(), b"worker")
            self.assertEqual((worker.parent / "_internal" / "runtime.dat").read_bytes(), b"runtime")

    def test_pack_installer_rejects_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            archive = root / "unsafe.zip"
            with zipfile.ZipFile(archive, "w") as bundle:
                bundle.writestr("../outside.txt", b"unsafe")
                bundle.writestr("PixelForge-NVIDIA-Worker.exe", b"worker")
            with self.assertRaises(RuntimeError):
                install_nvidia_pack(archive, root / "installed")
            self.assertFalse((root / "outside.txt").exists())

    def test_verified_pack_activation_replaces_previous_pack(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = root / "candidate"
            destination = root / "nvidia-pack"
            candidate.mkdir()
            destination.mkdir()
            (candidate / "PixelForge-NVIDIA-Worker.exe").write_bytes(b"new")
            (destination / "PixelForge-NVIDIA-Worker.exe").write_bytes(b"old")

            worker = activate_nvidia_pack(candidate, destination)

            self.assertEqual(worker.read_bytes(), b"new")
            self.assertFalse(candidate.exists())
            self.assertFalse((root / "nvidia-pack.previous").exists())

    def test_pack_activation_restores_previous_pack_if_promotion_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = root / "candidate"
            destination = root / "nvidia-pack"
            candidate.mkdir()
            destination.mkdir()
            (candidate / "PixelForge-NVIDIA-Worker.exe").write_bytes(b"new")
            (destination / "PixelForge-NVIDIA-Worker.exe").write_bytes(b"old")
            real_move = __import__("shutil").move
            calls = 0

            def fail_candidate_promotion(source, target):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("simulated promotion failure")
                return real_move(source, target)

            with mock.patch("pixelforge.nvidia.shutil.move", side_effect=fail_candidate_promotion):
                with self.assertRaises(OSError):
                    activate_nvidia_pack(candidate, destination)

            self.assertEqual((destination / "PixelForge-NVIDIA-Worker.exe").read_bytes(), b"old")

    def test_profile_validation_rejects_unknown_names(self) -> None:
        with self.assertRaises(ValueError):
            get_profile_preset("turbo", "live")
        with self.assertRaises(ValueError):
            get_profile_preset("balanced", "mystery")


class EngineCatalogTests(unittest.TestCase):
    def test_app_exposes_new_models_and_rife_versions(self) -> None:
        keys = {key for key, _ in APP.MODEL_DETAILS}
        self.assertIn("span-photo-x4", keys)
        self.assertIn("span-modern-animation-x2", keys)
        self.assertIn("realcugan-se-x2-n1", keys)
        self.assertIn("srmd-x4-n5", keys)
        rife_keys = [key for key, _ in APP.RIFE_MODEL_DETAILS]
        self.assertEqual(rife_keys[0], "rife-v4.25")
        self.assertIn("rife-v4.26", rife_keys)
        catalog_keys = [key for key, _ in RIFE_MODEL_DETAILS]
        self.assertEqual(rife_keys, catalog_keys)

    def test_runtime_engine_binaries_exist_in_dev_tree(self) -> None:
        for name in (
            "realcugan-ncnn-vulkan.exe",
            "srmd-ncnn-vulkan.exe",
            "rife-ncnn-vulkan.exe",
        ):
            self.assertTrue((ROOT / name).exists(), f"missing {name}")
        self.assertTrue((ROOT / "realcugan-models" / "models-se").is_dir())
        self.assertTrue((ROOT / "models-srmd" / "srmd_x4.bin").exists())
        self.assertTrue((ROOT / "rife-models" / "rife-v4.25").is_dir())
        self.assertTrue((ROOT / "onnx-models" / "4xSPANkendata_fp32.onnx").is_file())


class CompareHonestyTests(unittest.TestCase):
    def test_selected_pct_is_honored_exactly(self) -> None:
        plan = build_compare_timestamp_plan(100.0, start_time=10.0, clip_duration=40.0, frame_pct=25.0)
        self.assertAlmostEqual(plan.primary_ts, 20.0)
        self.assertEqual(plan.filmstrip_pcts, (25.0, 50.0, 75.0))
        self.assertIn("not the full video", plan.note.lower())

    def test_align_before_after_matches_resolution(self) -> None:
        try:
            from PIL import Image
        except ImportError:
            self.skipTest("Pillow required")
        before = Image.new("RGB", (100, 50), color=(10, 10, 10))
        after = Image.new("RGB", (400, 200), color=(20, 20, 20))
        aligned_before, aligned_after = align_before_after_images(before, after, Image.Resampling.LANCZOS)
        self.assertEqual(aligned_before.size, aligned_after.size)
        self.assertEqual(aligned_after.size, (400, 200))

    def test_pick_default_skips_near_black_mid_frame(self) -> None:
        from pixelforge.compare import pick_default_filmstrip_pct

        chosen = pick_default_filmstrip_pct({25.0: 4.0, 50.0: 3.0, 75.0: 90.0})
        self.assertEqual(chosen, 75.0)

    def test_pick_default_keeps_mid_when_bright(self) -> None:
        from pixelforge.compare import pick_default_filmstrip_pct

        chosen = pick_default_filmstrip_pct({25.0: 40.0, 50.0: 55.0, 75.0: 60.0})
        self.assertEqual(chosen, 50.0)


class PreviewUiStateTests(unittest.TestCase):
    def test_preview_cancel_button_tracks_active_work(self) -> None:
        class FakeButton:
            def __init__(self) -> None:
                self.state = None

            def winfo_exists(self) -> bool:
                return True

            def configure(self, **kwargs) -> None:
                self.state = kwargs.get("state")

        app = object.__new__(APP.V11BApp)
        app.preview_cancel_button = FakeButton()

        app._set_preview_cancel_enabled(True)
        self.assertEqual(app.preview_cancel_button.state, APP.tk.NORMAL)
        app._set_preview_cancel_enabled(False)
        self.assertEqual(app.preview_cancel_button.state, APP.tk.DISABLED)


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

    def test_checkout_cards_send_plan_ids_and_optional_server_pro(self) -> None:
        app = object.__new__(APP.V11BApp)
        app._server_billing_plans = []
        packages = app._get_billing_package_definitions()
        self.assertEqual(
            [package["plan_id"] for package in packages],
            ["starter_32", "creator_68", "pro_144"],
        )
        app._server_billing_plans = [
            {
                "id": "pro_lifetime",
                "label": "PixelForge AI Pro",
                "amount_cents": 9900,
                "badge": "One-time license",
            }
        ]
        packages = app._get_billing_package_definitions()
        self.assertEqual(packages[-1]["plan_id"], "pro_lifetime")
        self.assertEqual(packages[-1]["credits"], 0)
        self.assertEqual(packages[-1]["price_cents"], 9900)

    def test_billing_endpoint_rewrites_undeployed_knight_account(self) -> None:
        backend = APP.RemoteBillingBackend("https://knightlogics.com/api/knight-account", "a" * 64, APP.APP_VERSION)
        self.assertTrue(backend.endpoint.endswith("/api/pixelforge-license"))
        root_backend = APP.RemoteBillingBackend("https://knightlogics.com", "a" * 64, APP.APP_VERSION)
        self.assertTrue(root_backend.endpoint.endswith("/api/pixelforge-license"))

    def test_no_compiled_recovery_signing_secret(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(APP._get_recovery_hmac_secret())
            with self.assertRaises(RuntimeError):
                APP._build_recovery_token(100, "buyer@example.com")


class ChunkedPipelineTests(unittest.TestCase):
    def test_native_batches_release_decoded_frames_and_resume_from_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            frames_in = root / "frames_in"
            frames_out = root / "frames_out"
            frames_in.mkdir()
            frames_out.mkdir()
            for index in range(1, 66):
                (frames_in / f"frame_{index:08d}.png").write_bytes(f"frame-{index}".encode())

            runner = object.__new__(APP.PipelineRunner)
            runner.settings = SimpleNamespace(image_format="png", chunk_size=30, keep_intermediate=False)
            runner.stop_event = threading.Event()
            runner._set_stage_progress = lambda *_args, **_kwargs: None

            def fake_run(command, *_args, **_kwargs):
                source_dir = Path(command[command.index("-i") + 1])
                output_dir = Path(command[command.index("-o") + 1])
                output_dir.mkdir(parents=True, exist_ok=True)
                for source in source_dir.glob("*.png"):
                    (output_dir / source.name).write_bytes(source.read_bytes())

            runner._run_command = fake_run
            base_command = ["fake-engine", "-i", str(frames_in), "-o", str(frames_out)]
            runner._run_upscale_in_chunks(base_command, frames_in, frames_out, 65, "fake", root)

            self.assertEqual(len(list(frames_out.glob("*.png"))), 65)
            self.assertEqual(len(list(frames_in.glob("*.png"))), 0)
            self.assertFalse((root / "upscale_chunks").exists())

            # A resumed pass can validate the finished output even after raw frames were released.
            runner._run_upscale_in_chunks(base_command, frames_in, frames_out, 65, "fake", root)


if __name__ == "__main__":
    unittest.main()
