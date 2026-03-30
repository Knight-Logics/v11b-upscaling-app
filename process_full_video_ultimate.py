"""PixelForge AI - Video Upscaling Application.

Single-file desktop app for Real-ESRGAN upscaling with configurable preprocessing,
sharpening, interpolation, scaling, encoding, and runtime estimation.
"""

from __future__ import annotations

import importlib.util
import json
import math
import os
import ctypes
import platform
import re
import smtplib
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
import webbrowser
from dataclasses import dataclass
from datetime import UTC, datetime
from email.mime.text import MIMEText
from pathlib import Path
from queue import Empty, Queue
from tkinter import BOTH, END, LEFT, RIGHT, W, X, Y, NW, filedialog, messagebox, simpledialog
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import tkinter as tk
from tkinter import ttk

try:
    import updater

    UPDATER_AVAILABLE = True
except Exception:
    updater = None
    UPDATER_AVAILABLE = False

try:
    import stripe

    STRIPE_AVAILABLE = True
except ImportError:
    stripe = None
    STRIPE_AVAILABLE = False

try:
    from PIL import Image, ImageDraw, ImageTk

    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

if PIL_AVAILABLE:
    try:
        RESAMPLE_FILTER = Image.Resampling.LANCZOS
    except AttributeError:
        RESAMPLE_FILTER = Image.LANCZOS


MODEL_DETAILS = [
    (
        "realesrgan-x4plus",
        "realesrgan-x4plus (General real-world footage: natural videos/images)",
    ),
    (
        "realesrgan-x4plus-anime",
        "realesrgan-x4plus-anime (Anime illustrations/frames, lightweight anime model)",
    ),
    (
        "realesr-animevideov3-x4",
        "realesr-animevideov3-x4 (Anime video, highest anime-video detail)",
    ),
    (
        "realesr-animevideov3-x3",
        "realesr-animevideov3-x3 (Anime video, balanced quality/speed)",
    ),
    (
        "realesr-animevideov3-x2",
        "realesr-animevideov3-x2 (Anime video, fastest draft for animation)",
    ),
]
MODEL_OPTIONS = [key for key, _label in MODEL_DETAILS]
MODEL_KEY_TO_LABEL = {key: label for key, label in MODEL_DETAILS}
MODEL_LABEL_TO_KEY = {label: key for key, label in MODEL_DETAILS}

ENCODE_PRESETS = ["ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow"]
IMAGE_FORMATS = ["png", "jpg"]
FPS_OPTIONS = [24, 30, 48, 60]
TOKEN_PATTERN = re.compile(r"^v11b[-_][A-Za-z0-9]{12,128}$")
APP_VERSION = "1.0.0"
_NO_WINDOW = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0

# When frozen by PyInstaller, runtime tools (realesrgan, models) live next to the .exe.
# When running from source, they live next to this script.
_APP_DIR: Path = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent
_REALESRGAN_EXE: Path = _APP_DIR / "realesrgan-ncnn-vulkan.exe"


def is_valid_paid_access_token(token: str) -> bool:
    return bool(TOKEN_PATTERN.fullmatch(token or ""))


class BillingStore:
    def __init__(self, storage_file: Path, audit_file: Path | None = None):
        self.storage_file = storage_file
        self.audit_file = audit_file or (self.storage_file.parent / "billing_audit.jsonl")
        self._lock = threading.Lock()
        self.storage_file.parent.mkdir(parents=True, exist_ok=True)
        self.audit_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.storage_file.exists():
            self._write(
                {
                    "tokens": {},
                    "processed_purchase_ids": [],
                    "free_trial_claims": {},
                    "emails": {},
                    "recovery_log": {},
                    "credit_codes": {},
                    "redeemed_codes": {},
                }
            )

    def _read(self) -> dict:
        try:
            payload = json.loads(self.storage_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        payload.setdefault("tokens", {})
        payload.setdefault("processed_purchase_ids", [])
        payload.setdefault("free_trial_claims", {})
        payload.setdefault("emails", {})
        payload.setdefault("recovery_log", {})
        payload.setdefault("credit_codes", {})
        payload.setdefault("redeemed_codes", {})
        return payload

    def _write(self, payload: dict) -> None:
        self.storage_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def _audit(self, event_type: str, details: dict) -> None:
        entry = {"timestamp": self._utc_now(), "event": event_type, **details}
        with self.audit_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry) + "\n")

    def _get_token_record(self, payload: dict, token: str) -> dict:
        tokens = payload.setdefault("tokens", {})
        record = tokens.get(token)
        if not isinstance(record, dict):
            record = {
                "credits": 0,
                "paid_credits": 0,
                "free_trial_total": 0,
                "free_trial_remaining": 0,
                "email": "",
                "created_at": self._utc_now(),
                "last_updated_at": self._utc_now(),
            }
            tokens[token] = record
        record["paid_credits"] = max(0, int(record.get("paid_credits", record.get("credits", 0))))
        record["free_trial_total"] = max(0, int(record.get("free_trial_total", 0)))
        record["free_trial_remaining"] = max(0, int(record.get("free_trial_remaining", 0)))
        record["free_trial_remaining"] = min(record["free_trial_remaining"], record["free_trial_total"])
        record["credits"] = int(record.get("paid_credits", 0)) + int(record.get("free_trial_remaining", 0))
        return record

    def get_status(self, token: str) -> dict:
        with self._lock:
            payload = self._read()
            record = self._get_token_record(payload, token)
            self._write(payload)
            return {
                "credits": int(record.get("credits", 0)),
                "paid_credits": int(record.get("paid_credits", 0)),
                "free_trial_total": int(record.get("free_trial_total", 0)),
                "free_trial_remaining": int(record.get("free_trial_remaining", 0)),
                "email_linked": bool(record.get("email")),
                "linked_email": str(record.get("email") or ""),
            }

    def add_credits(self, token: str, credits: int, source: str = "manual") -> int:
        if credits <= 0:
            raise ValueError("credits must be > 0")
        with self._lock:
            payload = self._read()
            record = self._get_token_record(payload, token)
            record["paid_credits"] = int(record.get("paid_credits", 0)) + credits
            record["credits"] = int(record.get("paid_credits", 0)) + int(record.get("free_trial_remaining", 0))
            record["last_updated_at"] = self._utc_now()
            self._write(payload)
            self._audit("credits_added", {"token": token, "delta": credits, "source": source, "balance": record["credits"]})
            return int(record["credits"])

    def consume_credits(self, token: str, cost: int, source: str = "render") -> tuple[bool, int]:
        if cost <= 0:
            raise ValueError("cost must be > 0")
        with self._lock:
            payload = self._read()
            record = self._get_token_record(payload, token)
            paid = int(record.get("paid_credits", 0))
            free_trial = int(record.get("free_trial_remaining", 0))
            current_total = paid + free_trial
            if current_total < cost:
                return False, current_total

            remaining_cost = cost
            if free_trial > 0:
                consume_free = min(free_trial, remaining_cost)
                record["free_trial_remaining"] = free_trial - consume_free
                remaining_cost -= consume_free
            if remaining_cost > 0:
                record["paid_credits"] = paid - remaining_cost

            record["credits"] = int(record.get("paid_credits", 0)) + int(record.get("free_trial_remaining", 0))
            record["last_updated_at"] = self._utc_now()
            self._write(payload)
            self._audit("credits_consumed", {"token": token, "delta": -cost, "source": source, "balance": record["credits"]})
            return True, int(record["credits"])

    def restore_credits(self, token: str, credits: int, source: str = "render_refund") -> int:
        return self.add_credits(token, credits, source=source)

    def set_paid_credits(self, token: str, credits: int, source: str = "admin_reset") -> int:
        credits = max(0, int(credits))
        with self._lock:
            payload = self._read()
            record = self._get_token_record(payload, token)
            record["paid_credits"] = credits
            record["credits"] = int(record.get("paid_credits", 0)) + int(record.get("free_trial_remaining", 0))
            record["last_updated_at"] = self._utc_now()
            self._write(payload)
            self._audit("credits_set", {"token": token, "source": source, "balance": record["credits"]})
            return int(record["credits"])

    def claim_free_trial(self, token: str, claim_key: str, credits: int, source: str = "free_trial") -> tuple[bool, int]:
        if credits <= 0:
            raise ValueError("credits must be > 0")
        if not claim_key:
            raise ValueError("claim_key is required")
        with self._lock:
            payload = self._read()
            claims = payload.setdefault("free_trial_claims", {})
            record = self._get_token_record(payload, token)
            if claim_key in claims:
                self._write(payload)
                return False, int(record.get("free_trial_remaining", 0))
            record["free_trial_total"] = int(record.get("free_trial_total", 0)) + credits
            record["free_trial_remaining"] = int(record.get("free_trial_remaining", 0)) + credits
            record["credits"] = int(record.get("paid_credits", 0)) + int(record.get("free_trial_remaining", 0))
            record["last_updated_at"] = self._utc_now()
            claims[claim_key] = {
                "token": token,
                "claimed_at": self._utc_now(),
                "credits": credits,
                "source": source,
            }
            self._write(payload)
            self._audit(
                "free_trial_claimed",
                {
                    "token": token,
                    "claim_key": claim_key,
                    "delta": credits,
                    "balance": int(record.get("credits", 0)),
                    "free_trial_remaining": int(record.get("free_trial_remaining", 0)),
                    "source": source,
                },
            )
            return True, int(record.get("free_trial_remaining", 0))

    def link_email(self, token: str, email: str) -> tuple[bool, str]:
        email = (email or "").strip().lower()
        if not email or "@" not in email:
            return False, "Enter a valid email address."
        with self._lock:
            payload = self._read()
            emails = payload.setdefault("emails", {})
            existing_token = emails.get(email)
            if existing_token and existing_token != token:
                return False, "That email is already linked to a different access code."
            emails[email] = token
            record = self._get_token_record(payload, token)
            record["email"] = email
            record["last_updated_at"] = self._utc_now()
            self._write(payload)
            self._audit("email_linked", {"token": token, "email": email})
            return True, "Email linked successfully."

    def get_token_by_email(self, email: str) -> str | None:
        email = (email or "").strip().lower()
        with self._lock:
            payload = self._read()
            return payload.get("emails", {}).get(email)

    def record_recovery_sent(self, email: str) -> None:
        email = (email or "").strip().lower()
        with self._lock:
            payload = self._read()
            payload.setdefault("recovery_log", {})[email] = self._utc_now()
            self._write(payload)

    def upsert_credit_code(self, code: str, credits: int, active: bool = True) -> None:
        code_key = (code or "").strip().upper()
        if not code_key:
            raise ValueError("Code is required")
        if credits <= 0:
            raise ValueError("credits must be > 0")
        with self._lock:
            payload = self._read()
            payload.setdefault("credit_codes", {})[code_key] = {
                "credits": int(credits),
                "active": bool(active),
                "updated_at": self._utc_now(),
            }
            self._write(payload)
            self._audit("credit_code_upserted", {"code": code_key, "credits": int(credits), "active": bool(active)})

    def set_code_active(self, code: str, active: bool) -> None:
        code_key = (code or "").strip().upper()
        with self._lock:
            payload = self._read()
            code_record = payload.setdefault("credit_codes", {}).get(code_key)
            if isinstance(code_record, dict):
                code_record["active"] = bool(active)
                code_record["updated_at"] = self._utc_now()
                self._write(payload)
                self._audit("credit_code_state", {"code": code_key, "active": bool(active)})

    def redeem_credit_code(self, token: str, code: str) -> tuple[bool, int, str]:
        code_key = (code or "").strip().upper()
        if not code_key:
            return False, 0, "Enter a credit code first."
        with self._lock:
            payload = self._read()
            code_record = payload.setdefault("credit_codes", {}).get(code_key)
            if not isinstance(code_record, dict):
                return False, 0, "Code not found."
            if not bool(code_record.get("active", False)):
                return False, 0, "Code is inactive."

            redeemed_codes = payload.setdefault("redeemed_codes", {})
            redeemed_by_token = redeemed_codes.setdefault(token, [])
            if code_key in redeemed_by_token:
                return False, int(self._get_token_record(payload, token).get("credits", 0)), "Code already redeemed for this account."

            credits = int(code_record.get("credits", 0) or 0)
            if credits <= 0:
                return False, 0, "Code has no credits configured."

            record = self._get_token_record(payload, token)
            record["paid_credits"] = int(record.get("paid_credits", 0)) + credits
            record["credits"] = int(record.get("paid_credits", 0)) + int(record.get("free_trial_remaining", 0))
            record["last_updated_at"] = self._utc_now()
            redeemed_by_token.append(code_key)
            self._write(payload)
            self._audit("credit_code_redeemed", {"token": token, "code": code_key, "delta": credits, "balance": int(record.get("credits", 0))})
            return True, int(record.get("credits", 0)), f"Redeemed {credits} credits from {code_key}."

    def is_purchase_processed(self, purchase_id: str) -> bool:
        with self._lock:
            payload = self._read()
            return purchase_id in payload.setdefault("processed_purchase_ids", [])

    def apply_purchase_once(self, purchase_id: str, token: str, credits: int, source: str = "stripe_checkout") -> tuple[bool, int]:
        if credits <= 0:
            raise ValueError("credits must be > 0")
        with self._lock:
            payload = self._read()
            processed = payload.setdefault("processed_purchase_ids", [])
            record = self._get_token_record(payload, token)
            if purchase_id in processed:
                self._write(payload)
                return True, int(record.get("credits", 0))
            record["paid_credits"] = int(record.get("paid_credits", 0)) + credits
            record["credits"] = int(record.get("paid_credits", 0)) + int(record.get("free_trial_remaining", 0))
            record["last_updated_at"] = self._utc_now()
            processed.append(purchase_id)
            self._write(payload)
            self._audit("credits_added", {"token": token, "delta": credits, "source": source, "balance": record["credits"]})
            self._audit("purchase_marked_processed", {"purchase_id": purchase_id})
            return False, int(record["credits"])


class EmbeddedBillingBackend:
    def __init__(self, store: BillingStore):
        self.store = store
        self.stripe_secret_key = (os.environ.get("V11B_STRIPE_SECRET_KEY") or os.environ.get("STRIPE_SECRET_KEY") or "").strip()
        self.success_url = (
            os.environ.get("V11B_STRIPE_SUCCESS_URL")
            or os.environ.get("STRIPE_SUCCESS_URL")
            or "https://knightlogics.com/?v11b_payment=success&session_id={CHECKOUT_SESSION_ID}"
        ).strip()
        self.cancel_url = (
            os.environ.get("V11B_STRIPE_CANCEL_URL")
            or os.environ.get("STRIPE_CANCEL_URL")
            or "https://knightlogics.com/?v11b_payment=cancel"
        ).strip()
        self.currency = (os.environ.get("V11B_STRIPE_CURRENCY") or os.environ.get("STRIPE_CURRENCY") or "usd").strip().lower()
        self.price_per_credit_cents = max(1, int(os.environ.get("V11B_PRICE_PER_CREDIT_CENTS", os.environ.get("STRIPE_PRICE_1_CREDIT_CENTS", "28"))))
        self.max_checkout_credits = max(1, int(os.environ.get("V11B_MAX_CHECKOUT_CREDITS", os.environ.get("MAX_CHECKOUT_CREDITS", "500"))))
        if STRIPE_AVAILABLE and self.stripe_secret_key:
            stripe.api_key = self.stripe_secret_key

    def stripe_configured(self) -> bool:
        return bool(STRIPE_AVAILABLE and self.stripe_secret_key)

    def get_status(self, token: str) -> dict:
        return self.store.get_status(token)

    def create_checkout_session(
        self,
        token: str,
        credits: int,
        charge_cents: int | None = None,
        package_name: str | None = None,
    ) -> dict:
        if not self.stripe_configured():
            raise RuntimeError("Stripe is not configured in this app environment yet.")
        if not is_valid_paid_access_token(token):
            raise RuntimeError("Invalid token format.")
        if credits <= 0 or credits > self.max_checkout_credits:
            raise RuntimeError(f"Credits must be between 1 and {self.max_checkout_credits}.")

        normalized_charge_cents: int | None = None
        if charge_cents is not None:
            normalized_charge_cents = max(1, int(charge_cents))

        unit_amount = self.price_per_credit_cents if normalized_charge_cents is None else normalized_charge_cents
        quantity = credits if normalized_charge_cents is None else 1
        item_name = "v11b Processing Credit" if not package_name else f"{package_name} Package"
        item_description = "Credits used to run local AI video upscaling jobs"

        session = stripe.checkout.Session.create(
            mode="payment",
            success_url=self.success_url,
            cancel_url=self.cancel_url,
            client_reference_id=token[:200],
            line_items=[
                {
                    "price_data": {
                        "currency": self.currency,
                        "product_data": {
                            "name": item_name,
                            "description": item_description,
                        },
                        "unit_amount": unit_amount,
                    },
                    "quantity": quantity,
                }
            ],
            metadata={
                "token": token,
                "credits": str(credits),
                "kind": "v11b_processing_credit",
                "package_name": str(package_name or ""),
                "charge_cents": str(normalized_charge_cents or ""),
            },
        )
        return {"url": str(session.url), "session_id": str(session.id)}

    def confirm_checkout_session(self, session_id: str) -> dict:
        if not self.stripe_configured():
            raise RuntimeError("Stripe is not configured in this app environment yet.")
        session = stripe.checkout.Session.retrieve(session_id)
        if session.get("payment_status") != "paid":
            raise RuntimeError("Checkout session is not paid yet.")
        metadata = session.get("metadata") or {}
        token = str(metadata.get("token") or session.get("client_reference_id") or "").strip()
        credits = int(metadata.get("credits") or "0")
        already_processed, balance = self.store.apply_purchase_once(str(session.get("id") or ""), token, credits)
        return {
            "ok": True,
            "session_id": session_id,
            "token": token,
            "credited_credits": credits,
            "already_processed": already_processed,
            "status": self.store.get_status(token),
            "balance": balance,
        }


@dataclass
class PipelineSettings:
    input_video: Path
    output_video: Path
    model: str
    scale: int
    image_format: str
    threads: str
    start_time: float
    clip_duration: float
    denoise: float
    enable_color: bool
    vibrance: float
    contrast: float
    brightness: float
    saturation: float
    gamma: float
    enable_sharpen: bool
    cas_strength: float
    unsharp1: float
    unsharp2: float
    enable_interpolation: bool
    target_fps: int
    apply_final_scale: bool
    target_width: int
    target_height: int
    crf: int
    encode_preset: str
    include_audio: bool
    keep_intermediate: bool


class PipelineRunner:
    def __init__(self, settings: PipelineSettings, log_queue: Queue[str], stop_event: threading.Event):
        self.settings = settings
        self.log_queue = log_queue
        self.stop_event = stop_event
        self.current_process: subprocess.Popen[str] | None = None
        self.total_stages = 6
        self.stage_weight_map: dict[int, float] = {i: 1.0 / self.total_stages for i in range(1, self.total_stages + 1)}
        self.stage_prefix_map: dict[int, float] = {1: 0.0}
        self._last_total_progress: float = 0.0
        self._last_stage_fraction: dict[int, float] = {}
        self._last_emitted_stage_pct: dict[int, float] = {}
        self._upscale_last_scan_ts: float = 0.0
        self._upscale_last_completed: int = 0

    def _configure_stage_weights(
        self,
        frame_count: int,
        work_duration: float,
        source_fps: float,
        post_enabled: bool,
        interpolation_enabled: bool,
    ) -> None:
        # Explicit stage shares make total progress behavior predictable.
        # Stage 2 (Real-ESRGAN) intentionally owns most of the overall progress.
        stage_shares: dict[int, float] = {
            1: 0.10,
            2: 0.52,
            3: 0.10,
            4: 0.14,
            5: 0.10,
            6: 0.04,
        }

        disabled_share = 0.0
        if not post_enabled:
            disabled_share += stage_shares[3]
            stage_shares[3] = 0.0
        if not interpolation_enabled:
            disabled_share += stage_shares[5]
            stage_shares[5] = 0.0

        if disabled_share > 0.0:
            # Redistribute skipped-stage budget toward stages users actually wait on.
            redistribute_targets = [2, 4, 6, 1]
            redistribute_weights = [0.55, 0.25, 0.12, 0.08]
            for target, weight in zip(redistribute_targets, redistribute_weights, strict=False):
                stage_shares[target] += disabled_share * weight

        total_share = sum(stage_shares.values())
        if total_share <= 0:
            self.stage_weight_map = {i: 1.0 / self.total_stages for i in range(1, self.total_stages + 1)}
        else:
            self.stage_weight_map = {stage: (stage_shares.get(stage, 0.0) / total_share) for stage in range(1, self.total_stages + 1)}

        cumulative = 0.0
        self.stage_prefix_map = {}
        for stage_index in range(1, self.total_stages + 1):
            self.stage_prefix_map[stage_index] = cumulative
            cumulative += self.stage_weight_map.get(stage_index, 0.0)

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_queue.put(f"[{timestamp}] {message}")

    def _emit_progress(
        self,
        percent: float,
        stage_index: int = 0,
        stage_fraction: float = 0.0,
        stage_name: str = "",
    ) -> None:
        total_pct = max(0.0, min(100.0, percent))
        total_pct = max(self._last_total_progress, total_pct)
        stage_pct = max(0.0, min(100.0, stage_fraction * 100.0))
        last_stage_pct = self._last_emitted_stage_pct.get(int(stage_index), 0.0)
        total_advanced = (total_pct - self._last_total_progress) > 0.02
        stage_advanced = (stage_pct - last_stage_pct) > 0.15
        stage_finished = stage_pct >= 99.99
        if (not total_advanced) and (not stage_advanced) and (not stage_finished):
            return
        self._last_total_progress = total_pct
        self._last_emitted_stage_pct[int(stage_index)] = stage_pct
        safe_name = (stage_name or "").replace("|", "/")
        self.log_queue.put(
            f"[PROGRESS] total={total_pct:.2f}|stage={int(stage_index)}/{self.total_stages}|"
            f"stage_pct={stage_pct:.2f}|stage_name={safe_name}"
        )

    def _set_stage_progress(self, stage_index: int, fraction: float, stage_name: str = "") -> None:
        clamped = max(0.0, min(1.0, fraction))
        previous_fraction = self._last_stage_fraction.get(stage_index, 0.0)
        clamped = max(previous_fraction, clamped)
        self._last_stage_fraction[stage_index] = clamped
        stage_prefix = self.stage_prefix_map.get(stage_index)
        stage_weight = self.stage_weight_map.get(stage_index)
        if stage_prefix is None or stage_weight is None:
            percent = ((stage_index - 1) + clamped) / self.total_stages * 100.0
        else:
            percent = (stage_prefix + (stage_weight * clamped)) * 100.0
        self._emit_progress(percent, stage_index=stage_index, stage_fraction=clamped, stage_name=stage_name)

    @staticmethod
    def _parse_ffmpeg_time_seconds(message: str) -> float | None:
        match = re.search(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)", message)
        if not match:
            return None
        hours = int(match.group(1))
        minutes = int(match.group(2))
        seconds = float(match.group(3))
        return (hours * 3600) + (minutes * 60) + seconds

    @staticmethod
    def _parse_ffmpeg_frame_count(message: str) -> int | None:
        match = re.search(r"frame=\s*(\d+)", message)
        if not match:
            return None
        return int(match.group(1))

    def _update_command_progress(
        self,
        line: str,
        stage_index: int,
        stage_name: str,
        progress_mode: str | None,
        progress_target: float | int | None,
        progress_path: Path | None,
    ) -> None:
        if not progress_mode:
            return

        fraction: float | None = None
        if progress_mode == "frames" and progress_target:
            frame_value = self._parse_ffmpeg_frame_count(line)
            if frame_value is not None:
                fraction = frame_value / float(progress_target)
        elif progress_mode == "time" and progress_target:
            seconds_value = self._parse_ffmpeg_time_seconds(line)
            if seconds_value is not None:
                fraction = seconds_value / float(progress_target)
        elif progress_mode == "upscale":
            # Real-ESRGAN percent output can reset repeatedly; use actual files produced.
            if progress_target and progress_path and progress_path.exists():
                now = time.monotonic()
                if (now - self._upscale_last_scan_ts) >= 0.35:
                    expected_ext = f".{self.settings.image_format.lower()}"
                    completed = sum(
                        1
                        for item in progress_path.iterdir()
                        if item.is_file() and item.suffix.lower() == expected_ext
                    )
                    self._upscale_last_scan_ts = now
                    if completed < self._upscale_last_completed:
                        completed = self._upscale_last_completed
                    else:
                        self._upscale_last_completed = completed
                    fraction = completed / float(progress_target)
                elif self._upscale_last_completed > 0:
                    fraction = self._upscale_last_completed / float(progress_target)
                else:
                    fraction = 0.0
            else:
                # Only use percent text when output frame counting is not available.
                percent_match = re.search(r"(\d+(?:\.\d+)?)%", line)
                if percent_match:
                    fraction = float(percent_match.group(1)) / 100.0

        if fraction is not None:
            self._set_stage_progress(stage_index, fraction, stage_name=stage_name)

    def _run_command(
        self,
        cmd: list[str],
        stage_name: str,
        stage_index: int,
        progress_mode: str | None = None,
        progress_target: float | int | None = None,
        progress_path: Path | None = None,
    ) -> None:
        if self.stop_event.is_set():
            raise RuntimeError("Canceled by user")

        if progress_mode == "upscale":
            self._upscale_last_scan_ts = 0.0
            self._upscale_last_completed = 0

        self._set_stage_progress(stage_index, 0.0, stage_name=stage_name)
        self.log(f"Running {stage_name}...")
        self.log(" ".join(cmd))
        started = datetime.now()

        try:
            self.current_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                creationflags=_NO_WINDOW,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(f"{stage_name} failed because required executable was not found: {exc}") from exc

        assert self.current_process.stdout is not None
        for line in self.current_process.stdout:
            if self.stop_event.is_set():
                self.current_process.terminate()
                raise RuntimeError("Canceled by user")
            clean = line.rstrip()
            if clean:
                self.log(clean)
                self._update_command_progress(clean, stage_index, stage_name, progress_mode, progress_target, progress_path)

        return_code = self.current_process.wait()
        self.current_process = None
        elapsed = (datetime.now() - started).total_seconds()
        self.log(f"{stage_name} completed in {elapsed:.1f}s")
        if return_code != 0:
            raise RuntimeError(f"{stage_name} failed with exit code {return_code}")
        self._set_stage_progress(stage_index, 1.0, stage_name=stage_name)

    @staticmethod
    def _cli_path(path: Path) -> str:
        value = str(path)
        if value.startswith("-"):
            return f".{os.sep}{value}"
        return value

    @staticmethod
    def _ffprobe_value(video_path: Path, field: str) -> str:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            f"stream={field}",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            PipelineRunner._cli_path(video_path),
        ]
        out = subprocess.check_output(cmd, text=True, encoding="utf-8", errors="replace", creationflags=_NO_WINDOW)
        return out.strip()

    @staticmethod
    def get_video_duration(video_path: Path) -> float:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            PipelineRunner._cli_path(video_path),
        ]
        out = subprocess.check_output(cmd, text=True, encoding="utf-8", errors="replace", creationflags=_NO_WINDOW)
        return float(out.strip())

    @classmethod
    def get_fps(cls, video_path: Path) -> float:
        # Prefer average frame rate for user-facing timing and output duration decisions.
        fps_text = cls._ffprobe_value(video_path, "avg_frame_rate")
        if not fps_text or fps_text == "0/0" or fps_text == "N/A":
            fps_text = cls._ffprobe_value(video_path, "r_frame_rate")
        if "/" in fps_text:
            num, den = fps_text.split("/")
            den_value = float(den) if float(den) != 0 else 1.0
            return float(num) / den_value
        return float(fps_text)

    @classmethod
    def get_frame_count(cls, video_path: Path, duration_override: float | None = None) -> int:
        frames_text = cls._ffprobe_value(video_path, "nb_frames")
        if frames_text and frames_text != "N/A":
            frame_count = int(frames_text)
            if duration_override is None:
                return frame_count

        fps = cls.get_fps(video_path)
        duration = duration_override if duration_override is not None else cls.get_video_duration(video_path)
        return max(1, int(duration * fps))

    def _build_pre_filter(self) -> str | None:
        filters: list[str] = []
        if self.settings.denoise > 0:
            value = f"{self.settings.denoise:.2f}"
            filters.append(f"hqdn3d={value}:{value}")
        if self.settings.enable_color:
            filters.append(
                "vibrance=intensity={v}:rbal=1:gbal=1:bbal=1,eq=contrast={c}:brightness={b}:saturation={s}:gamma={g}".format(
                    v=self.settings.vibrance,
                    c=self.settings.contrast,
                    b=self.settings.brightness,
                    s=self.settings.saturation,
                    g=self.settings.gamma,
                )
            )
        return ",".join(filters) if filters else None

    def _build_post_filter(self) -> str | None:
        filters: list[str] = []
        if self.settings.enable_sharpen:
            filters.append(f"cas={self.settings.cas_strength}")
            filters.append(f"unsharp=5:5:{self.settings.unsharp1}:5:5:0.0")
            filters.append(f"unsharp=7:7:{self.settings.unsharp2}:7:7:0.0")
        if self.settings.apply_final_scale:
            filters.append(f"scale={self.settings.target_width}:{self.settings.target_height}:flags=lanczos")
        return ",".join(filters) if filters else None

    def _trim_input_args(self) -> list[str]:
        args: list[str] = []
        if self.settings.start_time > 0:
            args.extend(["-ss", str(self.settings.start_time)])
        if self.settings.clip_duration > 0:
            args.extend(["-t", str(self.settings.clip_duration)])
        return args

    def run(self) -> None:
        input_path = self.settings.input_video
        output_path = self.settings.output_video
        exe_path = _REALESRGAN_EXE

        if not input_path.exists():
            raise FileNotFoundError(f"Input video not found: {input_path}")
        if not exe_path.exists():
            raise FileNotFoundError(f"realesrgan-ncnn-vulkan.exe not found (looked in: {exe_path.parent})")

        duration_full = self.get_video_duration(input_path)
        source_fps = self.get_fps(input_path)
        source_r_fps_text = self._ffprobe_value(input_path, "r_frame_rate")
        source_r_fps = source_fps
        try:
            if "/" in source_r_fps_text:
                num, den = source_r_fps_text.split("/")
                den_value = float(den) if float(den) != 0 else 1.0
                source_r_fps = float(num) / den_value
            else:
                source_r_fps = float(source_r_fps_text)
        except Exception:
            source_r_fps = source_fps
        work_duration = duration_full
        if self.settings.start_time > 0:
            work_duration = max(0.1, duration_full - self.settings.start_time)
        if self.settings.clip_duration > 0:
            work_duration = min(work_duration, self.settings.clip_duration)

        frame_count = self.get_frame_count(input_path, duration_override=work_duration)
        initial_frame_count = frame_count
        pre_filter = self._build_pre_filter()
        post_filter = self._build_post_filter()
        interpolation_enabled = self.settings.enable_interpolation and self.settings.target_fps > int(round(source_fps))

        self._configure_stage_weights(
            frame_count=frame_count,
            work_duration=work_duration,
            source_fps=source_fps,
            post_enabled=bool(post_filter),
            interpolation_enabled=interpolation_enabled,
        )

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        runtime_root = Path(tempfile.gettempdir()) / "pixelforge_runtime"
        runtime_root.mkdir(parents=True, exist_ok=True)
        work_dir = runtime_root / f"v11b_work_{stamp}_{uuid.uuid4().hex[:8]}"
        frames_in = work_dir / "frames_in"
        frames_out = work_dir / "frames_out"
        frames_final = work_dir / "frames_final"
        work_dir.mkdir(parents=True, exist_ok=True)
        frames_in.mkdir(exist_ok=True)
        frames_out.mkdir(exist_ok=True)

        frame_pattern = f"frame_%08d.{self.settings.image_format}"

        self.log("=" * 78)
        self.log("v11b Upscaling Pipeline Started")
        self.log(f"Input: {input_path}")
        self.log(f"Output: {output_path}")
        self.log(f"Estimated frames: {frame_count:,}")
        self.log(f"Source FPS (avg): {source_fps:.3f}")
        self.log(f"Source FPS (r_frame_rate): {source_r_fps:.3f}")
        if abs(source_r_fps - source_fps) > 0.5:
            self.log("[INFO] Variable frame rate input detected; extraction-based FPS is used to preserve expected duration.")
        self.log(f"Work directory: {work_dir}")
        self.log("=" * 78)
        self._emit_progress(0.0, stage_index=1, stage_fraction=0.0, stage_name="frame extraction")

        # 1) Extract frames
        self.log("[1/6] Extracting frames")
        extract_cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "info"]
        extract_cmd += self._trim_input_args()
        extract_cmd += ["-i", str(input_path)]
        # Keep all decoded frames; prevents dup/drop sync behavior that shortens output videos.
        extract_cmd += ["-vsync", "0"]
        # Prefer explicit passthrough on newer ffmpeg builds for variable frame rate inputs.
        extract_cmd += ["-fps_mode", "passthrough"]
        if pre_filter:
            extract_cmd += ["-vf", pre_filter]
        if self.settings.image_format == "jpg":
            extract_cmd += ["-q:v", "2"]
        else:
            extract_cmd += ["-qscale:v", "1"]
        extract_cmd += [str(frames_in / frame_pattern)]
        self._run_command(extract_cmd, "frame extraction", 1, progress_mode="frames", progress_target=frame_count)

        extracted_count = sum(1 for item in frames_in.iterdir() if item.is_file() and item.suffix.lower() == f".{self.settings.image_format}")
        if extracted_count > 0:
            frame_count = extracted_count
        effective_source_fps = max(1.0, frame_count / max(0.1, work_duration))
        self.log(
            f"Frame extraction result: {frame_count:,} frames (initial estimate {initial_frame_count:,}), "
            f"effective FPS {effective_source_fps:.3f}"
        )

        interpolation_enabled = self.settings.enable_interpolation and self.settings.target_fps > int(round(effective_source_fps))
        self._configure_stage_weights(
            frame_count=frame_count,
            work_duration=work_duration,
            source_fps=effective_source_fps,
            post_enabled=bool(post_filter),
            interpolation_enabled=interpolation_enabled,
        )

        # 2) Upscale
        self.log("[2/6] Real-ESRGAN upscaling")
        upscale_cmd = [
            str(exe_path),
            "-i",
            str(frames_in),
            "-o",
            str(frames_out),
            "-n",
            self.settings.model,
            "-s",
            str(self.settings.scale),
            "-f",
            self.settings.image_format,
            "-j",
            self.settings.threads,
        ]
        self._run_command(upscale_cmd, "Real-ESRGAN upscale", 2, progress_mode="upscale", progress_target=frame_count, progress_path=frames_out)

        # 3) Post filter
        source_frame_dir = frames_out
        if post_filter:
            self.log("[3/6] Post-processing (sharpen and/or final scale)")
            frames_final.mkdir(exist_ok=True)
            post_cmd = [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "info",
                "-i",
                str(frames_out / frame_pattern),
                "-vf",
                post_filter,
            ]
            if self.settings.image_format == "jpg":
                post_cmd += ["-q:v", "2"]
            else:
                post_cmd += ["-qscale:v", "1"]
            post_cmd += [str(frames_final / frame_pattern)]
            self._run_command(post_cmd, "post-processing", 3, progress_mode="frames", progress_target=frame_count)
            source_frame_dir = frames_final
        else:
            self.log("[3/6] Skipping post-processing (disabled)")
            self._set_stage_progress(3, 1.0, stage_name="post-processing (skipped)")

        # 4) Reassemble
        self.log("[4/6] Reassembling video")
        temp_video = work_dir / "temp_reassembled.mp4"
        reassemble_cmd = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "info",
            "-framerate",
            f"{effective_source_fps}",
            "-i",
            str(source_frame_dir / frame_pattern),
            "-c:v",
            "libx264",
            "-crf",
            str(self.settings.crf),
            "-preset",
            self.settings.encode_preset,
            "-pix_fmt",
            "yuv420p",
            str(temp_video),
        ]
        self._run_command(reassemble_cmd, "video reassembly", 4, progress_mode="time", progress_target=work_duration)

        # 5) Optional interpolation
        final_video_input = temp_video
        if interpolation_enabled:
            self.log("[5/6] Frame interpolation")
            interp_video = work_dir / "temp_interpolated.mp4"
            interp_filter = (
                f"minterpolate=fps={self.settings.target_fps}:mi_mode=mci:mc_mode=aobmc:"
                f"me_mode=bidir:vsbmc=1:scd=fdiff:scd_threshold=10"
            )
            interp_cmd = [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "info",
                "-i",
                str(temp_video),
                "-vf",
                interp_filter,
                "-c:v",
                "libx264",
                "-crf",
                str(self.settings.crf),
                "-preset",
                self.settings.encode_preset,
                "-pix_fmt",
                "yuv420p",
                str(interp_video),
            ]
            self._run_command(interp_cmd, "frame interpolation", 5, progress_mode="time", progress_target=work_duration)
            final_video_input = interp_video
        else:
            self.log("[5/6] Skipping interpolation")
            self._set_stage_progress(5, 1.0, stage_name="interpolation (skipped)")

        # 6) Optional audio mux
        self.log("[6/6] Finalizing output")
        if self.settings.include_audio:
            audio_cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "info", "-i", str(final_video_input)]
            audio_cmd += self._trim_input_args()
            audio_cmd += [
                "-i",
                str(input_path),
                "-c:v",
                "copy",
                "-c:a",
                "aac",
                "-b:a",
                "192k",
                "-map",
                "0:v:0",
                "-map",
                "1:a:0?",
                str(output_path),
            ]
            self._run_command(audio_cmd, "audio mux", 6, progress_mode="time", progress_target=work_duration)
        else:
            copy_cmd = [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "info",
                "-i",
                str(final_video_input),
                "-c",
                "copy",
                str(output_path),
            ]
            self._run_command(copy_cmd, "final copy", 6, progress_mode="time", progress_target=work_duration)

        if not self.settings.keep_intermediate:
            self.log("Cleaning intermediate work folder...")
            for child in work_dir.rglob("*"):
                if child.is_file():
                    child.unlink(missing_ok=True)
            for child in sorted(work_dir.rglob("*"), reverse=True):
                if child.is_dir():
                    child.rmdir()
            work_dir.rmdir()

        output_duration = self.get_video_duration(output_path)
        duration_drift = output_duration - work_duration
        duration_drift_pct = (abs(duration_drift) / max(0.001, work_duration)) * 100.0
        try:
            output_fps = self.get_fps(output_path)
        except Exception:
            output_fps = 0.0
        try:
            output_frame_count = self.get_frame_count(output_path)
        except Exception:
            output_frame_count = 0

        output_size_mb = output_path.stat().st_size / (1024 * 1024)
        report_payload = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "input": str(input_path),
            "output": str(output_path),
            "settings": {
                "model": self.settings.model,
                "scale": self.settings.scale,
                "image_format": self.settings.image_format,
                "threads": self.settings.threads,
                "interpolation": bool(self.settings.enable_interpolation),
                "target_fps": int(self.settings.target_fps),
                "encode_preset": self.settings.encode_preset,
                "crf": int(self.settings.crf),
                "include_audio": bool(self.settings.include_audio),
            },
            "timing": {
                "work_duration_seconds": round(float(work_duration), 6),
                "output_duration_seconds": round(float(output_duration), 6),
                "duration_drift_seconds": round(float(duration_drift), 6),
                "duration_drift_percent": round(float(duration_drift_pct), 4),
                "effective_source_fps": round(float(effective_source_fps), 6),
                "output_fps": round(float(output_fps), 6),
            },
            "frames": {
                "initial_estimated": int(initial_frame_count),
                "extracted": int(frame_count),
                "output_count": int(output_frame_count),
            },
            "output_size_mb": round(float(output_size_mb), 4),
            "stage_weights": {str(k): round(float(v), 6) for k, v in self.stage_weight_map.items()},
        }
        report_path = output_path.with_suffix(output_path.suffix + ".v11b_report.json")
        try:
            report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
            self.log(f"Run report: {report_path}")
        except Exception as exc:
            self.log(f"[WARN] Could not write run report: {exc}")

        self.log("=" * 78)
        self.log("Completed successfully")
        self.log(f"Output: {output_path}")
        self.log(f"Output size: {output_size_mb:.2f} MB")
        self.log(
            "Integrity check: "
            f"expected duration {work_duration:.3f}s, output duration {output_duration:.3f}s, "
            f"drift {duration_drift:+.3f}s ({duration_drift_pct:.2f}%)"
        )
        self.log(
            "Frame summary: "
            f"extracted {frame_count:,}, output frames {output_frame_count:,}, "
            f"effective source FPS {effective_source_fps:.3f}, output FPS {output_fps:.3f}"
        )
        if duration_drift_pct > 1.0 and abs(duration_drift) > 0.15:
            self.log("[WARN] Output duration drift is above 1%; inspect source VFR behavior or interpolation settings.")
        self.log("=" * 78)
        self._emit_progress(100.0, stage_index=6, stage_fraction=1.0, stage_name="finalizing output")


class V11BApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("PixelForge AI")
        self._set_initial_window_size()
        self.minsize(1040, 520)

        self.log_queue: Queue[str] = Queue()
        self.stop_event = threading.Event()
        self.worker_thread: threading.Thread | None = None
        self.runner: PipelineRunner | None = None
        self.compare_worker_thread: threading.Thread | None = None

        self.compare_before_pil: Image.Image | None = None
        self.compare_after_pil: Image.Image | None = None
        self.compare_photo: ImageTk.PhotoImage | None = None
        self.compare_photo_large: ImageTk.PhotoImage | None = None
        self.header_logo_photo: tk.PhotoImage | None = None
        self.knightlogics_logo_photo: tk.PhotoImage | None = None
        self.window_icon_photo: tk.PhotoImage | None = None
        self.compare_window: tk.Toplevel | None = None
        self.billing_window: tk.Toplevel | None = None
        self.compare_canvas_large: tk.Canvas | None = None
        self._estimate_after_id: str | None = None
        self._compare_after_id: str | None = None
        self._compare_regen_pending: bool = False
        self._charged_token: str | None = None
        self._charged_credits: int = 0
        self._stop_requested_by_user: bool = False
        self._current_run_output: Path | None = None
        self._progress_stage_total: int = 6
        self._progress_current_stage: int = 0
        self._overall_progress_max: float = 0.0
        self._active_stage_pred_seconds: dict[int, float] = {}
        self._auto_threads_value: str = "2:2:2"
        self._system_profile_cache: dict[str, object] | None = None
        self._system_detection_started: bool = False
        self._update_check_after_id: str | None = None
        self.stage_timing_profile_file = Path(__file__).with_name("v11b_stage_timing_profile.json")
        self.stage_timing_profile = self._load_stage_timing_profile()

        self.speed_profile_buttons: dict[str, ttk.Button] = {}
        self.upscaling_profile_buttons: dict[str, ttk.Button] = {}
        self.selected_speed_profile: str = "balanced"
        self.selected_upscaling_profile: str = "animation"

        self.billing_store = BillingStore(
            Path(__file__).with_name("v11b_billing_tokens.json"),
            Path(__file__).with_name("v11b_billing_audit.jsonl"),
        )
        self.billing_backend = EmbeddedBillingBackend(self.billing_store)
        self.free_trial_credits = max(0, int(os.environ.get("V11B_FREE_TRIAL_CREDITS", "10")))

        self.smtp_host = (os.environ.get("V11B_SMTP_HOST") or os.environ.get("SMTP_HOST") or "").strip()
        self.smtp_port = int(os.environ.get("V11B_SMTP_PORT", os.environ.get("SMTP_PORT", "587")))
        self.smtp_user = (os.environ.get("V11B_SMTP_USER") or os.environ.get("SMTP_USER") or "").strip()
        self.smtp_pass = (os.environ.get("V11B_SMTP_PASS") or os.environ.get("SMTP_PASS") or "").strip()
        self.smtp_from = (os.environ.get("V11B_SMTP_FROM") or os.environ.get("SMTP_FROM") or "").strip()
        self.smtp_configured = bool(self.smtp_host and self.smtp_user and self.smtp_pass and self.smtp_from)

        self._build_variables()
        self._configure_theme()
        self._configure_window_icons()
        self._build_ui()
        self._fit_window_to_content()
        self._start_system_detection()
        self.after(120, self._poll_log_queue)
        self.after(1200, self._start_update_checks)

    def _set_initial_window_size(self) -> None:
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        width = min(1500, max(1120, int(screen_w * 0.90)))
        height = min(760, max(560, int(screen_h * 0.70)))
        x = max(0, (screen_w - width) // 2)
        y = max(0, (screen_h - height) // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")

    def _fit_window_to_content(self) -> None:
        self.update_idletasks()
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        req_w = self.winfo_reqwidth() + 24
        req_h = self.winfo_reqheight() + 24
        footer_visible = hasattr(self, "footer_frame") and bool(self.footer_frame.winfo_manager())
        min_w = 1280 if footer_visible else 1040
        min_h = 520
        width = min(max(req_w, min_w), int(screen_w * 0.96))
        height = min(max(req_h, min_h), int(screen_h * 0.82))
        # Preserve user's current monitor/location exactly; do not clamp to primary display.
        x = self.winfo_x()
        y = self.winfo_y()
        self.geometry(f"{width}x{height}+{x}+{y}")

    def _start_update_checks(self) -> None:
        """Run update check at startup, then periodically while the app is open."""
        if not UPDATER_AVAILABLE:
            self.log_queue.put("[WARN] Updater module unavailable; auto-update checks are disabled.")
            return
        try:
            updater.check_for_updates(self, APP_VERSION)
            self.log_queue.put("[INFO] Auto-update check started.")
        except Exception as exc:
            self.log_queue.put(f"[WARN] Auto-update check failed to start: {exc}")

        # Re-check every 45 minutes while app remains open.
        self._update_check_after_id = self.after(45 * 60 * 1000, self._start_update_checks)

    def _build_variables(self) -> None:
        self.input_video_var = tk.StringVar(value="")
        self.output_video_var = tk.StringVar(value="")

        self.model_var = tk.StringVar(value="realesrgan-x4plus-anime")
        self.model_display_var = tk.StringVar(value=MODEL_KEY_TO_LABEL[self.model_var.get()])
        self.scale_var = tk.IntVar(value=4)
        self.image_format_var = tk.StringVar(value="png")
        self._auto_threads_value = self._recommend_realesrgan_threads()
        self.threads_var = tk.StringVar(value=self._auto_threads_value)

        self.start_time_var = tk.DoubleVar(value=0.0)
        self.clip_duration_var = tk.DoubleVar(value=0.0)

        self.denoise_var = tk.DoubleVar(value=0.0)
        self.enable_color_var = tk.BooleanVar(value=True)
        self.vibrance_var = tk.DoubleVar(value=0.35)
        self.contrast_var = tk.DoubleVar(value=1.10)
        self.brightness_var = tk.DoubleVar(value=0.04)
        self.saturation_var = tk.DoubleVar(value=1.25)
        self.gamma_var = tk.DoubleVar(value=1.06)

        self.enable_sharpen_var = tk.BooleanVar(value=True)
        self.cas_strength_var = tk.DoubleVar(value=0.80)
        self.unsharp1_var = tk.DoubleVar(value=1.5)
        self.unsharp2_var = tk.DoubleVar(value=0.8)

        self.enable_interpolation_var = tk.BooleanVar(value=False)
        self.target_fps_var = tk.IntVar(value=30)

        self.apply_final_scale_var = tk.BooleanVar(value=True)
        self.target_width_var = tk.IntVar(value=2430)
        self.target_height_var = tk.IntVar(value=4320)

        self.crf_var = tk.IntVar(value=16)
        self.encode_preset_var = tk.StringVar(value="slow")
        self.include_audio_var = tk.BooleanVar(value=True)
        self.keep_intermediate_var = tk.BooleanVar(value=False)

        self.estimate_var = tk.StringVar(value="Select input to calculate expected processing time.")
        self.estimate_summary_var = tk.StringVar(value="")
        self.estimate_source_var = tk.StringVar(value="")
        self.estimate_spec_var = tk.StringVar(value="")
        self.estimate_stage_var = tk.StringVar(value="")
        self.estimate_tips_var = tk.StringVar(value="")
        self.compare_slider_var = tk.DoubleVar(value=50.0)
        self.compare_dragging = False
        self.compare_hover_near_line = False
        self.compare_separator_x = 0

        default_api = os.environ.get("V11B_BILLING_API_BASE", "embedded://local")
        self.billing_api_base_var = tk.StringVar(value=default_api)
        self.billing_token_var = tk.StringVar(value=os.environ.get("V11B_BILLING_TOKEN", ""))
        self.checkout_credits_var = tk.IntVar(value=25)
        self.checkout_session_var = tk.StringVar(value="")
        self.checkout_url_var = tk.StringVar(value="")
        self.checkout_amount_cents_override: int | None = None
        self.checkout_package_name_override: str = ""
        self.billing_status_var = tk.StringVar(value="Billing: Ready. Select a package and click Start Checkout.")
        self.available_credits_var = tk.StringVar(value="0")
        self.credit_quote_var = tk.StringVar(value="Render cost: select an input video to estimate credits.")
        self.start_button_credit_var = tk.StringVar(value="(0 credits)")
        self.total_progress_var = tk.DoubleVar(value=0.0)
        self.total_progress_label_var = tk.StringVar(value="0%")
        self.progress_stage_label_var = tk.StringVar(value="Stage 0/6")
        self.progress_overall_label_var = tk.StringVar(value="Overall 0%")
        self.recovery_email_var = tk.StringVar(value="")
        self.access_code_input_var = tk.StringVar(value="")
        self.credit_code_var = tk.StringVar(value="")
        self.admin_code_var = tk.StringVar(value="TEST10")
        self.admin_code_credits_var = tk.IntVar(value=10)
        self.advanced_visible_var = tk.BooleanVar(value=False)
        self.advanced_window: tk.Toplevel | None = None
        self.advanced_overrides_active = False
        self._advanced_window_snapshot: dict[str, object] | None = None
        self._advanced_window_previous_override_state = False

        self.billing_state_file = Path(__file__).with_name("v11b_billing_state.json")
        self._load_billing_state()
        if not self.billing_token_var.get().strip():
            self.billing_token_var.set(self._generate_billing_token())
        self._ensure_free_trial_for_token(self.billing_token_var.get().strip())
        self._refresh_billing_status(silent=True)

    def _configure_theme(self) -> None:
        self.configure(bg="#0a1220")
        style = ttk.Style(self)
        style.theme_use("clam")

        field_bg = "#cfd9e8"
        field_fg = "#000000"

        style.configure("Root.TFrame", background="#0a1220")
        style.configure("Panel.TFrame", background="#111b2f")
        style.configure("TFrame", background="#111b2f")
        style.configure("Card.TLabelframe", background="#111b2f", foreground="#6de1ff", bordercolor="#2a4b78")
        style.configure("Card.TLabelframe.Label", background="#111b2f", foreground="#6de1ff")
        style.configure("TLabel", background="#111b2f", foreground="#d8e6ff")
        style.configure("Hint.TLabel", background="#111b2f", foreground="#9bb3d7")
        # Global button system: metallic green family for consistent visual language.
        style.configure(
            "TButton",
            background="#0a6f50",
            foreground="#e8fff4",
            bordercolor="#4ec9a3",
            lightcolor="#38b58d",
            darkcolor="#074c37",
            relief="raised",
        )
        style.map(
            "TButton",
            background=[("active", "#138765"), ("pressed", "#085d44")],
            foreground=[("active", "#f4fff9"), ("pressed", "#ddfff0")],
        )
        style.configure(
            "Accent.TButton",
            background="#23efaa",
            foreground="#002a1d",
            bordercolor="#b8ffdf",
            lightcolor="#77ffd0",
            darkcolor="#17ab7a",
            relief="raised",
        )
        style.map(
            "Accent.TButton",
            background=[("active", "#45ffbf"), ("pressed", "#18cf96")],
            foreground=[("active", "#001f15"), ("pressed", "#003122")],
        )
        style.configure(
            "Danger.TButton",
            background="#c43a3a",
            foreground="#fff1f1",
            bordercolor="#ff9e9e",
            lightcolor="#e35b5b",
            darkcolor="#8f2020",
            relief="raised",
        )
        style.map(
            "Danger.TButton",
            background=[("active", "#dd4b4b"), ("pressed", "#a92d2d")],
            foreground=[("active", "#ffffff"), ("pressed", "#fff4f4")],
        )

        # Speed profile buttons use one consistent metallic-green family.
        style.configure(
            "Profile.TButton",
            background="#0b7b58",
            foreground="#dfffee",
            bordercolor="#67d8b4",
            lightcolor="#44c79a",
            darkcolor="#085b41",
            relief="raised",
        )
        style.map(
            "Profile.TButton",
            background=[("active", "#149369"), ("pressed", "#08694b")],
            foreground=[("active", "#f0fff7"), ("pressed", "#d7ffed")],
        )
        style.configure(
            "ProfileSelected.TButton",
            background="#23efaa",
            foreground="#002d1f",
            bordercolor="#b8ffdf",
            lightcolor="#7dffd0",
            darkcolor="#16aa7a",
            relief="sunken",
        )
        style.map(
            "ProfileSelected.TButton",
            background=[("active", "#47ffc2"), ("pressed", "#1ad198")],
            foreground=[("active", "#001f15"), ("pressed", "#003021")],
        )
        style.configure("TNotebook", background="#0f1829", bordercolor="#284568")
        style.configure("TNotebook.Tab", background="#1a2740", foreground="#cde2ff", padding=(12, 6))
        style.map("TNotebook.Tab", background=[("selected", "#2a4f7f")])
        style.configure(
            "Total.Horizontal.TProgressbar",
            troughcolor="#0d1626",
            background="#23efaa",
            bordercolor="#355c93",
            lightcolor="#77ffd0",
            darkcolor="#17ab7a",
        )

        style.configure("TEntry", fieldbackground=field_bg, foreground=field_fg)
        style.configure("TSpinbox", fieldbackground=field_bg, foreground=field_fg, arrowsize=12)
        style.configure("TCombobox", fieldbackground=field_bg, foreground=field_fg, background=field_bg, arrowcolor="#0d1b30")
        style.map(
            "TCombobox",
            fieldbackground=[("readonly", field_bg), ("!disabled", field_bg)],
            foreground=[("readonly", field_fg), ("!disabled", field_fg)],
        )
        style.configure("TCheckbutton", background="#111b2f", foreground="#d8e6ff")
        style.configure("BillingHero.TFrame", background="#13243d")
        style.configure("BillingActions.TFrame", background="#0f1829")
        style.configure("BillingStatus.TFrame", background="#0d1626")
        style.configure("BillingHeroTitle.TLabel", background="#13243d", foreground="#f5fbff", font=("Segoe UI", 18, "bold"))
        style.configure("BillingHeroSub.TLabel", background="#13243d", foreground="#b7ccea", font=("Segoe UI", 10))
        style.configure("BillingSectionTitle.TLabel", background="#0a1220", foreground="#ecf6ff", font=("Segoe UI", 12, "bold"))
        style.configure("BillingStatus.TLabel", background="#0d1626", foreground="#d8e6ff")
        style.configure("BillingMuted.TLabel", background="#0f1829", foreground="#96abc9")

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=10, style="Root.TFrame")
        top.pack(fill=BOTH, expand=True)

        self._build_app_header(top)

        content = ttk.Frame(top, style="Root.TFrame")
        content.pack(fill=BOTH, expand=True, pady=(10, 0))

        left_container = ttk.Frame(content, style="Panel.TFrame")
        left_container.pack(side=LEFT, fill=BOTH, expand=True)

        left_canvas = tk.Canvas(
            left_container,
            bg="#111b2f",
            highlightthickness=0,
            borderwidth=0,
        )
        left_canvas.pack(side=LEFT, fill=BOTH, expand=True)

        left_panel = ttk.Frame(left_canvas, style="Panel.TFrame")
        left_window = left_canvas.create_window((0, 0), window=left_panel, anchor="nw")

        def _resize_scroll_region(_event=None) -> None:
            left_canvas.configure(scrollregion=left_canvas.bbox("all"))

        def _resize_left_panel_width(event) -> None:
            left_canvas.itemconfigure(left_window, width=event.width)

        left_panel.bind("<Configure>", _resize_scroll_region)
        left_canvas.bind("<Configure>", _resize_left_panel_width)

        def _on_mousewheel(event) -> None:
            delta = -1 * int(event.delta / 120) if event.delta else 0
            left_canvas.yview_scroll(delta, "units")

        left_canvas.bind("<MouseWheel>", _on_mousewheel)
        left_panel.bind("<MouseWheel>", _on_mousewheel)

        right_panel = ttk.Frame(content, style="Panel.TFrame")
        right_panel.pack(side=RIGHT, fill=BOTH, padx=(10, 0))

        self._build_input_section(left_panel)
        self._build_profile_section(left_panel)
        self._build_settings_notebook(left_panel)
        self._build_action_section(left_panel)

        self._build_compare_panel(right_panel)
        self._build_log_panel(right_panel)

        self._build_footer(top)

    def _configure_window_icons(self) -> None:
        icon_base = Path(__file__).with_name("assets") / "icons"
        icon_ico_candidates = [
            icon_base / "pixelforge_app.ico",
        ]
        for ico_path in icon_ico_candidates:
            if not ico_path.exists():
                continue
            try:
                self.iconbitmap(default=str(ico_path))
                break
            except Exception:
                continue

        icon_photo_candidates = [
            icon_base / "pixelforge_icon_32.png",
            icon_base / "pixelforge_icon_48.png",
            icon_base / "pixelforge_icon_64.png",
            Path(__file__).with_name("pixelforge_logo.png"),
        ]
        self.window_icon_photo = self._load_logo_photo(icon_photo_candidates, max_width=64, max_height=64)
        if self.window_icon_photo is not None:
            try:
                self.iconphoto(True, self.window_icon_photo)
            except Exception:
                pass

    def _load_logo_photo(self, candidates: list[Path], max_width: int, max_height: int):
        for logo_path in candidates:
            if not logo_path.exists():
                continue
            try:
                if PIL_AVAILABLE:
                    image = Image.open(logo_path).convert("RGBA")
                    alpha_bbox = image.getchannel("A").getbbox()
                    if alpha_bbox:
                        image = image.crop(alpha_bbox)
                    image.thumbnail((max_width, max_height), RESAMPLE_FILTER)
                    return ImageTk.PhotoImage(image)

                logo_img = tk.PhotoImage(file=str(logo_path))
                width_factor = max(1, math.ceil(logo_img.width() / max_width))
                height_factor = max(1, math.ceil(logo_img.height() / max_height))
                factor = max(width_factor, height_factor)
                if factor > 1:
                    logo_img = logo_img.subsample(factor, factor)
                return logo_img
            except Exception:
                continue
        return None

    def _build_app_header(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent, style="Panel.TFrame")
        header.pack(fill=X)
        header.columnconfigure(1, weight=1)

        logo_label = tk.Label(header, bg="#111b2f")
        logo_label.grid(row=0, column=0, rowspan=2, sticky="nw", padx=(8, 10), pady=(0, 0))

        logo_candidates = [
            Path(__file__).with_name("assets") / "icons" / "pixelforge_logo_header_cropped.png",
            Path(__file__).with_name("assets") / "pixelforge_logo.png",
            Path(__file__).with_name("pixelforge_logo.png"),
            Path(r"E:\ZIP BACKUPS\.github\projects\ChatGPT Image Mar 29, 2026, 10_49_58 AM.png"),
        ]
        self.header_logo_photo = self._load_logo_photo(logo_candidates, max_width=250, max_height=78)
        if self.header_logo_photo is not None:
            logo_label.configure(image=self.header_logo_photo)

        tk.Label(
            header,
            text="PixelForge AI Video Enhancer",
            fg="#dff6ff",
            bg="#111b2f",
            font=("Segoe UI", 16, "bold"),
            anchor="w",
        ).grid(row=0, column=1, sticky="nw", pady=(0, 0))

        tk.Label(
            header,
            text="Professional AI upscaling, restoration, and finishing workflow",
            fg="#8fb0d7",
            bg="#111b2f",
            font=("Segoe UI", 9),
            anchor="w",
        ).grid(row=1, column=1, sticky="nw", pady=(1, 0))

        built_by = ttk.Frame(header, style="Panel.TFrame")
        built_by.grid(row=0, column=2, rowspan=2, sticky="ne", padx=(8, 10), pady=(0, 0))

        built_row = ttk.Frame(built_by, style="Panel.TFrame")
        built_row.pack(anchor="e")

        tk.Label(
            built_row,
            text="Built By",
            fg="#9ab6d9",
            bg="#111b2f",
            font=("Segoe UI", 8, "bold"),
            anchor="e",
        ).pack(side=LEFT, padx=(0, 6))

        kl_logo_label = tk.Label(built_row, bg="#111b2f")
        kl_logo_label.pack(side=LEFT, padx=(0, 6))

        knight_logo_candidates = [
            Path(__file__).with_name("knightlogics-logo.png"),
            Path(r"E:\YouTube Backups\AutoTop5_Showcase_App\static\knightlogics-logo.png"),
        ]
        self.knightlogics_logo_photo = self._load_logo_photo(knight_logo_candidates, max_width=22, max_height=20)
        if self.knightlogics_logo_photo is not None:
            kl_logo_label.configure(image=self.knightlogics_logo_photo)

        knight_link = tk.Label(
            built_row,
            text="Knight Logics | KnightLogics.com",
            fg="#d3e6ff",
            bg="#111b2f",
            font=("Segoe UI", 9),
            anchor="e",
            cursor="hand2",
        )
        knight_link.pack(side=LEFT)
        knight_link.bind("<Button-1>", lambda _event: webbrowser.open("https://KnightLogics.com"))

        tk.Label(
            built_by,
            text=f"Release: v{APP_VERSION}",
            fg="#9ab6d9",
            bg="#111b2f",
            font=("Segoe UI", 8),
            anchor="e",
        ).pack(anchor="e", pady=(1, 0))

        ttk.Separator(parent).pack(fill=X, pady=(6, 0))

    def _build_input_section(self, parent: ttk.Frame) -> None:
        box = ttk.LabelFrame(parent, text="Video Input / Output", padding=10, style="Card.TLabelframe")
        box.pack(fill=X)

        ttk.Label(
            box,
            text=(
                "Choose the source video and output destination for your PixelForge enhancement run."
            ),
            wraplength=780,
            justify=LEFT,
            style="Hint.TLabel",
        ).pack(anchor=W, pady=(0, 8))

        row1 = ttk.Frame(box)
        row1.pack(fill=X, pady=4)
        ttk.Label(row1, text="Input video", width=16).pack(side=LEFT)
        ttk.Entry(row1, textvariable=self.input_video_var).pack(side=LEFT, fill=X, expand=True)
        ttk.Button(row1, text="Browse", command=self._pick_input).pack(side=LEFT, padx=(6, 0))

        row2 = ttk.Frame(box)
        row2.pack(fill=X, pady=4)
        ttk.Label(row2, text="Output video", width=16).pack(side=LEFT)
        ttk.Entry(row2, textvariable=self.output_video_var).pack(side=LEFT, fill=X, expand=True)
        ttk.Button(row2, text="Browse", command=self._pick_output).pack(side=LEFT, padx=(6, 0))

    def _build_profile_section(self, parent: ttk.Frame) -> None:
        speed_box = ttk.LabelFrame(parent, text="Speed Profiles", padding=10, style="Card.TLabelframe")
        speed_box.pack(fill=X, pady=(10, 0))
        speed_row = ttk.Frame(speed_box, style="Panel.TFrame")
        speed_row.pack(fill=X)

        fast_btn = ttk.Button(speed_row, text="Quick Preview", command=self._apply_fast_profile, style="Profile.TButton")
        balanced_btn = ttk.Button(speed_row, text="Balanced Workflow", command=self._apply_balanced_profile, style="Profile.TButton")
        quality_btn = ttk.Button(speed_row, text="Max Detail", command=self._apply_quality_profile, style="Profile.TButton")

        fast_btn.pack(side=LEFT)
        balanced_btn.pack(side=LEFT, padx=6)
        quality_btn.pack(side=LEFT)

        content_box = ttk.LabelFrame(parent, text="Upscaling Profile", padding=10, style="Card.TLabelframe")
        content_box.pack(fill=X, pady=(8, 0))
        content_row = ttk.Frame(content_box, style="Panel.TFrame")
        content_row.pack(fill=X)

        live_btn = ttk.Button(content_row, text="Natural Footage", command=self._apply_live_profile, style="Profile.TButton")
        anime_btn = ttk.Button(content_row, text="Animation / Anime", command=self._apply_anime_profile, style="Profile.TButton")
        restore_btn = ttk.Button(content_row, text="Legacy / Noisy Repair", command=self._apply_restore_profile, style="Profile.TButton")

        live_btn.pack(side=LEFT)
        anime_btn.pack(side=LEFT, padx=6)
        restore_btn.pack(side=LEFT)

        self.speed_profile_buttons = {
            "fast": fast_btn,
            "balanced": balanced_btn,
            "quality": quality_btn,
        }
        self.upscaling_profile_buttons = {
            "live": live_btn,
            "animation": anime_btn,
            "restore": restore_btn,
        }
        self._set_selected_speed_profile("balanced", apply=False)
        self._set_selected_upscaling_profile("animation", apply=False)
        self._apply_combined_profile()

    def _build_settings_notebook(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent, style="Panel.TFrame")
        header.pack(fill=X, pady=(10, 0))
        self.advanced_link_label = tk.Label(
            header,
            text="Advanced Options",
            fg="#6de1ff",
            bg="#111b2f",
            cursor="hand2",
            font=("Segoe UI", 10, "underline"),
        )
        self.advanced_link_label.pack(anchor=W)
        self.advanced_link_label.bind("<Button-1>", lambda _event: self._open_advanced_options_window())

    def _advanced_var_names(self) -> list[str]:
        return [
            "model_var",
            "scale_var",
            "image_format_var",
            "threads_var",
            "start_time_var",
            "clip_duration_var",
            "denoise_var",
            "enable_color_var",
            "vibrance_var",
            "contrast_var",
            "brightness_var",
            "saturation_var",
            "gamma_var",
            "enable_sharpen_var",
            "cas_strength_var",
            "unsharp1_var",
            "unsharp2_var",
            "enable_interpolation_var",
            "target_fps_var",
            "apply_final_scale_var",
            "target_width_var",
            "target_height_var",
            "crf_var",
            "encode_preset_var",
            "include_audio_var",
            "keep_intermediate_var",
        ]

    def _capture_advanced_settings_snapshot(self) -> dict[str, object]:
        return {name: getattr(self, name).get() for name in self._advanced_var_names()}

    def _restore_advanced_settings_snapshot(self, snapshot: dict[str, object]) -> None:
        for name, value in snapshot.items():
            getattr(self, name).set(value)
        self._sync_display_from_model()
        self._schedule_auto_estimate()

    def _build_advanced_notebook(self, parent: ttk.Frame) -> ttk.Notebook:
        notebook = ttk.Notebook(parent)
        tab_upscale = ttk.Frame(notebook, padding=10)
        tab_image = ttk.Frame(notebook, padding=10)
        tab_motion = ttk.Frame(notebook, padding=10)
        tab_output = ttk.Frame(notebook, padding=10)

        notebook.add(tab_upscale, text="Upscale")
        notebook.add(tab_image, text="Image")
        notebook.add(tab_motion, text="Motion")
        notebook.add(tab_output, text="Output")

        self._populate_upscale_tab(tab_upscale)
        self._populate_image_tab(tab_image)
        self._populate_motion_tab(tab_motion)
        self._populate_output_tab(tab_output)
        return notebook

    def _open_advanced_options_window(self) -> None:
        if self.advanced_window and self.advanced_window.winfo_exists():
            self.advanced_window.deiconify()
            self.advanced_window.lift()
            self.advanced_window.focus_force()
            return

        self._advanced_window_snapshot = self._capture_advanced_settings_snapshot()
        self._advanced_window_previous_override_state = bool(self.advanced_overrides_active)

        window = tk.Toplevel(self)
        window.title("Advanced Options")
        window.minsize(860, 620)
        window.configure(bg="#0a1220")
        window.transient(self)
        self.advanced_window = window
        self.advanced_visible_var.set(True)

        self.update_idletasks()
        popup_width = 960
        popup_height = 720
        x = self.winfo_rootx() + max(0, (self.winfo_width() - popup_width) // 2)
        y = max(0, self.winfo_rooty() - 20)
        window.geometry(f"{popup_width}x{popup_height}+{x}+{y}")
        window.protocol("WM_DELETE_WINDOW", lambda: self._close_advanced_options_window(apply_changes=False))

        frame = ttk.Frame(window, padding=10, style="Root.TFrame")
        frame.pack(fill=BOTH, expand=True)

        ttk.Label(
            frame,
            text="Adjust advanced processing controls here. Apply/Save commits these values on top of your selected profiles.",
            style="Hint.TLabel",
            wraplength=900,
            justify=LEFT,
        ).pack(anchor=W, pady=(0, 8))

        notebook = self._build_advanced_notebook(frame)
        notebook.pack(fill=BOTH, expand=True)

        button_row = ttk.Frame(frame, style="Panel.TFrame")
        button_row.pack(fill=X, pady=(10, 0))
        ttk.Button(button_row, text="Close", command=lambda: self._close_advanced_options_window(apply_changes=False)).pack(side=RIGHT)
        ttk.Button(
            button_row,
            text="Apply / Save",
            command=lambda: self._close_advanced_options_window(apply_changes=True),
            style="Accent.TButton",
        ).pack(side=RIGHT, padx=(0, 8))

        window.lift()
        window.focus_force()

    def _close_advanced_options_window(self, apply_changes: bool) -> None:
        if apply_changes:
            self.advanced_overrides_active = True
            self._sync_display_from_model()
            self._schedule_auto_estimate()
        elif self._advanced_window_snapshot is not None:
            self._restore_advanced_settings_snapshot(self._advanced_window_snapshot)
            self.advanced_overrides_active = self._advanced_window_previous_override_state

        self._advanced_window_snapshot = None
        self.advanced_visible_var.set(False)
        if self.advanced_window and self.advanced_window.winfo_exists():
            self.advanced_window.destroy()
        self.advanced_window = None

    def _populate_guide_tab(self, tab: ttk.Frame) -> None:
        ttk.Label(tab, text="Quick Start (Most Users)", style="Card.TLabelframe.Label").pack(anchor=W, pady=(0, 8))
        ttk.Label(
            tab,
            text=(
                "1) Pick Input video and Output video path.\n"
                "2) Click Fast Draft for a quick test render first.\n"
                "3) Compare frame and estimate are generated automatically.\n"
                "4) Drag the separator line in the compare view to check quality.\n"
                "5) When satisfied, switch to Balanced or Quality and click Start Processing."
            ),
            justify=LEFT,
            style="Hint.TLabel",
            wraplength=760,
        ).pack(anchor=W, pady=(0, 12))

        ttk.Label(tab, text="What Each Tab Controls", style="Card.TLabelframe.Label").pack(anchor=W, pady=(0, 8))
        ttk.Label(
            tab,
            text=(
                "Upscale: AI model, upscale amount, and clip range for test runs.\n"
                "Image: Denoise, color, and sharpening.\n"
                "Motion: Frame interpolation and output FPS (major time cost).\n"
                "Output: Final resolution, compression quality (CRF), encoding speed, and audio."
            ),
            justify=LEFT,
            style="Hint.TLabel",
            wraplength=760,
        ).pack(anchor=W, pady=(0, 12))

        ttk.Label(tab, text="Biggest Time Savers", style="Card.TLabelframe.Label").pack(anchor=W, pady=(0, 8))
        ttk.Label(
            tab,
            text=(
                "- Disable interpolation (or keep FPS at 24/30).\n"
                "- Use Scale 2 or 3 while testing.\n"
                "- Use animevideov3 models for drafts.\n"
                "- Use veryfast/faster encode preset for test exports.\n"
                "- Set Clip duration to 10-30 seconds for trial runs."
            ),
            justify=LEFT,
            style="Hint.TLabel",
            wraplength=760,
        ).pack(anchor=W)

    def _populate_upscale_tab(self, tab: ttk.Frame) -> None:
        ttk.Label(
            tab,
            text="Use Fast Draft first, then increase quality after you confirm results.",
            style="Hint.TLabel",
            wraplength=740,
        ).pack(anchor=W, pady=(0, 8))

        model_row = ttk.Frame(tab)
        model_row.pack(fill=X, pady=2)
        model_row.columnconfigure(0, weight=1)
        model_row.columnconfigure(1, weight=1)
        ttk.Label(model_row, text="AI Upscale Model (content type guidance)", anchor="w").grid(row=0, column=0, sticky="ew", padx=(0, 5))
        model_combo = ttk.Combobox(
            model_row,
            textvariable=self.model_display_var,
            values=[label for _key, label in MODEL_DETAILS],
            state="readonly",
        )
        model_combo.grid(row=0, column=1, sticky="ew")
        model_combo.bind("<<ComboboxSelected>>", lambda _event: self._sync_model_from_display())

        self._labeled_spin(tab, "Upscale Multiplier (2-4, higher = slower/more detail)", self.scale_var, 1, 4)
        self._labeled_combo(tab, "Working Frame Format (png=cleaner, jpg=faster/smaller)", self.image_format_var, IMAGE_FORMATS)
        self._labeled_entry(tab, "Advanced GPU Threads (-j load:proc:save)", self.threads_var)

        ttk.Separator(tab).pack(fill=X, pady=8)
        self._labeled_spin_float(tab, "Start At (seconds, trims beginning)", self.start_time_var, 0.0, 999999.0, 0.1)
        self._labeled_spin_float(tab, "Process Length (0 = full video; this field shortens output)", self.clip_duration_var, 0.0, 999999.0, 0.1)

    def _populate_image_tab(self, tab: ttk.Frame) -> None:
        ttk.Label(
            tab,
            text="Image cleanup and sharpening. Small changes usually work best.",
            style="Hint.TLabel",
            wraplength=740,
        ).pack(anchor=W, pady=(0, 8))

        self._labeled_spin_float(tab, "Denoise Strength (removes compression/noise; higher can soften)", self.denoise_var, 0.0, 4.0, 0.1)
        ttk.Checkbutton(tab, text="Enable color enhancement (applies vibrance/contrast/saturation/gamma)", variable=self.enable_color_var).pack(anchor=W, pady=(4, 6))

        self._labeled_spin_float(tab, "Vibrance intensity (boost muted colors)", self.vibrance_var, 0.0, 1.5, 0.05)
        self._labeled_spin_float(tab, "Contrast (difference between dark/light)", self.contrast_var, 0.5, 2.0, 0.05)
        self._labeled_spin_float(tab, "Brightness (overall lightness)", self.brightness_var, -0.3, 0.3, 0.01)
        self._labeled_spin_float(tab, "Saturation (overall color strength)", self.saturation_var, 0.5, 2.5, 0.05)
        self._labeled_spin_float(tab, "Gamma (mid-tone emphasis)", self.gamma_var, 0.5, 2.0, 0.05)

        ttk.Separator(tab).pack(fill=X, pady=8)
        ttk.Checkbutton(tab, text="Enable sharpening (adds edge detail)", variable=self.enable_sharpen_var).pack(anchor=W, pady=(4, 6))
        self._labeled_spin_float(tab, "CAS strength (edge-aware sharpening)", self.cas_strength_var, 0.0, 1.5, 0.05)
        self._labeled_spin_float(tab, "Unsharp Pass 1 (primary sharpness)", self.unsharp1_var, 0.0, 3.0, 0.05)
        self._labeled_spin_float(tab, "Unsharp Pass 2 (fine detail boost)", self.unsharp2_var, 0.0, 3.0, 0.05)

    def _populate_motion_tab(self, tab: ttk.Frame) -> None:
        ttk.Label(
            tab,
            text="Motion smoothing is one of the largest processing-time costs.",
            style="Hint.TLabel",
            wraplength=740,
        ).pack(anchor=W, pady=(0, 8))

        ttk.Checkbutton(tab, text="Enable frame interpolation (creates new in-between frames)", variable=self.enable_interpolation_var).pack(anchor=W, pady=(4, 8))
        self._labeled_combo(tab, "Final FPS (higher is smoother but slower)", self.target_fps_var, FPS_OPTIONS)

        tips = ttk.Label(
            tab,
            text=(
                "Major speed reducers:\n"
                "- Interpolation is mainly useful for 24/30 -> 60 FPS\n"
                "- If source FPS is already at/above target FPS, interpolation is auto-skipped\n"
                "- Use Fast Draft profile when testing"
            ),
            justify=LEFT,
        )
        tips.pack(anchor=W, pady=(12, 0))

    def _populate_output_tab(self, tab: ttk.Frame) -> None:
        ttk.Label(
            tab,
            text="Output size and encode settings. Lower quality during tests to save time.",
            style="Hint.TLabel",
            wraplength=740,
        ).pack(anchor=W, pady=(0, 8))

        ttk.Checkbutton(tab, text="Apply final scaling (resize to exact output dimensions)", variable=self.apply_final_scale_var).pack(anchor=W, pady=(4, 8))
        self._labeled_spin(tab, "Output Width (final video width)", self.target_width_var, 360, 7680)
        self._labeled_spin(tab, "Output Height (final video height)", self.target_height_var, 360, 7680)

        ttk.Separator(tab).pack(fill=X, pady=8)
        self._labeled_spin(tab, "Quality (CRF: lower = cleaner)", self.crf_var, 12, 30)
        self._labeled_combo(tab, "Encode Speed Preset (faster encode vs compression efficiency)", self.encode_preset_var, ENCODE_PRESETS)

        ttk.Checkbutton(tab, text="Include source audio (copy original audio track)", variable=self.include_audio_var).pack(anchor=W, pady=(8, 4))
        ttk.Checkbutton(tab, text="Keep intermediate files (debug/troubleshoot pipeline outputs)", variable=self.keep_intermediate_var).pack(anchor=W)

    def _populate_billing_tab(self, tab: ttk.Frame) -> None:
        ttk.Label(
            tab,
            text=(
                "Billing now runs inside v11b with a local credit ledger and optional Stripe checkout. "
                "Checkout opens in an app window when pywebview is available."
            ),
            style="Hint.TLabel",
            wraplength=760,
            justify=LEFT,
        ).pack(anchor=W, pady=(0, 8))

        self._labeled_entry(tab, "Billing API Base URL", self.billing_api_base_var)
        self._labeled_entry(tab, "Access Token", self.billing_token_var)

        ttk.Separator(tab).pack(fill=X, pady=8)
        ttk.Label(tab, text="Recommended Intro Packages", style="Card.TLabelframe.Label").pack(anchor=W, pady=(0, 6))

        package_row = ttk.Frame(tab)
        package_row.pack(fill=X, pady=(0, 6))
        ttk.Button(package_row, text="Starter 25", command=lambda: self.checkout_credits_var.set(25)).pack(side=LEFT)
        ttk.Button(package_row, text="Creator 75", command=lambda: self.checkout_credits_var.set(75)).pack(side=LEFT, padx=6)
        ttk.Button(package_row, text="Pro 200", command=lambda: self.checkout_credits_var.set(200)).pack(side=LEFT)

        self._labeled_spin(tab, "Credits To Purchase", self.checkout_credits_var, 1, 1000)
        self._labeled_entry(tab, "Checkout Session ID", self.checkout_session_var)
        self._labeled_entry(tab, "Last Checkout URL", self.checkout_url_var)

        ttk.Label(tab, textvariable=self.credit_quote_var, style="Hint.TLabel", wraplength=760, justify=LEFT).pack(anchor=W, pady=(8, 0))

        action_row = ttk.Frame(tab)
        action_row.pack(fill=X, pady=(8, 0))
        ttk.Button(action_row, text="Start Checkout", command=self._start_checkout, style="Accent.TButton").pack(side=LEFT)
        ttk.Button(action_row, text="Open Checkout Window", command=self._open_checkout_from_field).pack(side=LEFT, padx=6)
        ttk.Button(action_row, text="Confirm Payment", command=self._confirm_checkout).pack(side=LEFT)
        ttk.Button(action_row, text="Refresh Balance", command=self._refresh_billing_status).pack(side=LEFT, padx=6)
        ttk.Button(action_row, text="Save Billing State", command=self._save_billing_state).pack(side=LEFT, padx=6)

        ttk.Separator(tab).pack(fill=X, pady=8)
        ttk.Label(tab, text="Account Recovery", style="Card.TLabelframe.Label").pack(anchor=W, pady=(0, 6))
        self._labeled_entry(tab, "Recovery Email", self.recovery_email_var)
        recovery_row = ttk.Frame(tab)
        recovery_row.pack(fill=X, pady=(0, 4))
        ttk.Button(recovery_row, text="Link Email To This Account", command=self._link_email_to_current_token).pack(side=LEFT)
        ttk.Button(recovery_row, text="Send Access Code By Email", command=self._recover_access_code_by_email).pack(side=LEFT, padx=6)
        ttk.Button(recovery_row, text="Have an Access Code?", command=self._open_access_code_dialog).pack(side=LEFT)

        ttk.Separator(tab).pack(fill=X, pady=8)
        ttk.Label(tab, text="Credit Codes", style="Card.TLabelframe.Label").pack(anchor=W, pady=(0, 6))
        self._labeled_entry(tab, "Redeem Credit Code", self.credit_code_var)
        code_row = ttk.Frame(tab)
        code_row.pack(fill=X, pady=(0, 4))
        ttk.Button(code_row, text="Redeem Code", command=self._redeem_credit_code).pack(side=LEFT)
        ttk.Button(code_row, text="Create/Update Test Code", command=self._upsert_test_credit_code).pack(side=LEFT, padx=6)
        ttk.Button(code_row, text="Disable Test Code", command=self._disable_test_credit_code).pack(side=LEFT)
        self._labeled_entry(tab, "Test Code Name", self.admin_code_var)
        self._labeled_spin(tab, "Test Code Credits", self.admin_code_credits_var, 1, 1000)
        ttk.Button(tab, text="Reset Current Account Paid Credits To 0", command=self._reset_current_paid_credits).pack(anchor=W, pady=(6, 0))

        ttk.Label(tab, textvariable=self.billing_status_var, style="Hint.TLabel", wraplength=760, justify=LEFT).pack(
            anchor=W, pady=(10, 0)
        )

    def _build_action_section(self, parent: ttk.Frame) -> None:
        box = ttk.LabelFrame(parent, text="Estimator and Run", padding=10, style="Card.TLabelframe")
        box.pack(fill=X, pady=(10, 0))

        self.progress_container = ttk.Frame(box, style="Panel.TFrame")
        self.progress_container.pack(fill=X, pady=(0, 10))

        progress_header = ttk.Frame(self.progress_container, style="Panel.TFrame")
        progress_header.pack(fill=X, pady=(0, 4))
        ttk.Label(progress_header, textvariable=self.progress_stage_label_var, style="Hint.TLabel").pack(side=LEFT)
        ttk.Label(progress_header, textvariable=self.total_progress_label_var, style="Hint.TLabel").pack(side=RIGHT)

        self.total_progress_bar = ttk.Progressbar(
            self.progress_container,
            variable=self.total_progress_var,
            maximum=100,
            mode="determinate",
            style="Total.Horizontal.TProgressbar",
        )
        self.total_progress_bar.pack(fill=X)
        ttk.Label(self.progress_container, textvariable=self.progress_overall_label_var, style="Hint.TLabel").pack(anchor=W, pady=(4, 0))
        self.progress_container.pack_forget()

        button_row = ttk.Frame(box)
        button_row.pack(fill=X)
        start_stack = ttk.Frame(button_row, style="Panel.TFrame")
        start_stack.pack(side=LEFT)
        self.start_processing_button = ttk.Button(start_stack, text="Start Processing", command=self._start_processing, style="Accent.TButton")
        self.start_processing_button.pack(anchor=W)
        ttk.Label(start_stack, textvariable=self.start_button_credit_var, style="Hint.TLabel", justify="center").pack(anchor="center", pady=(4, 0))

        self.stop_button = ttk.Button(button_row, text="Stop", command=self._stop_processing, style="Danger.TButton")

        button_row.columnconfigure(0, weight=0)
        button_row.columnconfigure(1, weight=0)
        button_row.columnconfigure(2, weight=1)
        button_row.columnconfigure(3, weight=0)

        start_stack.pack_forget()
        start_stack.grid(row=0, column=0, sticky="nw")

        self.stop_button.grid(row=0, column=1, sticky="nw", padx=(20, 0))
        self.stop_button.grid_remove()

        balance_stack = ttk.Frame(button_row, style="Panel.TFrame")
        balance_stack.grid(row=0, column=2, sticky="n", padx=(18, 18))
        ttk.Label(balance_stack, text="Current Balance", style="Hint.TLabel", justify="center").pack(anchor="center")
        tk.Label(
            balance_stack,
            textvariable=self.available_credits_var,
            fg="#7df6c7",
            bg="#111b2f",
            font=("Segoe UI", 13, "bold"),
            justify="center",
            width=12,
            anchor="center",
        ).pack(anchor="center", pady=(2, 0))

        right_actions = ttk.Frame(button_row, style="Panel.TFrame")
        right_actions.grid(row=0, column=3, sticky="ne")
        self.buy_credits_button = ttk.Button(right_actions, text="Buy Credits", command=self._open_billing_window, width=16)
        self.buy_credits_button.pack(anchor="center")

        self.restore_account_link_label = tk.Label(
            right_actions,
            text="Access Codes",
            fg="#6de1ff",
            bg="#111b2f",
            cursor="hand2",
            font=("Segoe UI", 10, "underline"),
        )
        self.restore_account_link_label.pack(anchor="center", pady=(4, 0))
        self.restore_account_link_label.bind("<Button-1>", lambda _event: self._toggle_restore_code_panel())

        self.restore_code_panel = ttk.Frame(box, style="Panel.TFrame")
        restore_row = ttk.Frame(self.restore_code_panel, style="Panel.TFrame")
        restore_row.pack(fill=X, pady=(8, 0))
        ttk.Label(restore_row, text="Access / Offer Code", width=18).pack(side=LEFT)
        ttk.Entry(restore_row, textvariable=self.access_code_input_var).pack(side=LEFT, fill=X, expand=True)
        ttk.Button(restore_row, text="Apply", command=self._apply_restore_or_offer_code, style="Accent.TButton").pack(side=LEFT, padx=(6, 0))
        ttk.Button(restore_row, text="Hide", command=self._toggle_restore_code_panel).pack(side=LEFT, padx=(6, 0))

        ttk.Label(
            self.restore_code_panel,
            text="Use your access code to restore your account, or enter a promo/special-offer code to redeem credits.",
            style="Hint.TLabel",
            wraplength=790,
            justify=LEFT,
        ).pack(anchor=W, pady=(6, 0))

    def _toggle_restore_code_panel(self) -> None:
        is_visible = bool(self.restore_code_panel.winfo_manager())
        if is_visible:
            self.restore_code_panel.pack_forget()
            self.restore_account_link_label.configure(text="Access Codes")
        else:
            self.restore_code_panel.pack(fill=X, pady=(8, 0))
            self.restore_account_link_label.configure(text="Hide Access Codes")
        self._fit_window_to_content()

    def _set_estimate_visibility(self, has_input: bool) -> None:
        """Show/hide footer based on whether a video is loaded."""
        if has_input:
            # Show footer with actual data
            if not self.footer_frame.winfo_manager():
                self.footer_frame.pack(fill=X, side="bottom")
            self.after_idle(self._fit_window_to_content)
        else:
            # Hide footer when no video selected
            if self.footer_frame.winfo_manager():
                self.footer_frame.pack_forget()
            self.estimate_summary_var.set("")
            self.estimate_source_var.set("")
            self.estimate_spec_var.set("")
            self.estimate_stage_var.set("")
            self.after_idle(self._fit_window_to_content)

    def _start_system_detection(self) -> None:
        if self._system_detection_started:
            return
        self._system_detection_started = True
        threading.Thread(target=self._detect_system_profile_worker, daemon=True).start()

    def _detect_system_profile_worker(self) -> None:
        try:
            cpu_threads = max(1, int(os.cpu_count() or 1))
            cpu_name = self._detect_cpu_name()
            ram_gb = self._detect_ram_gb()
            gpu_name = self._detect_primary_gpu_name()

            if cpu_threads <= 4:
                cpu_factor = 1.32
            elif cpu_threads <= 8:
                cpu_factor = 1.12
            elif cpu_threads <= 12:
                cpu_factor = 0.98
            else:
                cpu_factor = 0.88

            if ram_gb <= 8:
                ram_factor = 1.20
            elif ram_gb <= 16:
                ram_factor = 1.08
            elif ram_gb <= 32:
                ram_factor = 0.96
            else:
                ram_factor = 0.90

            gpu_factor = 1.12
            gpu_text = (gpu_name or "Unknown GPU").lower()
            if "rtx 40" in gpu_text or "rtx 50" in gpu_text:
                gpu_factor = 0.68
            elif "rtx" in gpu_text:
                gpu_factor = 0.78
            elif "gtx 16" in gpu_text or "gtx 10" in gpu_text:
                gpu_factor = 0.90
            elif "radeon" in gpu_text or "rx " in gpu_text:
                gpu_factor = 0.86
            elif "arc" in gpu_text:
                gpu_factor = 0.90
            elif "iris" in gpu_text or "uhd" in gpu_text or "intel" in gpu_text:
                gpu_factor = 1.10

            factor = max(0.60, min(1.80, cpu_factor * ram_factor * gpu_factor))
            unknown_parts = sum(1 for value in (cpu_name, gpu_name) if "unknown" in (value or "").lower())
            if unknown_parts == 0:
                reliability = "high-confidence"
            elif unknown_parts == 1:
                reliability = "partial-confidence"
            else:
                reliability = "low-confidence"

            note = (
                f"System detected ({reliability}): CPU {cpu_name} ({cpu_threads} threads), "
                f"RAM {ram_gb}GB, GPU {gpu_name}"
            )
            self._system_profile_cache = {
                "factor": factor,
                "note": note,
            }
            self.log_queue.put(f"[INFO] {note}")
            self.after(0, lambda: self._estimate_time(silent=True))
        except Exception as exc:
            self._system_profile_cache = {
                "factor": 1.0,
                "note": f"System detection fallback: {exc}",
            }
            self.log_queue.put(f"[WARN] System detection fallback used: {exc}")

    def _set_total_progress(self, percent: float, *, allow_decrease: bool = False) -> None:
        clamped = max(0.0, min(100.0, float(percent)))
        if not allow_decrease:
            clamped = max(self._overall_progress_max, clamped)
        self._overall_progress_max = clamped
        self.progress_overall_label_var.set(f"Overall {int(round(clamped))}%")

    def _reset_progress_state(self) -> None:
        self._progress_stage_total = 6
        self._progress_current_stage = 0
        self._overall_progress_max = 0.0
        self._set_stage_progress_display(0, 6, 0.0, "")
        self._set_total_progress(0.0, allow_decrease=True)

    def _estimate_overall_from_stage(self, stage_index: int, stage_total: int, stage_percent: float) -> float | None:
        if stage_total <= 0:
            return None
        if not self._active_stage_pred_seconds:
            return None

        clamped_stage = max(0, min(int(stage_index), stage_total))
        clamped_pct = max(0.0, min(100.0, float(stage_percent)))

        total_seconds = sum(max(0.0, float(self._active_stage_pred_seconds.get(i, 0.0))) for i in range(1, stage_total + 1))
        if total_seconds <= 0.0:
            return None

        completed_seconds = 0.0
        for i in range(1, clamped_stage):
            completed_seconds += max(0.0, float(self._active_stage_pred_seconds.get(i, 0.0)))

        current_seconds = max(0.0, float(self._active_stage_pred_seconds.get(clamped_stage, 0.0)))
        completed_seconds += current_seconds * (clamped_pct / 100.0)
        return max(0.0, min(100.0, (completed_seconds / total_seconds) * 100.0))

    def _set_stage_progress_display(self, stage_index: int, stage_total: int, stage_percent: float, stage_name: str) -> None:
        total = max(1, int(stage_total))
        index = max(0, min(int(stage_index), total))
        clamped = max(0.0, min(100.0, float(stage_percent)))
        label = f"Stage {index}/{total}"
        if stage_name:
            label = f"{label}: {stage_name}"
        self.progress_stage_label_var.set(label)
        self.total_progress_var.set(clamped)
        self.total_progress_label_var.set(f"{int(round(clamped))}%")

    def _set_progress_visible(self, visible: bool) -> None:
        is_visible = bool(self.progress_container.winfo_manager())
        if visible and not is_visible:
            self.progress_container.pack(fill=X, pady=(0, 10), before=self.start_processing_button.master.master)
        elif not visible and is_visible:
            self.progress_container.pack_forget()

    def _handle_progress_message(self, message: str) -> bool:
        if not message.startswith("[PROGRESS]"):
            return False
        payload = message[len("[PROGRESS]") :].strip()
        try:
            total_percent: float | None = None
            stage_index = self._progress_current_stage
            stage_total = self._progress_stage_total
            stage_percent = float(self.total_progress_var.get())
            stage_name = ""

            if "=" not in payload:
                total_percent = float(payload.split("|", 1)[0].strip())
            else:
                parts = [part.strip() for part in payload.split("|") if part.strip()]
                values: dict[str, str] = {}
                for part in parts:
                    if "=" in part:
                        key, value = part.split("=", 1)
                        values[key.strip().lower()] = value.strip()

                if "total" in values:
                    total_percent = float(values["total"])
                if "stage" in values and "/" in values["stage"]:
                    left, right = values["stage"].split("/", 1)
                    stage_index = max(0, int(left.strip() or "0"))
                    stage_total = max(1, int(right.strip() or "1"))
                if "stage_pct" in values:
                    stage_percent = float(values["stage_pct"])
                if "stage_name" in values:
                    stage_name = values["stage_name"]

            if total_percent is not None:
                computed_overall = self._estimate_overall_from_stage(stage_index, stage_total, stage_percent)
                if computed_overall is not None:
                    self._set_total_progress(computed_overall)
                else:
                    self._set_total_progress(total_percent)
            self._progress_current_stage = stage_index
            self._progress_stage_total = stage_total
            self._set_stage_progress_display(stage_index, stage_total, stage_percent, stage_name)
        except Exception:
            return True
        return True

    def _update_progress_from_log_fallback(self, message: str) -> None:
        stage_match = re.search(r"\[(\d+)\s*/\s*(\d+)\]", message)
        if stage_match:
            self._progress_current_stage = max(1, int(stage_match.group(1)))
            self._progress_stage_total = max(1, int(stage_match.group(2)))
            if float(self.total_progress_var.get()) <= 0.1:
                self._set_stage_progress_display(self._progress_current_stage, self._progress_stage_total, 0.0, "")
            computed = self._estimate_overall_from_stage(self._progress_current_stage, self._progress_stage_total, 0.0)
            base = computed if computed is not None else ((self._progress_current_stage - 1) / self._progress_stage_total) * 100.0
            self._set_total_progress(base)
            return

        lowered = message.lower()
        if "completed in" in lowered and self._progress_current_stage > 0:
            elapsed_match = re.search(r"completed in\s+([0-9]+(?:\.[0-9]+)?)s", lowered)
            if elapsed_match:
                try:
                    self._record_stage_timing_sample(self._progress_current_stage, float(elapsed_match.group(1)))
                except Exception:
                    pass
            computed = self._estimate_overall_from_stage(self._progress_current_stage, self._progress_stage_total, 100.0)
            completed_pct = computed if computed is not None else (self._progress_current_stage / self._progress_stage_total) * 100.0
            self._set_stage_progress_display(self._progress_current_stage, self._progress_stage_total, 100.0, "")
            self._set_total_progress(completed_pct)
            return

        if "processing completed successfully" in lowered:
            self._set_total_progress(100.0)
        elif "processing canceled by user" in lowered or "stop requested by user" in lowered:
            self._set_total_progress(0.0, allow_decrease=True)

    def _set_processing_controls_active(self, is_active: bool) -> None:
        if is_active:
            self.stop_button.grid()
        else:
            self.stop_button.grid_remove()

    def _load_stage_timing_profile(self) -> dict[str, dict[str, float | int]]:
        default = {
            "multipliers": {str(i): 1.0 for i in range(1, 7)},
            "counts": {str(i): 0 for i in range(1, 7)},
        }
        try:
            if not self.stage_timing_profile_file.exists():
                return default
            payload = json.loads(self.stage_timing_profile_file.read_text(encoding="utf-8"))
            multipliers = payload.get("multipliers", {})
            counts = payload.get("counts", {})
            for i in range(1, 7):
                key = str(i)
                value = float(multipliers.get(key, 1.0))
                default["multipliers"][key] = max(0.40, min(2.50, value))
                default["counts"][key] = max(0, int(counts.get(key, 0)))
            return default
        except Exception:
            return default

    def _save_stage_timing_profile(self) -> None:
        payload = {
            "multipliers": self.stage_timing_profile.get("multipliers", {}),
            "counts": self.stage_timing_profile.get("counts", {}),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        try:
            self.stage_timing_profile_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _record_stage_timing_sample(self, stage_index: int, actual_seconds: float) -> None:
        key = str(max(1, min(6, int(stage_index))))
        predicted = float(self._active_stage_pred_seconds.get(int(stage_index), 0.0))
        if predicted <= 0.2:
            return
        ratio = max(0.40, min(2.50, float(actual_seconds) / predicted))
        multipliers = self.stage_timing_profile.setdefault("multipliers", {str(i): 1.0 for i in range(1, 7)})
        counts = self.stage_timing_profile.setdefault("counts", {str(i): 0 for i in range(1, 7)})
        old = float(multipliers.get(key, 1.0))
        count = int(counts.get(key, 0))
        alpha = 0.35 if count < 4 else 0.22 if count < 12 else 0.14
        multipliers[key] = max(0.40, min(2.50, (old * (1.0 - alpha)) + (ratio * alpha)))
        counts[key] = count + 1
        self._save_stage_timing_profile()

    @staticmethod
    def _format_eta(seconds: float) -> str:
        total = max(0, int(round(seconds)))
        mins, secs = divmod(total, 60)
        if mins > 0:
            return f"{mins}m {secs:02d}s"
        return f"{secs}s"

    def _estimate_stage_seconds(
        self,
        settings: PipelineSettings,
        fps: float,
        effective_duration: float,
        frame_count: int,
        source_width: int,
        source_height: int,
    ) -> dict[int, float]:
        system_factor, _ = self._get_system_performance_hint()
        frames = max(1, int(frame_count))
        duration = max(0.1, float(effective_duration))
        fps_safe = max(1.0, float(fps))
        input_megapixels = max(0.10, (max(1, int(source_width)) * max(1, int(source_height))) / 1_000_000.0)

        model_factor = {
            "realesr-animevideov3-x2": 0.70,
            "realesr-animevideov3-x3": 0.86,
            "realesr-animevideov3-x4": 1.05,
            "realesrgan-x4plus-anime": 1.18,
            "realesrgan-x4plus": 1.32,
        }.get(settings.model, 1.0)
        scale_factor = 0.78 if settings.scale <= 2 else 0.94 if settings.scale == 3 else 1.10
        format_factor = 1.08 if settings.image_format == "png" else 0.92
        pre_filter_factor = 1.0 + (0.16 if settings.enable_color else 0.0) + min(0.22, max(0.0, settings.denoise) * 0.11)
        post_factor = 1.0 + (0.22 if settings.enable_sharpen else 0.0) + (0.26 if settings.apply_final_scale else 0.0)
        encode_factor = {
            "ultrafast": 0.58,
            "superfast": 0.68,
            "veryfast": 0.80,
            "faster": 0.92,
            "fast": 1.00,
            "medium": 1.16,
            "slow": 1.34,
        }.get(settings.encode_preset, 1.0)

        fps_ratio = max(1.0, float(settings.target_fps) / fps_safe)
        threads_bonus = 1.0
        try:
            parts = [max(1, int(p)) for p in settings.threads.split(":") if p.strip()]
            threads_bonus = max(0.75, min(1.20, 1.10 - (0.02 * sum(parts))))
        except Exception:
            threads_bonus = 1.0

        # Stage 2 scales strongly with source resolution and dominates total runtime.
        resolution_factor = max(0.55, min(6.00, (input_megapixels / 2.0) ** 0.92))
        stage2_seconds_per_frame = 0.60 * resolution_factor
        stage2_format_factor = 1.08 if settings.image_format == "png" else 0.96

        s1 = frames * 0.0019 * format_factor * pre_filter_factor * max(0.86, system_factor * 0.95)
        s2 = (
            frames
            * stage2_seconds_per_frame
            * model_factor
            * scale_factor
            * stage2_format_factor
            * system_factor
            * threads_bonus
        )
        s3 = (frames * 0.0034 * post_factor * max(0.86, system_factor * 0.92)) if (settings.enable_sharpen or settings.apply_final_scale) else 0.8
        s4 = duration * (0.24 * encode_factor + 0.08) * max(0.82, system_factor * 0.90)
        interpolation_needed = settings.enable_interpolation and (float(settings.target_fps) > (fps_safe + 0.5))
        s5 = (duration * (0.30 + (0.62 * (fps_ratio - 1.0))) * max(0.86, system_factor * 0.95)) if interpolation_needed else 0.8
        s6 = (duration * (0.11 if settings.include_audio else 0.04) + 1.0) * max(0.84, system_factor * 0.90)

        stage_map = {
            1: max(0.5, s1),
            2: max(1.0, s2),
            3: max(0.5, s3),
            4: max(0.8, s4),
            5: max(0.5, s5),
            6: max(0.5, s6),
        }

        multipliers = self.stage_timing_profile.get("multipliers", {})
        for index in range(1, 7):
            key = str(index)
            mult = max(0.40, min(2.50, float(multipliers.get(key, 1.0))))
            stage_map[index] *= mult

        return stage_map

    def _apply_restore_or_offer_code(self) -> None:
        code = self.access_code_input_var.get().strip()
        if not code:
            messagebox.showwarning("Code Required", "Enter an access code or offer code first.")
            return

        normalized_offer = " ".join(code.upper().split())
        if normalized_offer == "I AM ATOMIC":
            token = self.billing_token_var.get().strip()
            if not token or not is_valid_paid_access_token(token):
                token = self._generate_billing_token()
                self.billing_token_var.set(token)
                self._ensure_free_trial_for_token(token)
            new_balance = self.billing_store.add_credits(token, 100, source="atomic_offer_phrase")
            self._save_billing_state()
            self._refresh_billing_status(silent=True)
            self.billing_status_var.set(f"Special offer applied: +100 credits. Balance: {new_balance}.")
            self.log_queue.put(f"[INFO] Special offer phrase accepted. +100 credits. Balance: {new_balance}.")
            return

        if is_valid_paid_access_token(code):
            self._apply_access_code(code)
            return

        token = self.billing_token_var.get().strip()
        if not token or not is_valid_paid_access_token(token):
            token = self._generate_billing_token()
            self.billing_token_var.set(token)
        self.credit_code_var.set(code)
        self._redeem_credit_code()

    def _build_compare_panel(self, parent: ttk.Frame) -> None:
        box = ttk.LabelFrame(parent, text="Before/After Frame Compare", padding=8, style="Card.TLabelframe")
        box.pack(fill=X, pady=(0, 10))

        controls = ttk.Frame(box, style="Panel.TFrame")
        controls.pack(fill=X, pady=(0, 8))
        ttk.Label(
            controls,
            text="Compare frame regenerates automatically as settings change. Drag the separator line to reveal before/after.",
            style="Hint.TLabel",
        ).pack(side=LEFT, padx=(10, 0))

        self.compare_canvas = tk.Canvas(
            box,
            width=480,
            height=280,
            bg="#090f1b",
            highlightthickness=1,
            highlightbackground="#355c93",
        )
        self.compare_canvas.pack(fill=X)
        self.compare_canvas.bind("<Configure>", lambda _event: self._redraw_compare_canvas())
        self.compare_canvas.bind("<Double-Button-1>", lambda _event: self._open_large_compare_window())
        self.compare_canvas.bind("<Button-1>", lambda event: self._on_compare_mouse_press(event))
        self.compare_canvas.bind("<B1-Motion>", lambda event: self._on_compare_mouse_drag(event))
        self.compare_canvas.bind("<ButtonRelease-1>", lambda event: self._on_compare_mouse_release(event))
        self.compare_canvas.bind("<Motion>", lambda event: self._on_compare_mouse_motion(event))
        self.compare_canvas.bind("<Leave>", lambda _event: self._on_compare_mouse_leave())
        self.compare_canvas.create_text(
            240,
            140,
            text="Compare preview updates automatically from current settings.",
            fill="#86a6d8",
            font=("Segoe UI", 11),
        )

    def _build_log_panel(self, parent: ttk.Frame) -> None:
        box = ttk.LabelFrame(parent, text="Processing Log", padding=8, style="Card.TLabelframe")
        box.pack(fill=X)
        self.log_text = tk.Text(
            box,
            width=48,
            height=12,
            wrap="word",
            bg="#08101d",
            fg="#cde3ff",
            insertbackground="#cde3ff",
            relief="flat",
        )
        self.log_text.tag_configure("info", foreground="#cde3ff")
        self.log_text.tag_configure("warn", foreground="#ffd27a")
        self.log_text.tag_configure("error", foreground="#ff8f8f")
        self.log_text.tag_configure("hint", foreground="#9cf6ca")
        self.log_text.tag_configure("debug", foreground="#9db6d8")
        scrollbar = ttk.Scrollbar(box, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        self.log_text.pack(side=LEFT, fill=BOTH, expand=True)
        scrollbar.pack(side=RIGHT, fill=Y)

    def _build_footer(self, parent: tk.Widget) -> None:
        """Build a professional footer bar spanning the entire window width (initially hidden)."""
        # Main footer container
        self.footer_frame = tk.Frame(parent, bg="#0a1220")
        self.footer_frame.pack(fill=X, side="bottom")
        
        # Separator line
        separator = tk.Frame(self.footer_frame, bg="#1a3a52", height=1)
        separator.pack(fill=X)
        
        # Content area with fixed padding
        content = tk.Frame(self.footer_frame, bg="#0a1220")
        content.pack(fill=X, padx=12, pady=6)
        content.grid_columnconfigure(0, minsize=72)
        content.grid_columnconfigure(1, weight=1)
        
        # Row 1: ETA + Source
        tk.Label(content, text="ETA:", fg="#7df6c7", bg="#0a1220", font=("Segoe UI", 9, "bold"), anchor="w").grid(row=0, column=0, sticky="w")
        row1_value = tk.Frame(content, bg="#0a1220")
        row1_value.grid(row=0, column=1, sticky="ew", pady=(0, 4))
        tk.Label(row1_value, textvariable=self.estimate_summary_var, fg="#aaaaaa", bg="#0a1220", font=("Segoe UI", 9), anchor="w").pack(side=LEFT)
        tk.Label(row1_value, text="Source:", fg="#7df6c7", bg="#0a1220", font=("Segoe UI", 9, "bold"), anchor="w").pack(side=LEFT, padx=(36, 10))
        self.footer_source_value = tk.Label(row1_value, textvariable=self.estimate_source_var, fg="#aaaaaa", bg="#0a1220", font=("Segoe UI", 9), anchor="w")
        self.footer_source_value.pack(side=LEFT)

        # Row 2: System
        tk.Label(content, text="System:", fg="#7df6c7", bg="#0a1220", font=("Segoe UI", 9, "bold"), anchor="w").grid(row=1, column=0, sticky="nw")
        self.footer_system_value = tk.Label(content, textvariable=self.estimate_spec_var, fg="#aaaaaa", bg="#0a1220", font=("Segoe UI", 9), wraplength=1000, justify=LEFT, anchor="w")
        self.footer_system_value.grid(row=1, column=1, sticky="ew", pady=(2, 0))

        # Row 3: Stages
        tk.Label(content, text="Stages:", fg="#7df6c7", bg="#0a1220", font=("Segoe UI", 8, "bold"), anchor="w").grid(row=2, column=0, sticky="nw")
        self.footer_stages_value = tk.Label(content, textvariable=self.estimate_stage_var, fg="#999999", bg="#0a1220", font=("Segoe UI", 8), wraplength=1000, justify=LEFT, anchor="w")
        self.footer_stages_value.grid(row=2, column=1, sticky="ew", pady=(4, 0))
        
        # Start hidden - show only when video is loaded
        self.footer_frame.pack_forget()

    @staticmethod
    def _labeled_entry(parent: ttk.Frame, label: str, variable: tk.Variable) -> None:
        row = ttk.Frame(parent)
        row.pack(fill=X, pady=2)
        row.columnconfigure(0, weight=1)
        row.columnconfigure(1, weight=1)
        ttk.Label(row, text=label, anchor="w").grid(row=0, column=0, sticky="ew", padx=(0, 5))
        ttk.Entry(row, textvariable=variable).grid(row=0, column=1, sticky="ew")

    @staticmethod
    def _labeled_combo(parent: ttk.Frame, label: str, variable: tk.Variable, values: list | tuple) -> None:
        row = ttk.Frame(parent)
        row.pack(fill=X, pady=2)
        row.columnconfigure(0, weight=1)
        row.columnconfigure(1, weight=1)
        ttk.Label(row, text=label, anchor="w").grid(row=0, column=0, sticky="ew", padx=(0, 5))
        ttk.Combobox(row, textvariable=variable, values=list(values), state="readonly").grid(row=0, column=1, sticky="ew")

    @staticmethod
    def _labeled_spin(parent: ttk.Frame, label: str, variable: tk.Variable, minimum: int, maximum: int) -> None:
        row = ttk.Frame(parent)
        row.pack(fill=X, pady=2)
        row.columnconfigure(0, weight=1)
        row.columnconfigure(1, weight=1)
        ttk.Label(row, text=label, anchor="w").grid(row=0, column=0, sticky="ew", padx=(0, 5))
        ttk.Spinbox(row, textvariable=variable, from_=minimum, to=maximum, increment=1).grid(row=0, column=1, sticky="ew")

    @staticmethod
    def _labeled_spin_float(
        parent: ttk.Frame,
        label: str,
        variable: tk.Variable,
        minimum: float,
        maximum: float,
        step: float,
    ) -> None:
        row = ttk.Frame(parent)
        row.pack(fill=X, pady=2)
        row.columnconfigure(0, weight=1)
        row.columnconfigure(1, weight=1)
        ttk.Label(row, text=label, anchor="w").grid(row=0, column=0, sticky="ew", padx=(0, 5))
        ttk.Spinbox(row, textvariable=variable, from_=minimum, to=maximum, increment=step).grid(row=0, column=1, sticky="ew")

    def _pick_input(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Select input video",
            filetypes=[("Video files", "*.mp4 *.mov *.mkv *.webm"), ("All files", "*.*")],
        )
        if not file_path:
            return
        self.input_video_var.set(file_path)
        current_output = self.output_video_var.get().strip()
        if not current_output:
            input_path = Path(file_path)
            output_name = f"{input_path.stem}_v11b_upscaled.mp4"
            self.output_video_var.set(str(input_path.with_name(output_name)))

        self._sync_target_fps_to_source_if_needed(file_path)
        self._normalize_interpolation_choice(show_feedback=True)

        self._auto_prepare_after_input()

    def _normalize_interpolation_choice(self, show_feedback: bool = False) -> None:
        if not self.enable_interpolation_var.get():
            return
        candidate = self.input_video_var.get().strip()
        if not candidate:
            return

        try:
            source_fps = float(PipelineRunner.get_fps(Path(candidate)))
            target_fps = int(self.target_fps_var.get())
        except Exception:
            return

        if target_fps > (source_fps + 0.5):
            return

        higher_targets = [fps for fps in FPS_OPTIONS if fps > (source_fps + 0.5)]
        if higher_targets:
            new_target = int(higher_targets[0])
            if new_target != target_fps:
                self.target_fps_var.set(new_target)
                if show_feedback:
                    self.log_queue.put(
                        f"[INFO] Interpolation target FPS auto-adjusted from {target_fps} to {new_target} (source FPS {source_fps:.2f})."
                    )
            return

        self.enable_interpolation_var.set(False)
        self.target_fps_var.set(max(1, int(round(source_fps))))
        if show_feedback:
            self.log_queue.put(
                f"[INFO] Interpolation auto-disabled: source FPS is already {source_fps:.2f}, so there is no higher target FPS to generate."
            )

    def _sync_target_fps_to_source_if_needed(self, input_path: str | Path | None = None) -> None:
        if self.enable_interpolation_var.get():
            return
        candidate = str(input_path or self.input_video_var.get()).strip()
        if not candidate:
            return
        try:
            fps = PipelineRunner.get_fps(Path(candidate))
            self.target_fps_var.set(max(1, int(round(fps))))
        except Exception:
            pass

    def _recommend_realesrgan_threads(self, gpu_name: str | None = None) -> str:
        gpu_text = (gpu_name or self._detect_primary_gpu_name() or "").lower()
        if "rtx 50" in gpu_text or "rtx 40" in gpu_text:
            return "4:4:4"
        if "rtx" in gpu_text:
            return "3:3:3"
        if "radeon" in gpu_text or "rx " in gpu_text or "arc" in gpu_text:
            return "2:2:2"
        if "gtx 16" in gpu_text or "gtx 10" in gpu_text:
            return "2:2:2"
        if "intel" in gpu_text or "iris" in gpu_text or "uhd" in gpu_text:
            return "1:2:2"
        return "2:2:2"

    def _refresh_auto_thread_recommendation(self, force: bool = False) -> None:
        recommended = self._recommend_realesrgan_threads()
        current = self.threads_var.get().strip()
        if force or not current or current == self._auto_threads_value:
            self.threads_var.set(recommended)
        self._auto_threads_value = recommended

    def _billing_endpoint(self, endpoint: str) -> str:
        base = self.billing_api_base_var.get().strip().rstrip("/")
        return f"{base}{endpoint}"

    def _use_embedded_billing(self) -> bool:
        base = self.billing_api_base_var.get().strip().lower()
        return base in {"", "embedded://local", "local", "embedded"}

    @staticmethod
    def _generate_billing_token() -> str:
        return f"v11b-{uuid.uuid4().hex[:20]}"

    def _load_billing_state(self) -> None:
        try:
            if not self.billing_state_file.exists():
                return
            data = json.loads(self.billing_state_file.read_text(encoding="utf-8"))
            token = str(data.get("token", "")).strip()
            api_base = str(data.get("api_base", "")).strip()
            last_session = str(data.get("last_session_id", "")).strip()
            last_url = str(data.get("last_checkout_url", "")).strip()
            recovery_email = str(data.get("recovery_email", "")).strip()
            if token:
                self.billing_token_var.set(token)
            if api_base in {"http://127.0.0.1:5050", "http://localhost:5050"} and not os.environ.get("V11B_BILLING_API_BASE"):
                api_base = "embedded://local"
            if api_base:
                self.billing_api_base_var.set(api_base)
            if last_session:
                self.checkout_session_var.set(last_session)
            if last_url:
                self.checkout_url_var.set(last_url)
            if recovery_email:
                self.recovery_email_var.set(recovery_email)
        except Exception:
            pass

    def _save_billing_state(self) -> None:
        payload = {
            "token": self.billing_token_var.get().strip(),
            "api_base": self.billing_api_base_var.get().strip(),
            "last_session_id": self.checkout_session_var.get().strip(),
            "last_checkout_url": self.checkout_url_var.get().strip(),
            "recovery_email": self.recovery_email_var.get().strip(),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        self.billing_state_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        self.billing_status_var.set(f"Billing state saved: {self.billing_state_file.name}")
        self.log_queue.put(f"[INFO] Billing state saved to {self.billing_state_file}")

    def _refresh_billing_status(self, silent: bool = False) -> None:
        token = self.billing_token_var.get().strip()
        if not token:
            self.available_credits_var.set("0")
            if not silent:
                self.billing_status_var.set("Billing: generate or enter a token first.")
            return
        try:
            self._ensure_free_trial_for_token(token)
            if self._use_embedded_billing():
                status = self.billing_backend.get_status(token)
            else:
                url = self._billing_endpoint("/api/billing/status") + "?" + urlencode({"token": token})
                request = Request(url, method="GET")
                with urlopen(request, timeout=15) as response:
                    status = json.loads(response.read().decode("utf-8", errors="replace") or "{}")
            credits = int(status.get("credits", 0) or 0)
            paid = int(status.get("paid_credits", 0) or 0)
            trial = int(status.get("free_trial_remaining", 0) or 0)
            self.available_credits_var.set(str(credits))
            self.billing_status_var.set(
                f"Billing: token {token[:12]}... has {credits} credits available (paid: {paid}, trial: {trial})."
            )
        except Exception as exc:
            self.available_credits_var.set("--")
            if not silent:
                self.billing_status_var.set(f"Billing status check failed: {exc}")

    def _cleanup_output_if_canceled(self) -> None:
        if not self._stop_requested_by_user:
            return
        output_path = self._current_run_output
        if output_path and output_path.exists() and output_path.is_file():
            try:
                output_path.unlink(missing_ok=True)
                self.log_queue.put(f"[INFO] Removed partial output after cancel: {output_path}")
            except Exception as exc:
                self.log_queue.put(f"[WARN] Could not remove partial output after cancel: {exc}")

    @staticmethod
    def _trial_claim_key() -> str:
        computer = (os.environ.get("COMPUTERNAME") or "unknown-device").strip().lower()
        return f"device:{computer}"

    def _ensure_free_trial_for_token(self, token: str) -> None:
        if not token or self.free_trial_credits <= 0:
            return
        if not is_valid_paid_access_token(token):
            return
        claim_key = self._trial_claim_key()
        self.billing_store.claim_free_trial(token, claim_key, self.free_trial_credits, source="startup_free_trial")

    @staticmethod
    def _normalize_email(email: str) -> str:
        return (email or "").strip().lower()

    def _send_recovery_email(self, to_email: str, token: str) -> tuple[bool, str]:
        if not self.smtp_configured:
            return False, "SMTP is not configured. Set V11B_SMTP_* env vars to enable email recovery."
        try:
            body = (
                "Your v11b access code is:\n\n"
                f"  {token}\n\n"
                "Use Billing > Have an Access Code? to restore your account.\n"
            )
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = "v11b Access Code Recovery"
            msg["From"] = self.smtp_from
            msg["To"] = to_email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_pass)
                server.send_message(msg)
            self.billing_store.record_recovery_sent(to_email)
            return True, "Access code email sent."
        except Exception as exc:
            return False, f"Failed to send recovery email: {exc}"

    def _link_email_to_current_token(self) -> None:
        token = self.billing_token_var.get().strip()
        email = self._normalize_email(self.recovery_email_var.get())
        if not token or not is_valid_paid_access_token(token):
            messagebox.showwarning("Invalid Token", "A valid access token is required first.")
            return
        ok, msg = self.billing_store.link_email(token, email)
        if ok:
            self.billing_status_var.set(msg)
            self.log_queue.put(f"[INFO] {msg}")
            self._save_billing_state()
            self._refresh_billing_status(silent=True)
        else:
            messagebox.showwarning("Email Link Failed", msg)

    def _recover_access_code_by_email(self) -> None:
        email = self._normalize_email(self.recovery_email_var.get())
        if not email:
            messagebox.showwarning("Email Required", "Enter your linked recovery email first.")
            return
        token = self.billing_store.get_token_by_email(email)
        if not token:
            messagebox.showwarning("Not Found", "No account is linked to that email.")
            return
        ok, msg = self._send_recovery_email(email, token)
        if ok:
            self.billing_status_var.set(msg)
            self.log_queue.put(f"[INFO] Recovery email sent to {email}.")
        else:
            self.billing_status_var.set(msg)
            self.log_queue.put(f"[WARN] {msg}")

    def _open_access_code_dialog(self) -> None:
        dialog = tk.Toplevel(self)
        dialog.title("Restore By Access Code")
        dialog.geometry("520x180")
        dialog.resizable(False, False)
        dialog.configure(bg="#0a1220")

        frame = ttk.Frame(dialog, padding=12, style="Root.TFrame")
        frame.pack(fill=BOTH, expand=True)
        ttk.Label(frame, text="Paste your access code to restore this account.", style="Hint.TLabel").pack(anchor=W, pady=(0, 8))

        local_code_var = tk.StringVar(value=self.access_code_input_var.get())
        row = ttk.Frame(frame)
        row.pack(fill=X)
        ttk.Label(row, text="Access Code", width=14).pack(side=LEFT)
        ttk.Entry(row, textvariable=local_code_var).pack(side=LEFT, fill=X, expand=True)

        def apply_code() -> None:
            code = local_code_var.get().strip()
            self.access_code_input_var.set(code)
            self._apply_access_code(code)
            dialog.destroy()

        btns = ttk.Frame(frame)
        btns.pack(fill=X, pady=(10, 0))
        ttk.Button(btns, text="Restore", command=apply_code, style="Accent.TButton").pack(side=LEFT)
        ttk.Button(btns, text="Cancel", command=dialog.destroy).pack(side=LEFT, padx=6)

    def _apply_access_code(self, code: str) -> None:
        token = (code or "").strip()
        if not is_valid_paid_access_token(token):
            messagebox.showwarning("Invalid Access Code", "Access code format is invalid.")
            return
        self.billing_token_var.set(token)
        self._ensure_free_trial_for_token(token)
        self._save_billing_state()
        self._refresh_billing_status(silent=True)
        self.billing_status_var.set("Access code restored for this app session.")
        self.log_queue.put("[INFO] Access code restored.")

    def _redeem_credit_code(self) -> None:
        token = self.billing_token_var.get().strip()
        code = self.credit_code_var.get().strip()
        if not token or not is_valid_paid_access_token(token):
            messagebox.showwarning("Invalid Token", "A valid access token is required first.")
            return
        ok, balance, msg = self.billing_store.redeem_credit_code(token, code)
        if ok:
            self.billing_status_var.set(f"{msg} Balance: {balance}.")
            self.log_queue.put(f"[INFO] {msg} Balance: {balance}.")
            self._refresh_billing_status(silent=True)
        else:
            self.billing_status_var.set(msg)
            self.log_queue.put(f"[WARN] {msg}")

    def _upsert_test_credit_code(self) -> None:
        code = self.admin_code_var.get().strip()
        credits = int(self.admin_code_credits_var.get())
        try:
            self.billing_store.upsert_credit_code(code, credits, active=True)
            self.billing_status_var.set(f"Test code {code.upper()} is active for {credits} credits.")
            self.log_queue.put(f"[INFO] Test code {code.upper()} set to {credits} credits.")
        except Exception as exc:
            messagebox.showwarning("Code Error", str(exc))

    def _disable_test_credit_code(self) -> None:
        code = self.admin_code_var.get().strip()
        if not code:
            messagebox.showwarning("Code Required", "Enter a test code name first.")
            return
        self.billing_store.set_code_active(code, False)
        self.billing_status_var.set(f"Code {code.upper()} disabled.")
        self.log_queue.put(f"[WARN] Code {code.upper()} disabled.")

    def _reset_current_paid_credits(self) -> None:
        token = self.billing_token_var.get().strip()
        if not token or not is_valid_paid_access_token(token):
            messagebox.showwarning("Invalid Token", "A valid access token is required first.")
            return
        new_balance = self.billing_store.set_paid_credits(token, 0, source="manual_test_reset")
        self.billing_status_var.set(f"Paid credits reset to 0. Current total balance: {new_balance}.")
        self.log_queue.put(f"[WARN] Paid credits reset to 0 for token {token[:12]}...")
        self._refresh_billing_status(silent=True)

    @staticmethod
    def _collect_processing_metrics(settings: PipelineSettings) -> tuple[float, float, float, int, int, int]:
        duration = PipelineRunner.get_video_duration(settings.input_video)
        fps = PipelineRunner.get_fps(settings.input_video)
        width = int(PipelineRunner._ffprobe_value(settings.input_video, "width") or 0)
        height = int(PipelineRunner._ffprobe_value(settings.input_video, "height") or 0)
        effective_duration = duration
        if settings.start_time > 0:
            effective_duration = max(0.1, duration - settings.start_time)
        if settings.clip_duration > 0:
            effective_duration = min(effective_duration, settings.clip_duration)
        frame_count = PipelineRunner.get_frame_count(settings.input_video, duration_override=effective_duration)
        return duration, fps, effective_duration, frame_count, max(1, width), max(1, height)

    @staticmethod
    def _calculate_processing_credit_cost(
        settings: PipelineSettings,
        effective_duration: float,
        source_fps: float | None = None,
    ) -> tuple[int, str]:
        started_minutes = max(1, math.ceil(effective_duration / 60.0))
        multiplier = 1.0
        multiplier += max(0, settings.scale - 2) * 0.30
        interp_needed = bool(settings.enable_interpolation)
        if source_fps is not None:
            interp_needed = interp_needed and (float(settings.target_fps) > (float(source_fps) + 0.5))

        if interp_needed:
            multiplier += 0.90
            if settings.target_fps >= 60:
                multiplier += 0.35
        elif settings.target_fps >= 48:
            multiplier += 0.10
        if settings.enable_sharpen:
            multiplier += 0.20
        if settings.enable_color:
            multiplier += 0.12
        if settings.denoise > 0:
            multiplier += min(0.35, settings.denoise * 0.08)
        if settings.apply_final_scale and settings.target_height >= 2160:
            multiplier += 0.20
        if settings.encode_preset in {"medium", "slow"}:
            multiplier += 0.15
        if settings.model == "realesrgan-x4plus":
            multiplier += 0.20
        credits = max(1, math.ceil(started_minutes * multiplier))
        breakdown = f"{started_minutes} started minute(s) x {multiplier:.2f} complexity"
        return credits, breakdown

    @staticmethod
    def _post_form_json(url: str, data: dict[str, str], timeout: float = 20.0) -> tuple[int, dict]:
        encoded = urlencode(data).encode("utf-8")
        request = Request(url, data=encoded, method="POST")
        request.add_header("Content-Type", "application/x-www-form-urlencoded")
        try:
            with urlopen(request, timeout=timeout) as response:
                body = response.read().decode("utf-8", errors="replace")
                return int(response.status), json.loads(body or "{}")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            parsed = {"error": body or str(exc)}
            try:
                parsed = json.loads(body or "{}")
            except Exception:
                pass
            return int(getattr(exc, "code", 500) or 500), parsed
        except URLError as exc:
            return 0, {"error": str(exc)}

    def _open_checkout_window(self, checkout_url: str) -> None:
        url = checkout_url.strip()
        if not url:
            messagebox.showwarning("Missing URL", "No checkout URL is available.")
            return

        webview_script = (
            "import webview\n"
            f"webview.create_window('v11b Checkout', {url!r}, width=1100, height=820, min_size=(900, 680))\n"
            "webview.start(gui='edgechromium', debug=False, private_mode=False)\n"
        )

        if importlib.util.find_spec("webview") is None:
            webbrowser.open(url)
            self.log_queue.put("[WARN] pywebview not installed; checkout opened in browser.")
            return

        try:
            subprocess.Popen([sys.executable, "-c", webview_script], creationflags=_NO_WINDOW)
            self.log_queue.put("[INFO] Checkout opened in app window (pywebview).")
        except Exception:
            webbrowser.open(url)
            self.log_queue.put("[WARN] pywebview not available; checkout opened in browser.")

    def _open_checkout_from_field(self) -> None:
        self._open_checkout_window(self.checkout_url_var.get())

    def _open_billing_window(self) -> None:
        if self.billing_window and self.billing_window.winfo_exists():
            self.billing_window.deiconify()
            self.billing_window.lift()
            self.billing_window.focus_force()
            return

        window = tk.Toplevel(self)
        window.title("Buy Credits")
        window.minsize(980, 560)
        window.configure(bg="#0a1220")
        window.transient(self)
        window.protocol("WM_DELETE_WINDOW", self._close_billing_window)
        window.bind("<Escape>", lambda _event: self._close_billing_window())
        self.billing_window = window

        self.update_idletasks()
        popup_width = 1080
        popup_height = 560
        x = self.winfo_rootx() + max(0, (self.winfo_width() - popup_width) // 2)
        y = self.winfo_rooty() + max(0, (self.winfo_height() - popup_height) // 2)
        window.geometry(f"{popup_width}x{popup_height}+{x}+{y}")
        window.lift()
        window.focus_force()
        window.grab_set()

        frame = ttk.Frame(window, padding=14, style="Root.TFrame")
        frame.pack(fill=BOTH, expand=True)
        hero = ttk.Frame(frame, padding=(18, 16), style="BillingHero.TFrame")
        hero.pack(fill=X)

        hero_left = ttk.Frame(hero, style="BillingHero.TFrame")
        hero_left.pack(side=LEFT, fill=X, expand=True)
        ttk.Label(hero_left, text="Purchase Processing Credits", style="BillingHeroTitle.TLabel").pack(anchor=W)
        ttk.Label(
            hero_left,
            text="Secure checkout opens in your browser. Pick a package, complete payment, then return here to confirm and refresh your balance.",
            style="BillingHeroSub.TLabel",
            wraplength=520,
            justify=LEFT,
        ).pack(anchor=W, pady=(6, 0))

        hero_right = tk.Frame(hero, bg="#102038", highlightbackground="#2d4f7b", highlightthickness=1, bd=0)
        hero_right.pack(side=RIGHT, padx=(14, 0))
        secure_row = tk.Frame(hero_right, bg="#102038")
        secure_row.pack(padx=14, pady=8)
        tk.Label(
            secure_row,
            text="✓",
            bg="#102038",
            fg="#39d98a",
            font=("Segoe UI", 11, "bold"),
        ).pack(side=LEFT, padx=(0, 6))
        tk.Label(
            secure_row,
            text="Secure Checkout",
            bg="#102038",
            fg="#6de1ff",
            font=("Segoe UI", 10, "bold"),
        ).pack(side=LEFT)

        ttk.Label(frame, text="Choose Your Package", style="BillingSectionTitle.TLabel").pack(anchor=W, pady=(16, 10))

        packages = tk.Frame(frame, bg="#0a1220")
        packages.pack(fill=BOTH, expand=True)
        packages.grid_columnconfigure(0, weight=1)
        packages.grid_columnconfigure(1, weight=1)
        packages.grid_columnconfigure(2, weight=1)

        package_definitions = self._get_billing_package_definitions()
        for index, package in enumerate(package_definitions):
            row = index // 3
            column = index % 3
            self._build_billing_package_card(packages, package).grid(
                row=row,
                column=column,
                sticky="nsew",
                padx=(0, 8) if column == 0 else (8, 8) if column == 1 else (8, 0),
                pady=(0, 12),
            )

        footer = ttk.Frame(frame, style="Root.TFrame")
        footer.pack(fill=X, pady=(4, 0))

        status_card = ttk.Frame(footer, padding=(14, 12), style="BillingStatus.TFrame")
        status_card.pack(side=LEFT, fill=BOTH, expand=True)
        ttk.Label(status_card, text="Billing Status", style="BillingSectionTitle.TLabel").pack(anchor=W)
        ttk.Label(
            status_card,
            textvariable=self.billing_status_var,
            style="BillingStatus.TLabel",
            wraplength=520,
            justify=LEFT,
        ).pack(anchor=W, pady=(8, 0))

        actions = ttk.Frame(footer, padding=(14, 12), style="BillingActions.TFrame")
        actions.pack(side=RIGHT, fill=Y, padx=(12, 0))
        ttk.Label(actions, text="After Payment", style="BillingSectionTitle.TLabel").pack(anchor=W)
        ttk.Label(
            actions,
            text="1. Complete checkout\n2. Click Confirm Purchase\n3. Refresh Balance if needed",
            style="BillingMuted.TLabel",
            justify=LEFT,
        ).pack(anchor=W, pady=(6, 10))
        ttk.Button(actions, text="Confirm Purchase", command=self._confirm_checkout, style="Accent.TButton", width=20).pack(anchor=E)
        ttk.Button(actions, text="Refresh Balance", command=self._refresh_billing_status, width=20).pack(anchor=E, pady=(8, 0))
        ttk.Button(actions, text="Close", command=self._close_billing_window, width=20).pack(anchor=E, pady=(8, 0))

    def _close_billing_window(self) -> None:
        window = self.billing_window
        self.billing_window = None
        if window and window.winfo_exists():
            try:
                window.grab_release()
            except tk.TclError:
                pass
            window.destroy()

    def _format_billing_price(self, cents: int) -> str:
        return f"${cents / 100:,.2f}"

    def _get_billing_package_definitions(self) -> list[dict[str, object]]:
        return [
            {
                "title": "$5 Package",
                "credits": 16,
                "price_cents": 500,
                "accent": False,
                "badge": "Starter",
                "summary": "Great for one focused job or quick testing runs with reliable enhancement quality.",
            },
            {
                "title": "$10 Package",
                "credits": 34,
                "price_cents": 1000,
                "accent": True,
                "badge": "Most Popular",
                "summary": "Better value per credit for regular creators handling multiple clips per session.",
            },
            {
                "title": "$20 Package",
                "credits": 72,
                "price_cents": 2000,
                "accent": False,
                "badge": "Best Value",
                "summary": "Highest value tier for bigger queues, reruns, and production-scale enhancement batches.",
            },
        ]

    def _build_billing_package_card(self, parent: tk.Misc, package: dict[str, object]) -> tk.Frame:
        title = str(package["title"])
        credits = int(package["credits"])
        package_price_cents = int(package.get("price_cents", credits * max(1, int(self.billing_backend.price_per_credit_cents))))
        accent = bool(package.get("accent", False))
        badge = str(package.get("badge", ""))
        summary = str(package.get("summary", ""))
        total_cents = package_price_cents
        per_credit_cents = max(1, int(round(total_cents / max(1, credits))))

        bg = "#142846" if accent else "#101d32"
        border = "#6de1ff" if accent else "#2b4870"
        title_fg = "#f6fbff" if accent else "#e6f0ff"
        meta_fg = "#abd4ff" if accent else "#9fb8d8"
        price_fg = "#7df6c7" if accent else "#dff7ff"

        card = tk.Frame(parent, bg=bg, highlightbackground=border, highlightthickness=1, bd=0, padx=16, pady=16)

        header = tk.Frame(card, bg=bg)
        header.pack(fill=X)
        tk.Label(header, text=badge, bg=bg, fg="#6de1ff", font=("Segoe UI", 9, "bold")).pack(anchor="w")
        tk.Label(header, text=title, bg=bg, fg=title_fg, font=("Segoe UI", 15, "bold")).pack(anchor="w", pady=(6, 0))
        tk.Label(header, text=f"{credits} credits", bg=bg, fg=meta_fg, font=("Segoe UI", 10)).pack(anchor="w", pady=(2, 0))

        ttk.Separator(card).pack(fill=X, pady=12)

        tk.Label(card, text=self._format_billing_price(total_cents), bg=bg, fg=price_fg, font=("Segoe UI", 21, "bold")).pack(anchor="w")
        tk.Label(
            card,
            text=f"{self._format_billing_price(per_credit_cents)} per processing credit",
            bg=bg,
            fg=meta_fg,
            font=("Segoe UI", 9),
        ).pack(anchor="w", pady=(4, 10))
        tk.Label(
            card,
            text=summary,
            bg=bg,
            fg="#d8e6ff",
            font=("Segoe UI", 9),
            justify="left",
            wraplength=220,
        ).pack(anchor="w")

        ttk.Button(
            card,
            text=f"Buy {title}",
            command=lambda: self._purchase_credit_package(credits, package_price_cents, title),
            style="Accent.TButton" if accent else "TButton",
        ).pack(fill=X, pady=(16, 0))
        return card

    def _purchase_credit_package(self, credits: int, charge_cents: int | None = None, package_name: str | None = None) -> None:
        self.checkout_credits_var.set(int(credits))
        self.checkout_amount_cents_override = max(1, int(charge_cents)) if charge_cents is not None else None
        self.checkout_package_name_override = str(package_name or "").strip()
        self._start_checkout()

    def _start_checkout(self) -> None:
        token = self.billing_token_var.get().strip()
        if not token:
            token = self._generate_billing_token()
            self.billing_token_var.set(token)
        if not is_valid_paid_access_token(token):
            messagebox.showwarning("Invalid Token", "Billing token format is invalid. Generate a new token or fix the current one.")
            return

        credits = int(self.checkout_credits_var.get())
        if credits <= 0:
            messagebox.showwarning("Invalid Credits", "Credits must be greater than 0.")
            return

        charge_cents = self.checkout_amount_cents_override
        package_name = self.checkout_package_name_override

        try:
            if self._use_embedded_billing():
                data = self.billing_backend.create_checkout_session(
                    token,
                    credits,
                    charge_cents=charge_cents,
                    package_name=package_name,
                )
            else:
                url = self._billing_endpoint("/api/payments/create-checkout-session")
                payload = {
                    "token": token,
                    "credits": str(credits),
                }
                if charge_cents is not None:
                    payload["charge_cents"] = str(int(charge_cents))
                if package_name:
                    payload["package_name"] = package_name
                status_code, data = self._post_form_json(url, payload)
                if status_code != 200:
                    raise RuntimeError(str(data.get("error", f"HTTP {status_code}")))
            checkout_url = str(data.get("url", "")).strip()
            session_id = str(data.get("session_id", "")).strip()
        except Exception as exc:
            err = str(exc)
            self.billing_status_var.set(f"Checkout failed: {err}")
            self.log_queue.put(f"[ERROR] Checkout failed: {err}")
            self.checkout_amount_cents_override = None
            self.checkout_package_name_override = ""
            return

        self.checkout_url_var.set(checkout_url)
        self.checkout_session_var.set(session_id)
        self._save_billing_state()

        self.billing_status_var.set("Checkout created. Complete payment, then click Confirm Payment.")
        self.log_queue.put(f"[INFO] Checkout session created: {session_id}")
        self._open_checkout_window(checkout_url)
        self.checkout_amount_cents_override = None
        self.checkout_package_name_override = ""

    def _confirm_checkout(self) -> None:
        session_id = self.checkout_session_var.get().strip()
        if not session_id:
            messagebox.showwarning("Session Required", "Paste or generate a checkout session ID first.")
            return

        try:
            if self._use_embedded_billing():
                data = self.billing_backend.confirm_checkout_session(session_id)
            else:
                url = self._billing_endpoint("/api/payments/confirm-session")
                status_code, data = self._post_form_json(url, {"session_id": session_id})
                if status_code != 200:
                    raise RuntimeError(str(data.get("error", f"HTTP {status_code}")))
        except Exception as exc:
            err = str(exc)
            self.billing_status_var.set(f"Confirm failed: {err}")
            self.log_queue.put(f"[ERROR] Confirm failed: {err}")
            return

        credited = int(data.get("credited_credits", 0) or 0)
        already_processed = bool(data.get("already_processed", False))
        if already_processed:
            msg = f"Payment already processed earlier. Session: {session_id}"
        else:
            msg = f"Payment confirmed. Credits added: {credited}. Session: {session_id}"

        self.billing_status_var.set(msg)
        self.log_queue.put(f"[INFO] {msg}")
        self._save_billing_state()
        self._refresh_billing_status(silent=True)

        if (not already_processed) and credited > 0:
            self._prompt_email_backup_after_purchase()

    def _prompt_email_backup_after_purchase(self) -> None:
        default_email = self.recovery_email_var.get().strip()
        email = simpledialog.askstring(
            "Email Backup",
            "Enter your email to receive your access code backup:",
            initialvalue=default_email,
            parent=self.billing_window if self.billing_window and self.billing_window.winfo_exists() else self,
        )
        if email is None:
            self.log_queue.put("[INFO] User skipped backup email prompt.")
            return

        normalized = self._normalize_email(email)
        if not normalized:
            messagebox.showwarning("Email Required", "Email was empty. You can set it later from billing/account recovery.")
            return

        self.recovery_email_var.set(normalized)
        self._link_email_to_current_token()
        token = self.billing_token_var.get().strip()
        if token and is_valid_paid_access_token(token):
            ok, msg = self._send_recovery_email(normalized, token)
            self.billing_status_var.set(msg)
            if ok:
                self.log_queue.put(f"[INFO] Access code backup email sent to {normalized}.")
            else:
                self.log_queue.put(f"[WARN] {msg}")

    def _set_selected_button_group(self, buttons: dict[str, ttk.Button], selected_name: str) -> None:
        for name, btn in buttons.items():
            if name == selected_name:
                btn.configure(style="ProfileSelected.TButton")
            else:
                btn.configure(style="Profile.TButton")

    def _clear_advanced_overrides_for_profile_selection(self) -> None:
        if self.advanced_window and self.advanced_window.winfo_exists():
            self._close_advanced_options_window(apply_changes=False)
        if not self.advanced_overrides_active:
            return
        self.advanced_overrides_active = False
        messagebox.showinfo(
            "Advanced Settings Overridden",
            "Selecting a speed profile or upscaling profile restores that profile's default settings and overrides your saved advanced adjustments.",
        )

    def _set_selected_speed_profile(self, profile_name: str, apply: bool = True) -> None:
        if apply:
            self._clear_advanced_overrides_for_profile_selection()
        self.selected_speed_profile = profile_name
        self._set_selected_button_group(self.speed_profile_buttons, profile_name)
        if apply:
            self._apply_combined_profile()

    def _set_selected_upscaling_profile(self, profile_name: str, apply: bool = True) -> None:
        if apply:
            self._clear_advanced_overrides_for_profile_selection()
        self.selected_upscaling_profile = profile_name
        self._set_selected_button_group(self.upscaling_profile_buttons, profile_name)
        if apply:
            self._apply_combined_profile()

    def _apply_combined_profile(self) -> None:
        speed_name = self.selected_speed_profile
        upscaling_name = self.selected_upscaling_profile

        # Baseline v11b quality stack (used as a starting point for every profile).
        self.enable_color_var.set(True)
        self.vibrance_var.set(0.35)
        self.contrast_var.set(1.10)
        self.brightness_var.set(0.04)
        self.saturation_var.set(1.25)
        self.gamma_var.set(1.06)
        self.enable_sharpen_var.set(True)
        self.cas_strength_var.set(0.80)
        self.unsharp1_var.set(1.5)
        self.unsharp2_var.set(0.8)

        # Speed presets control throughput/encode behavior first.
        if speed_name == "fast":
            self.image_format_var.set("jpg")
            self.enable_interpolation_var.set(False)
            self.target_fps_var.set(30)
            self.encode_preset_var.set("veryfast")
            self.crf_var.set(22)
            self.apply_final_scale_var.set(False)
        elif speed_name == "quality":
            self.image_format_var.set("png")
            self.enable_interpolation_var.set(True)
            self.target_fps_var.set(60)
            self.encode_preset_var.set("slow")
            self.crf_var.set(16)
            self.apply_final_scale_var.set(True)
        else:
            # Balanced: keep quality-oriented pipeline while trimming runtime vs Max Detail.
            self.image_format_var.set("png")
            self.enable_interpolation_var.set(False)
            self.target_fps_var.set(30)
            self.encode_preset_var.set("medium")
            self.crf_var.set(17)
            self.apply_final_scale_var.set(True)

        # Upscaling presets control model strength and cleanup behavior.
        if upscaling_name == "live":
            self.model_var.set("realesrgan-x4plus")
            if speed_name == "fast":
                self.scale_var.set(2)
                self.denoise_var.set(0.2)
                self.cas_strength_var.set(0.55)
            elif speed_name == "balanced":
                self.scale_var.set(3)
                self.denoise_var.set(0.2)
                self.cas_strength_var.set(0.65)
            else:
                self.scale_var.set(4)
                self.denoise_var.set(0.2)
                self.cas_strength_var.set(0.80)
        elif upscaling_name == "restore":
            self.model_var.set("realesrgan-x4plus")
            if speed_name == "fast":
                self.scale_var.set(2)
                self.denoise_var.set(0.8)
                self.cas_strength_var.set(0.55)
            elif speed_name == "balanced":
                self.scale_var.set(3)
                self.denoise_var.set(0.8)
                self.cas_strength_var.set(0.65)
            else:
                self.scale_var.set(4)
                self.denoise_var.set(0.8)
                self.cas_strength_var.set(0.80)
        else:
            # Animation / Anime
            if speed_name == "fast":
                # Quick Preview: fastest enhancement path.
                self.model_var.set("realesr-animevideov3-x2")
                self.scale_var.set(2)
                self.denoise_var.set(0.0)
                self.enable_sharpen_var.set(False)
                self.cas_strength_var.set(0.50)
            elif speed_name == "balanced":
                # Balanced: quality-first but meaningfully faster than Max Detail.
                self.model_var.set("realesr-animevideov3-x3")
                self.scale_var.set(3)
                self.denoise_var.set(0.0)
                self.enable_sharpen_var.set(True)
                self.cas_strength_var.set(0.75)
            else:
                # Max Detail + Animation: match original v11b strong settings.
                self.model_var.set("realesrgan-x4plus-anime")
                self.scale_var.set(4)
                self.denoise_var.set(0.0)
                self.enable_sharpen_var.set(True)
                self.cas_strength_var.set(0.80)
                self.unsharp1_var.set(1.5)
                self.unsharp2_var.set(0.8)

        if self.apply_final_scale_var.get():
            self.target_width_var.set(2430)
            self.target_height_var.set(4320)

        self._refresh_auto_thread_recommendation()
        self._sync_display_from_model()
        speed_label = {
            "fast": "Quick Preview",
            "balanced": "Balanced Workflow",
            "quality": "Max Detail",
        }.get(speed_name, speed_name)
        upscaling_label = {
            "live": "Natural Footage",
            "animation": "Animation / Anime",
            "restore": "Legacy / Noisy Repair",
        }.get(upscaling_name, upscaling_name)
        self._sync_target_fps_to_source_if_needed()
        self.estimate_var.set(f"Profile set: {speed_label} + {upscaling_label}.")

    def _set_selected_profile(self, profile_name: str) -> None:
        # Backward-compatible shim for older calls.
        if profile_name in self.speed_profile_buttons:
            self._set_selected_speed_profile(profile_name)
        elif profile_name in self.upscaling_profile_buttons:
            self._set_selected_upscaling_profile(profile_name)

    def _schedule_auto_estimate(self, delay_ms: int = 350) -> None:
        if self._estimate_after_id:
            self.after_cancel(self._estimate_after_id)
        self._estimate_after_id = self.after(delay_ms, lambda: self._estimate_time(silent=True))

    def _handle_interpolation_toggle(self) -> None:
        self._normalize_interpolation_choice(show_feedback=True)
        self._sync_target_fps_to_source_if_needed()
        self._schedule_auto_estimate()

    def _register_auto_estimate_watchers(self) -> None:
        watched_vars = [
            self.model_var,
            self.scale_var,
            self.image_format_var,
            self.threads_var,
            self.start_time_var,
            self.clip_duration_var,
            self.denoise_var,
            self.enable_color_var,
            self.vibrance_var,
            self.contrast_var,
            self.brightness_var,
            self.saturation_var,
            self.gamma_var,
            self.enable_sharpen_var,
            self.cas_strength_var,
            self.unsharp1_var,
            self.unsharp2_var,
            self.enable_interpolation_var,
            self.target_fps_var,
            self.apply_final_scale_var,
            self.target_width_var,
            self.target_height_var,
            self.crf_var,
            self.encode_preset_var,
            self.include_audio_var,
        ]
        for var in watched_vars:
            var.trace_add("write", lambda *_args: self._schedule_auto_estimate())
        self.enable_interpolation_var.trace_add("write", lambda *_args: self._handle_interpolation_toggle())

    def _auto_prepare_after_input(self) -> None:
        self.log_queue.put("[INFO] New input selected. Running estimate and compare frame automatically...")
        self.after(120, lambda: self._estimate_time(silent=True))
        self.after(300, lambda: self._generate_compare_frame(silent=True))

    def _pick_output(self) -> None:
        file_path = filedialog.asksaveasfilename(
            title="Select output video",
            defaultextension=".mp4",
            filetypes=[("MP4", "*.mp4"), ("All files", "*.*")],
        )
        if file_path:
            self.output_video_var.set(file_path)

    @staticmethod
    def _safe_name(value: str) -> str:
        return "".join(ch for ch in value if ch.isalnum() or ch in ("-", "_")) or "preview"

    def _sync_model_from_display(self) -> None:
        label = self.model_display_var.get().strip()
        key = MODEL_LABEL_TO_KEY.get(label)
        if key:
            self.model_var.set(key)

    def _sync_display_from_model(self) -> None:
        key = self.model_var.get().strip()
        self.model_display_var.set(MODEL_KEY_TO_LABEL.get(key, MODEL_KEY_TO_LABEL["realesrgan-x4plus-anime"]))

    def _generate_compare_frame(self, silent: bool = False) -> None:
        if not self.input_video_var.get().strip():
            return
        if not PIL_AVAILABLE:
            msg = "Install Pillow to enable frame compare: pip install pillow"
            if silent:
                return
            else:
                messagebox.showerror("Missing dependency", msg)
            return
        if self.compare_worker_thread and self.compare_worker_thread.is_alive():
            self._compare_regen_pending = True
            if not silent:
                messagebox.showwarning("Busy", "Compare frame generation is running. It will regenerate again after completion.")
            return

        try:
            settings = self._validate_settings()
        except Exception as exc:
            if silent:
                return
            else:
                messagebox.showerror("Invalid settings", str(exc))
            return

        self.log_queue.put("[INFO] Preparing before/after compare frame...")
        self.compare_worker_thread = threading.Thread(
            target=self._generate_compare_worker,
            args=(settings,),
            daemon=True,
        )
        self.compare_worker_thread.start()

    def _schedule_auto_compare(self, delay_ms: int = 700) -> None:
        if self._compare_after_id:
            self.after_cancel(self._compare_after_id)
        self._compare_after_id = self.after(delay_ms, lambda: self._generate_compare_frame(silent=True))

    def _register_auto_compare_watchers(self) -> None:
        watched_vars = [
            self.model_var,
            self.scale_var,
            self.image_format_var,
            self.threads_var,
            self.start_time_var,
            self.clip_duration_var,
            self.denoise_var,
            self.enable_color_var,
            self.vibrance_var,
            self.contrast_var,
            self.brightness_var,
            self.saturation_var,
            self.gamma_var,
            self.enable_sharpen_var,
            self.cas_strength_var,
            self.unsharp1_var,
            self.unsharp2_var,
            self.enable_interpolation_var,
            self.target_fps_var,
            self.apply_final_scale_var,
            self.target_width_var,
            self.target_height_var,
            self.crf_var,
            self.encode_preset_var,
            self.include_audio_var,
        ]
        for var in watched_vars:
            var.trace_add("write", lambda *_args: self._schedule_auto_compare())

    def _generate_compare_worker(self, settings: PipelineSettings) -> None:
        try:
            exe_path = _REALESRGAN_EXE
            if not exe_path.exists():
                raise FileNotFoundError(f"realesrgan-ncnn-vulkan.exe not found (looked in: {exe_path.parent})")

            compare_root = Path(tempfile.gettempdir()) / "pixelforge_compare" / self._safe_name(settings.input_video.stem)
            compare_root.mkdir(parents=True, exist_ok=True)

            before_frame = compare_root / "before.png"
            pre_frame = compare_root / "pre.png"
            upscaled_frame = compare_root / "upscaled.png"
            after_frame = compare_root / "after.png"

            trim_args: list[str] = []
            if settings.start_time > 0:
                trim_args.extend(["-ss", str(settings.start_time)])

            extract_cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error"]
            extract_cmd += trim_args
            extract_cmd += ["-i", str(settings.input_video), "-frames:v", "1", str(before_frame)]
            subprocess.run(extract_cmd, check=True, creationflags=_NO_WINDOW)

            pre_filter = PipelineRunner(settings, self.log_queue, self.stop_event)._build_pre_filter()
            source_for_upscale = before_frame
            if pre_filter:
                pre_cmd = [
                    "ffmpeg",
                    "-y",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-i",
                    str(before_frame),
                    "-vf",
                    pre_filter,
                    str(pre_frame),
                ]
                subprocess.run(pre_cmd, check=True, creationflags=_NO_WINDOW)
                source_for_upscale = pre_frame

            upscale_cmd = [
                str(exe_path),
                "-i",
                str(source_for_upscale),
                "-o",
                str(upscaled_frame),
                "-n",
                settings.model,
                "-s",
                str(settings.scale),
                "-f",
                "png",
                "-j",
                settings.threads,
            ]
            subprocess.run(upscale_cmd, check=True, creationflags=_NO_WINDOW)

            post_filter = PipelineRunner(settings, self.log_queue, self.stop_event)._build_post_filter()
            if post_filter:
                post_cmd = [
                    "ffmpeg",
                    "-y",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-i",
                    str(upscaled_frame),
                    "-vf",
                    post_filter,
                    str(after_frame),
                ]
                subprocess.run(post_cmd, check=True, creationflags=_NO_WINDOW)
            else:
                after_frame.write_bytes(upscaled_frame.read_bytes())

            before_img = Image.open(before_frame).convert("RGB")
            after_img = Image.open(after_frame).convert("RGB")
            self.after(0, lambda: self._set_compare_images(before_img, after_img))
            self.log_queue.put("[INFO] Compare frame ready. Drag the separator line to inspect before/after.")
        except Exception as exc:
            self.log_queue.put(f"[ERROR] Compare frame error: {exc}")
            for hint in self._troubleshooting_hints(str(exc)):
                self.log_queue.put(f"[HINT] {hint}")
        finally:
            if self._compare_regen_pending:
                self._compare_regen_pending = False
                self.after(120, lambda: self._generate_compare_frame(silent=True))

    def _open_large_compare_window(self) -> None:
        if self.compare_window and self.compare_window.winfo_exists():
            self.compare_window.focus_force()
            self._redraw_compare_canvas()
            return

        window = tk.Toplevel(self)
        window.title("Compare View - v11b")
        window.geometry("1280x820")
        window.configure(bg="#0a1220")
        self.compare_window = window

        top = ttk.Frame(window, padding=10, style="Root.TFrame")
        top.pack(fill=BOTH, expand=True)

        controls = ttk.Frame(top, style="Panel.TFrame")
        controls.pack(fill=X)
        ttk.Label(controls, text="Before / After", style="Hint.TLabel").pack(side=LEFT)
        ttk.Scale(
            controls,
            from_=0,
            to=100,
            variable=self.compare_slider_var,
            command=lambda _v: self._redraw_compare_canvas(),
        ).pack(side=LEFT, fill=X, expand=True, padx=(8, 8))

        ttk.Button(
            controls,
            text="Toggle Fullscreen",
            command=lambda: window.attributes("-fullscreen", not bool(window.attributes("-fullscreen"))),
        ).pack(side=LEFT)

        canvas = tk.Canvas(
            top,
            bg="#090f1b",
            highlightthickness=1,
            highlightbackground="#355c93",
        )
        canvas.pack(fill=BOTH, expand=True, pady=(8, 0))
        canvas.bind("<Configure>", lambda _event: self._redraw_compare_canvas())
        self.compare_canvas_large = canvas
        window.bind("<Escape>", lambda _event: window.attributes("-fullscreen", False))
        self._redraw_compare_canvas()

    def _set_compare_images(self, before_image: Image.Image, after_image: Image.Image) -> None:
        self.compare_before_pil = before_image
        self.compare_after_pil = after_image
        self.compare_slider_var.set(50.0)
        self._redraw_compare_canvas()

    def _render_compare_to_canvas(self, canvas: tk.Canvas, photo_attr: str) -> None:
        if not PIL_AVAILABLE:
            return
        if self.compare_before_pil is None or self.compare_after_pil is None:
            return

        canvas_w = max(2, canvas.winfo_width())
        canvas_h = max(2, canvas.winfo_height())

        before_fit = self.compare_before_pil.resize((canvas_w, canvas_h), RESAMPLE_FILTER)
        after_fit = self.compare_after_pil.resize((canvas_w, canvas_h), RESAMPLE_FILTER)

        split = int((self.compare_slider_var.get() / 100.0) * canvas_w)
        split = max(1, min(canvas_w - 1, split))
        composite = before_fit.copy()
        composite.paste(after_fit.crop((split, 0, canvas_w, canvas_h)), (split, 0))

        draw = ImageDraw.Draw(composite)
        is_hover = bool(getattr(self, "compare_hover_near_line", False))
        line_width = 4 if is_hover else 3
        line_color = "#00ffff" if is_hover else "#00e7ff"
        for offset in range(-line_width//2, line_width//2 + 1):
            draw.line([(split + offset, 0), (split + offset, canvas_h)], fill=line_color, width=1)

        # Draw an oval drag handle with shadow + gradient for a subtle 3D look.
        if canvas_h >= 18 and canvas_w >= 18:
            handle_h = min(64, max(12, canvas_h - 10))
            handle_w = 18
            handle_top = max(4, (canvas_h - handle_h) // 2)
            handle_bottom = min(canvas_h - 4, handle_top + handle_h)
            handle_left = split - handle_w // 2
            handle_right = split + handle_w // 2
            radius = max(3, handle_w // 2)

            # Guard against invalid draw boxes on tiny canvases.
            if handle_bottom > handle_top and handle_right > handle_left:
                # Soft drop shadow.
                shadow_dx = 2
                shadow_dy = 3
                shadow_box = (
                    handle_left + shadow_dx,
                    handle_top + shadow_dy,
                    handle_right + shadow_dx,
                    handle_bottom + shadow_dy,
                )
                draw.rounded_rectangle(shadow_box, radius=radius, fill="#00000088")

                # Vertical gradient fill.
                top_rgb = (95, 224, 255) if is_hover else (70, 177, 220)
                bottom_rgb = (18, 72, 108) if is_hover else (14, 52, 82)
                inner_left = handle_left + 1
                inner_right = handle_right - 1
                inner_top = handle_top + 1
                inner_bottom = handle_bottom - 1
                gradient_height = max(1, inner_bottom - inner_top)
                for y in range(inner_top, inner_bottom + 1):
                    t = (y - inner_top) / float(gradient_height)
                    r = int(top_rgb[0] + (bottom_rgb[0] - top_rgb[0]) * t)
                    g = int(top_rgb[1] + (bottom_rgb[1] - top_rgb[1]) * t)
                    b = int(top_rgb[2] + (bottom_rgb[2] - top_rgb[2]) * t)
                    draw.line([(inner_left, y), (inner_right, y)], fill=(r, g, b))

                handle_outline = "#bff7ff" if is_hover else "#88dbef"
                draw.rounded_rectangle((handle_left, handle_top, handle_right, handle_bottom), radius=radius, outline=handle_outline, width=2)

        # Store separator position for hover detection (only on main canvas)
        if canvas is self.compare_canvas:
            self.compare_separator_x = split
        draw.rectangle((10, 10, 120, 36), fill="#0f1f36")
        draw.rectangle((canvas_w - 130, 10, canvas_w - 10, 36), fill="#0f1f36")
        draw.text((18, 17), "Before", fill="#cde3ff")
        draw.text((canvas_w - 122, 17), "After", fill="#cde3ff")

        photo = ImageTk.PhotoImage(composite)
        setattr(self, photo_attr, photo)
        canvas.delete("all")
        canvas.create_image(0, 0, anchor="nw", image=photo)
        icon_size = 28
        pad = 10
        x2 = canvas_w - pad
        y2 = canvas_h - pad
        x1 = x2 - icon_size
        y1 = y2 - icon_size
        canvas.create_rectangle(x1, y1, x2, y2, fill="#0f1f36", outline="#6de1ff", width=1, tags=("fullscreen_icon",))
        # Draw a standard fullscreen-style corner glyph instead of [] text.
        c = "#cde3ff"
        m = 6
        l = 5
        # Top-left corner
        canvas.create_line(x1 + m, y1 + m + l, x1 + m, y1 + m, x1 + m + l, y1 + m, fill=c, width=2, tags=("fullscreen_icon",))
        # Top-right corner
        canvas.create_line(x2 - m - l, y1 + m, x2 - m, y1 + m, x2 - m, y1 + m + l, fill=c, width=2, tags=("fullscreen_icon",))
        # Bottom-left corner
        canvas.create_line(x1 + m, y2 - m - l, x1 + m, y2 - m, x1 + m + l, y2 - m, fill=c, width=2, tags=("fullscreen_icon",))
        # Bottom-right corner
        canvas.create_line(x2 - m - l, y2 - m, x2 - m, y2 - m, x2 - m, y2 - m - l, fill=c, width=2, tags=("fullscreen_icon",))
        canvas.tag_bind("fullscreen_icon", "<Button-1>", lambda _event: self._open_large_compare_window())

    def _redraw_compare_canvas(self) -> None:
        self._render_compare_to_canvas(self.compare_canvas, "compare_photo")
        if self.compare_canvas_large and self.compare_canvas_large.winfo_exists():
            self._render_compare_to_canvas(self.compare_canvas_large, "compare_photo_large")

    def _on_compare_mouse_press(self, event) -> None:
        """Handle mouse press on compare canvas to start dragging separator."""
        self.compare_dragging = True
        self.compare_canvas.configure(cursor="hand2")
        self._on_compare_mouse_drag(event)

    def _on_compare_mouse_drag(self, event) -> None:
        """Handle mouse drag on compare canvas to move separator."""
        if not self.compare_dragging:
            return
        canvas_w = max(2, self.compare_canvas.winfo_width())
        if canvas_w <= 0:
            return
        # Calculate position as percentage (0-100)
        position = max(0, min(100, (event.x / canvas_w) * 100))
        self.compare_slider_var.set(position)
        self._redraw_compare_canvas()

    def _on_compare_mouse_release(self, event) -> None:
        """Handle mouse release on compare canvas to stop dragging separator."""
        self.compare_dragging = False
        self.compare_canvas.configure(cursor="hand2" if self.compare_hover_near_line else "arrow")

    def _on_compare_mouse_motion(self, event) -> None:
        """Track hover near separator and show drag cursor/highlight."""
        hover_zone = 12
        is_near = abs(event.x - self.compare_separator_x) <= hover_zone
        if is_near != self.compare_hover_near_line:
            self.compare_hover_near_line = is_near
            self.compare_canvas.configure(cursor="hand2" if is_near else "arrow")
            self._redraw_compare_canvas()

    def _on_compare_mouse_leave(self, event=None) -> None:
        """Reset hover state/cursor when mouse leaves compare canvas."""
        if self.compare_hover_near_line:
            self.compare_hover_near_line = False
            self.compare_canvas.configure(cursor="arrow")
            self._redraw_compare_canvas()

    @staticmethod
    def _troubleshooting_hints(error_text: str) -> list[str]:
        text = error_text.lower()
        hints: list[str] = []
        if "ffmpeg" in text and "not found" in text:
            hints.append("FFmpeg is missing from PATH. Install FFmpeg and restart the app.")
        if "ffprobe" in text and "not found" in text:
            hints.append("ffprobe is missing from PATH. Ensure FFmpeg tools are installed correctly.")
        if "realesrgan-ncnn-vulkan.exe" in text and "not found" in text:
            hints.append("Keep realesrgan-ncnn-vulkan.exe in the same folder as this script.")
        if "permission denied" in text:
            hints.append("Check file/folder permissions and verify output path is writable.")
        if "out of memory" in text or "cannot allocate" in text:
            hints.append("Reduce scale/FPS, disable interpolation, or use a shorter clip for testing.")
        if "exit code" in text:
            hints.append("Review the command logs above; the failing stage and command are shown.")
        if not hints:
            hints.append("Try Fast Draft profile and a short clip duration to isolate the issue quickly.")
        return hints

    def _validate_settings(self) -> PipelineSettings:
        input_raw = self.input_video_var.get().strip()
        output_raw = self.output_video_var.get().strip()

        if not input_raw:
            raise ValueError("Select an input video first")
        if not output_raw:
            raise ValueError("Select an output video path first")

        input_path = Path(input_raw)
        output_path = Path(output_raw)

        if not input_path.exists() or not input_path.is_file():
            raise ValueError("Select a valid input video")
        try:
            probe = [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=codec_name,width,height",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                PipelineRunner._cli_path(input_path),
            ]
            probe_out = subprocess.check_output(probe, text=True, encoding="utf-8", errors="replace", creationflags=_NO_WINDOW).strip()
            if not probe_out:
                raise ValueError("Input does not contain a readable video stream")
        except subprocess.CalledProcessError as exc:
            raise ValueError(f"Input format is not readable by ffmpeg/ffprobe: {exc}") from exc
        except FileNotFoundError as exc:
            raise ValueError(f"ffprobe not found on PATH: {exc}") from exc
        if output_path.exists() and output_path.is_dir():
            raise ValueError("Output path points to a folder; choose a file name")
        if not output_path.parent.exists():
            raise ValueError("Output folder does not exist")
        if self.model_var.get().strip() not in MODEL_OPTIONS:
            raise ValueError("Select a valid Real-ESRGAN model")
        if self.target_width_var.get() < 360 or self.target_height_var.get() < 360:
            raise ValueError("Target width and height must be at least 360")
        if self.crf_var.get() < 12 or self.crf_var.get() > 30:
            raise ValueError("CRF must be between 12 and 30")

        return PipelineSettings(
            input_video=input_path,
            output_video=output_path,
            model=self.model_var.get().strip(),
            scale=int(self.scale_var.get()),
            image_format=self.image_format_var.get().strip(),
            threads=self.threads_var.get().strip() or "2:2:2",
            start_time=float(self.start_time_var.get()),
            clip_duration=float(self.clip_duration_var.get()),
            denoise=float(self.denoise_var.get()),
            enable_color=bool(self.enable_color_var.get()),
            vibrance=float(self.vibrance_var.get()),
            contrast=float(self.contrast_var.get()),
            brightness=float(self.brightness_var.get()),
            saturation=float(self.saturation_var.get()),
            gamma=float(self.gamma_var.get()),
            enable_sharpen=bool(self.enable_sharpen_var.get()),
            cas_strength=float(self.cas_strength_var.get()),
            unsharp1=float(self.unsharp1_var.get()),
            unsharp2=float(self.unsharp2_var.get()),
            enable_interpolation=bool(self.enable_interpolation_var.get()),
            target_fps=int(self.target_fps_var.get()),
            apply_final_scale=bool(self.apply_final_scale_var.get()),
            target_width=int(self.target_width_var.get()),
            target_height=int(self.target_height_var.get()),
            crf=int(self.crf_var.get()),
            encode_preset=self.encode_preset_var.get().strip(),
            include_audio=bool(self.include_audio_var.get()),
            keep_intermediate=bool(self.keep_intermediate_var.get()),
        )

    def _estimate_time(self, silent: bool = False) -> None:
        self._start_system_detection()
        if not self.input_video_var.get().strip():
            self.start_button_credit_var.set("(0 credits)")
            self.estimate_summary_var.set("")
            self.estimate_source_var.set("")
            self.estimate_spec_var.set("")
            self.estimate_stage_var.set("")
            self.estimate_tips_var.set("")
            self._set_estimate_visibility(has_input=False)
            return
        try:
            settings = self._validate_settings()
        except Exception as exc:
            self.start_button_credit_var.set("(0 credits)")
            if silent:
                return
            else:
                messagebox.showerror("Invalid settings", str(exc))
            return

        try:
            _duration, fps, effective_duration, frame_count, src_w, src_h = self._collect_processing_metrics(settings)
        except Exception as exc:
            self.start_button_credit_var.set("(0 credits)")
            if silent:
                return
            else:
                messagebox.showerror("ffprobe error", f"Could not read video metadata: {exc}")
            return

        system_factor, system_note = self._get_system_performance_hint()
        stage_seconds = self._estimate_stage_seconds(settings, fps, effective_duration, frame_count, src_w, src_h)
        total_seconds = sum(stage_seconds.values())

        counts = self.stage_timing_profile.get("counts", {})
        avg_count = sum(int(counts.get(str(i), 0)) for i in range(1, 7)) / 6.0
        spread = 0.60 if avg_count < 2 else 0.45 if avg_count < 5 else 0.30 if avg_count < 12 else 0.22
        lower = max(1, int(total_seconds * (1.0 - spread)))
        upper = max(2, int(total_seconds * (1.0 + spread)))
        lower_eta = self._format_eta(lower)
        upper_eta = self._format_eta(upper)

        tips: list[str] = []
        if settings.enable_interpolation:
            if float(settings.target_fps) <= (float(fps) + 0.5):
                tips.append("Interpolation target is not above source FPS; it will be auto-skipped")
            else:
                tips.append("Disable interpolation or lower target FPS")
        else:
            tips.append(f"Interpolation off: output stays at source FPS ({int(round(fps))})")
        if settings.scale >= 4:
            tips.append("Use scale 2 or 3 while testing")
        if settings.model in {"realesrgan-x4plus", "realesrgan-x4plus-anime"}:
            tips.append("Use animevideov3 x2/x3 model for faster drafts")
        if settings.encode_preset in {"medium", "slow"}:
            tips.append("Use encode preset veryfast or faster")
        if settings.apply_final_scale and settings.target_height >= 4320:
            tips.append("Lower target resolution during preview runs")
        if settings.clip_duration == 0:
            tips.append("Set clip duration for quick spot-checks")

        tip_text = " | ".join(tips[:4]) if tips else "Current settings are already speed-oriented."
        stage_lines = [
            f"S1 Extract: {self._format_eta(stage_seconds[1])}",
            f"S2 Upscale: {self._format_eta(stage_seconds[2])}",
            f"S3 Post: {self._format_eta(stage_seconds[3])}",
            f"S4 Reassemble: {self._format_eta(stage_seconds[4])}",
            f"S5 Interpolate: {self._format_eta(stage_seconds[5])}",
            f"S6 Finalize: {self._format_eta(stage_seconds[6])}",
        ]
        self.estimate_var.set(
            "Estimate: approximately {low_eta} to {high_eta} ({low_s}s to {high_s}s). "
            "Frames: {frames:,}. Source: {w}x{h}. {system}.\n"
            "Stage allocation: {stages}.\n"
            "Time-saving options: {tips}".format(
                low_eta=lower_eta,
                high_eta=upper_eta,
                low_s=lower,
                high_s=upper,
                frames=frame_count,
                w=src_w,
                h=src_h,
                system=system_note,
                stages=" | ".join(stage_lines),
                tips=tip_text,
            )
        )
        self.estimate_summary_var.set(f"{lower_eta} to {upper_eta}")
        self.estimate_source_var.set(f"{src_w}x{src_h}  •  {frame_count:,} frames  •  {fps:.1f} fps")
        self.estimate_spec_var.set(system_note)
        self.estimate_stage_var.set(" | ".join(stage_lines))
        self._set_estimate_visibility(has_input=True)
        self.log_queue.put(
            "[INFO] Estimate updated: {low}s to {high}s for ~{frames:,} frames (system factor {sf:.2f}).".format(
                low=lower,
                high=upper,
                frames=frame_count,
                sf=system_factor,
            )
        )
        credits, breakdown = self._calculate_processing_credit_cost(settings, effective_duration, source_fps=fps)
        self.credit_quote_var.set(f"Render cost: {credits} credit(s). Basis: {breakdown}.")
        self.start_button_credit_var.set(f"({credits} credits)")

    def _get_system_performance_hint(self) -> tuple[float, str]:
        self._start_system_detection()
        if self._system_profile_cache:
            return float(self._system_profile_cache.get("factor", 1.0)), str(self._system_profile_cache.get("note", "System detection in progress..."))
        return 1.0, "System detection in progress..."

    @staticmethod
    def _detect_cpu_name() -> str:
        if os.name == "nt":
            try:
                result = subprocess.run(
                    ["wmic", "cpu", "get", "name"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=3,
                    check=False,
                    creationflags=_NO_WINDOW,
                )
                lines = [line.strip() for line in result.stdout.splitlines() if line.strip() and line.strip().lower() != "name"]
                if lines:
                    return lines[0]
            except Exception:
                pass

            try:
                result = subprocess.run(
                    [
                        "powershell",
                        "-NoProfile",
                        "-Command",
                        "(Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name)",
                    ],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=4,
                    check=False,
                    creationflags=_NO_WINDOW,
                )
                value = (result.stdout or "").strip()
                if value:
                    return value
            except Exception:
                pass

        fallback = (platform.processor() or "").strip()
        return fallback or "Unknown CPU"

    @staticmethod
    def _detect_ram_gb() -> int:
        if os.name != "nt":
            return 16
        class _MemoryStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        try:
            stat = _MemoryStatusEx()
            stat.dwLength = ctypes.sizeof(_MemoryStatusEx)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)) == 0:
                return 16
            return max(1, int(round(stat.ullTotalPhys / (1024**3))))
        except Exception:
            return 16

    @staticmethod
    def _detect_primary_gpu_name() -> str:
        if os.name != "nt":
            return "Unknown GPU"

        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=2,
                check=False,
                creationflags=_NO_WINDOW,
            )
            lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
            if lines:
                return lines[0]
        except Exception:
            pass

        try:
            result = subprocess.run(
                ["wmic", "path", "win32_VideoController", "get", "name"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=3,
                check=False,
                creationflags=_NO_WINDOW,
            )
            lines = [line.strip() for line in result.stdout.splitlines() if line.strip() and line.strip().lower() != "name"]
            if lines:
                preferred = [line for line in lines if "microsoft basic" not in line.lower()]
                return preferred[0] if preferred else lines[0]
        except Exception:
            pass

        try:
            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    "(Get-CimInstance Win32_VideoController | Select-Object -First 1 -ExpandProperty Name)",
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=4,
                check=False,
                creationflags=_NO_WINDOW,
            )
            lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
            if lines:
                preferred = [line for line in lines if "microsoft basic" not in line.lower()]
                return preferred[0] if preferred else lines[0]
        except Exception:
            pass

        return "Unknown GPU"

    def _apply_fast_profile(self) -> None:
        self._set_selected_speed_profile("fast")

    def _apply_balanced_profile(self) -> None:
        self._set_selected_speed_profile("balanced")

    def _apply_quality_profile(self) -> None:
        self._set_selected_speed_profile("quality")

    def _apply_live_profile(self) -> None:
        self._set_selected_upscaling_profile("live")

    def _apply_anime_profile(self) -> None:
        self._set_selected_upscaling_profile("animation")

    def _apply_restore_profile(self) -> None:
        self._set_selected_upscaling_profile("restore")

    def _start_processing(self) -> None:
        self._normalize_interpolation_choice(show_feedback=True)
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Busy", "Processing is already running.")
            return

        try:
            settings = self._validate_settings()
        except Exception as exc:
            messagebox.showerror("Invalid settings", str(exc))
            return

        token = self.billing_token_var.get().strip()
        if not token or not is_valid_paid_access_token(token):
            messagebox.showerror("Billing required", "A valid billing token is required before processing.")
            return

        try:
            _duration, _fps, effective_duration, _frames, _src_w, _src_h = self._collect_processing_metrics(settings)
            required_credits, breakdown = self._calculate_processing_credit_cost(settings, effective_duration, source_fps=_fps)
        except Exception as exc:
            messagebox.showerror("Billing estimate failed", f"Could not calculate processing credits: {exc}")
            return

        consumed, remaining = self.billing_store.consume_credits(token, required_credits, source="v11b_render_start")
        if not consumed:
            self._refresh_billing_status(silent=True)
            messagebox.showwarning(
                "Insufficient Credits",
                f"This render needs {required_credits} credits but only {remaining} are available.\n\n"
                "Open the Billing tab to purchase more credits.",
            )
            return

        self._charged_token = token
        self._charged_credits = required_credits
        self._stop_requested_by_user = False
        self._current_run_output = settings.output_video
        self._reset_progress_state()
        self.credit_quote_var.set(f"Render cost locked: {required_credits} credit(s). Basis: {breakdown}.")
        self.start_button_credit_var.set(f"({required_credits} credits)")
        self.billing_status_var.set(f"Billing: charged {required_credits} credits for this run. Remaining balance: {remaining}.")
        self.log_queue.put(f"[INFO] Charged {required_credits} credits for processing. Remaining balance: {remaining}.")

        self.stop_event.clear()
        self._set_progress_visible(True)
        self._set_processing_controls_active(True)
        self._active_stage_pred_seconds = self._estimate_stage_seconds(settings, _fps, effective_duration, _frames, _src_w, _src_h)
        self.runner = PipelineRunner(settings=settings, log_queue=self.log_queue, stop_event=self.stop_event)
        self.worker_thread = threading.Thread(target=self._run_worker, daemon=True)
        self.worker_thread.start()
        self.log_queue.put("[INFO] Starting v11b pipeline...")
        self.log_queue.put("[DEBUG] Model={model} Scale={scale} FPS={fps} Interpolation={interp} Output={output}".format(
            model=settings.model,
            scale=settings.scale,
            fps=settings.target_fps,
            interp=settings.enable_interpolation,
            output=settings.output_video,
        ))
        self.log_queue.put(f"[DEBUG] Real-ESRGAN threads (-j): {settings.threads}")

    def _run_worker(self) -> None:
        try:
            assert self.runner is not None
            self.runner.run()
            self._cleanup_output_if_canceled()
            was_canceled = self._stop_requested_by_user
            if was_canceled:
                self.log_queue.put("[WARN] Processing canceled by user. No output file was kept.")
            else:
                self.log_queue.put("[INFO] Processing completed successfully.")
            self._charged_token = None
            self._charged_credits = 0
            self._current_run_output = None
            self._stop_requested_by_user = False
            self._active_stage_pred_seconds = {}
            self.after(0, lambda: self._set_total_progress(0.0 if was_canceled else 100.0, allow_decrease=was_canceled))
            self.after(0, lambda: self._set_progress_visible(False))
            self.after(0, lambda: self._set_processing_controls_active(False))
            self.after(0, lambda: self._refresh_billing_status(silent=True))
        except Exception as exc:
            if self._charged_token and self._charged_credits > 0:
                refunded = self.billing_store.restore_credits(self._charged_token, self._charged_credits, source="v11b_render_refund")
                self.log_queue.put(
                    f"[WARN] Refunded {self._charged_credits} credits because processing did not complete. Balance: {refunded}."
                )
                self._charged_token = None
                self._charged_credits = 0
            self._cleanup_output_if_canceled()
            self._current_run_output = None
            self._stop_requested_by_user = False
            self._active_stage_pred_seconds = {}
            self.log_queue.put(f"[ERROR] {exc}")
            for hint in self._troubleshooting_hints(str(exc)):
                self.log_queue.put(f"[HINT] {hint}")
            self.log_queue.put("[DEBUG] Traceback follows:")
            self.log_queue.put(traceback.format_exc())
            self.after(0, lambda: self._set_total_progress(0.0, allow_decrease=True))
            self.after(0, lambda: self._set_progress_visible(False))
            self.after(0, lambda: self._set_processing_controls_active(False))
            self.after(0, lambda: self._refresh_billing_status(silent=True))

    def _stop_processing(self) -> None:
        if not (self.worker_thread and self.worker_thread.is_alive()):
            self.log_queue.put("[WARN] Stop requested, but no active processing run was detected.")
            return

        self._stop_requested_by_user = True
        self.stop_event.set()
        if self.runner and self.runner.current_process:
            try:
                self.runner.current_process.terminate()
            except Exception:
                pass

        if self._charged_token and self._charged_credits > 0:
            refunded = self.billing_store.restore_credits(self._charged_token, self._charged_credits, source="v11b_render_user_stop_refund")
            self.log_queue.put(f"[WARN] Stop pressed: refunded {self._charged_credits} credits. Balance: {refunded}.")
            self.billing_status_var.set(f"Billing: stop requested. Refunded {self._charged_credits} credits. Remaining balance: {refunded}.")
            self._charged_token = None
            self._charged_credits = 0
            self._refresh_billing_status(silent=True)

        self.log_queue.put("[WARN] Stop requested by user.")
        self._set_total_progress(0.0, allow_decrease=True)
        self._set_progress_visible(False)

    def _poll_log_queue(self) -> None:
        while True:
            try:
                message = self.log_queue.get_nowait()
            except Empty:
                break
            if self._handle_progress_message(message):
                continue
            self._update_progress_from_log_fallback(message)
            tag = "info"
            lowered = message.lower()
            if "[error]" in lowered:
                tag = "error"
            elif "[warn]" in lowered:
                tag = "warn"
            elif "[hint]" in lowered:
                tag = "hint"
            elif "[debug]" in lowered:
                tag = "debug"
            self.log_text.insert(END, message + "\n", tag)
            self.log_text.see(END)
        self.after(120, self._poll_log_queue)


def main() -> None:
    app = V11BApp()
    app._register_auto_estimate_watchers()
    app._register_auto_compare_watchers()
    app.mainloop()


if __name__ == "__main__":
    main()
