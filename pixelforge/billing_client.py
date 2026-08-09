"""Knight Account billing client used by PixelForge (and portable to sibling apps)."""

from __future__ import annotations

import json
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


DEFAULT_KNIGHT_API = "https://knightlogics.com/api/pixelforge-license"
LEGACY_PIXELFORGE_API = "https://knightlogics.com/api/pixelforge-license"
# Mistaken migration target that was never deployed (404s in production).
UNDEPLOYED_KNIGHT_ACCOUNT_API = "https://knightlogics.com/api/knight-account"

# Must match MainSite api/_lib/pixelforge-billing.js PIXELFORGE_PLANS exactly.
PIXELFORGE_PACKS = {
    "starter_12": {"credits": 12, "price_cents": 500, "label": "Starter 12"},
    "creator_30": {"credits": 30, "price_cents": 1000, "label": "Creator 30"},
    "studio_72": {"credits": 72, "price_cents": 2000, "label": "Studio 72"},
}

# Server checkout is authoritative. This floor is used only to format local
# fallback estimates when the plan catalog has not loaded yet.
PRICE_PER_CREDIT_CENTS_FLOOR = 27


def is_loopback_url(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return host in {"127.0.0.1", "localhost", "::1"}


class KnightAccountClient:
    """Server-authoritative Knight Logics account API client."""

    PRODUCT_ID = "pixelforge"
    PLAN_BY_CREDITS = {pack["credits"]: plan_id for plan_id, pack in PIXELFORGE_PACKS.items()}

    def __init__(self, api_base: str, machine_id: str, app_version: str, product_id: str = "pixelforge"):
        self.api_base = str(api_base or "").strip().rstrip("/")
        self.machine_id = machine_id
        self.app_version = app_version
        self.product_id = product_id
        self.stripe_mode = "server"
        self.price_per_credit_cents = PRICE_PER_CREDIT_CENTS_FLOOR
        parsed = urlparse(self.api_base)
        if parsed.scheme != "https" and not is_loopback_url(self.api_base):
            raise RuntimeError("Knight billing requires HTTPS (or a loopback developer server).")

    @property
    def endpoint(self) -> str:
        base = self.api_base.rstrip("/")
        # Production billing lives at /api/pixelforge-license (deployed).
        # /api/knight-account was never shipped and 404s — rewrite it.
        if base.endswith("/api/knight-account"):
            return base[: -len("/api/knight-account")] + "/api/pixelforge-license"
        if base.endswith("/api/pixelforge-license") or base.endswith("/pixelforge-license"):
            return base if base.endswith("/api/pixelforge-license") else f"{base}"
        if "/api/" in base:
            return base
        return f"{base}/api/pixelforge-license"

    def _post(self, action: str, payload: dict | None = None, timeout: int = 20, allow_error: bool = False) -> dict:
        body = {
            "action": action,
            "product_id": self.product_id,
            "machine_id": self.machine_id,
            "app_version": self.app_version,
            **(payload or {}),
        }
        request = Request(
            self.endpoint,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                data = json.loads(response.read().decode("utf-8", errors="replace") or "{}")
        except HTTPError as exc:
            try:
                data = json.loads(exc.read().decode("utf-8", errors="replace") or "{}")
            except Exception:
                data = {"ok": False, "error": f"Billing server returned HTTP {exc.code}."}
            data.setdefault("ok", False)
            data["http_status"] = int(exc.code)
            if allow_error:
                return data
            raise RuntimeError(
                str(
                    data.get("error")
                    or data.get("message")
                    or f"Billing server returned HTTP {exc.code}."
                )
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"Could not reach Knight Account billing: {exc}") from exc
        if not isinstance(data, dict):
            raise RuntimeError("Knight Account billing returned an invalid response.")
        if not data.get("ok", False) and not allow_error:
            raise RuntimeError(str(data.get("error") or "Knight Account billing request failed."))
        return data

    def register_or_login(self, email: str) -> dict:
        return self._post("register_or_login", {"email": str(email or "").strip().lower()})

    def get_status(self, token: str = "", email: str = "") -> dict:
        return self._post("status", {"token": token, "email": email})

    def claim_trial(self, email: str = "", token: str = "") -> dict:
        return self._post("claim_trial", {"email": email, "token": token, "event_id": uuid.uuid4().hex})

    def reserve_credits(self, token: str, credits: int, metadata: dict | None = None) -> dict:
        return self._post(
            "reserve",
            {
                "token": token,
                "credits": int(credits),
                "metadata": metadata or {},
                "event_id": uuid.uuid4().hex,
            },
            allow_error=True,
        )

    def commit_reservation(self, token: str, reservation_id: str) -> dict:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                return self._post("commit", {"token": token, "reservation_id": reservation_id})
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    import time

                    time.sleep(0.6 * (attempt + 1))
        raise RuntimeError(str(last_error or "Could not commit the render charge."))

    def release_reservation(self, token: str, reservation_id: str) -> dict:
        return self._post(
            "release",
            {"token": token, "reservation_id": reservation_id},
            allow_error=True,
        )

    def create_checkout_session(
        self,
        token: str,
        credits: int,
        charge_cents: int | None = None,
        package_name: str | None = None,
        success_url_override: str | None = None,
        cancel_url_override: str | None = None,
        email: str = "",
    ) -> dict:
        plan_id = package_name or self.PLAN_BY_CREDITS.get(int(credits))
        if not plan_id:
            raise RuntimeError("Choose one of the displayed PixelForge credit packages.")
        return self._post(
            "create_checkout",
            {
                "token": token,
                "email": email,
                "plan_id": plan_id,
                "success_url": str(success_url_override or ""),
                "cancel_url": str(cancel_url_override or ""),
                "event_id": uuid.uuid4().hex,
            },
        )

    def confirm_checkout_session(self, session_id: str, token: str = "") -> dict:
        return self._post(
            "confirm_session",
            {"session_id": str(session_id), "token": token, "event_id": uuid.uuid4().hex},
        )

    def restore(self, email: str, recovery_code: str) -> dict:
        return self._post(
            "restore",
            {"email": email, "recovery_code": recovery_code, "event_id": uuid.uuid4().hex},
        )

    def portal(self, email: str, token: str = "") -> dict:
        return self._post("portal", {"email": email, "token": token})

    def record_event(self, event_name: str, metadata: dict | None = None) -> dict:
        return self._post(
            "event",
            {
                "event_name": event_name,
                "event_id": uuid.uuid4().hex,
                "metadata": metadata or {},
            },
            timeout=8,
            allow_error=True,
        )


# Backward-compatible alias used while migrating the monolith.
RemoteBillingBackend = KnightAccountClient
