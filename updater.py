"""Auto-update helper for PixelForge AI.

Checks GitHub Releases for newer versions. If found, shows a dialog that
downloads the latest Windows EXE asset directly and applies the update
without redirecting to a browser.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import tkinter as tk
from pathlib import Path
from urllib.request import Request, urlopen

APP_NAME = "PixelForge AI"
GITHUB_REPO = os.environ.get("V11B_UPDATE_REPO", "Knight-Logics/v11b-upscaling-app").strip()
RELEASES_API = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
APPDATA_ROOT = os.environ.get("APPDATA", os.path.expanduser("~"))
RUNTIME_DIR = os.path.join(APPDATA_ROOT, "KnightLogics", "PixelForgeAI")
UPDATES_DIR = os.path.join(RUNTIME_DIR, "updates")
PENDING_UPDATE_PATH = os.path.join(RUNTIME_DIR, "pending_update.json")

os.makedirs(UPDATES_DIR, exist_ok=True)


def _parse_version(text: str) -> tuple[int, ...]:
    parts = []
    for part in text.lstrip("vV").split("."):
        if part.isdigit():
            parts.append(int(part))
    return tuple(parts) if parts else (0,)


def _fetch_latest_release() -> dict | None:
    try:
        req = Request(RELEASES_API, headers={"User-Agent": "v11b-updater/1.0"})
        with urlopen(req, timeout=7) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))
    except Exception:
        return None


def _read_pending_update() -> dict:
    try:
        with open(PENDING_UPDATE_PATH, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {}


def _write_pending_update(tag: str) -> None:
    try:
        with open(PENDING_UPDATE_PATH, "w", encoding="utf-8") as fh:
            json.dump({"tag": tag, "ts": int(time.time())}, fh)
    except Exception:
        pass


def _clear_pending_update() -> None:
    try:
        if os.path.exists(PENDING_UPDATE_PATH):
            os.remove(PENDING_UPDATE_PATH)
    except Exception:
        pass


def _should_suppress_prompt(current_version: str, latest_tag: str) -> bool:
    pending = _read_pending_update()
    pending_tag = str(pending.get("tag", "")).strip()
    pending_ts = int(pending.get("ts", 0) or 0)
    if not pending_tag:
        return False
    if _parse_version(current_version) >= _parse_version(pending_tag):
        _clear_pending_update()
        return False
    if pending_tag == latest_tag and (time.time() - pending_ts) < 900:
        return True
    return False


def _select_windows_exe_asset(release_payload: dict) -> tuple[str | None, str | None, int]:
    assets = release_payload.get("assets", [])
    tag_clean = str(release_payload.get("tag_name", "")).strip().lower().lstrip("v")

    # Prefer files with v11b naming and explicit version in filename.
    for asset in assets:
        name = str(asset.get("name", "")).strip()
        lname = name.lower()
        url = str(asset.get("browser_download_url", "")).strip()
        if not url or not lname.endswith(".exe"):
            continue
        if "v11b" in lname and (tag_clean and tag_clean in lname):
            return url, name, int(asset.get("size", 0) or 0)

    # Fallback: first .exe asset.
    for asset in assets:
        name = str(asset.get("name", "")).strip()
        url = str(asset.get("browser_download_url", "")).strip()
        if url and name.lower().endswith(".exe"):
                return url, name, int(asset.get("size", 0) or 0)

            return None, None, 0


def _download_asset(url: str, filename: str, expected_size: int = 0, status_cb=None) -> str:
    target_path = os.path.join(UPDATES_DIR, filename)
    temp_path = f"{target_path}.part"
    req = Request(url, headers={"User-Agent": "v11b-updater/1.0"})
    with urlopen(req, timeout=45) as response, open(temp_path, "wb") as out:
        total = int(response.headers.get("Content-Length", "0") or 0)
        downloaded = 0
        while True:
            chunk = response.read(1024 * 256)
            if not chunk:
                break
            out.write(chunk)
            downloaded += len(chunk)
            if status_cb and total > 0:
                pct = int((downloaded / total) * 100)
                status_cb(f"Downloading update... {pct}%")
    if expected_size > 0 and downloaded != expected_size:
        try:
            os.remove(temp_path)
        except Exception:
            pass
        raise RuntimeError(f"Downloaded file size mismatch (got {downloaded}, expected {expected_size}).")
    if downloaded < 50 * 1024 * 1024:
        try:
            os.remove(temp_path)
        except Exception:
            pass
        raise RuntimeError("Downloaded executable is unexpectedly small; aborting update.")
    os.replace(temp_path, target_path)
    return target_path


def _create_swap_script(old_exe: str, new_exe: str, launch_args: list[str] | None = None) -> str:
    args = launch_args or []
    args_str = " ".join(f'"{a}"' for a in args)
    script = f"""@echo off
setlocal
set OLD_EXE={old_exe}
set NEW_EXE={new_exe}

timeout /t 2 /nobreak >nul
:retry_copy
copy /Y "%NEW_EXE%" "%OLD_EXE%" >nul
if errorlevel 1 (
  timeout /t 1 /nobreak >nul
  goto retry_copy
)
start "" "%OLD_EXE%" {args_str}
endlocal
"""
    fd, path = tempfile.mkstemp(prefix="pixelforge_update_", suffix=".cmd")
    os.close(fd)
    Path(path).write_text(script, encoding="utf-8")
    return path


def _show_update_dialog(parent: tk.Tk, current_version: str, latest_tag: str, asset_url: str | None, asset_name: str | None, asset_size: int = 0) -> None:
    dlg = tk.Toplevel(parent)
    dlg.title("Update Available")
    dlg.configure(bg="#0f1115")
    dlg.resizable(False, False)
    dlg.transient(parent)
    dlg.attributes("-topmost", True)
    dlg.lift()
    dlg.grab_set()

    parent.update_idletasks()
    px, py = parent.winfo_x(), parent.winfo_y()
    pw, ph = parent.winfo_width(), parent.winfo_height()
    w, h = 500, 220
    dlg.geometry(f"{w}x{h}+{px + (pw - w)//2}+{py + (ph - h)//2}")

    status_var = tk.StringVar(value="Ready to download and apply update.")
    busy = {"value": False}

    tk.Label(dlg, text="Update Available", bg="#0f1115", fg="#f2f7ff", font=("Segoe UI", 13, "bold")).pack(anchor="w", padx=22, pady=(20, 4))
    tk.Label(
        dlg,
        text=f"A new version is available: {latest_tag}\nCurrent version: v{current_version}",
        bg="#0f1115",
        fg="#9aa7b7",
        font=("Segoe UI", 10),
        justify="left",
        wraplength=450,
    ).pack(anchor="w", padx=22, pady=(0, 8))
    tk.Label(dlg, textvariable=status_var, bg="#0f1115", fg="#73d9b5", font=("Segoe UI", 9), wraplength=450, justify="left").pack(anchor="w", padx=22, pady=(2, 12))

    row = tk.Frame(dlg, bg="#0f1115")
    row.pack(anchor="e", padx=22, pady=(0, 18))

    def _set_status(message: str) -> None:
        dlg.after(0, lambda: status_var.set(message))

    def _do_update() -> None:
        if busy["value"]:
            return
        if not asset_url or not asset_name:
            status_var.set("No Windows .exe update asset found in latest release.")
            return

        resolved_url = str(asset_url)
        resolved_name = str(asset_name)

        busy["value"] = True
        update_btn.configure(state=tk.DISABLED)

        def _worker() -> None:
            try:
                attempts = 0
                last_exc: Exception | None = None
                new_path = ""
                while attempts < 3:
                    attempts += 1
                    try:
                        if attempts > 1:
                            _set_status(f"Retrying download ({attempts}/3)...")
                        new_path = _download_asset(resolved_url, resolved_name, expected_size=asset_size, status_cb=_set_status)
                        break
                    except Exception as exc:
                        last_exc = exc
                if not new_path:
                    raise RuntimeError(f"Update download failed after 3 attempts: {last_exc}")
                _write_pending_update(latest_tag)

                if getattr(sys, "frozen", False):
                    old_exe = os.path.abspath(sys.executable)
                    swap_script = _create_swap_script(old_exe, new_path)
                    _set_status("Applying update and restarting...")
                    subprocess.Popen(["cmd", "/c", swap_script], creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
                    parent.after(500, parent.destroy)
                else:
                    _set_status("Update downloaded. Running downloaded executable...")
                    subprocess.Popen([new_path], creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            except Exception as exc:
                _set_status(f"Update failed: {exc}")
                dlg.after(0, lambda: update_btn.configure(state=tk.NORMAL))
                busy["value"] = False

        threading.Thread(target=_worker, daemon=True, name="v11b-updater-download").start()

    update_btn = tk.Button(
        row,
        text="Download and Update",
        command=_do_update,
        bg="#30c18d",
        fg="#0a1310",
        activebackground="#3ed9a0",
        activeforeground="#08110e",
        relief=tk.FLAT,
        bd=0,
        padx=14,
        pady=6,
        font=("Segoe UI", 10, "bold"),
        cursor="hand2",
    )
    update_btn.pack(side=tk.LEFT, padx=(0, 8))

    tk.Button(
        row,
        text="Later",
        command=dlg.destroy,
        bg="#252b37",
        fg="#d4dde9",
        activebackground="#2d3444",
        activeforeground="#f2f7ff",
        relief=tk.FLAT,
        bd=0,
        padx=14,
        pady=6,
        font=("Segoe UI", 10),
        cursor="hand2",
    ).pack(side=tk.LEFT)


def check_for_updates(parent: tk.Tk, current_version: str, *, silent: bool = True) -> None:
    """Check updates in background and prompt user if new release exists."""

    def _worker() -> None:
        payload = _fetch_latest_release()
        if not payload:
            return
        try:
            latest_tag = str(payload.get("tag_name", "")).strip()
            if not latest_tag:
                return
            if _should_suppress_prompt(current_version, latest_tag):
                return
            if _parse_version(latest_tag) > _parse_version(current_version):
                asset_url, asset_name, asset_size = _select_windows_exe_asset(payload)
                parent.after(0, lambda: _show_update_dialog(parent, current_version, latest_tag, asset_url, asset_name, asset_size))
        except Exception:
            if not silent:
                raise

    threading.Thread(target=_worker, daemon=True, name="v11b-updater-check").start()
