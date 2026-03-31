"""Auto-update helper for PixelForge AI.

Checks GitHub Releases for newer versions. If found, shows a dialog that
downloads the latest Windows EXE asset directly and applies the update
without redirecting to a browser.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import tkinter as tk
import traceback
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen

APP_NAME = "PixelForge AI"
GITHUB_REPO = os.environ.get("V11B_UPDATE_REPO", "Knight-Logics/v11b-upscaling-app").strip()
RELEASES_API = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
APPDATA_ROOT = os.environ.get("APPDATA", os.path.expanduser("~"))
RUNTIME_DIR = os.path.join(APPDATA_ROOT, "KnightLogics", "PixelForgeAI")
UPDATES_DIR = os.path.join(RUNTIME_DIR, "updates")
PENDING_UPDATE_PATH = os.path.join(RUNTIME_DIR, "pending_update.json")
UPDATE_LOG_PATH = os.path.join(RUNTIME_DIR, "updater.log")

os.makedirs(UPDATES_DIR, exist_ok=True)


def _log_update(message: str) -> None:
    try:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(UPDATE_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(f"[{stamp}] {message}\n")
    except Exception:
        pass


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _normalize_asset_digest(asset: dict) -> str | None:
    digest = str(asset.get("digest", "")).strip()
    if digest.lower().startswith("sha256:"):
        digest = digest.split(":", 1)[1].strip()
    return digest or None


def _select_windows_exe_asset(release_payload: dict) -> tuple[str | None, str | None, int, str | None]:
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
            return url, name, int(asset.get("size", 0) or 0), _normalize_asset_digest(asset)

    # Fallback: first .exe asset.
    for asset in assets:
        name = str(asset.get("name", "")).strip()
        url = str(asset.get("browser_download_url", "")).strip()
        if url and name.lower().endswith(".exe"):
            return url, name, int(asset.get("size", 0) or 0), _normalize_asset_digest(asset)

    return None, None, 0, None


def _download_asset(url: str, filename: str, expected_size: int = 0, expected_sha256: str | None = None, status_cb=None) -> str:
    target_path = os.path.join(UPDATES_DIR, filename)
    temp_path = f"{target_path}.part"
    _log_update(
        f"Starting asset download: url={url} filename={filename} "
        f"expected_size={expected_size} expected_sha256={expected_sha256 or 'n/a'}"
    )
    req = Request(url, headers={"User-Agent": "v11b-updater/1.0"})
    with urlopen(req, timeout=45) as response, open(temp_path, "wb") as out:
        total = int(response.headers.get("Content-Length", "0") or 0)
        _log_update(f"Download response received: content_length={total} target_temp={temp_path}")
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
    if expected_sha256:
        actual_sha256 = _sha256_file(temp_path)
        if actual_sha256.lower() != expected_sha256.lower():
            try:
                os.remove(temp_path)
            except Exception:
                pass
            raise RuntimeError(f"Downloaded executable hash mismatch (got {actual_sha256}, expected {expected_sha256}).")
        _log_update(f"Downloaded executable hash verified: sha256={actual_sha256}")
    os.replace(temp_path, target_path)
    _log_update(f"Asset download completed: path={target_path} size={downloaded}")
    return target_path


def _create_swap_script(old_exe: str, new_exe: str, log_path: str, launch_args: list[str] | None = None) -> str:
    args = launch_args or []
    args_str = " ".join(f'"{a}"' for a in args)
    script = f"""@echo off
setlocal
set "OLD_EXE={old_exe}"
set "NEW_EXE={new_exe}"
set "BACKUP_EXE={old_exe}.previous"
set "LOG_FILE={log_path}"
set "LAUNCH_ARGS={args_str}"

>> "%LOG_FILE%" echo [%date% %time%] Update swap script started. old=%OLD_EXE% new=%NEW_EXE%

timeout /t 3 /nobreak >nul
set RETRIES=0
:retry_swap
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; if (Test-Path -LiteralPath $env:BACKUP_EXE) {{ Remove-Item -LiteralPath $env:BACKUP_EXE -Force -ErrorAction SilentlyContinue }}; if (Test-Path -LiteralPath $env:OLD_EXE) {{ Move-Item -LiteralPath $env:OLD_EXE -Destination $env:BACKUP_EXE -Force }}; Copy-Item -LiteralPath $env:NEW_EXE -Destination $env:OLD_EXE -Force; $src=(Get-FileHash -LiteralPath $env:NEW_EXE -Algorithm SHA256).Hash; $dst=(Get-FileHash -LiteralPath $env:OLD_EXE -Algorithm SHA256).Hash; if ($src -ne $dst) {{ throw 'Hash mismatch after copy' }}"
if errorlevel 1 (
    set /a RETRIES+=1
    >> "%LOG_FILE%" echo [%date% %time%] Update copy attempt failed. retry=%RETRIES%
    if %RETRIES% GEQ 15 goto copy_failed
    timeout /t 1 /nobreak >nul
    goto retry_swap
)
>> "%LOG_FILE%" echo [%date% %time%] Update copy verified.

>> "%LOG_FILE%" echo [%date% %time%] Launching updated executable.
powershell -NoProfile -Command "Remove-Item Env:_MEIPASS2 -ErrorAction SilentlyContinue; Get-ChildItem Env: | Where-Object {{ $_.Name -like '_PYI*' }} | ForEach-Object {{ Remove-Item -Path ('Env:' + $_.Name) -ErrorAction SilentlyContinue }}; Remove-Item Env:PYTHONHOME -ErrorAction SilentlyContinue; Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue; $p = if ($env:LAUNCH_ARGS) {{ Start-Process -FilePath $env:OLD_EXE -ArgumentList $env:LAUNCH_ARGS -PassThru }} else {{ Start-Process -FilePath $env:OLD_EXE -PassThru }}; Start-Sleep -Seconds 20; if (-not $p -or $p.HasExited) {{ Write-Output ('EXITED:' + ($p.ExitCode -as [string])); exit 2 }}; Write-Output ('RUNNING:' + $p.Id)" > "%TEMP%\\pixelforge_launch_result.txt"
if errorlevel 2 goto launch_failed
for /f "usebackq delims=" %%L in ("%TEMP%\\pixelforge_launch_result.txt") do >> "%LOG_FILE%" echo [%date% %time%] %%L

powershell -NoProfile -Command "if (Test-Path -LiteralPath $env:BACKUP_EXE) {{ Remove-Item -LiteralPath $env:BACKUP_EXE -Force -ErrorAction SilentlyContinue }}; if (Test-Path -LiteralPath $env:NEW_EXE) {{ Remove-Item -LiteralPath $env:NEW_EXE -Force -ErrorAction SilentlyContinue }}"
if exist "%TEMP%\\pixelforge_launch_result.txt" del "%TEMP%\\pixelforge_launch_result.txt" >nul 2>nul
endlocal
exit /b 0

:copy_failed
>> "%LOG_FILE%" echo [%date% %time%] Update failed during copy stage.
endlocal
exit /b 1

:launch_failed
for /f "usebackq delims=" %%L in ("%TEMP%\\pixelforge_launch_result.txt") do >> "%LOG_FILE%" echo [%date% %time%] %%L
powershell -NoProfile -Command "$ErrorActionPreference='Continue'; if (Test-Path -LiteralPath $env:BACKUP_EXE) {{ Copy-Item -LiteralPath $env:BACKUP_EXE -Destination $env:OLD_EXE -Force }}"
>> "%LOG_FILE%" echo [%date% %time%] Restored previous executable after failed launch.
if exist "%TEMP%\\pixelforge_launch_result.txt" del "%TEMP%\\pixelforge_launch_result.txt" >nul 2>nul
endlocal
exit /b 2
"""
    fd, path = tempfile.mkstemp(prefix="pixelforge_update_", suffix=".cmd")
    os.close(fd)
    Path(path).write_text(script, encoding="utf-8")
    _log_update(f"Created swap script: {path}")
    return path


def _show_update_dialog(
    parent: tk.Tk,
    current_version: str,
    latest_tag: str,
    asset_url: str | None,
    asset_name: str | None,
    asset_size: int = 0,
    asset_sha256: str | None = None,
) -> None:
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
        resolved_sha256 = str(asset_sha256 or "").strip() or None
        _log_update(f"User accepted update: current=v{current_version} latest={latest_tag} asset={resolved_name}")

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
                        new_path = _download_asset(
                            resolved_url,
                            resolved_name,
                            expected_size=asset_size,
                            expected_sha256=resolved_sha256,
                            status_cb=_set_status,
                        )
                        break
                    except Exception as exc:
                        _log_update(f"Update download attempt failed ({attempts}/3): {exc}")
                        last_exc = exc
                if not new_path:
                    raise RuntimeError(f"Update download failed after 3 attempts: {last_exc}")
                _write_pending_update(latest_tag)
                _log_update(f"Pending update marker written for {latest_tag}")

                if getattr(sys, "frozen", False):
                    old_exe = os.path.abspath(sys.executable)
                    _log_update(f"Frozen update mode active. current_exe={old_exe} downloaded_exe={new_path}")
                    swap_script = _create_swap_script(old_exe, new_path, UPDATE_LOG_PATH)
                    _set_status("Applying update and restarting...")
                    clean_env = dict(os.environ)
                    clean_env.pop("_MEIPASS2", None)
                    clean_env.pop("PYTHONHOME", None)
                    clean_env.pop("PYTHONPATH", None)
                    for env_key in list(clean_env.keys()):
                        if str(env_key).startswith("_PYI"):
                            clean_env.pop(env_key, None)
                    subprocess.Popen(
                        ["cmd", "/c", swap_script],
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                        env=clean_env,
                    )
                    parent.after(500, parent.destroy)
                else:
                    _set_status("Update downloaded. Running downloaded executable...")
                    _log_update(f"Non-frozen update test launch: {new_path}")
                    subprocess.Popen([new_path], creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            except Exception as exc:
                _log_update(f"Update flow failed before restart: {exc}")
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
            _log_update("Latest release lookup returned no payload.")
            return
        try:
            latest_tag = str(payload.get("tag_name", "")).strip()
            if not latest_tag:
                _log_update("Latest release payload missing tag_name.")
                return
            if _should_suppress_prompt(current_version, latest_tag):
                _log_update(f"Update prompt suppressed for tag={latest_tag} current=v{current_version}")
                return
            if _parse_version(latest_tag) > _parse_version(current_version):
                asset_url, asset_name, asset_size, asset_sha256 = _select_windows_exe_asset(payload)
                _log_update(
                    f"Update available: current=v{current_version} latest={latest_tag} "
                    f"asset={asset_name} size={asset_size} sha256={asset_sha256 or 'n/a'}"
                )
                parent.after(
                    0,
                    lambda: _show_update_dialog(
                        parent,
                        current_version,
                        latest_tag,
                        asset_url,
                        asset_name,
                        asset_size,
                        asset_sha256,
                    ),
                )
        except Exception:
            _log_update("Update check raised an exception.\n" + traceback.format_exc())
            if not silent:
                raise

    threading.Thread(target=_worker, daemon=True, name="v11b-updater-check").start()
