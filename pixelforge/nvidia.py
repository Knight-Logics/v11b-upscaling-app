"""NVIDIA RTX Video Super Resolution capability and worker discovery.

The NVIDIA runtime is intentionally optional: its wheel is hundreds of MB and
only works on Tensor Core GPUs.  The main application uses this small module to
gate the UI without importing CUDA libraries on AMD/Intel systems.
"""

from __future__ import annotations

import os
import re
import hashlib
import shutil
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence
from urllib.request import Request, urlopen


NVIDIA_MODEL_KEY = "nvidia-rtx-vsr"
NVIDIA_MIN_WINDOWS_DRIVER = (570, 65)
NVIDIA_PACK_VERSION = "0.1.0.1"
NVIDIA_PACK_URL = (
    "https://github.com/Knight-Logics/v11b-upscaling-app/releases/download/"
    "v1.0.21/PixelForge-NVIDIA-Pack_1.0.21_windows_x64.zip"
)
NVIDIA_PACK_SHA256 = "e420425e3a9a10859777747bc139acc9b58112ea34240dc85071a9a997c23ec0"


@dataclass(frozen=True)
class NvidiaHardware:
    supported: bool
    name: str = ""
    driver: str = ""
    vram_mb: int = 0
    compute_capability: str = ""
    reason: str = ""


@dataclass(frozen=True)
class NvidiaWorker:
    command: tuple[str, ...]
    source: str


def _version_tuple(value: str) -> tuple[int, int]:
    numbers = [int(item) for item in re.findall(r"\d+", str(value))[:2]]
    while len(numbers) < 2:
        numbers.append(0)
    return numbers[0], numbers[1]


def parse_nvidia_smi_row(row: str) -> NvidiaHardware:
    """Parse the stable CSV emitted by our nvidia-smi capability query."""
    parts = [part.strip() for part in str(row or "").split(",")]
    if len(parts) < 4:
        return NvidiaHardware(False, reason="NVIDIA driver did not return complete GPU details")
    name, driver, memory_text, compute_capability = parts[:4]
    memory_match = re.search(r"\d+", memory_text)
    vram_mb = int(memory_match.group(0)) if memory_match else 0
    tensor_core_family = any(
        token in name.lower()
        for token in ("rtx", "t4", "a10", "a100", "a30", "a40", "l4", "l40", "h100", "h200", "b100", "b200")
    )
    if not tensor_core_family:
        return NvidiaHardware(
            False, name, driver, vram_mb, compute_capability,
            "NVIDIA VSR requires a Tensor Core GPU (RTX/Turing or newer)",
        )
    if _version_tuple(driver) < NVIDIA_MIN_WINDOWS_DRIVER:
        return NvidiaHardware(
            False, name, driver, vram_mb, compute_capability,
            f"NVIDIA driver {driver} is older than required 570.65",
        )
    if vram_mb and vram_mb < 4096:
        return NvidiaHardware(
            False, name, driver, vram_mb, compute_capability,
            "NVIDIA VSR requires at least 4 GB of VRAM",
        )
    return NvidiaHardware(True, name, driver, vram_mb, compute_capability, "Compatible NVIDIA RTX runtime")


def detect_nvidia_hardware(
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> NvidiaHardware:
    if os.name != "nt":
        return NvidiaHardware(False, reason="The NVIDIA RTX pack is currently available for Windows only")
    try:
        result = runner(
            [
                "nvidia-smi",
                "--query-gpu=name,driver_version,memory.total,compute_cap",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=3,
            check=False,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return NvidiaHardware(False, reason=f"NVIDIA CUDA driver not detected: {exc}")
    rows = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    if not rows:
        return NvidiaHardware(False, reason="No NVIDIA CUDA GPU detected")
    candidates = [parse_nvidia_smi_row(row) for row in rows]
    return next((item for item in candidates if item.supported), candidates[0])


def discover_nvidia_worker(app_dir: Path, data_dir: Path) -> NvidiaWorker | None:
    """Find a packaged worker, with a source-tree development fallback."""
    override = str(os.environ.get("PIXELFORGE_NVIDIA_WORKER", "")).strip()
    candidates: list[tuple[Path, str]] = []
    if override:
        candidates.append((Path(override), "environment override"))
    candidates.extend(
        [
            (Path(data_dir) / "nvidia-pack" / "PixelForge-NVIDIA-Worker.exe", "installed RTX pack"),
            (Path(app_dir) / "nvidia-pack" / "PixelForge-NVIDIA-Worker.exe", "sidecar RTX pack"),
            (Path(app_dir) / "PixelForge-NVIDIA-Worker.exe", "sidecar RTX worker"),
        ]
    )
    for candidate, source in candidates:
        if candidate.is_file():
            return NvidiaWorker((str(candidate),), source)

    # Source checkout: keep the 600+ MB CUDA dependencies isolated from the
    # universal app environment while retaining a fully testable development path.
    source_worker = Path(app_dir) / "pixelforge" / "nvidia_worker.py"
    dev_python = Path(app_dir) / ".venv-nvidia" / "Scripts" / "python.exe"
    if source_worker.is_file() and dev_python.is_file() and not getattr(sys, "frozen", False):
        return NvidiaWorker((str(dev_python), str(source_worker)), "isolated development RTX pack")
    return None


def nvidia_worker_command(worker: NvidiaWorker, arguments: Sequence[str]) -> list[str]:
    return [*worker.command, *[str(value) for value in arguments]]


def frame_rate_choice(choice: str, source_fps: float) -> tuple[bool, int]:
    """Translate the simple main-screen choice into interpolation settings."""
    normalized = str(choice or "").strip().lower()
    if normalized == "smooth 60 fps" and float(source_fps) < 59.5:
        return True, 60
    return False, max(1, int(round(float(source_fps))))


def download_nvidia_pack(
    destination: Path,
    *,
    url: str = NVIDIA_PACK_URL,
    expected_sha256: str = NVIDIA_PACK_SHA256,
    progress: Callable[[int, int], None] | None = None,
) -> Path:
    """Download and cryptographically verify the optional engine pack."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = Request(str(url), headers={"User-Agent": "PixelForge-AI-NVIDIA-Pack"})
    digest = hashlib.sha256()
    downloaded = 0
    with urlopen(request, timeout=60) as response, destination.open("wb") as output:
        try:
            total = int(response.headers.get("Content-Length") or 0)
        except (TypeError, ValueError):
            total = 0
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            output.write(chunk)
            digest.update(chunk)
            downloaded += len(chunk)
            if progress:
                progress(downloaded, total)
    actual = digest.hexdigest().lower()
    expected = str(expected_sha256).strip().lower()
    if expected and actual != expected:
        destination.unlink(missing_ok=True)
        raise RuntimeError(f"NVIDIA pack checksum mismatch: expected {expected}, received {actual}")
    return destination


def install_nvidia_pack(archive: Path, destination: Path) -> Path:
    """Safely extract a verified pack and return its worker executable."""
    archive = Path(archive).resolve()
    destination = Path(destination).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix="nvidia-pack-staging-", dir=destination.parent))
    try:
        with zipfile.ZipFile(archive) as bundle:
            for item in bundle.infolist():
                target = (staging / item.filename).resolve()
                try:
                    target.relative_to(staging)
                except ValueError as exc:
                    raise RuntimeError(f"Unsafe path in NVIDIA pack: {item.filename}") from exc
            bundle.extractall(staging)
        worker = staging / "PixelForge-NVIDIA-Worker.exe"
        if not worker.is_file():
            raise RuntimeError("NVIDIA pack is missing PixelForge-NVIDIA-Worker.exe")
        if destination.exists():
            shutil.rmtree(destination)
        shutil.move(str(staging), str(destination))
        staging = destination
        return destination / "PixelForge-NVIDIA-Worker.exe"
    finally:
        if staging.exists() and staging != destination:
            shutil.rmtree(staging, ignore_errors=True)


def activate_nvidia_pack(candidate: Path, destination: Path) -> Path:
    """Atomically promote a probed candidate and restore the previous pack on failure."""
    candidate = Path(candidate).resolve()
    destination = Path(destination).resolve()
    candidate_worker = candidate / "PixelForge-NVIDIA-Worker.exe"
    if not candidate_worker.is_file():
        raise RuntimeError("Verified NVIDIA pack candidate is missing its worker")
    destination.parent.mkdir(parents=True, exist_ok=True)
    backup = destination.with_name(f"{destination.name}.previous")
    if backup.exists():
        shutil.rmtree(backup, ignore_errors=True)
    if destination.exists():
        shutil.move(str(destination), str(backup))
    try:
        shutil.move(str(candidate), str(destination))
    except Exception:
        if destination.exists():
            shutil.rmtree(destination, ignore_errors=True)
        if backup.exists():
            shutil.move(str(backup), str(destination))
        raise
    shutil.rmtree(backup, ignore_errors=True)
    return destination / "PixelForge-NVIDIA-Worker.exe"
