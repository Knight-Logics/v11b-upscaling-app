# PixelForge AI

Desktop video and image upscaling app using Real-ESRGAN, RealSR, and Waifu2x with quality profiles, compare view, ETA estimation, billing hooks, and auto-update support.

## Features

- Profile-based quality/speed controls
- Before/after compare with draggable separator
- Stage-based ETA with hardware-aware estimation
- Direct in-app auto-update from GitHub Releases
- Windows standalone EXE packaging

## Local Run

```powershell
python process_full_video_ultimate.py
```

## Build Standalone EXE

```powershell
./build_release.ps1 -Version 1.0.17
```

Build output is generated in `release/`:

- `PixelForge-AI.exe` (single-file app)
- `PixelForge-AI_<version>_windows_x64.zip`

## Release Asset Naming

Auto-updater picks the latest `.exe` asset from GitHub release, preferring names with `PixelForge` or `v11b` and version in the filename.
