# v11b Upscaling App

Desktop upscaling app using Real-ESRGAN with quality profiles, compare view, ETA estimation, billing hooks, and auto-update support.

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
./build_release.ps1 -Version 1.0.0
```

Build output is generated in `release/`:

- `v11b-upscaling-app.exe` (single-file app)
- `v11b-upscaling-app_<version>_windows_x64.zip`

## Release Asset Naming

Auto-updater picks the latest `.exe` asset from GitHub release, preferring names with `v11b` and version in the filename.
