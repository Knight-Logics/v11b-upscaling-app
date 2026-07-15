# PixelForge AI

Desktop video and image upscaling app using Real-ESRGAN, RealSR, Waifu2x, and RIFE with quality profiles, compare view, ETA estimation, secure credit billing, and auto-update support.

## Features

- Profile-based quality/speed controls
- Before/after compare with draggable separator
- Stage-based ETA with hardware-aware estimation
- Server-authoritative free-trial and purchased credits
- Failure-safe render reservations: commit on valid output, return on cancel/failure
- Direct in-app auto-update from GitHub Releases
- Windows standalone EXE packaging

## Recommended First Run

- Start with **Balanced Workflow + Natural Footage** for phones, cameras, people, products, and real-world scenes.
- Use **Animation / Anime** only for animation, line art, anime, or game captures.
- Use **Legacy / Noisy Repair** for visibly compressed, noisy, blocky, or older sources.
- Frame interpolation, color enhancement, sharpening, and forced final dimensions are opt-in Advanced settings.

PixelForge presets use only native model/scale combinations. The bundled 4x-only networks must remain at 4x; requesting 2x or 3x from those networks can create tiled or cropped output.

## Secrets and Billing Configuration

Customer builds use `https://knightlogics.com/api/pixelforge-license`; no Stripe, SMTP, database, or signing secret is shipped in the EXE. Package price and credit quantity are validated server-side. Do not commit or package `billing.env`, `shared_billing.env`, passwords, or secret keys.

Anonymous diagnostics include only an app-scoped hashed device ID, app version, selected profile/model, coarse source dimensions/duration, and outcome. File names, media, computer name, raw machine identifiers, and email are not sent as diagnostics. Set `V11B_DISABLE_ANONYMOUS_DIAGNOSTICS=1` to opt out.

Local embedded billing and legacy code testing require both `V11B_DEV_MODE=1` and `V11B_BILLING_API_BASE=embedded://local`. They are not customer fallbacks.

## Local Run

```powershell
python process_full_video_ultimate.py
```

## Build Standalone EXE

```powershell
./build_release.ps1 -Version 1.0.18
```

Build output is generated in `release/`:

- `PixelForge-AI.exe` (single-file app)
- `PixelForge-AI_<version>_windows_x64.zip`

## Release Asset Naming

Auto-updater picks the latest `.exe` asset from GitHub release, preferring names with `PixelForge` or `v11b` and version in the filename.
