# PixelForge AI

Local Windows video and image enhancement app using SPAN DirectML, Real-ESRGAN, Real-CUGAN, SRMD, RealSR, Waifu2x, and RIFE with content-aware profiles, target-driven output, professional media controls, secure credit billing, and auto-update support.

## Features

- Profile-based quality/speed controls
- Three cached source-frame crop previews with progress, cancellation, and a draggable separator
- Same resolution, 1080p, 1440p, 4K, and 8K delivery targets
- Stage-based ETA with hardware-aware estimation
- Bounded-memory DirectML streaming with a single final encode; disk-backed checkpoint batches for directory-only NCNN/RIFE engines
- Foolproof quality guard: automatic interlace correction, HDR/10-bit precision-safe routing, and confirmation for enlargement beyond a credible 4x detail-recovery range
- H.264, HEVC 10-bit, AV1 10-bit, ProRes 422 HQ, image sequences, NVENC, multiple audio tracks, subtitles, chapters, and metadata preservation
- Server-authoritative free-trial and purchased credits
- Failure-safe render reservations: commit on valid output, return on cancel/failure
- Direct in-app auto-update from GitHub Releases
- Windows standalone EXE packaging

## Engines (v1.0.20)

| Content | Preferred free engine | Notes |
|---|---|---|
| People / camera | **SPAN Photo / RealSR DF2K** | Fast DirectML preview; benchmark-leading RealSR final presets |
| Anime / game | **ModernSpanimation / Real-CUGAN SE** | Fast current SPAN preview; conservative Real-CUGAN detail presets |
| Old / noisy | **SRMD** (+ RealSR JPEG quality) | Fast denoise + restore |
| Frame interpolation | **RIFE v4.25** (recommended) | 4.22-lite for previews; 4.26 available as the newer optional model |

## Recommended First Run

- A first-run picker asks **People / camera**, **Anime / game**, or **Old / noisy**.
- Default Balanced + People/camera uses **RealSR DF2K** after the local preset benchmark.
- Anime profiles use ModernSpanimation or Real-CUGAN; restore profiles use SRMD or RealSR JPEG.
- Choose the delivery target rather than an engine scale; PixelForge resolves the internal native model pass and final dimensions.
- The compare view shows three actual source frame numbers and defaults to a fast detail crop. Still frames do not represent interpolated motion.
- Advanced options stay collapsed behind the Advanced Options link.

DirectML SPAN video processing preserves high-bit sources as RGB48 through FP32 ONNX inference and feeds RGB48 directly to the final 10-bit encoder. The directory-only NCNN/RIFE and Pillow still-preview paths remain 8-bit; HDR metadata is preserved, but the bundled SPAN models are not advertised as HDR-trained models.

PixelForge does not make an unmeasured universal "better than Topaz" claim. See `QUALITY_BENCHMARK.md` and `tools/compare_topaz.py` for the matched-source gate used before category-specific quality claims.

## Secrets and Billing Configuration

Customer builds use `POST https://knightlogics.com/api/pixelforge-license`. No Stripe, SMTP, database, or signing secret is shipped in the EXE. Packs: 32/$5.00, 68/$10.00, 144/$20.00.

Anonymous diagnostics include only an app-scoped hashed device ID, app version, selected profile/model, coarse source dimensions/duration, and outcome. File names, media, computer name, raw machine identifiers, and email are not sent as diagnostics. Set `V11B_DISABLE_ANONYMOUS_DIAGNOSTICS=1` to opt out.

Local embedded billing and legacy code testing require both `V11B_DEV_MODE=1` and `V11B_BILLING_API_BASE=embedded://local`. They are not customer fallbacks.

## Local Run

```powershell
python process_full_video_ultimate.py
```

## Build Standalone EXE

```powershell
./build_release.ps1 -Version 1.0.20
```

Build output is generated in `release/`:

- `PixelForge-AI.exe` (single-file app)
- `PixelForge-AI_<version>_windows_x64.zip`

## Release Asset Naming

The auto-updater selects the latest `.exe` asset from GitHub Releases, preferring names with PixelForge or v11b and a matching version.
