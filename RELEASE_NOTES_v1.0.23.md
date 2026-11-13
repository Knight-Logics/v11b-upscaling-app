# PixelForge AI v1.0.23

Prepared only. This file does not publish a release. Installed downloads still serve v1.0.22 until a release is published by hand.

## Default look

The previous default was Balanced + People/camera on RealSR DF2K, with output size set to Same resolution. Same resolution runs the 4x model and then Lanczos-scales the frame back to the source size, so most of the recovered pixels never reach the file. RealSR's same-resolution round trip on the product-still sample differed from the input by only 1.20 mean absolute levels (0–255). That is the "I can't see a difference" path. There is no separate Fidelity Restore blend in this tree.

v1.0.23 changes two defaults:

- Enhanced (the new default) uses Real-ESRGAN x4plus for camera footage and keeps the full network output. Faithful keeps RealSR DF2K.
- A low-resolution source now opens at a larger delivery size. The automatic choice is the smallest standard size that enlarges by at least 1.8x, and it never goes past 4K. A tiny frame, including 360p and smaller, uses 1080p instead of falling back to Same resolution. A 4K or larger landscape source stays at its own size. Same resolution remains an explicit choice and is the one path that scales the model back to the source.

Fast Preview still uses SPAN Photo. Anime Enhanced turns Real-CUGAN denoise off; Faithful keeps mild denoise. Old/noisy is unchanged. Max Detail uses the Enhanced model with a slower, higher-bitrate encode. It is not a different model, and it is not described as a measured fidelity winner.

## What the CPU gate measured

`python tools/quality_gate.py` on this host wrote `_benchmarks/quality-gate/GATE_RESULTS.json` and `quality-gate.cpu.done`. `quality-gate.done` was not written. The shipping Vulkan executables are Windows binaries and did not run. LPIPS was not computed (`lpips` / `torch` are not installed). libvmaf is not in this ffmpeg. NVIDIA RTX VSR was not run.

Real-ESRGAN x4plus, via the NCNN Python package on CPU using the bundled `models/realesrgan-x4plus` weights, against a Lanczos 4x of the same JPEG-degraded input:

| Still | Sharpness vs Lanczos | Mean abs diff vs Lanczos | PSNR (model / Lanczos) | SSIM (model / Lanczos) |
|---|---:|---:|---:|---:|
| Synthetic detail | 4.63× | 14.84 | 14.71 / 14.37 | 0.559 / 0.455 |
| Product-still crop | 1.31× | 4.52 | 28.47 / 27.90 | 0.842 / 0.837 |

Those two stills are the whole measured set. A second CPU gate run after the delivery-plan fix reproduced the same four numbers for Real-ESRGAN on the synthetic still (4.63×, 14.84, 14.71, 0.559) and on the product-still crop (1.31×, 4.52, 28.47, 0.842).

The doubled letter strokes are a Real-ESRGAN x4plus behavior, not a second sharpen pass and not a tile seam. Enhanced leaves sharpening off. The crop script runs the full frame through the NCNN weights, so there is no tile overlap. SPAN tiles of 64 and 1024 disagree by at most 1 level. On one text row, the ground truth has 32 rising edges, Lanczos has 4 or 5, and Real-ESRGAN has 38 without JPEG and 40 with JPEG quality 40. That extra edge response is why the letters look double. Removing it would mean blending back toward Lanczos, which is the bug this release is fixing, so it is left as a model limit. The crop is in `assets/comparisons/`. It is not a claim that text is reconstructed correctly, and it is not a video result.

Stills with EXIF orientation are transposed before the model runs, and a PNG alpha channel is resized to the delivered frame and put back. Video keeps using FFmpeg's display size, including a 90-degree rotation, so the raw-frame plan matches the decoded picture.

## Withdrawn video numbers

The v1.0.20–v1.0.22 representative-video VMAF, SSIM, and PSNR figures are withdrawn. Those runs did not search for a frame offset, so a one-frame seek error scored the wrong pictures. `tools/benchmark_representative_video.py` now aligns frames before scoring. The in-app Watch Demo link is not a quality-gate result.

## Comparison crops

`python tools/generate_comparison_crops.py` rewrites `assets/comparisons/` for the site slider (`lanczos.png` before, `pixelforge.png` after, plus zoomed crops). The manifest records CPU NCNN, not the Vulkan executable. Run it again on the Windows machine after `RUN_QUALITY_GATE.vbs` if the slider should show that executable.

## What the owner still has to run

`RUN_QUALITY_GATE.vbs` on Windows with a Vulkan driver. That is the shipping `realesrgan-ncnn-vulkan.exe` path. An NVIDIA RTX GPU is required before any RTX VSR number is quoted. Install `lpips` and `torch` before treating LPIPS as measured. An ffmpeg build with libvmaf is required before quoting VMAF again.

## Billing

Credit and license purchases stay on `https://knightlogics.com/api/pixelforge-license`. The client still sends `starter_12`, `creator_30`, and `studio_72`, which the Worker in [knight-account-api#1](https://github.com/Knight-Logics/knight-account-api/pull/1) accepts. If that server cannot be reached, status keeps the last saved balance and says so, checkout does not start a purchase, and a render does not start. A cached Pro entitlement can be used for seven days without creating a new charge. Credit balances are not invented while the server is down.
