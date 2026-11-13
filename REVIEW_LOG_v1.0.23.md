# PixelForge AI v1.0.23 review log

This log is the release record for the upscale-then-shrink fix. No git tag was created, no GitHub release was published, and no binary was uploaded.

## What was checked

The shipped default in v1.0.22 is Balanced + People/camera on RealSR DF2K, with the output size set to Same resolution. `PipelineRunner._build_post_filter` then scales the 4x model frame back to the source size. That one filter is shared by a single still, a video, the chunked frame batch, and the DirectML/NVIDIA streaming path. There is no separate Fidelity Restore blend.

v1.0.23 puts the size decision in `plan_delivery`. Automatic delivery is the smallest of 1080p, 1440p, and 4K that enlarges by at least 1.8x. It never selects 8K and it never shrinks the source. Same resolution stays available and is the documented path that returns the model output to the source size. Faithful still selects RealSR DF2K for camera footage, with sharpening off, and it uses the same output size as Enhanced.

Rotated video is planned from the display size (rotation and sample aspect), because FFmpeg autorotates the decoded frame. A still with an EXIF orientation is transposed before that decode. A still with an alpha channel is split, the color is upscaled, and the alpha is resized onto the delivered frame.

## Commands and outcomes

All commands were run from the repository root on Linux, Python 3.12.3.

| Command | Exit | Result |
|---|---:|---|
| `python3 -m unittest discover -s tests -q` | 0 | 65 tests passed |
| `python3 -m ruff check pixelforge tests tools updater.py --select E,F --ignore E501,E402` | 0 | All checks passed |
| `python3 -m mypy pixelforge --ignore-missing-imports` | 0 | No errors |
| `python3 tools/quality_gate.py` | 0 | CPU gate passed. `quality-gate.done` was not written. Vulkan executables did not run. |
| `python3 -m PyInstaller --noconfirm --clean --onedir ... process_full_video_ultimate.py` | 0 | Linux onedir build at `/tmp/pf-linux-dist/PixelForge-AI/PixelForge-AI` (ELF, 4.7 MB bootloader). Models landed in `_internal/`. Not uploaded. |
| `command -v pwsh` | 1 | PowerShell is not installed. `build_release.ps1` was not executed. |

The first PyInstaller attempt failed before analysis: `--specpath /tmp/pf-linux-spec` made the relative `--add-data assets:assets` look for `/tmp/pf-linux-spec/assets`. The retry used absolute data paths and completed. The Windows script already passes absolute paths via `Join-Path`, so that failure is specific to the Linux check invocation.

## Quality numbers

Real-ESRGAN x4plus, CPU NCNN, bundled `models/realesrgan-x4plus` weights, versus Lanczos 4x of the same JPEG-quality-40 input. The run after the delivery-plan change matched the run before it.

| Still | Sharpness vs Lanczos | Mean abs diff | PSNR model / Lanczos | SSIM model / Lanczos |
|---|---:|---:|---:|---:|
| Synthetic detail | 4.6263× | 14.8398 | 14.7063 / 14.3709 | 0.558751 / 0.455116 |
| Product-still crop | 1.311× | 4.5222 | 28.4682 / 27.8997 | 0.841866 / 0.837329 |

RealSR DF2K on the product-still crop, scaled back to the low-resolution input, differs from that input by 1.2042 mean absolute levels. That is the old default.

Text-stroke measurement on one row of the synthetic wordmark, counted as rising edges above 40 levels:

| Input | Rising edges |
|---|---:|
| Ground truth | 32 |
| Lanczos, JPEG 40 | 5 |
| Lanczos, no JPEG | 4 |
| Real-ESRGAN, JPEG 40 | 40 |
| Real-ESRGAN, no JPEG | 38 |

SPAN Photo tiled at 64 and at 1024 differed by a mean of 0.000002 and a maximum of 1 level. Enhanced does not enable CAS or unsharp. The extra strokes are the Real-ESRGAN weights. They were not "fixed" by blending the output back toward Lanczos.

LPIPS was not computed. `ffmpeg -filters` has `psnr` and `ssim` and does not have `libvmaf`.

## CI log

Workflow run `43402455807`, job `131808416631`, check `gitleaks`, head `16422a354c26d80edc0659ea7596509f62da43d5`. The log was read from runner startup through cleanup.

- Runner 2.338.2, Ubuntu 24.04.5, image `ubuntu-24.04` version `20261101.360.1`.
- Checkout `actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5` fetched the pull-request merge ref `1a0a5c63621a242fee062d011ee1eef37d74871e`.
- Gitleaks 8.30.1 hash check printed `/tmp/gitleaks.tar.gz: OK`.
- Scan range `origin/main...HEAD`. Log line: `1 commits scanned`, `scanned ~76664 bytes (76.66 KB) in 138ms`, `no leaks found`.
- Two annotations, neither of them a failed check:
  - Warning: Node.js 20 is deprecated for that checkout SHA.
  - Notice: `ubuntu-latest` will move to Ubuntu 26.
- This change pins the secret-scan job to `ubuntu-24.04` and `actions/checkout` `08c6903cd8c0fde910a37f88322edcfb5dd907a8` (v5.0.0, commit message "Prepare v5.0.0 release"). The next CI run is what confirms those annotations are gone. The previous run itself had no leak and no failed step.

## Errors and fixes

| Error | Root cause | Fix |
|---|---|---|
| Sources at or below 360p, and a 2×2 frame, stayed on Same resolution | `suggested_output_target` ignored any standard size above 2.35×, and 1080p is already 3× for a 640-wide frame | Automatic choice is now the smallest standard target at or above 1.8×, still capped at 4K |
| A 90-degree video would be planned with the coded axis | `probe_media` used coded `width`/`height` while FFmpeg autorotates | Display size includes rotation and sample aspect |
| A rotated JPEG still and a PNG with alpha did not follow that plan | Pillow does not apply EXIF unless asked, and the model path is RGB | `prepare_still` transposes first; `attach_alpha` restores alpha at the delivered size |
| `mypy pixelforge` failed on three lines | Untyped probe stream, `subprocess.CREATE_NO_WINDOW` on Linux, and rebinding a Pillow image inside `with` | Typed the stream, used `getattr(..., 0)`, and kept the converted image in a new name |
| `ruff` F401 on the crop script and `tests/test_targets_media.py` | Unused imports | Removed |
| PyInstaller Linux check, first attempt | Relative `--add-data` resolved from `--specpath` | Retried with absolute paths. Exit 0 |
| Doubled text strokes | Real-ESRGAN edge response (38–40 rising edges versus 32 in the ground truth). Not tiles, not a second sharpener | Documented. Not blended away |

## Version check

`APP_VERSION` is `1.0.23`. `assets/version_info.txt` is file and product version `1.0.23.0` / `(1, 0, 23, 0)`. The three PowerShell build scripts default to `1.0.23`. `RELEASE_NOTES_v1.0.23.md` is the note `build_release.ps1` copies.

The updater reads GitHub Releases, not a separate manifest. `_parse_version("1.0.23")` is greater than `_parse_version("v1.0.22")`, so a v1.0.23 build will not offer the currently published v1.0.22 release as an update. `_select_windows_asset` selects `PixelForge-AI_1.0.23_windows_x64.zip` when that tag and asset exist. Until a release with that asset exists, installed v1.0.22 clients keep running and v1.0.23 clients stay quiet.

The NVIDIA pack URL and hash stay on the v1.0.21 artifact. This review did not build or upload a new pack. The tag workflow is what rebuilds it, and that workflow was not run.

## Remaining risks

- `quality-gate.done` does not exist. The shipping Vulkan executables were not executed. NVIDIA RTX VSR was not executed. LPIPS and VMAF were not computed.
- The text crop is sharper than Lanczos and has extra strokes. That is the model.
- The Linux package is an ELF onedir used to prove the graph collects. It is not the Windows exe, and `build_release.ps1` still has to run on Windows.
- `onnxruntime-directml`, `stripe`, and `pywebview` were not installed for the Linux package, because the DirectML wheel is Windows-only and those imports are optional. The Windows build installs `requirements.txt`.
- The secret-scan pin change has not been observed in a completed GitHub run yet.
- Publishing is still a human step. The tag workflow has `contents: write` and creates the GitHub release.

## Release steps to approve

1. On the Windows PC, run `RUN_QUALITY_GATE.vbs`. Read `_benchmarks/quality-gate/GATE_RESULTS.json`. Quote a Vulkan or RTX number only after that file shows the executable ran. Install `lpips` and `torch` before quoting LPIPS. Use an ffmpeg build with libvmaf before quoting VMAF.
2. Read this log, `RELEASE_NOTES_v1.0.23.md`, and the comparison crops under `assets/comparisons/`.
3. Merge the pull request when the review is accepted. This agent was told not to merge.
4. Tag the merged commit `v1.0.23` and push the tag. `.github/workflows/release.yml` then builds the Windows exe, the zip named `PixelForge-AI_1.0.23_windows_x64.zip`, the NVIDIA pack, and the GitHub release. Do not tag before that approval. This agent was told not to tag or publish.
5. After the release asset exists, an installed v1.0.22 client can offer the update. Confirm the asset name contains `1.0.23` and `windows_x64.zip` so the updater selects it.
