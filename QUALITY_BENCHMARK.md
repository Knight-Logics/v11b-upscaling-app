# PixelForge quality benchmark

PixelForge does not claim to beat Topaz Video globally. The v1.0.20 presets are instead gated by repeatable, category-specific tests using the exact same degraded input for every candidate.

## v1.0.20 representative-video run

The release run used six short, lossless reference clips covering live-action faces and texture, animation faces and architecture, 30-to-60 FPS motion, and interlaced motion. Each degraded H.264 source was processed at the reference dimensions by every applicable PixelForge profile and a conventional FFmpeg baseline. Full-reference VMAF, SSIM, and PSNR were calculated across the entire output, and comparison sheets were reviewed for obvious artifacts.

Hardware: Windows, NVIDIA GeForce RTX 5070 Ti. Software H.264 was used for every output so encoder behavior stayed comparable. Times are wall-clock measurements for the short test clips, not projected production render times.

| Test | Best PixelForge result | VMAF | Useful comparison | Measured conclusion |
|---|---:|---:|---:|---|
| Live-action faces, 2x | NVIDIA Standard | 66.12 | RealSR Max Detail: 64.02 | NVIDIA was about 7.5x faster than RealSR and had the best VMAF. |
| Compressed live-action detail, 4x | RealSR JPEG Max Detail | 26.69 | NVIDIA Standard: 26.35 | RealSR narrowly led VMAF; NVIDIA was about 2.9x faster and led SSIM/PSNR. |
| Animation faces, 2x | Real-CUGAN Max Detail | 58.21 | NVIDIA Ultra: 50.32 | Real-CUGAN recovered substantially more perceptual detail. |
| Animation architecture, 2x | Real-CUGAN Balanced | 48.56 | NVIDIA Ultra: 42.25 | Balanced narrowly beat Max Detail and materially beat Lanczos. |
| 30-to-60 FPS city motion, 2x | RealSR Balanced + RIFE | 47.36 | NVIDIA + RIFE: 45.59 | RealSR + RIFE led quality; NVIDIA + RIFE was about 2.8x faster. |
| Interlaced city motion, 2x | NVIDIA Standard | 36.77 | RealSR Balanced: 36.52 | NVIDIA slightly led VMAF and was about 8.4x faster. |

The animation sweep changed one shipping default: compatible RTX systems now use NVIDIA Ultra rather than clean-Ultra for animation. Real-CUGAN remains the quality-first animation choice. NVIDIA denoise/deblur are not forced during enlargement because the current VFX runtime rejects that combination; PixelForge keeps those operations limited to valid same-resolution work.

The result file is `_benchmarks/representative-v1020/report/results.json` with SHA-256 `7cd3dc02b6651be3d2826b50538384fbf441bf4a2227f65f8af610117b67efe9`. The benchmark media and generated outputs are intentionally excluded from the product and repository.

## Reference material and attribution

- Live-action: Blender Foundation's *Tears of Steel* 720p open-movie encode: <https://download.blender.org/demo/movies/ToS/tears_of_steel_720p.mov>
- Animation: Blender Foundation's *Sintel* 1080p open-movie encode. *Sintel* is shared under CC BY 3.0: <https://durian.blender.org/sharing/>
- Native 60 FPS and interlace tests: `city_4cif.y4m` from Xiph's Derf test-media collection: <https://media.xiph.org/video/derf/>
- Netflix Open Content was reviewed as the next HDR/10-bit corpus. Its professional HDR masters are intentionally not part of this short release run because downloading multi-gigabyte assets merely to imply HDR-trained restoration would overstate what the current models prove: <https://opencontent.netflix.com/home>

Xiph notes that its hosted sequences are believed freely redistributable but may carry individual restrictions; the benchmark keeps source acquisition explicit and does not redistribute the footage. The script never downloads reference media automatically.

## Reproduce or add a Topaz result

Prepare the `reference` and `degraded` folders described by `tools/benchmark_representative_video.py`, then run:

```powershell
python tools\benchmark_representative_video.py --corpus _benchmarks\representative-v1020 --force
```

To make a direct Topaz comparison, process the identical degraded input to the identical target dimensions in Topaz and place it at `<corpus>\topaz\<case>.mp4`. The harness will include it without special treatment. No Topaz installation or output was available for this release run, so no Topaz number is reported or inferred.

For matched stills, run:

```powershell
python tools\compare_topaz.py --reference reference.png --source degraded.png --pixelforge pixelforge.png --topaz topaz.png --output _benchmarks\topaz-match
```

Objective metrics do not prove that a result is subjectively better. Blind review is still required for hallucinated detail, face identity, text, temporal flicker, and motion artifacts. A future public claim must include multiple clips per category, exact versions and settings, render time, peak storage, and full-resolution crops.

## Research engines worth an optional future tier

- FlashVSR v1.1: Apache-2.0, streaming one-step diffusion VSR and a promising research direction. Its CUDA and block-sparse-attention build remains too fragile for the default Windows install.
- SeedVR2: Apache-2.0, high-quality one-step diffusion restoration with official inference weights. Its multi-gigabyte model and PyTorch/CUDA requirements belong in an optional download, not the base installer.

An experimental engine must pass installation rollback, VRAM preflight, tiled/chunked rendering, checkpoint/resume, color preservation, temporal tests, and license verification before appearing in the normal profile buttons. NVIDIA VFX VSR remains the optional production tier because it has a supported Windows runtime and consumer RTX path.
