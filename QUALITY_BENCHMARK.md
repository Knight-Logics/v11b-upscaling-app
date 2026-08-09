# PixelForge quality claim gate

PixelForge should not claim to beat Topaz Video globally until matched-source tests support that statement.

## Current baseline

PixelForge v1.0.20 prioritizes local, redistributable and predictable processing. It now protects HDR/10-bit video from accidental 8-bit AI inference, automatically corrects interlaced sources, requires confirmation above a credible 4x detail-recovery range, preserves professional media streams, and uses preview crops before a paid render.

The optional NVIDIA RTX VSR engine has passed real inference on an RTX 5070 Ti plus end-to-end video checks for 720p output, audio/duration preservation, streaming without frame directories, and the combined NVIDIA + RIFE 60 FPS path. These are functional acceptance tests, not proof that its perceptual quality exceeds Topaz.

Topaz remains broader today: it includes dedicated temporal enhancement, deinterlacing, stabilization, motion deblur, SDR-to-HDR, second-pass enhancement and diffusion models. PixelForge can still win on particular sources, local control, media preservation, privacy, pricing and reproducibility, but those are narrower claims.

## Direct comparison procedure

1. Select at least five reference clips per category: camera/people, animation/game, old/compressed, interlaced and HDR/10-bit.
2. Create one degraded source from every reference and give the identical source and target resolution to both applications.
3. Record model, settings, version, GPU, render time and peak storage.
4. Compare three crops from every clip and run blind A/B review before revealing the application.
5. Measure full-reference quality where a reference exists, plus temporal flicker, face identity, text legibility and motion artifacts.
6. Publish category-specific results. Do not turn one winning clip into a universal claim.

For matched stills, run:

```powershell
python tools\compare_topaz.py --reference reference.png --source degraded.png --pixelforge pixelforge.png --topaz topaz.png --output _benchmarks\topaz-match
```

## Research engines worth an optional future tier

- FlashVSR v1.1: Apache-2.0, streaming one-step diffusion VSR and the most practical current research direction. Its CUDA and block-sparse-attention build remains too fragile for the default Windows install.
- SeedVR2: Apache-2.0, high-quality one-step diffusion restoration with official inference weights. Its multi-gigabyte model and PyTorch/CUDA requirements belong in an optional download, not the base installer.

An experimental engine must pass installation rollback, VRAM preflight, tiled/chunked rendering, checkpoint/resume, color preservation, temporal tests and license verification before appearing in the normal profile buttons.

NVIDIA VFX VSR is the current optional production tier because it has a supported Windows runtime and consumer RTX hardware path. FlashVSR and SeedVR2 remain evaluation candidates until their Windows packaging, VRAM behavior, consumer-GPU support, and temporal quality meet the same release gate.
