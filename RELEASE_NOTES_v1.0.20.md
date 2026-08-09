# PixelForge AI v1.0.20

- Adds current free local SPAN DirectML models for photo/video and animation upscaling.
- Adds an optional, hardware-gated NVIDIA RTX VSR Ultra profile using NVIDIA VFX 0.1.0.1. It appears only on compatible Tensor Core GPUs, installs through a SHA-256-verified sidecar pack, runs a real GPU probe before activation, and leaves universal profiles untouched on failure.
- Adds a simple Keep source / Smooth 60 FPS choice. RIFE v4.25 performs interpolation before the single final encode and automatically skips interpolation for sources already near 60 FPS.
- Replaces engine-scale choices with Same resolution, 1080p, 1440p, 4K, and 8K output targets.
- Rebuilds preview generation around three real source frames, selectable detail crops, cancellation, progress, and caching.
- Refines Fast, Balanced, and Max Detail defaults using a 52-variant still benchmark plus a six-case matched-source video run with VMAF, SSIM, PSNR, runtime, and comparison-sheet review.
- Adds a bounded-memory DirectML video path with one final encode and high-bit RGB48/FP32 inference for 10-bit sources.
- Adds disk preflight, native-engine batches, resume checkpoints, RIFE-before-final-encode, NVENC, professional codecs, and media-stream preservation.
- Adds automatic interlace correction, HDR/10-bit precision-safe model routing, and an explicit warning before extreme enlargement above 4x.
- Adds reproducible matched-source video and still PixelForge/Topaz comparison tools and an evidence-based claim gate. No Topaz output was available in this run, so the release makes no fabricated direct-comparison claim.
- Fixes checkout package plan IDs, verifies Stripe test checkout/idempotency, and prepares an optional server-priced lifetime Pro entitlement.
- Removes unused NumPy and ONNX developer/test tooling from the Windows bundle while retaining their maintained runtime hooks.

The optional NVIDIA engine is currently restricted to 8-bit SDR input. HDR/10-bit sources are automatically routed to the RGB48/FP32 DirectML precision path. The directory-only NCNN and RIFE engines still use disk-backed frame checkpoints. HDR metadata is preserved, but the bundled SPAN models are not advertised as HDR-trained models.

Representative video testing found NVIDIA Standard strongest for the tested live-action faces and interlaced motion, RealSR strongest for quality-first live restoration and 60 FPS motion, and Real-CUGAN strongest for animation. The measured animation sweep changed the RTX animation default from clean-Ultra to Ultra. Full results and source attribution are in `QUALITY_BENCHMARK.md`.
