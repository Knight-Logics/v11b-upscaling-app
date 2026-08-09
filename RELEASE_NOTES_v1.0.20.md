# PixelForge AI v1.0.20

- Adds current free local SPAN DirectML models for photo/video and animation upscaling.
- Replaces engine-scale choices with Same resolution, 1080p, 1440p, 4K, and 8K output targets.
- Rebuilds preview generation around three real source frames, selectable detail crops, cancellation, progress, and caching.
- Refines Fast, Balanced, and Max Detail defaults using a 52-variant visual benchmark.
- Adds a bounded-memory DirectML video path with one final encode and high-bit RGB48/FP32 inference for 10-bit sources.
- Adds disk preflight, native-engine batches, resume checkpoints, RIFE-before-final-encode, NVENC, professional codecs, and media-stream preservation.
- Adds automatic interlace correction, HDR/10-bit precision-safe model routing, and an explicit warning before extreme enlargement above 4x.
- Adds a reproducible matched-source PixelForge/Topaz comparison tool and claim gate.
- Fixes checkout package plan IDs, verifies Stripe test checkout/idempotency, and prepares an optional server-priced lifetime Pro entitlement.
- Removes unused NumPy and ONNX developer/test tooling from the Windows bundle while retaining their maintained runtime hooks.

The directory-only NCNN and RIFE engines still use disk-backed frame checkpoints. HDR metadata is preserved, but the bundled SPAN models are not advertised as HDR-trained models.
