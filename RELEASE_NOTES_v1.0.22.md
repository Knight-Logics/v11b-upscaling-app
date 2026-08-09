# PixelForge AI v1.0.22

## RTX installer integrity hotfix

- The release workflow now builds the NVIDIA pack before the universal app and embeds the exact pack SHA-256 produced by that clean release run.
- Automatic first-run RTX setup therefore verifies the same public artifact attached to the release instead of relying on a checksum from a different build machine.
- The all-in-one NVIDIA edition is assembled only after the integrity-pinned universal executable is complete.

All v1.0.21 interface, preview, pricing, pipeline, media-preservation, and quality-profile improvements remain included.
