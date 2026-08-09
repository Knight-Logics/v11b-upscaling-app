# PixelForge AI v1.0.21

## One-screen desktop layout

- Removed the hidden main-window scrolling canvas. The Processing Log is now the only scrollable region.
- Reserved the Run card at the bottom of the left panel so Start Processing and Buy Credits cannot be pushed below the window.
- Compacted the header, source delivery controls, profiles, preview controls, log, and loaded-media footer.
- Window sizing now respects the usable Windows work area, including the taskbar.
- Added a repeatable loaded-state layout smoke that verifies the Run actions, Advanced link, footer, and sole log scrollbar are visible.

## Clear quality and performance choices

- The main screen now explains that Balanced is the best overall default, Max Detail is the highest-fidelity and slowest path, Fast Preview is the quickest test, and NVIDIA RTX is the fastest compatible SDR path.
- Compatible NVIDIA GPUs receive a one-time automatic setup offer instead of requiring users to discover the Setup RTX button first.
- The verified RTX pack still discloses its approximate 585 MB download and 1 GB installed size and keeps HDR/10-bit sources on the precision-safe DirectML route.
- The release build can produce an all-in-one NVIDIA RTX ZIP with the sidecar engine already included.

## Revised credit packs

- New accounts receive 8 server-backed trial credits.
- Current packs are 12 credits for $5, 30 for $10, and 72 for $20.
- The app renders the server-authoritative plan catalog and keeps older app checkout IDs compatible without advertising them.
- Fixed the in-app Buy Now path assigning the package after it was referenced.
