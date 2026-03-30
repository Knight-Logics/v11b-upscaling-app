# v11b Workspace Scope

This folder is the active project workspace for the v11b upscaling application.

## Active Goal
Turn `process_full_video_ultimate.py` into a finished desktop upscaling app for portfolio and Upwork showcase use.

## Project Rules
- Keep processing logic in `process_full_video_ultimate.py`.
- Keep Real-ESRGAN binaries and `models/` in this same folder.
- Prioritize configurable settings and clear speed/quality tradeoffs.
- Include runtime estimation and fast profile options for quick test iterations.

## Main App Entry
- `process_full_video_ultimate.py` (Tkinter desktop GUI)

## Current Status
- GUI includes adjustable settings for:
  - model, scale, threads, image format
  - denoise and color controls
  - sharpen controls
  - interpolation and target FPS
  - final scaling, CRF, encoder preset, audio include
  - clip range (start time and duration)
- Includes runtime estimator and speed-focused profiles (Fast Draft, Balanced, Quality).
