"""Launch PixelForge with a known input so the real three-frame preview can be visually checked."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import process_full_video_ultimate as app_module  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    args = parser.parse_args()
    input_path = args.input.resolve()
    if not input_path.is_file():
        raise FileNotFoundError(input_path)

    app = app_module.V11BApp()
    app._register_auto_estimate_watchers()
    app._register_auto_compare_watchers()
    def load_input() -> None:
        app.input_video_var.set(str(input_path))
        app.output_video_var.set(str(input_path.with_name(f"{input_path.stem}_pixelforge_smoke.mkv")))
        app._sync_target_dimensions_from_source(input_path)
        app._sync_target_fps_to_source_if_needed(str(input_path))
        app._normalize_interpolation_choice(show_feedback=False)
        app._auto_prepare_after_input()

    app.after(350, load_input)
    app.after(120, app._ensure_window_visible)
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
