"""Measure the main window and capture a representative loaded-state screenshot."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from PIL import ImageGrab


if os.name == "nt":
    try:
        import ctypes

        ctypes.windll.user32.SetProcessDpiAwarenessContext(ctypes.c_void_p(-4))
    except Exception:
        try:
            ctypes.windll.user32.SetProcessDPIAware()
        except Exception:
            pass


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("V11B_DISABLE_ANONYMOUS_DIAGNOSTICS", "1")
os.environ.setdefault("PIXELFORGE_DISABLE_NVIDIA_AUTO_SETUP", "1")

from process_full_video_ultimate import V11BApp  # noqa: E402


def descendants(widget):
    for child in widget.winfo_children():
        yield child
        yield from descendants(child)


def relative_bounds(widget, root_x: int, root_y: int) -> dict[str, int]:
    return {
        "x": widget.winfo_rootx() - root_x,
        "y": widget.winfo_rooty() - root_y,
        "width": widget.winfo_width(),
        "height": widget.winfo_height(),
    }


def main() -> None:
    app = V11BApp()
    try:
        app.input_video_var.set(r"C:\Users\creator\Videos\representative-camera-footage.mkv")
        app.output_video_var.set(r"C:\Users\creator\Videos\representative-camera-footage_pixelforge.mkv")
        app.quality_guard_notice_var.set("Quality guard: HDR and 10-bit precision protection active.")
        app.estimate_summary_var.set("18m 24s to 42m 10s")
        app.estimate_source_var.set("2560x1440 · 6,382 frames · 60 FPS")
        app.estimate_spec_var.set("RTX-class NVIDIA GPU · NVENC available · 32 GB RAM")
        app.estimate_stage_var.set("Extract · Upscale · Post · Interpolate · Finalize")
        app._set_estimate_visibility(True)
        app.update_idletasks()
        app._fit_window_to_content()
        app.update_idletasks()
        app.deiconify()
        app.update()
        time.sleep(0.8)
        app.update()

        root_x = app.winfo_rootx()
        root_y = app.winfo_rooty()
        root_width = app.winfo_width()
        root_height = app.winfo_height()
        work_x, work_y, work_width, work_height = app._visible_work_area()

        run_bounds = relative_bounds(app.run_section, root_x, root_y)
        content_bounds = relative_bounds(app.main_content_frame, root_x, root_y)
        footer_bounds = relative_bounds(app.footer_frame, root_x, root_y)
        log_bounds = relative_bounds(app.log_section, root_x, root_y)
        start_bounds = relative_bounds(app.start_processing_button, root_x, root_y)
        buy_bounds = relative_bounds(app.buy_credits_button, root_x, root_y)
        advanced_bounds = relative_bounds(app.advanced_link_label, root_x, root_y)
        left_canvas_count = sum(1 for item in descendants(app.left_panel) if item.winfo_class() == "Canvas")
        scrollbars = [item for item in descendants(app) if item.winfo_class() == "TScrollbar"]
        log_descendants = set(descendants(app.log_section))
        outside_log_scrollbars = [item for item in scrollbars if item not in log_descendants]

        checks = {
            "no_main_scroll_canvas": left_canvas_count == 0,
            "only_log_has_scrollbar": len(scrollbars) == 1 and not outside_log_scrollbars,
            "run_card_fully_visible": (
                run_bounds["y"] >= content_bounds["y"]
                and run_bounds["y"] + run_bounds["height"]
                <= content_bounds["y"] + content_bounds["height"]
            ),
            "run_actions_visible": (
                bool(app.start_processing_button.winfo_ismapped())
                and bool(app.buy_credits_button.winfo_ismapped())
                and start_bounds["y"] + start_bounds["height"] <= root_height
                and buy_bounds["y"] + buy_bounds["height"] <= root_height
            ),
            "advanced_link_visible": (
                bool(app.advanced_link_label.winfo_ismapped())
                and advanced_bounds["y"] + advanced_bounds["height"] <= run_bounds["y"]
            ),
            "footer_fully_visible": footer_bounds["y"] + footer_bounds["height"] <= root_height,
            "window_inside_work_area": (
                root_x >= work_x
                and root_y >= work_y
                and root_x + root_width <= work_x + work_width
                and root_y + root_height <= work_y + work_height
            ),
        }

        output_dir = ROOT / "artifacts" / "ui-audit"
        output_dir.mkdir(parents=True, exist_ok=True)
        screenshot = output_dir / "pixelforge-v1.0.21-loaded-layout.png"
        result = {
            "passed": all(checks.values()),
            "checks": checks,
            "root": {"x": root_x, "y": root_y, "width": root_width, "height": root_height},
            "work_area": {"x": work_x, "y": work_y, "width": work_width, "height": work_height},
            "content": content_bounds,
            "run": run_bounds,
            "start_button": start_bounds,
            "buy_button": buy_bounds,
            "advanced_link": advanced_bounds,
            "log": log_bounds,
            "footer": footer_bounds,
            "scrollbar_count": len(scrollbars),
            "mapped": {
                "state": app.state(),
                "root": bool(app.winfo_ismapped()),
                "start_button": bool(app.start_processing_button.winfo_ismapped()),
                "buy_button": bool(app.buy_credits_button.winfo_ismapped()),
                "advanced_link": bool(app.advanced_link_label.winfo_ismapped()),
            },
            "screenshot_captured": False,
            "screenshot": str(screenshot),
        }
        (output_dir / "layout-result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        try:
            ImageGrab.grab(
                bbox=(root_x, root_y, root_x + root_width, root_y + root_height),
                all_screens=True,
            ).save(screenshot)
            result["screenshot_captured"] = True
            (output_dir / "layout-result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        except OSError:
            pass
        print(json.dumps(result, indent=2))
        if not result["passed"]:
            raise SystemExit(1)
    finally:
        app.destroy()


if __name__ == "__main__":
    main()
