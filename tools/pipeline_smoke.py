"""Run a tiny end-to-end pipeline without billing or GUI state."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from queue import Queue
from threading import Event


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
MODULE_PATH = ROOT / "process_full_video_ultimate.py"
SPEC = importlib.util.spec_from_file_location("pixelforge_smoke_app", MODULE_PATH)
assert SPEC and SPEC.loader
APP = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = APP
SPEC.loader.exec_module(APP)


def main() -> None:
    work = ROOT / "_benchmarks" / "pipeline-smoke"
    work.mkdir(parents=True, exist_ok=True)
    source = work / "source.mkv"
    output = work / "output.mkv"
    fixture = ROOT / "tests" / "fixtures"
    subprocess.run(
        [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=10:duration=1",
            "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=1",
            "-f", "lavfi", "-i", "sine=frequency=660:sample_rate=48000:duration=1",
            "-i", str(fixture / "pipeline-smoke.srt"),
            "-i", str(fixture / "pipeline-smoke.ffmeta"),
            "-map", "0:v:0", "-map", "1:a:0", "-map", "2:a:0", "-map", "3:s:0",
            "-map_metadata", "4", "-map_chapters", "4",
            "-metadata:s:a:0", "language=eng", "-metadata:s:a:1", "language=spa",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-c:s", "srt",
            str(source),
        ],
        check=True,
    )

    settings = APP.PipelineSettings(
        input_video=source,
        output_video=output,
        model="span-photo-x4",
        scale=4,
        image_format="png",
        threads="2:2:2",
        start_time=0.0,
        clip_duration=0.0,
        denoise=0.0,
        enable_color=False,
        vibrance=0.0,
        contrast=1.0,
        brightness=0.0,
        saturation=1.0,
        gamma=1.0,
        enable_sharpen=False,
        cas_strength=0.0,
        unsharp1=0.0,
        unsharp2=0.0,
        enable_interpolation=False,
        target_fps=10,
        interp_engine=APP.INTERP_ENGINE_RIFE,
        rife_model="rife-v4.25",
        apply_final_scale=True,
        preserve_aspect_ratio=False,
        target_width=320,
        target_height=180,
        crf=21,
        encode_preset="veryfast",
        video_codec="H.264",
        prefer_hardware_encode=True,
        preserve_media=True,
        chunk_size=4,
        resume_job=True,
        include_audio=True,
        keep_intermediate=False,
    )
    messages: Queue[str] = Queue()
    APP.PipelineRunner(settings, messages, Event()).run()
    rife_output = work / "output-rife.mkv"
    rife_settings = replace(
        settings,
        output_video=rife_output,
        enable_interpolation=True,
        target_fps=20,
    )
    APP.PipelineRunner(rife_settings, messages, Event()).run()
    high_bit_source = work / "source-10bit.mkv"
    high_bit_output = work / "output-10bit.mkv"
    subprocess.run(
        [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=2:duration=1",
            "-vf", "format=yuv420p10le", "-c:v", "libx265", "-preset", "ultrafast",
            "-x265-params", "log-level=error", str(high_bit_source),
        ],
        check=True,
    )
    high_bit_settings = replace(
        settings,
        input_video=high_bit_source,
        output_video=high_bit_output,
        target_width=160,
        target_height=90,
        target_fps=2,
        video_codec="HEVC 10-bit",
        include_audio=False,
        preserve_media=False,
    )
    APP.PipelineRunner(high_bit_settings, messages, Event()).run()
    interlaced_source = work / "source-interlaced.mkv"
    interlaced_output = work / "output-deinterlaced.mkv"
    subprocess.run(
        [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "testsrc2=size=160x96:rate=10:duration=1",
            "-vf", "tinterlace=mode=interleave_top", "-flags", "+ilme+ildct", "-top", "1",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", str(interlaced_source),
        ],
        check=True,
    )
    interlaced_settings = replace(
        settings,
        input_video=interlaced_source,
        output_video=interlaced_output,
        target_width=160,
        target_height=96,
        target_fps=5,
        include_audio=False,
        preserve_media=False,
        auto_deinterlace=True,
    )
    APP.PipelineRunner(interlaced_settings, messages, Event()).run()
    probe = json.loads(
        subprocess.check_output(
            ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", "-show_chapters", str(output)],
            text=True,
        )
    )
    streams = probe.get("streams", [])
    summary = {
        "video_codec": next(stream["codec_name"] for stream in streams if stream.get("codec_type") == "video"),
        "audio_tracks": sum(stream.get("codec_type") == "audio" for stream in streams),
        "subtitles": sum(stream.get("codec_type") == "subtitle" for stream in streams),
        "chapters": len(probe.get("chapters", [])),
        "output": str(output),
    }
    rife_probe = json.loads(
        subprocess.check_output(
            ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", "-show_chapters", str(rife_output)],
            text=True,
        )
    )
    rife_streams = rife_probe.get("streams", [])
    rife_video = next(stream for stream in rife_streams if stream.get("codec_type") == "video")
    summary["rife"] = {
        "fps": rife_video.get("avg_frame_rate"),
        "audio_tracks": sum(stream.get("codec_type") == "audio" for stream in rife_streams),
        "subtitles": sum(stream.get("codec_type") == "subtitle" for stream in rife_streams),
        "chapters": len(rife_probe.get("chapters", [])),
        "output": str(rife_output),
    }
    high_bit_probe = json.loads(
        subprocess.check_output(
            ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", str(high_bit_output)],
            text=True,
        )
    )
    high_bit_video = next(
        stream for stream in high_bit_probe.get("streams", []) if stream.get("codec_type") == "video"
    )
    summary["high_bit_directml"] = {
        "codec": high_bit_video.get("codec_name"),
        "pixel_format": high_bit_video.get("pix_fmt"),
        "bits_per_raw_sample": high_bit_video.get("bits_per_raw_sample"),
        "output": str(high_bit_output),
    }
    interlaced_probe = json.loads(
        subprocess.check_output(
            ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", str(interlaced_output)],
            text=True,
        )
    )
    interlaced_video = next(
        stream for stream in interlaced_probe.get("streams", []) if stream.get("codec_type") == "video"
    )
    summary["auto_deinterlace"] = {
        "field_order": interlaced_video.get("field_order"),
        "output": str(interlaced_output),
    }
    print(json.dumps(summary, indent=2))
    if summary["audio_tracks"] != 2 or summary["subtitles"] != 1 or summary["chapters"] != 1:
        raise SystemExit("media preservation smoke failed")
    if summary["rife"]["fps"] != "20/1" or summary["rife"]["audio_tracks"] != 2:
        raise SystemExit("RIFE single-encode smoke failed")
    if "10" not in str(summary["high_bit_directml"]["pixel_format"]):
        raise SystemExit("high-bit DirectML output smoke failed")
    if summary["auto_deinterlace"]["field_order"] not in {"progressive", "unknown", None}:
        raise SystemExit("automatic deinterlace smoke failed")


if __name__ == "__main__":
    main()
