"""Create an evidence report from matched PixelForge and Topaz outputs.

Use a known high-resolution reference, degrade it once, then process that same
degraded input in both applications. This avoids subjective or mismatched demos.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from benchmark_presets import contact_sheet, metrics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference", required=True, type=Path, help="Known high-resolution reference")
    parser.add_argument("--source", required=True, type=Path, help="Identical degraded source used by both apps")
    parser.add_argument("--pixelforge", required=True, type=Path, help="PixelForge result")
    parser.add_argument("--topaz", required=True, type=Path, help="Topaz result")
    parser.add_argument("--output", required=True, type=Path, help="Output report directory")
    args = parser.parse_args()

    for item in (args.reference, args.source, args.pixelforge, args.topaz):
        if not item.is_file():
            raise FileNotFoundError(item)

    args.output.mkdir(parents=True, exist_ok=True)
    results = {
        "method": "matched-source full-reference still comparison",
        "limitations": [
            "These still metrics do not measure temporal consistency, face identity, or motion artifacts.",
            "A product-level superiority claim also requires representative video clips and blind human review.",
        ],
        "pixelforge": metrics(args.reference, args.pixelforge),
        "topaz": metrics(args.reference, args.topaz),
    }
    results["metric_winner"] = max(
        ("pixelforge", "topaz"),
        key=lambda name: float(results[name]["score"]),
    )
    contact_sheet(
        args.reference,
        args.source,
        [("PixelForge", args.pixelforge), ("Topaz", args.topaz)],
        args.output / "comparison.png",
    )
    (args.output / "results.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
