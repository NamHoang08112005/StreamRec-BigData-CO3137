#!/usr/bin/env python3
"""Purge synthetic/old catalog then ETL ShortVideo real mp4 + interactions."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/raw/shortvideo")
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--max-videos", type=int, default=300)
    ap.add_argument("--max-interactions", type=int, default=20000)
    args = ap.parse_args()

    root = Path(args.root)
    if not (root / "raw_file").exists():
        print(
            f"Place ShortVideo dataset under {root}/ "
            "(raw_file/*.mp4 + interaction_sampled.csv; "
            "prefer interactions_history.csv after split_interactions.py).\n"
            "See docs/data-provenance.md and docs/data-simulation.md",
            file=sys.stderr,
        )
        return 1

    cmd = [
        sys.executable,
        str(Path(__file__).with_name("etl_shortvideo.py")),
        "--root",
        args.root,
        "--api",
        args.api,
        "--max-videos",
        str(args.max_videos),
        "--max-interactions",
        str(args.max_interactions),
        "--purge",
    ]
    print("Running:", " ".join(cmd))
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
