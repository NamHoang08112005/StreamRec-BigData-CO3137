#!/usr/bin/env python3
"""Move a fraction of downloaded ShortVideo mp4s into the cold-start upload pool."""
from __future__ import annotations

import argparse
import random
import shutil
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", default="data/raw/shortvideo/raw_file")
    ap.add_argument("--pool-dir", default="data/coldstart/upload_pool")
    ap.add_argument("--fraction", type=float, default=0.30)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--copy", action="store_true", help="Copy instead of move")
    args = ap.parse_args()

    raw = Path(args.raw_dir)
    pool = Path(args.pool_dir)
    pool.mkdir(parents=True, exist_ok=True)

    files = sorted(p for p in raw.glob("*.mp4") if p.is_file() and p.stat().st_size > 10_000)
    if not files:
        print(f"No mp4 found in {raw}")
        return 1

    n_cold = max(1, int(round(len(files) * args.fraction)))
    rng = random.Random(args.seed)
    cold = sorted(rng.sample(files, n_cold), key=lambda p: p.name)

    # Clear previous pool contents so fraction is exact.
    for old in pool.glob("*"):
        if old.is_file():
            old.unlink()

    for src in cold:
        dest = pool / src.name
        if args.copy:
            shutil.copy2(src, dest)
        else:
            shutil.move(str(src), str(dest))

    remain = len(list(raw.glob("*.mp4")))
    print(
        f"total_downloaded={len(files)} coldstart={n_cold} "
        f"catalog_remain={remain} pool={pool} mode={'copy' if args.copy else 'move'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
