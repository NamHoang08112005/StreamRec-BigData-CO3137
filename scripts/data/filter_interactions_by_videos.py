#!/usr/bin/env python3
"""Keep only interaction rows whose pid has a local mp4 (raw_file)."""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


def pid_of(row: dict) -> str:
    return str(row.get("pid") or row.get("video_id") or row.get("item_id") or "").strip()


def load_pids(raw_dir: Path) -> set[str]:
    return {
        p.stem
        for p in raw_dir.glob("*.mp4")
        if p.is_file() and p.stat().st_size > 10_000
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/raw/shortvideo")
    ap.add_argument("--raw-dir", default="", help="Override mp4 dir (default: <root>/raw_file)")
    ap.add_argument("--source", default="interaction_sampled.csv")
    ap.add_argument("--out", default="interaction_filtered.csv")
    args = ap.parse_args()

    root = Path(args.root)
    raw = Path(args.raw_dir) if args.raw_dir else root / "raw_file"
    src = root / args.source
    dest = root / args.out

    if not src.exists():
        print(f"Missing {src}", file=sys.stderr)
        return 1
    if not raw.exists():
        print(f"Missing {raw}", file=sys.stderr)
        return 1

    pids = load_pids(raw)
    if not pids:
        print(f"No mp4 found in {raw}", file=sys.stderr)
        return 1

    kept = dropped = 0
    pids_in_kept: set[str] = set()

    with src.open(encoding="utf-8", errors="ignore", newline="") as fin, dest.open(
        "w", encoding="utf-8", newline=""
    ) as fout:
        reader = csv.DictReader(fin)
        if reader.fieldnames:
            reader.fieldnames = [h.lstrip("\ufeff").strip() if h else h for h in reader.fieldnames]
        fieldnames = list(reader.fieldnames or [])
        writer = csv.DictWriter(fout, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in reader:
            if "\ufeffuser_id" in row and "user_id" not in row:
                row["user_id"] = row.get("\ufeffuser_id")
            pid = pid_of(row)
            if pid and pid in pids:
                writer.writerow(row)
                kept += 1
                pids_in_kept.add(pid)
            else:
                dropped += 1

    print(
        f"pids_on_disk={len(pids)} pids_in_kept={len(pids_in_kept)} "
        f"kept_rows={kept} dropped_rows={dropped} out={dest}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
