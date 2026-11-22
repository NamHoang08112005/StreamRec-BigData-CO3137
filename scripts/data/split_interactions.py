#!/usr/bin/env python3
"""Split ShortVideo interaction CSV into history (early 70%) vs stream (late 30%)."""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


def row_time(row: dict) -> int:
    for key in ("exposed_time", "p_date"):
        raw = str(row.get(key) or "").strip()
        if not raw:
            continue
        try:
            return int(float(raw))
        except ValueError:
            digits = "".join(ch for ch in raw if ch.isdigit())
            if digits:
                return int(digits)
    return 0


def normalize_reader(reader: csv.DictReader) -> None:
    if reader.fieldnames:
        reader.fieldnames = [h.lstrip("\ufeff").strip() if h else h for h in reader.fieldnames]


def normalize_row(row: dict) -> dict:
    if "\ufeffuser_id" in row and "user_id" not in row:
        row["user_id"] = row.get("\ufeffuser_id")
    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/raw/shortvideo")
    ap.add_argument("--source", default="interaction_sampled.csv")
    ap.add_argument("--history-ratio", type=float, default=0.70)
    ap.add_argument("--history-out", default="interactions_history.csv")
    ap.add_argument("--stream-out", default="interactions_stream.csv")
    args = ap.parse_args()

    root = Path(args.root)
    src = root / args.source
    if not src.exists():
        alt = root / "interaction.csv"
        if alt.exists():
            src = alt
        else:
            print(f"Missing {src}", file=sys.stderr)
            return 1

    ratio = min(0.95, max(0.05, args.history_ratio))
    print(f"Pass 1: scanning times from {src} …")
    times: list[int] = []
    with src.open(encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        normalize_reader(reader)
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            times.append(row_time(normalize_row(row)))

    n = len(times)
    if n == 0:
        print("No rows", file=sys.stderr)
        return 1

    ordered = sorted(times)
    cut_idx = min(n - 1, max(0, int(n * ratio) - 1))
    cutoff = ordered[cut_idx]
    print(f"rows={n} history_ratio={ratio} cutoff_time={cutoff}")

    hist_path = root / args.history_out
    stream_path = root / args.stream_out
    n_hist = n_stream = 0

    print("Pass 2: writing history / stream …")
    with (
        src.open(encoding="utf-8", errors="ignore", newline="") as fin,
        hist_path.open("w", encoding="utf-8", newline="") as fh,
        stream_path.open("w", encoding="utf-8", newline="") as fs,
    ):
        reader = csv.DictReader(fin)
        normalize_reader(reader)
        fieldnames = list(reader.fieldnames or fieldnames)
        wh = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        ws = csv.DictWriter(fs, fieldnames=fieldnames, extrasaction="ignore")
        wh.writeheader()
        ws.writeheader()
        for row in reader:
            row = normalize_row(row)
            # Keep only known columns in stable order
            clean = {k: row.get(k, "") for k in fieldnames}
            if row_time(row) <= cutoff:
                wh.writerow(clean)
                n_hist += 1
            else:
                ws.writerow(clean)
                n_stream += 1

    readme = root / "README_interactions.md"
    readme.write_text(
        f"""# Interaction splits

Source: `{src.name}`

| File | Rows | Rule |
|------|------|------|
| `{hist_path.name}` | {n_hist} | early ~{ratio:.0%} by `exposed_time` (fallback `p_date`) |
| `{stream_path.name}` | {n_stream} | late ~{1 - ratio:.0%} |

Cutoff time value: `{cutoff}`

- History → ETL seed + Batch CF
- Stream → traffic-simulator real replay
""",
        encoding="utf-8",
    )
    print(f"Wrote {hist_path} rows={n_hist}")
    print(f"Wrote {stream_path} rows={n_stream}")
    print(f"Wrote {readme}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
