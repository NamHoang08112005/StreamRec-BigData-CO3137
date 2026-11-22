#!/usr/bin/env python3
"""Offline builder for data/coldstart/pool_meta.json (no API needed)."""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
NOISE = frozenset({"shortvideo", "short_video", "demo", "auto_upload", "coldstart", "cold_start"})


def main() -> int:
    root = Path("data/raw/shortvideo")
    pool = {p.stem for p in Path("data/coldstart/upload_pool").glob("*.mp4")}
    cats: dict[str, str] = {}
    for name in ("categories_cn_en.csv", "category_cn_en.csv"):
        path = root / name
        if not path.exists():
            continue
        with path.open(encoding="utf-8", errors="ignore", newline="") as f:
            for row in csv.DictReader(f):
                cid = str(row.get("category_id") or "").strip()
                en = str(row.get("category_name_en") or row.get("category_en") or "").strip()
                if cid and en:
                    cats[cid] = en.lower().replace(" ", "_")
    titles: dict[str, str] = {}
    td = root / "title_en"
    if td.exists():
        for p in td.glob("*.txt"):
            text = p.read_text(encoding="utf-8", errors="ignore").strip()
            if text:
                titles[p.stem] = text
    meta = {pid: {"tags": set(), "category": "general", "title": titles.get(pid)} for pid in pool}
    src = root / "interaction_filtered.csv"
    with src.open(encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            pid = str(row.get("pid") or "").strip()
            if pid not in meta:
                continue
            tag = str(row.get("tag_name") or "").strip().lower().replace(" ", "_")
            if tag and tag not in NOISE:
                meta[pid]["tags"].add(tag)
            cid = str(row.get("category_id") or "").strip()
            if cid and cats.get(cid):
                meta[pid]["category"] = cats[cid]
            title = str(row.get("title") or "").strip()
            if title and not meta[pid].get("title"):
                meta[pid]["title"] = title
    out: dict[str, dict] = {}
    for pid, m in meta.items():
        tags = sorted(m["tags"])
        cat = m["category"]
        if cat and cat not in NOISE and cat != "general" and cat not in tags:
            tags = [cat, *tags]
        if not tags:
            tags = [cat]
        title = m.get("title") or f"{cat.replace('_', ' ').title()} · {pid}"
        out[pid] = {"tags": tags[:12], "category": cat, "title": title[:120]}
    dest = Path("data/coldstart/pool_meta.json")
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"pool_meta={len(out)} -> {dest}")
    if out:
        k = next(iter(out))
        print("sample", k, out[k])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
