#!/usr/bin/env python3
"""Build pool_meta.json + POST /api/admin/scrub-tags to replace fake ShortVideo tags."""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import httpx

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

_NOISE = frozenset(
    {"shortvideo", "short_video", "demo", "auto_upload", "coldstart", "cold_start"}
)


def clean_tag(tag: str) -> str | None:
    t = str(tag or "").strip()
    if not t:
        return None
    if t.lower().replace(" ", "_") in _NOISE:
        return None
    return t.lower().replace(" ", "_")


def load_category_names(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for name in ("categories_cn_en.csv", "category_cn_en.csv"):
        path = root / name
        if not path.exists():
            continue
        with path.open(encoding="utf-8", errors="ignore", newline="") as f:
            for row in csv.DictReader(f):
                cid = str(row.get("category_id") or "").strip()
                en = str(row.get("category_name_en") or row.get("category_en") or "").strip()
                if cid and en:
                    out[cid] = en.lower().replace(" ", "_")
    return out


def load_titles(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    title_dir = root / "title_en"
    if not title_dir.exists():
        return out
    for p in title_dir.glob("*.txt"):
        text = p.read_text(encoding="utf-8", errors="ignore").strip()
        if text:
            out[p.stem] = text
    return out


def build_meta(root: Path, pids: set[str]) -> dict[str, dict]:
    cats = load_category_names(root)
    titles = load_titles(root)
    meta: dict[str, dict] = {
        pid: {"tags": [], "category": "general", "title": titles.get(pid)} for pid in pids
    }
    tag_sets: dict[str, set[str]] = {pid: set() for pid in pids}

    src = None
    for name in (
        "interaction_filtered.csv",
        "interactions_history.csv",
        "interactions_stream.csv",
        "interaction_sampled.csv",
    ):
        path = root / name
        if path.exists():
            src = path
            break
    if not src:
        return meta

    with src.open(encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            pid = str(row.get("pid") or "").strip()
            if pid not in meta:
                continue
            m = meta[pid]
            tag = clean_tag(str(row.get("tag_name") or ""))
            if tag:
                tag_sets[pid].add(tag)
            cid = str(row.get("category_id") or "").strip()
            if cid and cats.get(cid):
                m["category"] = cats[cid]
            title = str(row.get("title") or "").strip()
            if title and not m.get("title"):
                m["title"] = title

    for pid, m in meta.items():
        tags = sorted(tag_sets.get(pid) or [])
        cat = m.get("category") or "general"
        if cat and cat not in _NOISE and cat != "general" and cat not in tags:
            tags = [cat, *tags]
        if not tags:
            tags = [cat if cat else "general"]
        m["tags"] = tags[:12]
        if not m.get("title"):
            pretty = cat.replace("_", " ").title() if cat else "Clip"
            m["title"] = f"{pretty} · {pid}"
    return meta


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/raw/shortvideo")
    ap.add_argument("--pool-dir", default="data/coldstart/upload_pool")
    ap.add_argument("--pool-meta", default="data/coldstart/pool_meta.json")
    ap.add_argument("--api", default="http://localhost:8000")
    args = ap.parse_args()

    root = Path(args.root)
    map_path = root / "video_id_map.json"
    mapped: dict[str, str] = {}
    if map_path.exists():
        mapped = json.loads(map_path.read_text(encoding="utf-8"))

    pool_dir = Path(args.pool_dir)
    pool_pids = {p.stem for p in pool_dir.glob("*.mp4")} if pool_dir.exists() else set()
    all_pids = set(mapped.keys()) | pool_pids
    if not all_pids:
        print("No pids from video_id_map / upload_pool", file=sys.stderr)
        return 1

    print(f"Building meta for {len(all_pids)} pids…")
    meta = build_meta(root, all_pids)

    pool_meta_path = Path(args.pool_meta)
    pool_meta_path.parent.mkdir(parents=True, exist_ok=True)
    pool_only = {pid: meta[pid] for pid in pool_pids if pid in meta}
    pool_meta_path.write_text(json.dumps(pool_only, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {pool_meta_path} ({len(pool_only)} pool entries)")

    items = []
    for pid, vid in mapped.items():
        m = meta.get(pid, {})
        items.append(
            {
                "video_id": vid,
                "tags": m.get("tags") or ["general"],
                "category": m.get("category") or "general",
                "title": str(m.get("title") or f"Clip {pid}")[:120],
            }
        )

    print(f"POST scrub-tags items={len(items)} → {args.api}")
    with httpx.Client(timeout=180) as client:
        r = client.post(
            f"{args.api.rstrip('/')}/api/admin/scrub-tags",
            json={"items": items, "drop_noise": True},
        )
        print(r.status_code, r.text[:800])
        return 0 if r.status_code < 400 else 1


if __name__ == "__main__":
    raise SystemExit(main())
