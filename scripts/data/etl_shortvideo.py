#!/usr/bin/env python3
"""
ETL ShortVideo (real mp4 + real interactions) → StreamRec API.

Prefers interactions_history.csv (time-split ~70% early for Batch CF seed).
Falls back to interaction_sampled.csv / legacy filtered names if history missing.
Writes video_id_map.json (pid → v_…) for traffic-simulator stream replay.

Supports dataset quirks:
- video id column: pid | video_id | item_id
- interaction file: interactions_history.csv | interaction_sampled.csv | …
- categories: categories_cn_en.csv | category_cn_en.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import httpx

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

_TAG_NOISE = frozenset(
    {"shortvideo", "short_video", "demo", "auto_upload", "coldstart", "cold_start"}
)


def _clean_tag(tag: str) -> str | None:
    t = str(tag or "").strip()
    if not t:
        return None
    low = t.lower().replace(" ", "_")
    if low in _TAG_NOISE:
        return None
    return t


def find_interaction_csv(root: Path) -> Path | None:
    # Prefer time-split history (long-term CF seed); then pid-filtered; then full dump.
    for name in (
        "interactions_history.csv",
        "interaction_filtered.csv",
        "interaciton_filtered.csv",
        "interaction_sampled.csv",
        "interaction.csv",
        "interaciton.csv",
    ):
        p = root / name
        if p.exists():
            return p
        p2 = root / "extracted" / name
        if p2.exists():
            return p2
    return None


def load_titles(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for base in (root, root / "extracted"):
        title_dir = base / "title_en"
        if not title_dir.exists():
            continue
        for p in title_dir.glob("*.txt"):
            text = p.read_text(encoding="utf-8", errors="ignore").strip()
            out[p.stem] = text or p.stem
    return out


def load_category_names(root: Path) -> dict[str, str]:
    """category_id -> english name."""
    out: dict[str, str] = {}
    for base in (root, root / "extracted"):
        for name in ("categories_cn_en.csv", "category_cn_en.csv"):
            path = base / name
            if not path.exists():
                continue
            with path.open(encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid = str(row.get("category_id") or "").strip()
                    en = str(row.get("category_name_en") or row.get("category_en") or "").strip()
                    if cid and en:
                        out[cid] = en.lower().replace(" ", "_")
    return out


def vid_of(row: dict) -> str:
    return str(row.get("pid") or row.get("video_id") or row.get("item_id") or "").strip()


def pick_event_type(row: dict) -> str:
    for key in ("cvm_like", "like", "is_like", "liked"):
        if key in row and str(row[key]).lower() in {"1", "true", "yes"}:
            return "like"
    for key in ("hate",):
        if key in row and str(row[key]).lower() in {"1", "true", "yes"}:
            return "skip"
    for key in ("forward", "collect"):
        if key in row and str(row[key]).lower() in {"1", "true", "yes"}:
            return "share"
    return "watch"


def watch_time_of(row: dict) -> float:
    for key in ("watch_time", "duration"):
        if row.get(key) not in (None, ""):
            try:
                return max(0.5, min(180.0, float(row[key])))
            except ValueError:
                continue
    return 5.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/raw/shortvideo")
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--max-videos", type=int, default=300)
    ap.add_argument("--max-interactions", type=int, default=30000)
    ap.add_argument("--replay-events", type=int, default=4000)
    ap.add_argument("--purge", action="store_true")
    ap.add_argument("--scan-rows", type=int, default=2_000_000, help="Max CSV rows to scan")
    args = ap.parse_args()

    root = Path(args.root)
    media_dirs = [p for p in (root / "raw_file", root / "extracted" / "raw_file") if p.exists()]
    if not media_dirs:
        print(f"Missing raw_file under {root}. Run download_shortvideo.py first.", file=sys.stderr)
        return 1

    mp4s: dict[str, Path] = {}
    for d in media_dirs:
        for p in d.glob("*.mp4"):
            mp4s[p.stem] = p
    print(f"Found {len(mp4s)} local mp4")

    inter_path = find_interaction_csv(root)
    if not inter_path:
        print("Missing interaction.csv — put Dropbox/server dump under root or extracted/", file=sys.stderr)
        return 1
    print(f"Using interactions: {inter_path}")

    titles = load_titles(root)
    cat_names = load_category_names(root)

    # Prefer videos that appear in interactions
    video_counts: Counter[str] = Counter()
    video_meta: dict[str, dict] = defaultdict(lambda: {"tags": set(), "category": None, "author": None, "title": None})
    interactions: list[dict] = []
    scanned = 0
    with inter_path.open(encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        # normalize BOM / whitespace in headers
        if reader.fieldnames:
            reader.fieldnames = [h.lstrip("\ufeff").strip() if h else h for h in reader.fieldnames]
        print(f"columns: {reader.fieldnames}")
        for row in reader:
            scanned += 1
            # also normalize keys if DictReader kept BOM on first key
            if "\ufeffuser_id" in row and "user_id" not in row:
                row["user_id"] = row.get("\ufeffuser_id")
            vid = vid_of(row)
            uid = str(row.get("user_id") or "").strip()
            if not vid or not uid:
                continue
            if vid in mp4s:
                video_counts[vid] += 1
                interactions.append(row)
                meta = video_meta[vid]
                tag = _clean_tag(row.get("tag_name") or "")
                if tag:
                    meta["tags"].add(tag)
                cid = str(row.get("category_id") or "").strip()
                if cid and cat_names.get(cid):
                    meta["category"] = cat_names[cid]
                elif cid and not meta["category"]:
                    meta["category"] = f"cat_{cid}"
                author = str(row.get("author_id") or "").strip()
                if author:
                    meta["author"] = author
                title = str(row.get("title") or "").strip()
                if title:
                    meta["title"] = title
            if len(interactions) >= args.max_interactions:
                break
            if scanned >= args.scan_rows:
                break
            if scanned % 500000 == 0:
                print(f"  scanned {scanned}, matched interactions {len(interactions)}")

    print(f"Scanned {scanned} rows; matched interactions={len(interactions)} videos_hit={len(video_counts)}")

    ranked = [vid for vid, _ in video_counts.most_common()]
    for vid in mp4s:
        if vid not in video_counts:
            ranked.append(vid)
    video_ids = ranked[: args.max_videos]
    keep = set(video_ids)
    interactions = [r for r in interactions if vid_of(r) in keep]
    print(f"Loading {len(keep)} videos, {len(interactions)} interactions")

    with httpx.Client(timeout=300) as client:
        client.get(f"{args.api}/health").raise_for_status()
        if args.purge:
            print("Purging old catalog…")
            r = client.post(f"{args.api}/api/admin/purge-catalog")
            print(r.status_code, r.text[:200])
            if r.status_code >= 400:
                return 1

        mapped: dict[str, str] = {}
        for i, vid in enumerate(video_ids, 1):
            path = mp4s[vid]
            meta = video_meta.get(vid, {})
            title = titles.get(vid) or meta.get("title") or f"Clip {vid}"
            category = meta.get("category") or "general"
            tags = {t for t in (_clean_tag(x) for x in (meta.get("tags") or set())) if t}
            if category and category.lower() not in _TAG_NOISE and category.lower() != "general":
                tags.add(str(category))
            if not tags and category:
                tags.add(str(category))
            author = meta.get("author") or f"creator_{abs(hash(vid)) % 50}"
            data = {
                "title": str(title)[:120],
                "tags": ",".join(sorted(t for t in tags if t)[:12]),
                "category": str(category)[:64],
                "creator_id": f"sv_{author}"[:64],
            }
            # stream file — avoid MemoryError on large mp4
            with path.open("rb") as fh:
                files = {"file": (path.name, fh, "video/mp4")}
                resp = client.post(f"{args.api}/api/upload", data=data, files=files)
            if resp.status_code >= 400:
                print(f"[{i}/{len(video_ids)}] FAIL {vid}: {resp.text[:160]}")
                continue
            new_id = resp.json()["video_id"]
            mapped[vid] = new_id
            print(f"[{i}/{len(video_ids)}] {vid} -> {new_id} | {category} | {str(title)[:40]}")

        if not mapped:
            print("No videos uploaded", file=sys.stderr)
            return 1

        map_path = root / "video_id_map.json"
        map_path.write_text(json.dumps(mapped, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote {map_path} ({len(mapped)} pids)")

        seed_rows = []
        replayed = 0
        for row in interactions:
            vid = vid_of(row)
            if vid not in mapped:
                continue
            uid = f"sv_{row.get('user_id')}"
            et = pick_event_type(row)
            wt = watch_time_of(row)
            weight = {"like": 3.0, "watch": 1.2, "view": 0.6, "skip": -0.4, "share": 4.0}.get(et, 1.0)
            seed_rows.append(
                {
                    "user_id": uid,
                    "video_id": mapped[vid],
                    "event_type": et,
                    "watch_time": wt,
                    "weight": weight,
                }
            )
            if replayed < args.replay_events:
                client.post(
                    f"{args.api}/api/events",
                    json={
                        "user_id": uid,
                        "video_id": mapped[vid],
                        "event_type": et if et in {"view", "watch", "like", "skip", "share"} else "watch",
                        "watch_time": wt,
                    },
                )
                replayed += 1

        for i in range(0, len(seed_rows), 1000):
            client.post(f"{args.api}/api/admin/seed-interactions", json=seed_rows[i : i + 1000])

        batch = client.post(f"{args.api}/api/admin/run-batch-cf")
        print(
            f"Done. videos={len(mapped)} interactions={len(seed_rows)} "
            f"kafka_replay={replayed} batch={batch.json()} map={map_path}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
