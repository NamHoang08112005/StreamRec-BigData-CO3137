#!/usr/bin/env python3
"""
Bootstrap demo catalog with REAL mp4 files (public sample/CC sources).
Uploads to MinIO and registers videos + warm-up interactions via API.

Primary path for production demo data remains ShortVideo dataset ETL
(see etl_shortvideo.py). This script gets a working Feed quickly.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
import time
import urllib.request
from pathlib import Path

import httpx

# Publicly reachable sample / demo mp4s (real video files)
SEED_CLIPS = [
    {
        "url": "https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4",
        "title": "Flower close-up",
        "tags": ["nature", "flower", "calm"],
        "category": "nature",
    },
    {
        "url": "https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.webm",
        "title": "Flower motion",
        "tags": ["nature", "garden"],
        "category": "nature",
        "skip_if_fail": True,
    },
    {
        "url": "https://www.w3schools.com/html/mov_bbb.mp4",
        "title": "Big Buck Bunny trailer clip",
        "tags": ["animation", "comedy", "animals"],
        "category": "comedy",
    },
    {
        "url": "https://www.w3schools.com/html/movie.mp4",
        "title": "Bear scene clip",
        "tags": ["animals", "nature", "wildlife"],
        "category": "pets",
    },
    {
        "url": "https://filesamples.com/samples/video/mp4/sample_640x360.mp4",
        "title": "City sample clip",
        "tags": ["travel", "city", "urban"],
        "category": "travel",
    },
    {
        "url": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4",
        "title": "For Bigger Blazes",
        "tags": ["sports", "action", "outdoors"],
        "category": "sports",
    },
    {
        "url": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerEscapes.mp4",
        "title": "For Bigger Escapes",
        "tags": ["travel", "adventure"],
        "category": "travel",
    },
    {
        "url": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerFun.mp4",
        "title": "For Bigger Fun",
        "tags": ["comedy", "fun", "music"],
        "category": "music",
    },
    {
        "url": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerJoyrides.mp4",
        "title": "For Bigger Joyrides",
        "tags": ["tech", "cars", "action"],
        "category": "tech",
    },
    {
        "url": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerMeltdowns.mp4",
        "title": "For Bigger Meltdowns",
        "tags": ["food", "comedy"],
        "category": "food",
    },
    {
        "url": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/SubaruOutbackOnStreetAndDirt.mp4",
        "title": "Subaru on street and dirt",
        "tags": ["travel", "cars", "outdoors"],
        "category": "travel",
    },
    {
        "url": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ElephantsDream.mp4",
        "title": "Elephants Dream excerpt",
        "tags": ["animation", "art", "tech"],
        "category": "tech",
    },
]


def download(url: str, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 10_000:
        return True
    try:
        print(f"Downloading {url}")
        urllib.request.urlretrieve(url, dest)
        return dest.exists() and dest.stat().st_size > 10_000
    except Exception as exc:  # noqa: BLE001
        print(f"  failed: {exc}")
        return False


def wait_api(base: str, timeout: int = 180) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{base}/health", timeout=3)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("API not ready")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--cache", default="data/seed/mp4")
    parser.add_argument("--users", type=int, default=40)
    parser.add_argument("--interactions", type=int, default=800)
    args = parser.parse_args()

    cache = Path(args.cache)
    wait_api(args.api)

    seeded = []
    with httpx.Client(timeout=120) as client:
        for clip in SEED_CLIPS:
            vid_hash = hashlib.md5(clip["url"].encode()).hexdigest()[:10]
            video_id = f"seed_{vid_hash}"
            ext = "webm" if clip["url"].endswith(".webm") else "mp4"
            local = cache / f"{video_id}.{ext}"
            ok = download(clip["url"], local)
            if not ok:
                if clip.get("skip_if_fail"):
                    continue
                print(f"Skip {clip['title']}")
                continue

            files = {"file": (local.name, local.read_bytes(), f"video/{ext}")}
            data = {
                "title": clip["title"],
                "tags": ",".join(clip["tags"]),
                "category": clip["category"],
                "creator_id": "seed_creator",
            }
            # Prefer upload endpoint so extract pipeline runs
            resp = client.post(f"{args.api}/api/upload", data=data, files=files)
            if resp.status_code >= 400:
                print("upload failed", resp.text)
                continue
            body = resp.json()
            seeded.append({**clip, "video_id": body["video_id"]})
            print("Uploaded", body["video_id"], clip["title"])

        if not seeded:
            print("No videos seeded", file=sys.stderr)
            return 1

        # warm interactions biased by category
        users = [f"seed_u{i:03d}" for i in range(args.users)]
        cats = {c["category"]: [] for c in seeded}
        for s in seeded:
            cats[s["category"]].append(s["video_id"])
        interests = list(cats.keys())
        user_pref = {u: random.sample(interests, k=min(2, len(interests))) for u in users}

        rows = []
        for _ in range(args.interactions):
            u = random.choice(users)
            prefs = user_pref[u]
            pool = []
            for p in prefs:
                pool.extend(cats.get(p, []))
            if not pool:
                pool = [s["video_id"] for s in seeded]
            vid = random.choice(pool)
            et = random.choices(["watch", "like", "view", "skip"], weights=[50, 20, 20, 10])[0]
            rows.append(
                {
                    "user_id": u,
                    "video_id": vid,
                    "event_type": et,
                    "watch_time": random.uniform(2, 20),
                    "weight": {"watch": 1.2, "like": 3, "view": 0.6, "skip": -0.4}[et],
                }
            )

        # also emit live events through API so Kafka sees traffic
        for row in rows[:200]:
            client.post(
                f"{args.api}/api/events",
                json={
                    "user_id": row["user_id"],
                    "video_id": row["video_id"],
                    "event_type": row["event_type"] if row["event_type"] != "watch" else "watch",
                    "watch_time": row["watch_time"],
                },
            )
        client.post(f"{args.api}/api/admin/seed-interactions", json=rows)
        batch = client.post(f"{args.api}/api/admin/run-batch-cf")
        print("batch-cf", batch.json())

    meta = {"videos": seeded, "users": args.users, "interactions": args.interactions}
    Path("data/seed/bootstrap_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"Done. Seeded {len(seeded)} real video files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
