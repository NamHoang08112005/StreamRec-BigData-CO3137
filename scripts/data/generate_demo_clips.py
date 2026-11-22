#!/usr/bin/env python3
"""DEPRECATED for main demo — synthetic colored lavfi clips only.

Use ShortVideo ETL instead:
  python scripts/data/reset_and_load_shortvideo.py

This script remains only as an emergency offline fallback when the
ShortVideo dataset is not yet downloaded.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

import httpx

CLIPS = [
    ("food_demo", "0xc45c26", "Street food sizzle", "food,street,demo", "food"),
    ("travel_demo", "0x2a6fdb", "Coastal travel reel", "travel,beach,demo", "travel"),
    ("sports_demo", "0xe63946", "Match day highlights", "sports,action,demo", "sports"),
    ("music_demo", "0x9b5de5", "Beat drop short", "music,party,demo", "music"),
    ("tech_demo", "0x00bbf9", "Gadget unboxing vibe", "tech,gadgets,demo", "tech"),
    ("pets_demo", "0xf15bb5", "Pet cuteness clip", "pets,animals,demo", "pets"),
    ("comedy_demo", "0xfee440", "Comedy skit teaser", "comedy,funny,demo", "comedy"),
    ("fashion_demo", "0x00f5d4", "Outfit transition", "fashion,style,demo", "fashion"),
    ("nature_demo", "0x1d3a2c", "Forest calm short", "nature,calm,demo", "nature"),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--volume", default="btl_new_media_data")
    ap.add_argument("--out", default="data/seed/mp4")
    args = ap.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    with httpx.Client(timeout=120) as client:
        for name, color, title, tags, category in CLIPS:
            container_path = f"/data/media/seed/{name}.mp4"
            print("ffmpeg", name)
            subprocess.check_call(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{args.volume}:/data/media",
                    "linuxserver/ffmpeg:latest",
                    "-y",
                    "-f",
                    "lavfi",
                    "-i",
                    f"color=c={color}:s=720x1280:d=3",
                    "-f",
                    "lavfi",
                    "-i",
                    "sine=frequency=520:duration=3",
                    "-c:v",
                    "libx264",
                    "-pix_fmt",
                    "yuv420p",
                    "-c:a",
                    "aac",
                    "-shortest",
                    container_path,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            host_file = out_dir / f"{name}.mp4"
            subprocess.check_call(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{args.volume}:/data/media",
                    "-v",
                    f"{host_file.parent.resolve()}:/out",
                    "alpine",
                    "cp",
                    container_path,
                    f"/out/{name}.mp4",
                ]
            )
            files = {"file": (host_file.name, host_file.read_bytes(), "video/mp4")}
            data = {
                "title": title,
                "tags": tags,
                "category": category,
                "creator_id": "seed_creator",
            }
            r = client.post(f"{args.api}/api/upload", data=data, files=files)
            print(r.status_code, r.text[:160])
        print(client.post(f"{args.api}/api/admin/run-batch-cf").json())


if __name__ == "__main__":
    main()
