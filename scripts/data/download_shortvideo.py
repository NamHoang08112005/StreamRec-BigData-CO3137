#!/usr/bin/env python3
"""
Fast ShortVideo downloader (Tsinghua HTTP basic auth).

Speed tips:
- Use --workers 3 (server policy for large files)
- Prefer small --max-videos (30–50 enough for laptop demo)
- Skip re-download if file already exists
- interaction.csv: reuse Dropbox tiny copy if present (huge); don't re-fetch unless asked
"""
from __future__ import annotations

import argparse
import re
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE = "http://fi.ee.tsinghua.edu.cn/datasets/short-video-dataset/"
USER = "videodata"
PASSWORD = "ShortVideo@10000"


def make_opener() -> urllib.request.OpenerDirector:
    passmgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    passmgr.add_password(None, BASE, USER, PASSWORD)
    return urllib.request.build_opener(urllib.request.HTTPBasicAuthHandler(passmgr))


def fetch(url: str, timeout: int = 120) -> bytes:
    return make_opener().open(url, timeout=timeout).read()


def list_mp4() -> list[str]:
    html = fetch(BASE + "raw_file/", timeout=300).decode("utf-8", "ignore")
    return sorted(set(re.findall(r'href="([^"]+\.mp4)"', html, flags=re.I)))


def download_one(name: str, dest: Path) -> tuple[str, bool, str]:
    if dest.exists() and dest.stat().st_size > 10_000:
        return name, True, "cached"
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        data = fetch(BASE + "raw_file/" + name.lstrip("/"), timeout=600)
        tmp.write_bytes(data)
        if tmp.stat().st_size <= 10_000:
            tmp.unlink(missing_ok=True)
            return name, False, "too_small"
        tmp.replace(dest)
        return name, True, f"{dest.stat().st_size}"
    except Exception as exc:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        return name, False, str(exc)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/shortvideo")
    ap.add_argument("--max-videos", type=int, default=40, help="Smaller = much faster")
    ap.add_argument("--workers", type=int, default=3, help="Server allows ~3 concurrent large files")
    ap.add_argument("--fetch-interaction", action="store_true", help="Also download interaction.csv (slow/huge)")
    args = ap.parse_args()

    out = Path(args.out)
    raw = out / "raw_file"
    raw.mkdir(parents=True, exist_ok=True)

    print("Listing remote mp4…")
    links = list_mp4()
    print(f"Remote mp4 count: {len(links)}")
    picked = links[: args.max_videos]
    print(f"Downloading {len(picked)} with {args.workers} workers…")

    ok = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futs = [pool.submit(download_one, name, raw / Path(name).name) for name in picked]
        for i, fut in enumerate(as_completed(futs), 1):
            name, success, info = fut.result()
            if success:
                ok += 1
            print(f"[{i}/{len(picked)}] {'OK' if success else 'FAIL'} {name} ({info})")

    # Lightweight metadata
    for rel in ("categories_cn_en.csv", "category_cn_en.csv"):
        try:
            data = fetch(BASE + rel, timeout=120)
            (out / Path(rel).name).write_bytes(data)
            print(f"Saved {rel}")
        except Exception as exc:  # noqa: BLE001
            print(f"Skip {rel}: {exc}")

    title_dir = out / "title_en"
    title_dir.mkdir(exist_ok=True)
    # titles are tiny .txt — can pull more parallel
    vids = [p.stem for p in raw.glob("*.mp4")]
    with ThreadPoolExecutor(max_workers=8) as pool:
        def one(vid: str):
            dest = title_dir / f"{vid}.txt"
            if dest.exists():
                return
            try:
                dest.write_bytes(fetch(BASE + f"title_en/{vid}.txt", timeout=60))
            except Exception:
                pass
        list(pool.map(one, vids))

    # Prefer local Dropbox interaction dump if already extracted
    local_inter = out / "extracted" / "interaction.csv"
    target_inter = out / "interaction.csv"
    if local_inter.exists() and not target_inter.exists():
        print(f"Linking existing interaction.csv from Dropbox extract…")
        try:
            target_inter.hardlink_to(local_inter)
        except Exception:
            # fallback copy is huge; use symlink on Windows may need admin — just note path
            print(f"Use --root with extracted folder or copy manually:\n  {local_inter}")

    if args.fetch_interaction:
        print("Fetching interaction.csv (large)…")
        try:
            data = fetch(BASE + "interaction.csv", timeout=3600)
            target_inter.write_bytes(data)
            print(f"Saved interaction.csv ({len(data)} bytes)")
        except Exception as exc:  # noqa: BLE001
            print(f"interaction.csv failed: {exc}")

    print(f"Done: {ok}/{len(picked)} videos in {raw}")
    print("Next: python scripts/data/reset_and_load_shortvideo.py --root", args.out)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
