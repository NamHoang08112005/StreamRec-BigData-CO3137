#!/usr/bin/env python3
"""
Fast path for ShortVideo demo data.

Server policy: ≤3 concurrent large-file connections (more workers → HTTP 429).
Real speedup: parse Apache listing sizes once, download ONLY small mp4s.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE = "http://fi.ee.tsinghua.edu.cn/datasets/short-video-dataset/"
USER = "videodata"
PASSWORD = "ShortVideo@10000"

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
_LISTING_RE = re.compile(
    r'href="([^"]+\.mp4)"[^<]*</a>\s+\S+\s+\S+\s+(\S+)',
    flags=re.I,
)


def opener():
    pm = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    pm.add_password(None, BASE, USER, PASSWORD)
    return urllib.request.build_opener(urllib.request.HTTPBasicAuthHandler(pm))


def fetch(url: str, timeout: int = 300) -> bytes:
    return opener().open(url, timeout=timeout).read()


def download_to(url: str, dest: Path, timeout: int = 600) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 1000:
        return
    tmp = dest.with_suffix(dest.suffix + ".part")
    data = fetch(url, timeout=timeout)
    tmp.write_bytes(data)
    tmp.replace(dest)


def parse_listing_size(token: str) -> int:
    s = token.strip().upper().replace(",", "")
    if not s or s == "-":
        return 0
    mult = 1
    if s[-1] == "K":
        mult = 1024
        s = s[:-1]
    elif s[-1] == "M":
        mult = 1024 * 1024
        s = s[:-1]
    elif s[-1] == "G":
        mult = 1024 * 1024 * 1024
        s = s[:-1]
    return int(float(s) * mult)


def load_remote_sizes() -> dict[str, int]:
    print("   fetching raw_file/ listing (one-time)…")
    t0 = time.time()
    html = opener().open(BASE + "raw_file/", timeout=180).read().decode("utf-8", "ignore")
    sizes: dict[str, int] = {}
    for name, token in _LISTING_RE.findall(html):
        vid = Path(name).stem
        sizes[vid] = parse_listing_size(token)
    print(f"   listing: {len(sizes)} files in {time.time() - t0:.1f}s")
    return sizes


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def download_mp4_stream(vid: str, dest: Path, *, max_bytes: int, retries: int = 5) -> tuple[str, bool, str]:
    if dest.exists() and dest.stat().st_size > 10_000:
        return vid, True, "cached"

    url = BASE + f"raw_file/{vid}.mp4"
    tmp = dest.with_suffix(dest.suffix + ".part")
    last_err = ""
    for attempt in range(1, retries + 1):
        try:
            with opener().open(url, timeout=180) as resp:
                written = 0
                with tmp.open("wb") as out:
                    while True:
                        chunk = resp.read(256 * 1024)
                        if not chunk:
                            break
                        written += len(chunk)
                        if written > max_bytes:
                            out.close()
                            _safe_unlink(tmp)
                            return vid, False, f"skip_large:{written}"
                        out.write(chunk)
            if written <= 10_000:
                _safe_unlink(tmp)
                return vid, False, "too_small"
            # Windows: antivirus / indexer may briefly lock .part after write.
            for replace_try in range(1, 6):
                try:
                    tmp.replace(dest)
                    break
                except OSError as exc:
                    last_err = str(exc)
                    time.sleep(0.2 * replace_try)
            else:
                raise OSError(last_err or "replace_failed")
            return vid, True, str(written)
        except urllib.error.HTTPError as exc:
            last_err = str(exc)
            _safe_unlink(tmp)
            if exc.code == 429:
                time.sleep(min(30, 2**attempt))
                continue
            break
        except Exception as exc:  # noqa: BLE001
            last_err = str(exc)
            _safe_unlink(tmp)
            time.sleep(min(8, attempt))
    return vid, False, last_err or "fail"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/shortvideo")
    ap.add_argument("--max-videos", type=int, default=40)
    ap.add_argument("--workers", type=int, default=3, help="Keep ≤3 (server rate-limit)")
    ap.add_argument("--max-mb", type=float, default=12.0, help="Only download mp4 ≤ this size")
    ap.add_argument(
        "--prefer-interactions",
        action="store_true",
        default=True,
        help="Prefer high-interaction ids among small files",
    )
    args = ap.parse_args()

    workers = max(1, min(3, args.workers))
    max_bytes = int(args.max_mb * 1024 * 1024)
    out = Path(args.out)
    raw = out / "raw_file"
    raw.mkdir(parents=True, exist_ok=True)

    inter = out / "interaction_sampled.csv"
    print("1) interaction_sampled.csv…")
    download_to(BASE + "interaction_sampled.csv", inter, timeout=600)
    print(f"   saved {inter.stat().st_size / 1e6:.1f} MB")

    print("2) remote sizes + interaction ranks…")
    sizes = load_remote_sizes()
    counts: Counter[str] = Counter()
    with inter.open(encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vid = str(row.get("pid") or row.get("video_id") or row.get("item_id") or "").strip()
            if vid:
                counts[vid] += 1

    already = {p.stem for p in raw.glob("*.mp4") if p.stat().st_size > 10_000}
    small_ranked = [
        vid
        for vid, _ in counts.most_common()
        if vid in sizes and 0 < sizes[vid] <= max_bytes and vid not in already
    ]
    # Fallback: any small remote files not yet cached
    if len(already) + len(small_ranked) < args.max_videos:
        extras = [
            vid
            for vid, sz in sorted(sizes.items(), key=lambda kv: (kv[1], kv[0]))
            if 0 < sz <= max_bytes and vid not in already and vid not in set(small_ranked)
        ]
        small_ranked.extend(extras)

    need = max(0, args.max_videos - len(already))
    queue = small_ranked[:need]
    print(
        f"   cached={len(already)} need={need} queue_small={len(queue)} "
        f"workers={workers} max_mb={args.max_mb}"
    )

    print("3) downloading small mp4s…")
    ok = len(already)
    failed = 0
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(download_mp4_stream, vid, raw / f"{vid}.mp4", max_bytes=max_bytes) for vid in queue]
        for fut in as_completed(futs):
            vid, success, info = fut.result()
            done += 1
            if success:
                ok += 1
                tag = "OK"
            else:
                failed += 1
                tag = "FAIL"
            print(f"   [{ok}/{args.max_videos} | {done}/{len(queue)}] {tag} {vid} ({info})")

    kept = sorted(p.stem for p in raw.glob("*.mp4") if p.stat().st_size > 10_000)
    print(f"   on_disk_now={len(kept)}")

    print("4) titles + categories…")
    try:
        download_to(BASE + "categories_cn_en.csv", out / "categories_cn_en.csv")
    except Exception as exc:  # noqa: BLE001
        print("   categories skip", exc)

    title_dir = out / "title_en"
    title_dir.mkdir(exist_ok=True)
    title_targets = kept[: args.max_videos]

    def one_title(vid: str) -> None:
        dest = title_dir / f"{vid}.txt"
        if dest.exists():
            return
        try:
            download_to(BASE + f"title_en/{vid}.txt", dest, timeout=60)
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(one_title, title_targets))

    target = out / "interaction.csv"
    if not target.exists():
        try:
            target.hardlink_to(inter)
        except Exception:
            print(f"   ETL will read {inter.name} directly")

    print(f"Done. mp4_ok={ok} failed={failed} target={args.max_videos}")
    print("Next: python scripts/data/split_coldstart_pool.py --fraction 0.30")
    return 0 if ok >= min(args.max_videos, 1) else 1


if __name__ == "__main__":
    raise SystemExit(main())
