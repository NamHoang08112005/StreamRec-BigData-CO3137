#!/usr/bin/env python3
"""Replace 0-byte media uploads with valid ShortVideo raw_file mp4s matched by title/duration."""
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw" / "shortvideo" / "raw_file"


def psql(sql: str) -> str:
    return subprocess.check_output(
        [
            "docker",
            "exec",
            "-e",
            "PGCLIENTENCODING=UTF8",
            "btl_new-postgres-1",
            "psql",
            "-U",
            "recsys",
            "-d",
            "recsys",
            "-t",
            "-A",
            "-F",
            "|",
            "-c",
            sql,
        ],
        encoding="utf-8",
        errors="replace",
    )


def empty_video_ids() -> list[str]:
    out = subprocess.check_output(
        [
            "docker",
            "exec",
            "btl_new-api-1",
            "sh",
            "-c",
            "find /data/media/uploads -name '*.mp4' -size 0 -printf '%f\\n'",
        ],
        encoding="utf-8",
        errors="replace",
    )
    return [line.replace(".mp4", "").strip() for line in out.splitlines() if line.strip()]


def main() -> int:
    bad_ids = empty_video_ids()
    if not bad_ids:
        print("No empty uploads found.")
        return 0
    print(f"Empty uploads: {len(bad_ids)}")

    in_list = ",".join(f"'{vid}'" for vid in bad_ids)
    rows = psql(
        f"SELECT video_id || '|' || title || '|' || COALESCE(duration_sec::text,'0') "
        f"FROM videos WHERE video_id IN ({in_list});"
    )
    db: dict[str, dict] = {}
    for line in rows.strip().splitlines():
        if not line.strip():
            continue
        vid, title, dur = line.split("|", 2)
        try:
            duration = float(dur)
        except ValueError:
            duration = 0.0
        db[vid] = {"title": title, "dur": duration}

    raw_ids = {p.stem for p in RAW.glob("*.mp4")}
    title_to_pid: dict[str, str] = {}
    dur_to_pids: dict[float, set[str]] = {}
    with (ROOT / "data" / "raw" / "shortvideo" / "interaction_sampled.csv").open(
        encoding="utf-8", errors="ignore"
    ) as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            pid = str(row.get("pid") or "").strip()
            if pid not in raw_ids:
                continue
            title = str(row.get("title") or "").strip()
            if title and title not in title_to_pid:
                title_to_pid[title] = pid
            try:
                d = round(float(row.get("duration") or 0), 3)
            except ValueError:
                continue
            dur_to_pids.setdefault(d, set()).add(pid)

    mapping: dict[str, str | None] = {}
    for vid, meta in db.items():
        pid = title_to_pid.get(meta["title"])
        if not pid:
            for t, p in title_to_pid.items():
                if meta["title"] and (t.startswith(meta["title"][:24]) or meta["title"].startswith(t[:24])):
                    pid = p
                    break
        if not pid:
            for key in (round(meta["dur"], 3), round(meta["dur"], 2), round(meta["dur"], 1)):
                cands = dur_to_pids.get(key)
                if cands and len(cands) == 1:
                    pid = next(iter(cands))
                    break
        mapping[vid] = pid
        print(f"{vid} -> {pid} | {meta['title'][:48]}")

    repaired = 0
    for vid, pid in mapping.items():
        if not pid:
            print(f"SKIP no match for {vid}")
            continue
        src = RAW / f"{pid}.mp4"
        if not src.exists() or src.stat().st_size < 10_000:
            print(f"SKIP missing/small source {src}")
            continue
        dest = f"/data/media/uploads/{vid}.mp4"
        print(f"Copy {src.name} ({src.stat().st_size/1e6:.1f}MB) -> {dest}")
        subprocess.check_call(["docker", "cp", str(src), f"btl_new-api-1:{dest}"])
        # ensure extract-worker / shared volume sees same file (same volume mount)
        repaired += 1

    print(json.dumps({"empty": len(bad_ids), "repaired": repaired, "mapping": mapping}, ensure_ascii=False, indent=2))
    return 0 if repaired == len(bad_ids) else 2


if __name__ == "__main__":
    raise SystemExit(main())
