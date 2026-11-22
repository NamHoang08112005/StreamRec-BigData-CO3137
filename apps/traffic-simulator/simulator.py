from __future__ import annotations

import csv
import json
import os
import sys
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

import httpx

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

API_BASE = os.getenv("API_BASE", "http://localhost:8000").rstrip("/")
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "200"))
SPEEDUP = float(os.getenv("SPEEDUP", "3600"))
MAX_GAP_SLEEP = float(os.getenv("MAX_GAP_SLEEP", "0.05"))
CONCURRENCY = max(1, int(os.getenv("CONCURRENCY", "12")))
STREAM_CSV = Path(os.getenv("STREAM_CSV", "/data/raw/shortvideo/interactions_stream.csv"))
VIDEO_MAP = Path(os.getenv("VIDEO_MAP", "/data/raw/shortvideo/video_id_map.json"))
LOOP_STREAM = os.getenv("LOOP_STREAM", "1") not in {"0", "false", "False"}


def wait_api(client: httpx.Client) -> None:
    while True:
        try:
            r = client.get(f"{API_BASE}/health", timeout=3)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)


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
    if str(row.get("click") or "").lower() in {"1", "true", "yes"}:
        return "view"
    return "watch"


def watch_time_of(row: dict) -> float:
    for key in ("watch_time", "duration"):
        if row.get(key) not in (None, ""):
            try:
                return max(0.5, min(180.0, float(row[key])))
            except ValueError:
                continue
    return 5.0


def vid_of(row: dict) -> str:
    return str(row.get("pid") or row.get("video_id") or row.get("item_id") or "").strip()


def exposed_ts(row: dict) -> float | None:
    for key in ("exposed_time", "p_date"):
        raw = row.get(key)
        if raw in (None, ""):
            continue
        try:
            return float(raw)
        except ValueError:
            continue
    return None


def load_map(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {str(k): str(v) for k, v in data.items()}


def pace_sleep(prev_ts: float | None, ts: float | None, min_interval: float) -> float | None:
    """Sleep according to CSV timeline / SPEEDUP; EVENTS_PER_SEC caps burst rate."""
    if ts is None:
        time.sleep(min_interval)
        return prev_ts
    if prev_ts is None:
        return ts
    gap = max(0.0, ts - prev_ts)
    sleep_s = min(gap / max(SPEEDUP, 1e-6), MAX_GAP_SLEEP)
    if sleep_s < min_interval:
        sleep_s = min_interval
    if sleep_s > 0:
        time.sleep(sleep_s)
    return ts


def iter_stream_rows(path: Path, mapped: dict[str, str]):
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open(encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            reader.fieldnames = [h.lstrip("\ufeff").strip() if h else h for h in reader.fieldnames]
        for row in reader:
            if "\ufeffuser_id" in row and "user_id" not in row:
                row["user_id"] = row.get("\ufeffuser_id")
            pid = vid_of(row)
            uid = str(row.get("user_id") or "").strip()
            if not pid or not uid or pid not in mapped:
                continue
            et = pick_event_type(row)
            if et not in {"view", "watch", "like", "skip", "share"}:
                et = "watch"
            wt = watch_time_of(row) if et in {"view", "watch"} else 0.0
            yield {
                "user_id": f"sv_{uid}",
                "video_id": mapped[pid],
                "event_type": et,
                "watch_time": wt,
                "_ts": exposed_ts(row),
            }


def post_event(client: httpx.Client, payload: dict) -> None:
    client.post(f"{API_BASE}/api/events", json=payload).raise_for_status()


def drain_done(pending: deque[Future], sent: int, skipped: int) -> tuple[int, int]:
    while pending and pending[0].done():
        fut = pending.popleft()
        try:
            fut.result()
            sent += 1
        except Exception as exc:
            skipped += 1
            if skipped <= 5:
                print(f"event failed: {exc}", flush=True)
    return sent, skipped


def main() -> None:
    min_interval = 1.0 / max(EVENTS_PER_SEC, 0.1)
    print(
        f"Traffic timeline replay -> {API_BASE} csv={STREAM_CSV} map={VIDEO_MAP} "
        f"speedup={SPEEDUP}x max_gap_sleep={MAX_GAP_SLEEP}s "
        f"eps_cap={EVENTS_PER_SEC} concurrency={CONCURRENCY} loop={LOOP_STREAM}",
        flush=True,
    )
    limits = httpx.Limits(max_connections=CONCURRENCY + 4, max_keepalive_connections=CONCURRENCY)
    with httpx.Client(timeout=10, limits=limits) as client:
        wait_api(client)
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            while True:
                mapped = load_map(VIDEO_MAP)
                if not mapped:
                    print(f"Waiting for video map at {VIDEO_MAP} …", flush=True)
                    time.sleep(5)
                    continue
                if not STREAM_CSV.exists():
                    print(f"Waiting for stream CSV at {STREAM_CSV} …", flush=True)
                    time.sleep(5)
                    continue

                sent = skipped = 0
                prev_ts: float | None = None
                pending: deque[Future] = deque()
                max_inflight = CONCURRENCY * 4
                try:
                    for payload in iter_stream_rows(STREAM_CSV, mapped):
                        ts = payload.pop("_ts", None)
                        prev_ts = pace_sleep(prev_ts, ts if isinstance(ts, float) else None, min_interval)
                        pending.append(pool.submit(post_event, client, payload))
                        sent, skipped = drain_done(pending, sent, skipped)
                        while len(pending) >= max_inflight:
                            fut = pending.popleft()
                            try:
                                fut.result()
                                sent += 1
                            except Exception as exc:
                                skipped += 1
                                if skipped <= 5:
                                    print(f"event failed: {exc}", flush=True)
                            sent, skipped = drain_done(pending, sent, skipped)
                        if sent and sent % 1000 == 0:
                            print(f"sent={sent} failed={skipped} last_ts={prev_ts}", flush=True)
                    while pending:
                        fut = pending.popleft()
                        try:
                            fut.result()
                            sent += 1
                        except Exception as exc:
                            skipped += 1
                            if skipped <= 5:
                                print(f"event failed: {exc}", flush=True)
                except FileNotFoundError as exc:
                    print(exc, flush=True)
                    time.sleep(5)
                    continue

                print(f"stream pass done sent={sent} failed={skipped}", flush=True)
                if not LOOP_STREAM:
                    time.sleep(30)
                    continue
                time.sleep(2)


if __name__ == "__main__":
    main()
