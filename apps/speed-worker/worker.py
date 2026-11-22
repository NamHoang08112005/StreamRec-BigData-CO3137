"""
Speed-layer consumer: Tumbling 60s window on events.user -> Redis affinity & hot tags.
Single writer for user:affinity:* and metrics:hot_tags (no double-counting with API).
Uses 3s micro-flushes so UI sees realtime updates quickly while tracking 60s tumbling windows.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import defaultdict

import redis
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("speed-worker")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
WINDOW_SEC = max(10, int(os.getenv("WINDOW_SEC", "60")))
FLUSH_INTERVAL_SEC = max(1.0, float(os.getenv("FLUSH_INTERVAL_SEC", "3")))

NOISE_TAGS = frozenset(
    {
        "shortvideo",
        "short_video",
        "demo",
        "auto_upload",
        "coldstart",
        "cold_start",
        "short_form",
        "mid_form",
        "long_form",
        "vertical",
        "landscape",
        "square",
        "hd",
        "high_fps",
        "general",
    }
)

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def clean_tag(tag: str) -> str | None:
    t = str(tag or "").strip().lower().replace(" ", "_")
    if not t or t in NOISE_TAGS:
        return None
    return t


class TumblingWindow:
    def __init__(self, window_sec: int, flush_interval_sec: float) -> None:
        self.window_sec = window_sec
        self.flush_interval_sec = flush_interval_sec
        now = time.time()
        self.bucket_start = int(now // window_sec) * window_sec
        self.last_flush_ts = now
        self.acc: dict[tuple[str, str], float] = defaultdict(float)
        self.window_events = 0

    def add(self, user_id: str, tag: str, weight: float) -> None:
        now = time.time()
        current_bucket = (int(now) // self.window_sec) * self.window_sec
        if current_bucket != self.bucket_start:
            self.flush(window_rollover=True)
            self.bucket_start = current_bucket
            self.window_events = 0
        self.acc[(user_id, tag)] += weight
        self.window_events += 1
        if now - self.last_flush_ts >= self.flush_interval_sec:
            self.flush(window_rollover=False)

    def maybe_flush(self) -> None:
        now = time.time()
        current_bucket = (int(now) // self.window_sec) * self.window_sec
        if current_bucket != self.bucket_start:
            self.flush(window_rollover=True)
            self.bucket_start = current_bucket
            self.window_events = 0
        elif self.acc and (now - self.last_flush_ts >= self.flush_interval_sec):
            self.flush(window_rollover=False)

    def flush(self, *, window_rollover: bool = False) -> None:
        now = time.time()
        self.last_flush_ts = now
        if not self.acc and not window_rollover:
            return
        pipe = r.pipeline()
        if window_rollover:
            pipe.delete("metrics:hot_tags:1m")
        for (user_id, tag), weight in self.acc.items():
            pipe.hincrbyfloat(f"user:affinity:{user_id}", tag, weight)
            pipe.expire(f"user:affinity:{user_id}", 86400)
            if weight > 0:
                pipe.zincrby("metrics:hot_tags", weight, tag)
                pipe.zincrby("metrics:hot_tags:1m", weight, tag)
        pipe.expire("metrics:hot_tags:1m", self.window_sec + 30)
        if window_rollover:
            pipe.hincrby("metrics:speed_windows", "windows_completed", 1)
        if self.acc:
            pipe.hincrby("metrics:speed_windows", "flushes", 1)
            pipe.hset("metrics:speed_windows", "last_flush_ts", str(int(now)))
            pipe.hset("metrics:speed_windows", "last_window_size", str(len(self.acc)))
        pipe.hset("metrics:speed_windows", "window_sec", str(self.window_sec))
        pipe.execute()
        if self.acc:
            logger.info(
                "Flushed speed micro-batch pairs=%s rollover=%s",
                len(self.acc),
                window_rollover,
            )
            self.acc.clear()


async def main() -> None:
    consumer = AIOKafkaConsumer(
        "events.user",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="speed-worker",
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    window = TumblingWindow(WINDOW_SEC, FLUSH_INTERVAL_SEC)
    await consumer.start()
    logger.info(
        "Speed worker listening (window=%ss, micro-flush=%ss)",
        WINDOW_SEC,
        FLUSH_INTERVAL_SEC,
    )
    try:
        while True:
            batch = await consumer.getmany(timeout_ms=1000, max_records=500)
            for _tp, records in batch.items():
                for record in records:
                    try:
                        msg = json.loads(record.value.decode("utf-8"))
                        user_id = msg.get("user_id")
                        if not user_id:
                            continue
                        tags = list(msg.get("tags") or [])
                        if msg.get("category"):
                            tags.append(msg["category"])
                        weight = float(msg.get("weight") or 1.0)
                        for raw_tag in tags:
                            t = clean_tag(raw_tag)
                            if t:
                                window.add(user_id, t, weight)
                    except Exception:  # noqa: BLE001
                        logger.exception("bad message")
            window.maybe_flush()
    finally:
        window.flush(window_rollover=True)
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
