from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("extract-worker")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://recsys:recsys@postgres:5432/recsys")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MEDIA_ROOT = Path(os.getenv("MEDIA_ROOT", "/data/media"))

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
r_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def probe_video(path: Path) -> dict:
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(path),
    ]
    out = subprocess.check_output(cmd, text=True)
    data = json.loads(out)
    video_stream = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), {})
    fmt = data.get("format", {})
    duration = float(fmt.get("duration") or video_stream.get("duration") or 0)
    width = int(video_stream.get("width") or 0)
    height = int(video_stream.get("height") or 0)
    fps_raw = video_stream.get("r_frame_rate") or "0/1"
    try:
        num, den = fps_raw.split("/")
        fps = float(num) / float(den) if float(den) else 0.0
    except Exception:  # noqa: BLE001
        fps = 0.0

    enriched = []
    if duration and duration < 15:
        enriched.append("short_form")
    elif duration and duration < 60:
        enriched.append("mid_form")
    else:
        enriched.append("long_form")
    if width and height:
        ratio = width / max(height, 1)
        enriched.append("vertical" if ratio < 0.85 else "landscape" if ratio > 1.2 else "square")
        if height >= 1080 or width >= 1080:
            enriched.append("hd")
    if fps >= 50:
        enriched.append("high_fps")

    return {
        "duration_sec": duration,
        "width": width or None,
        "height": height or None,
        "fps": fps,
        "enriched_tags": enriched,
    }


def make_thumbnail(src: Path, dest: Path) -> bool:
    cmd = ["ffmpeg", "-y", "-ss", "1", "-i", str(src), "-frames:v", "1", "-q:v", "3", str(dest)]
    try:
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return dest.exists()
    except Exception:  # noqa: BLE001
        return False


async def handle_extract(msg: dict, producer: AIOKafkaProducer) -> None:
    if msg.get("type") != "extract.requested":
        return
    video_id = msg["video_id"]
    media_key = msg["media_key"]
    local = MEDIA_ROOT / media_key
    logger.info("Extracting %s (%s)", video_id, local)
    if not local.exists():
        raise FileNotFoundError(local)

    t0 = time.perf_counter()
    meta = probe_video(local)
    thumb_key = None
    with tempfile.TemporaryDirectory() as tmp:
        thumb_path = Path(tmp) / "thumb.jpg"
        if make_thumbnail(local, thumb_path):
            thumb_key = f"thumbs/{video_id}.jpg"
            dest = MEDIA_ROOT / thumb_key
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(thumb_path.read_bytes())

    with engine.begin() as conn:
        row = conn.execute(text("SELECT tags FROM videos WHERE video_id=:id"), {"id": video_id}).mappings().first()
        existing = list(row["tags"] or []) if row else []
        enriched = sorted(set(existing + meta["enriched_tags"]))
        conn.execute(
            text(
                """
                UPDATE videos
                SET status='enriched',
                    duration_sec=:duration_sec,
                    width=:width,
                    height=:height,
                    fps=:fps,
                    thumb_key=:thumb_key,
                    enriched_tags=:enriched_tags,
                    updated_at=NOW()
                WHERE video_id=:video_id
                """
            ),
            {
                "video_id": video_id,
                "duration_sec": meta["duration_sec"],
                "width": meta["width"],
                "height": meta["height"],
                "fps": meta["fps"],
                "thumb_key": thumb_key,
                "enriched_tags": enriched,
            },
        )

    done = {
        "event_id": uuid.uuid4().hex,
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": "extract.completed",
        "video_id": video_id,
        "media_key": media_key,
        "thumb_key": thumb_key,
        "duration_sec": meta["duration_sec"],
        "width": meta["width"],
        "height": meta["height"],
        "fps": meta["fps"],
        "enriched_tags": meta["enriched_tags"],
    }
    await producer.send_and_wait(
        "events.extract",
        json.dumps(done).encode("utf-8"),
        key=video_id.encode("utf-8"),
    )
    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    try:
        pipe = r_client.pipeline()
        pipe.incr("metrics:extract:completed")
        pipe.set("metrics:extract:last_ms", str(elapsed_ms))
        pipe.execute()
    except Exception:  # noqa: BLE001
        pass
    logger.info("Extracted %s in %sms -> %s", video_id, elapsed_ms, meta["enriched_tags"])


async def main() -> None:
    MEDIA_ROOT.mkdir(parents=True, exist_ok=True)
    consumer = AIOKafkaConsumer(
        "events.extract",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="extract-worker",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await consumer.start()
    await producer.start()
    logger.info("Extract worker listening on events.extract")
    try:
        async for record in consumer:
            try:
                payload = json.loads(record.value.decode("utf-8"))
                await handle_extract(payload, producer)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to process extract message")
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
