from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import settings
from app.db import SessionLocal, fetch_all, fetch_one, get_db
from app.kafka_bus import publish, start_producer, stop_producer
from app.tag_filters import filter_content_tags, filter_for_hot_tags
from app.recommend import (
    clear_user_affinity,
    get_batch_cf_status,
    get_coldstart_user,
    get_metrics,
    get_user_inspector_state,
    get_weights,
    recommend_feed,
    record_active_user,
    record_event_metric,
    record_user_seen,
    run_batch_cf,
    set_coldstart_user,
    set_weights,
)
from app.storage import (
    MEDIA_ROOT,
    delete_media,
    ensure_bucket,
    media_file_ok,
    media_url,
    upload_bytes,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="TikTok-like Recsys Demo API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list + ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_batch_cf_task: asyncio.Task | None = None
AUTO_CREATOR_ID = "creator_auto"
AUTO_TAG = "auto_upload"


def get_auto_upload_status(db: Session | None = None) -> dict[str, Any]:
    """Status for Dashboard — uploads come from upload-simulator client, not server clone."""
    own_db = db is None
    if own_db:
        db = SessionLocal()
    try:
        last = fetch_one(
            db,
            """
            SELECT video_id, title, status, created_at
            FROM videos
            WHERE creator_id = :c OR :tag = ANY(COALESCE(tags, ARRAY[]::text[]))
            ORDER BY created_at DESC
            LIMIT 1
            """,
            {"c": AUTO_CREATOR_ID, "tag": AUTO_TAG},
        )
        last_payload = None
        if last:
            created = last.get("created_at")
            last_payload = {
                "video_id": last["video_id"],
                "title": last.get("title"),
                "status": last.get("status"),
                "finished_at": created.isoformat() if hasattr(created, "isoformat") else str(created),
            }
        return {
            "mode": "client_multipart",
            "pool": "data/coldstart/upload_pool",
            "interval_sec": int(settings.auto_upload_interval_sec or 90),
            "keep": int(settings.auto_upload_keep or 15),
            "last": last_payload,
        }
    finally:
        if own_db and db is not None:
            db.close()


def _run_batch_cf_job() -> dict[str, Any]:
    db = SessionLocal()
    try:
        return run_batch_cf(db)
    finally:
        db.close()


async def _batch_cf_scheduler() -> None:
    interval = max(15, int(settings.batch_cf_interval_sec or 90))
    logger.info("Batch CF scheduler started (every %ss)", interval)
    # First run shortly after boot so Serving has CF scores for demo.
    await asyncio.sleep(5)
    while True:
        try:
            result = await asyncio.to_thread(_run_batch_cf_job)
            logger.info("Batch CF finished: %s", result)
        except Exception:
            logger.exception("Batch CF scheduled run failed")
        await asyncio.sleep(interval)


async def _enqueue_extract_pipeline(
    *,
    db: Session,
    video_id: str,
    creator_id: str,
    title: str,
    tag_list: list[str],
    category: str,
    media_key: str,
) -> None:
    event = {
        "event_id": uuid.uuid4().hex,
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": "content.upload",
        "video_id": video_id,
        "creator_id": creator_id,
        "title": title,
        "tags": tag_list,
        "category": category,
        "media_key": media_key,
        "status": "draft",
    }
    await publish("events.content", event, key=video_id)
    extract_req = {
        "event_id": uuid.uuid4().hex,
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": "extract.requested",
        "video_id": video_id,
        "media_key": media_key,
    }
    await publish("events.extract", extract_req, key=video_id)
    db.execute(text("UPDATE videos SET status='extracting', updated_at=NOW() WHERE video_id=:id"), {"id": video_id})
    db.commit()
    record_event_metric(
        "upload",
        topic="events.content",
        user_id=creator_id,
        video_id=video_id,
    )


def _prune_auto_uploads(db: Session, keep: int) -> int:
    rows = fetch_all(
        db,
        """
        SELECT video_id, media_key, thumb_key
        FROM videos
        WHERE creator_id = :c OR :tag = ANY(tags)
        ORDER BY created_at DESC
        """,
        {"c": AUTO_CREATOR_ID, "tag": AUTO_TAG},
    )
    removed = 0
    for old in rows[keep:]:
        delete_media(old.get("media_key"))
        delete_media(old.get("thumb_key"))
        db.execute(text("DELETE FROM interactions WHERE video_id=:id"), {"id": old["video_id"]})
        db.execute(text("DELETE FROM cf_scores WHERE video_id=:id"), {"id": old["video_id"]})
        db.execute(text("DELETE FROM videos WHERE video_id=:id"), {"id": old["video_id"]})
        removed += 1
    if removed:
        db.commit()
    return removed


class EventIn(BaseModel):
    user_id: str
    video_id: str
    event_type: str = Field(pattern="^(view|watch|like|skip|share)$")
    watch_time: float = 0.0


class WeightsIn(BaseModel):
    alpha: float
    beta: float
    gamma: float


class ColdstartUserIn(BaseModel):
    user_id: str | None = None


class SeedVideo(BaseModel):
    video_id: str
    creator_id: str = "seed_creator"
    title: str
    tags: list[str] = []
    category: str | None = None
    media_key: str
    status: str = "enriched"
    duration_sec: float | None = None
    enriched_tags: list[str] = []


class FollowIn(BaseModel):
    user_id: str
    creator_id: str


async def _cancel_task(task: asyncio.Task | None) -> None:
    if not task:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@app.on_event("startup")
async def on_startup() -> None:
    global _batch_cf_task
    ensure_bucket()
    ensure_schema()
    if not any(getattr(r, "path", None) == "/media" for r in app.router.routes):
        app.mount("/media", StaticFiles(directory=str(MEDIA_ROOT)), name="media")
    await start_producer()
    _batch_cf_task = asyncio.create_task(_batch_cf_scheduler())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global _batch_cf_task
    await _cancel_task(_batch_cf_task)
    _batch_cf_task = None
    await stop_producer()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/weights")
def api_get_weights(db: Session = Depends(get_db)) -> dict[str, float]:
    return get_weights(db)


@app.put("/api/weights")
def api_set_weights(body: WeightsIn, db: Session = Depends(get_db)) -> dict[str, float]:
    return set_weights(db, body.alpha, body.beta, body.gamma)


@app.get("/api/feed/{user_id}")
def api_feed(user_id: str, limit: int = 20, db: Session = Depends(get_db)) -> dict[str, Any]:
    ensure_user(db, user_id)
    items = recommend_feed(db, user_id, limit=limit)
    inspector = get_user_inspector_state(user_id)
    return {"user_id": user_id, "items": items, **inspector}


@app.get("/api/feed/{user_id}/following")
def api_following_feed(user_id: str, limit: int = 40, db: Session = Depends(get_db)) -> dict[str, Any]:
    ensure_user(db, user_id)
    items = recommend_feed(db, user_id, limit=limit, following_only=True)
    return {"user_id": user_id, "items": items, "mode": "following"}


@app.post("/api/follow")
def api_follow(body: FollowIn, db: Session = Depends(get_db)) -> dict[str, Any]:
    ensure_user(db, body.user_id)
    ensure_user(db, body.creator_id)
    if body.user_id == body.creator_id:
        raise HTTPException(400, "Cannot follow yourself")
    db.execute(
        text(
            """
            INSERT INTO follows (user_id, creator_id)
            VALUES (:user_id, :creator_id)
            ON CONFLICT (user_id, creator_id) DO NOTHING
            """
        ),
        {"user_id": body.user_id, "creator_id": body.creator_id},
    )
    db.commit()
    record_event_metric(
        "follow",
        topic="events.user",
        user_id=body.user_id,
        video_id=body.creator_id,
    )
    return {"status": "ok", "followed": True, "creator_id": body.creator_id}


@app.post("/api/unfollow")
def api_unfollow(body: FollowIn, db: Session = Depends(get_db)) -> dict[str, Any]:
    db.execute(
        text("DELETE FROM follows WHERE user_id=:user_id AND creator_id=:creator_id"),
        {"user_id": body.user_id, "creator_id": body.creator_id},
    )
    db.commit()
    return {"status": "ok", "followed": False, "creator_id": body.creator_id}


@app.get("/api/creators/{creator_id}")
def api_creator(creator_id: str, viewer_id: str | None = None, db: Session = Depends(get_db)) -> dict[str, Any]:
    ensure_user(db, creator_id)
    counts = fetch_one(
        db,
        """
        SELECT
          (SELECT COUNT(*) FROM videos WHERE creator_id=:c) AS videos,
          (SELECT COUNT(*) FROM follows WHERE creator_id=:c) AS followers,
          (SELECT COUNT(*) FROM interactions i
             JOIN videos v ON v.video_id = i.video_id
            WHERE v.creator_id=:c AND i.event_type='like') AS likes,
          (SELECT COUNT(*) FROM interactions i
             JOIN videos v ON v.video_id = i.video_id
            WHERE v.creator_id=:c AND i.event_type IN ('view','watch')) AS views
        """,
        {"c": creator_id},
    )
    followed = False
    if viewer_id:
        row = fetch_one(
            db,
            "SELECT 1 AS ok FROM follows WHERE user_id=:u AND creator_id=:c",
            {"u": viewer_id, "c": creator_id},
        )
        followed = bool(row)
    rows = fetch_all(
        db,
        """
        SELECT video_id, creator_id, title, tags, category, media_key, thumb_key,
               status, duration_sec, enriched_tags, created_at
        FROM videos
        WHERE creator_id = :c
        ORDER BY created_at DESC
        LIMIT 60
        """,
        {"c": creator_id},
    )
    videos = []
    for r in rows:
        if not media_file_ok(r["media_key"]):
            continue
        videos.append(
            {
                "video_id": r["video_id"],
                "creator_id": r["creator_id"],
                "title": r["title"],
                "tags": r["tags"] or [],
                "enriched_tags": r["enriched_tags"] or [],
                "category": r["category"],
                "status": r["status"],
                "duration_sec": r["duration_sec"],
                "media_url": media_url(r["media_key"]),
                "thumb_url": media_url(r["thumb_key"]),
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
        )
    return {
        "creator_id": creator_id,
        "display_name": creator_id,
        "bio": "Provider channel · ShortVideo demo",
        "followed": followed,
        "counts": {
            "videos": int((counts or {}).get("videos") or 0),
            "followers": int((counts or {}).get("followers") or 0),
            "likes": int((counts or {}).get("likes") or 0),
            "views": int((counts or {}).get("views") or 0),
        },
        "videos": videos,
    }


@app.get("/api/studio/{creator_id}")
def api_studio(creator_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    """Provider studio: posts + per-video interaction stats."""
    ensure_user(db, creator_id)
    rows = fetch_all(
        db,
        """
        SELECT v.video_id, v.creator_id, v.title, v.tags, v.category, v.media_key, v.thumb_key,
               v.status, v.duration_sec, v.enriched_tags, v.created_at,
               COALESCE(s.likes, 0) AS likes,
               COALESCE(s.views, 0) AS views
        FROM videos v
        LEFT JOIN (
          SELECT video_id,
                 SUM(CASE WHEN event_type='like' THEN 1 ELSE 0 END) AS likes,
                 SUM(CASE WHEN event_type IN ('view','watch') THEN 1 ELSE 0 END) AS views
          FROM interactions
          GROUP BY video_id
        ) s ON s.video_id = v.video_id
        WHERE v.creator_id = :c
        ORDER BY v.created_at DESC
        LIMIT 100
        """,
        {"c": creator_id},
    )
    items = []
    for r in rows:
        items.append(
            {
                "video_id": r["video_id"],
                "creator_id": r["creator_id"],
                "title": r["title"],
                "tags": r["tags"] or [],
                "enriched_tags": r["enriched_tags"] or [],
                "category": r["category"],
                "status": r["status"],
                "duration_sec": r["duration_sec"],
                "media_url": media_url(r["media_key"]),
                "thumb_url": media_url(r["thumb_key"]),
                "likes": int(r["likes"] or 0),
                "views": int(r["views"] or 0),
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
        )
    followers = fetch_one(db, "SELECT COUNT(*) AS n FROM follows WHERE creator_id=:c", {"c": creator_id})
    return {
        "creator_id": creator_id,
        "followers": int((followers or {}).get("n") or 0),
        "items": items,
    }


@app.get("/api/demo-users")
def api_demo_users(limit: int = 40, db: Session = Depends(get_db)) -> dict[str, Any]:
    """Users for For You demo picker — includes marked cold-start user even at 0 events."""
    limit = max(1, min(limit, 100))
    cold = get_coldstart_user()
    rows = fetch_all(
        db,
        """
        SELECT u.user_id,
               COUNT(i.id) AS interactions,
               COUNT(DISTINCT i.video_id) AS videos_touched
        FROM users u
        LEFT JOIN interactions i ON i.user_id = u.user_id
        WHERE u.user_id NOT LIKE 'creator_%'
          AND u.user_id NOT LIKE 'creator_auto%'
        GROUP BY u.user_id
        HAVING COUNT(i.id) > 0 OR u.user_id = :cold
        ORDER BY (u.user_id = :cold) DESC, COUNT(i.id) DESC
        LIMIT :lim
        """,
        {"lim": limit, "cold": cold or ""},
    )
    items = [
        {
            "user_id": r["user_id"],
            "interactions": int(r["interactions"] or 0),
            "videos_touched": int(r["videos_touched"] or 0),
            "cold_start": bool(cold) and r["user_id"] == cold,
        }
        for r in rows
    ]
    if cold and not any(i["user_id"] == cold for i in items):
        ensure_user(db, cold)
        items.insert(
            0,
            {
                "user_id": cold,
                "interactions": 0,
                "videos_touched": 0,
                "cold_start": True,
            },
        )
    return {"items": items, "coldstart_user": cold}


@app.get("/api/trending")
def api_trending(limit: int = 24, window: str = "1m", db: Session = Depends(get_db)) -> dict[str, Any]:
    """Top hot videos by weighted interactions within a time window (1m, 24h, all)."""
    limit = max(1, min(limit, 30))
    win = window if window in {"1m", "24h", "all"} else "1m"
    time_filter = ""
    if win == "1m":
        time_filter = "WHERE created_at >= NOW() - INTERVAL '1 minute'"
    elif win == "24h":
        time_filter = "WHERE created_at >= NOW() - INTERVAL '24 hours'"

    rows = fetch_all(
        db,
        f"""
        SELECT v.video_id, v.creator_id, v.title, v.tags, v.category, v.media_key, v.thumb_key,
               v.status, v.duration_sec, v.enriched_tags,
               COALESCE(s.heat, 0) AS heat,
               COALESCE(s.likes, 0) AS likes,
               COALESCE(s.views, 0) AS views,
               COALESCE(s.shares, 0) AS shares
        FROM videos v
        JOIN (
          SELECT video_id,
                 SUM(CASE
                       WHEN event_type = 'like' THEN 3
                       WHEN event_type = 'share' THEN 4
                       WHEN event_type = 'watch' THEN 1.5
                       WHEN event_type = 'view' THEN 0.5
                       WHEN event_type = 'skip' THEN -0.5
                       ELSE 0
                     END) AS heat,
                 SUM(CASE WHEN event_type = 'like' THEN 1 ELSE 0 END) AS likes,
                 SUM(CASE WHEN event_type IN ('view','watch') THEN 1 ELSE 0 END) AS views,
                 SUM(CASE WHEN event_type = 'share' THEN 1 ELSE 0 END) AS shares
          FROM interactions
          {time_filter}
          GROUP BY video_id
        ) s ON s.video_id = v.video_id
        WHERE v.status = 'enriched'
        ORDER BY s.heat DESC, s.likes DESC, s.views DESC
        LIMIT :lim
        """,
        {"lim": limit * 3},
    )
    items = []
    for r in rows:
        if not media_file_ok(r["media_key"]):
            continue
        items.append(
            {
                "video_id": r["video_id"],
                "creator_id": r["creator_id"],
                "title": r["title"],
                "tags": r["tags"] or [],
                "enriched_tags": r["enriched_tags"] or [],
                "category": r["category"],
                "status": r["status"],
                "duration_sec": r["duration_sec"],
                "media_url": media_url(r["media_key"]),
                "thumb_url": media_url(r["thumb_key"]),
                "heat": round(float(r["heat"] or 0), 2),
                "likes": int(r["likes"] or 0),
                "views": int(r["views"] or 0),
                "shares": int(r["shares"] or 0),
            }
        )
        if len(items) >= limit:
            break
    return {"items": items, "limit": limit, "window": win}


@app.get("/api/videos")
def api_videos(creator_id: str | None = None, db: Session = Depends(get_db)) -> dict[str, Any]:
    if creator_id:
        rows = fetch_all(
            db,
            """
            SELECT video_id, creator_id, title, tags, category, media_key, thumb_key,
                   status, duration_sec, width, height, fps, enriched_tags, created_at
            FROM videos WHERE creator_id=:c ORDER BY created_at DESC LIMIT 200
            """,
            {"c": creator_id},
        )
    else:
        rows = fetch_all(
            db,
            """
            SELECT video_id, creator_id, title, tags, category, media_key, thumb_key,
                   status, duration_sec, width, height, fps, enriched_tags, created_at
            FROM videos ORDER BY created_at DESC LIMIT 200
            """,
        )
    items = []
    for r in rows:
        items.append(
            {
                **dict(r),
                "tags": r["tags"] or [],
                "enriched_tags": r["enriched_tags"] or [],
                "media_url": media_url(r["media_key"]),
                "thumb_url": media_url(r["thumb_key"]),
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
        )
    return {"items": items}


@app.post("/api/upload")
async def api_upload(
    file: UploadFile = File(...),
    title: str = Form(...),
    tags: str = Form(""),
    category: str = Form("general"),
    creator_id: str = Form("creator_demo"),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ensure_user(db, creator_id)
    video_id = f"v_{uuid.uuid4().hex[:12]}"
    data = await file.read()
    if not data:
        raise HTTPException(400, "Empty file")
    ext = (file.filename or "video.mp4").rsplit(".", 1)[-1].lower()
    if ext not in {"mp4", "webm", "mov", "mkv"}:
        ext = "mp4"
    media_key = f"uploads/{video_id}.{ext}"
    upload_bytes(media_key, data, file.content_type or "video/mp4")
    tag_list = filter_content_tags([t.strip() for t in tags.split(",") if t.strip()])

    db.execute(
        text(
            """
            INSERT INTO videos (video_id, creator_id, title, tags, category, media_key, status)
            VALUES (:video_id, :creator_id, :title, :tags, :category, :media_key, 'draft')
            """
        ),
        {
            "video_id": video_id,
            "creator_id": creator_id,
            "title": title,
            "tags": tag_list,
            "category": category,
            "media_key": media_key,
        },
    )
    db.commit()
    await _enqueue_extract_pipeline(
        db=db,
        video_id=video_id,
        creator_id=creator_id,
        title=title,
        tag_list=tag_list,
        category=category,
        media_key=media_key,
    )
    if creator_id == AUTO_CREATOR_ID or AUTO_TAG in tag_list:
        _prune_auto_uploads(db, keep=max(3, int(settings.auto_upload_keep or 15)))

    return {
        "video_id": video_id,
        "status": "extracting",
        "media_url": media_url(media_key),
        "title": title,
        "tags": tag_list,
        "category": category,
    }


@app.post("/api/events")
async def api_events(body: EventIn, db: Session = Depends(get_db)) -> dict[str, str]:
    ensure_user(db, body.user_id)
    video = fetch_one(
        db,
        "SELECT video_id, tags, enriched_tags, category, duration_sec FROM videos WHERE video_id=:id",
        {"id": body.video_id},
    )
    if not video:
        raise HTTPException(404, "Video not found")

    weight_map = {"view": 0.4, "watch": 1.0, "like": 3.0, "skip": -0.5, "share": 4.0}
    weight = weight_map.get(body.event_type, 1.0)
    if body.event_type == "watch" and body.watch_time > 0:
        dur = float(video.get("duration_sec") or 15.0)
        completion = min(2.0, body.watch_time / max(dur, 1.0))
        weight = round(min(4.0, 0.5 + 2.0 * completion), 3)

    db.execute(
        text(
            """
            INSERT INTO interactions (user_id, video_id, event_type, watch_time, weight)
            VALUES (:user_id, :video_id, :event_type, :watch_time, :weight)
            """
        ),
        {
            "user_id": body.user_id,
            "video_id": body.video_id,
            "event_type": body.event_type,
            "watch_time": body.watch_time,
            "weight": weight,
        },
    )
    db.commit()

    # Probabilistic DS updates (CO3137 Ch. 6): Bloom Filter seen-set + HyperLogLog active users.
    record_user_seen(body.user_id, body.video_id)
    record_active_user(body.user_id)

    affinity_tags = filter_content_tags(list(video["tags"] or []))
    if video["category"]:
        affinity_tags = filter_content_tags(affinity_tags + [str(video["category"])])

    # Publish to Kafka only — speed-worker is the single writer to Redis affinity & hot_tags.
    event = {
        "event_id": uuid.uuid4().hex,
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": f"user.{body.event_type}",
        "user_id": body.user_id,
        "video_id": body.video_id,
        "watch_time": body.watch_time,
        "weight": weight,
        "tags": affinity_tags,
        "category": video["category"],
    }
    await publish("events.user", event, key=body.user_id)
    record_event_metric(
        body.event_type,
        topic="events.user",
        user_id=body.user_id,
        video_id=body.video_id,
    )
    return {"status": "ok"}


@app.get("/api/simulation/live")
def api_simulation_live(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Latest 5 active Customers (events.user) + latest 5 uploaded Provider videos for Live Sim Wall."""
    customer_rows = fetch_all(
        db,
        """
        SELECT DISTINCT ON (i.user_id)
               i.id, i.user_id, i.video_id, i.event_type, i.watch_time, i.weight, i.created_at,
               v.title, v.category, v.creator_id, v.media_key, v.thumb_key, v.duration_sec
        FROM (
            SELECT id, user_id, video_id, event_type, watch_time, weight, created_at
            FROM interactions
            ORDER BY id DESC
            LIMIT 400
        ) i
        JOIN videos v ON v.video_id = i.video_id
        ORDER BY i.user_id, i.id DESC
        """,
    )
    customer_rows.sort(key=lambda r: int(r["id"]), reverse=True)
    customers = []
    for r in customer_rows[:5]:
        ts = r.get("created_at")
        customers.append(
            {
                "event_id": int(r["id"]),
                "user_id": r["user_id"],
                "video_id": r["video_id"],
                "event_type": r["event_type"],
                "watch_time": round(float(r.get("watch_time") or 0), 1),
                "weight": round(float(r.get("weight") or 0), 2),
                "title": r.get("title") or r["video_id"],
                "category": r.get("category") or "general",
                "creator_id": r.get("creator_id") or "creator",
                "duration_sec": round(float(r.get("duration_sec") or 15.0), 1),
                "media_url": media_url(r.get("media_key")),
                "thumb_url": media_url(r.get("thumb_key")),
                "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts or ""),
            }
        )

    provider_rows = fetch_all(
        db,
        """
        SELECT video_id, creator_id, title, category, status, media_key, thumb_key,
               duration_sec, enriched_tags, created_at
        FROM videos
        ORDER BY created_at DESC
        LIMIT 5
        """,
    )
    providers = []
    for r in provider_rows:
        ts = r.get("created_at")
        providers.append(
            {
                "video_id": r["video_id"],
                "creator_id": r.get("creator_id") or "creator_auto",
                "title": r.get("title") or r["video_id"],
                "category": r.get("category") or "general",
                "status": r.get("status") or "enriched",
                "duration_sec": round(float(r.get("duration_sec") or 0), 1) if r.get("duration_sec") else None,
                "enriched_tags": list(r.get("enriched_tags") or [])[:3],
                "media_url": media_url(r.get("media_key")),
                "thumb_url": media_url(r.get("thumb_key")),
                "created_at": ts.isoformat() if hasattr(ts, "isoformat") else str(ts or ""),
            }
        )

    m = get_metrics()
    return {
        "customers": customers,
        "providers": providers,
        "events_per_sec": m.get("events_per_sec", 0),
        "extract_stats": m.get("extract_stats", {}),
    }


@app.get("/api/metrics")
def api_metrics(db: Session = Depends(get_db)) -> dict[str, Any]:
    counts = fetch_one(
        db,
        """
        SELECT
          (SELECT COUNT(*) FROM videos) AS videos,
          (SELECT COUNT(*) FROM videos WHERE status='extracting') AS extracting,
          (SELECT COUNT(*) FROM videos WHERE status='enriched') AS enriched,
          (SELECT COUNT(*) FROM videos WHERE status='draft') AS draft,
          (SELECT COUNT(*) FROM interactions) AS interactions,
          (SELECT COUNT(*) FROM users) AS users,
          (SELECT COUNT(*) FROM follows) AS follows,
          (SELECT COUNT(DISTINCT category) FROM videos WHERE category IS NOT NULL AND category <> '') AS categories,
          (SELECT COUNT(*) FROM cf_scores) AS cf_pairs
        """,
    )
    return {
        **get_metrics(),
        "db": dict(counts) if counts else {},
        "weights": get_weights(db),
        "batch_cf": get_batch_cf_status(),
        "auto_upload": get_auto_upload_status(db),
    }


@app.post("/api/admin/purge-catalog")
def api_purge_catalog(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Wipe videos/interactions/cf_scores so ShortVideo ETL can load clean real data."""
    import shutil

    counts = {
        "cf_scores": db.execute(text("SELECT COUNT(*) FROM cf_scores")).scalar() or 0,
        "interactions": db.execute(text("SELECT COUNT(*) FROM interactions")).scalar() or 0,
        "videos": db.execute(text("SELECT COUNT(*) FROM videos")).scalar() or 0,
    }
    db.execute(text("DELETE FROM cf_scores"))
    db.execute(text("DELETE FROM interactions"))
    db.execute(text("DELETE FROM videos"))
    db.commit()
    for sub in ("uploads", "thumbs", "seed"):
        path = MEDIA_ROOT / sub
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
            path.mkdir(parents=True, exist_ok=True)
    ensure_bucket()
    return {"status": "purged", "deleted": counts}


@app.post("/api/admin/seed-videos")
def api_seed_videos(videos: list[SeedVideo], db: Session = Depends(get_db)) -> dict[str, int]:
    n = 0
    for v in videos:
        ensure_user(db, v.creator_id)
        db.execute(
            text(
                """
                INSERT INTO videos (video_id, creator_id, title, tags, category, media_key, status, duration_sec, enriched_tags)
                VALUES (:video_id, :creator_id, :title, :tags, :category, :media_key, :status, :duration_sec, :enriched_tags)
                ON CONFLICT (video_id) DO UPDATE SET
                  title=EXCLUDED.title,
                  tags=EXCLUDED.tags,
                  category=EXCLUDED.category,
                  media_key=EXCLUDED.media_key,
                  status=EXCLUDED.status,
                  duration_sec=EXCLUDED.duration_sec,
                  enriched_tags=EXCLUDED.enriched_tags,
                  updated_at=NOW()
                """
            ),
            {
                "video_id": v.video_id,
                "creator_id": v.creator_id,
                "title": v.title,
                "tags": v.tags,
                "category": v.category,
                "media_key": v.media_key,
                "status": v.status,
                "duration_sec": v.duration_sec,
                "enriched_tags": v.enriched_tags,
            },
        )
        n += 1
    db.commit()
    return {"upserted": n}


@app.post("/api/admin/seed-interactions")
def api_seed_interactions(rows: list[dict[str, Any]], db: Session = Depends(get_db)) -> dict[str, int]:
    n = 0
    for r in rows:
        ensure_user(db, r["user_id"])
        db.execute(
            text(
                """
                INSERT INTO interactions (user_id, video_id, event_type, watch_time, weight)
                VALUES (:user_id, :video_id, :event_type, :watch_time, :weight)
                """
            ),
            {
                "user_id": r["user_id"],
                "video_id": r["video_id"],
                "event_type": r.get("event_type", "watch"),
                "watch_time": float(r.get("watch_time", 5)),
                "weight": float(r.get("weight", 1)),
            },
        )
        n += 1
    db.commit()
    return {"inserted": n}


@app.post("/api/admin/run-batch-cf")
def api_run_batch_cf(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Manual trigger; same job also runs on BATCH_CF_INTERVAL_SEC schedule."""
    return run_batch_cf(db)


@app.post("/api/admin/scrub-tags")
def api_scrub_tags(body: dict[str, Any], db: Session = Depends(get_db)) -> dict[str, Any]:
    """Rewrite tags/titles from dataset meta and strip demo noise across catalog."""
    items = body.get("items") or []
    updated = 0
    for it in items:
        vid = str(it.get("video_id") or "").strip()
        if not vid:
            continue
        tags = filter_content_tags(list(it.get("tags") or []))
        category = str(it.get("category") or "general")[:64]
        title = str(it.get("title") or "").strip()[:120]
        params: dict[str, Any] = {"id": vid, "tags": tags, "category": category}
        if title:
            params["title"] = title
            db.execute(
                text(
                    """
                    UPDATE videos
                    SET tags=:tags, category=:category, title=:title, updated_at=NOW()
                    WHERE video_id=:id
                    """
                ),
                params,
            )
        else:
            db.execute(
                text(
                    """
                    UPDATE videos
                    SET tags=:tags, category=:category, updated_at=NOW()
                    WHERE video_id=:id
                    """
                ),
                params,
            )
        updated += 1

    # Global strip of noise tags still stuck in arrays; fix legacy fake titles.
    noise_removed = db.execute(
        text(
            """
            UPDATE videos
            SET tags = ARRAY(
                  SELECT DISTINCT t
                  FROM unnest(COALESCE(tags, ARRAY[]::text[])) AS t
                  WHERE lower(replace(t, ' ', '_')) NOT IN
                    ('shortvideo','short_video','demo','auto_upload','coldstart','cold_start')
                ),
                updated_at = NOW()
            WHERE tags && ARRAY['shortvideo','short_video','demo','auto_upload','coldstart','cold_start']::text[]
            """
        )
    ).rowcount
    titles_fixed = db.execute(
        text(
            """
            UPDATE videos
            SET title = CASE
                  WHEN category IS NOT NULL AND category <> '' AND lower(category) <> 'general'
                    THEN initcap(replace(category, '_', ' ')) || ' clip'
                  ELSE 'Clip ' || right(video_id, 6)
                END,
                updated_at = NOW()
            WHERE title ILIKE 'ShortVideo%'
               OR title ILIKE '[auto]%'
            """
        )
    ).rowcount
    db.commit()
    # Drop noisy hot-tag keys from Redis so Dashboard reflects clean tags.
    try:
        from app.recommend import _redis

        for bad in ("shortvideo", "short_video", "demo", "auto_upload", "coldstart", "cold_start"):
            _redis.zrem("metrics:hot_tags", bad)
    except Exception:
        pass
    return {
        "status": "ok",
        "items_updated": updated,
        "noise_rows": int(noise_removed or 0),
        "titles_fixed": int(titles_fixed or 0),
    }


@app.post("/api/admin/coldstart-user")
def api_coldstart_user(body: ColdstartUserIn, db: Session = Depends(get_db)) -> dict[str, Any]:
    """Wipe one user's history so Demo-as can show cold-start recommendations."""
    user_id = (body.user_id or "").strip()
    if not user_id:
        row = fetch_one(
            db,
            """
            SELECT user_id, COUNT(*) AS n
            FROM interactions
            WHERE user_id LIKE 'sv_%'
            GROUP BY user_id
            ORDER BY n DESC
            LIMIT 1
            """,
        )
        if not row:
            raise HTTPException(404, "No sv_* users with interactions to cold-start")
        user_id = str(row["user_id"])

    ensure_user(db, user_id)
    deleted_i = db.execute(
        text("DELETE FROM interactions WHERE user_id=:u"),
        {"u": user_id},
    ).rowcount
    deleted_cf = db.execute(
        text("DELETE FROM cf_scores WHERE user_id=:u"),
        {"u": user_id},
    ).rowcount
    db.commit()
    clear_user_affinity(user_id)
    set_coldstart_user(user_id)
    return {
        "status": "ok",
        "user_id": user_id,
        "deleted_interactions": int(deleted_i or 0),
        "deleted_cf_scores": int(deleted_cf or 0),
        "cold_start": True,
    }


@app.websocket("/ws/metrics")
async def ws_metrics(ws: WebSocket) -> None:
    await ws.accept()
    try:
        while True:
            db = SessionLocal()
            try:
                payload = {
                    **get_metrics(),
                    "weights": get_weights(db),
                    "batch_cf": get_batch_cf_status(),
                    "auto_upload": get_auto_upload_status(db),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            finally:
                db.close()
            await ws.send_json(payload)
            await asyncio.sleep(1.5)
    except WebSocketDisconnect:
        return


def ensure_schema() -> None:
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS follows (
                    user_id    TEXT NOT NULL,
                    creator_id TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (user_id, creator_id)
                )
                """
            )
        )
        db.execute(text("CREATE INDEX IF NOT EXISTS idx_follows_creator ON follows(creator_id)"))
        db.execute(text("CREATE INDEX IF NOT EXISTS idx_videos_creator ON videos(creator_id)"))
        db.commit()
    finally:
        db.close()


def ensure_user(db: Session, user_id: str) -> None:
    db.execute(
        text(
            """
            INSERT INTO users (user_id, display_name)
            VALUES (:user_id, :name)
            ON CONFLICT (user_id) DO NOTHING
            """
        ),
        {"user_id": user_id, "name": user_id},
    )
    db.commit()
