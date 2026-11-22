from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import numpy as np
import redis
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import settings
from app.db import fetch_all, fetch_one
from app.probabilistic import (
    bloom_add,
    bloom_contains_many,
    bloom_seen_estimate,
    hll_add_user,
    hll_unique_users,
    hll_unique_users_1m,
)
from app.storage import media_file_ok, media_url
from app.tag_filters import (
    ENRICHED_INTERNAL,
    filter_content_tags,
    filter_enriched_display,
    filter_for_hot_tags,
    is_noise_tag,
)

_redis = redis.Redis.from_url(settings.redis_url, decode_responses=True)
_VELOCITY_WINDOW_SEC = 30
_VELOCITY_BUCKET_PREFIX = "metrics:eps:"
_VELOCITY_BUCKET_TTL_SEC = 90


def _velocity_bucket_key(when: datetime | None = None) -> str:
    ts = when or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return f"{_VELOCITY_BUCKET_PREFIX}{int(ts.timestamp())}"


def _bump_velocity_counter(when: datetime | None = None) -> None:
    """Count throughput in 1-second Redis buckets (independent of event_tail sample)."""
    key = _velocity_bucket_key(when)
    pipe = _redis.pipeline()
    pipe.incr(key)
    pipe.expire(key, _VELOCITY_BUCKET_TTL_SEC)
    pipe.execute()


def get_weights(db: Session) -> dict[str, float]:
    row = fetch_one(db, "SELECT alpha, beta, gamma FROM serving_weights WHERE id = 1")
    if not row:
        return {"alpha": 0.35, "beta": 0.35, "gamma": 0.30}
    return {"alpha": float(row["alpha"]), "beta": float(row["beta"]), "gamma": float(row["gamma"])}


def set_weights(db: Session, alpha: float, beta: float, gamma: float) -> dict[str, float]:
    total = alpha + beta + gamma
    if total <= 0:
        alpha, beta, gamma = 0.35, 0.35, 0.30
    else:
        alpha, beta, gamma = alpha / total, beta / total, gamma / total
    db.execute(
        __import__("sqlalchemy").text(
            "UPDATE serving_weights SET alpha=:a, beta=:b, gamma=:g, updated_at=NOW() WHERE id=1"
        ),
        {"a": alpha, "b": beta, "g": gamma},
    )
    db.commit()
    return {"alpha": alpha, "beta": beta, "gamma": gamma}


def _tag_vector(tags: list[str], vocab: list[str]) -> np.ndarray:
    c = Counter(t.lower() for t in tags if t)
    if not vocab:
        return np.zeros(1)
    vec = np.array([float(c.get(t, 0.0)) for t in vocab], dtype=np.float64)
    n = np.linalg.norm(vec)
    return vec / n if n > 0 else vec


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    if a.size == 0 or b.size == 0:
        return 0.0
    denom = float(np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


def user_realtime_affinity(user_id: str) -> dict[str, float]:
    raw = _redis.hgetall(f"user:affinity:{user_id}")
    return {k: float(v) for k, v in raw.items()}


def bump_affinity(user_id: str, tags: list[str], delta: float = 1.0) -> None:
    key = f"user:affinity:{user_id}"
    pipe = _redis.pipeline()
    for tag in tags:
        if tag:
            pipe.hincrbyfloat(key, tag.lower(), delta)
    pipe.expire(key, 60 * 60 * 24)
    pipe.execute()


def clear_user_affinity(user_id: str) -> None:
    _redis.delete(f"user:affinity:{user_id}")
    _redis.delete(f"bf:seen:{user_id}")


def record_user_seen(user_id: str, video_id: str) -> None:
    bloom_add(_redis, user_id, video_id)


def record_active_user(user_id: str) -> None:
    hll_add_user(_redis, user_id)


def get_user_inspector_state(user_id: str) -> dict[str, Any]:
    affinity = user_realtime_affinity(user_id)
    cleaned = {
        k: round(float(v), 2)
        for k, v in affinity.items()
        if float(v) > 0 and not is_noise_tag(k)
    }
    top_affinity = dict(sorted(cleaned.items(), key=lambda x: x[1], reverse=True)[:8])
    return {
        "bloom_seen_count": bloom_seen_estimate(_redis, user_id),
        "user_affinity": top_affinity,
    }


def set_coldstart_user(user_id: str) -> None:
    _redis.set("demo:coldstart_user", user_id)


def get_coldstart_user() -> str | None:
    val = _redis.get("demo:coldstart_user")
    return str(val) if val else None


def record_event_metric(
    event_type: str,
    *,
    topic: str = "events.user",
    user_id: str | None = None,
    video_id: str | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    _redis.hincrby("metrics:events", event_type, 1)
    _redis.incr("metrics:events:total")
    _bump_velocity_counter(now)
    entry = {
        "ts": now.isoformat(),
        "topic": topic,
        "event_type": event_type,
        "user_id": user_id,
        "video_id": video_id,
    }
    # Sample wall only — not used for velocity math.
    _redis.lpush("metrics:event_tail", json.dumps(entry, separators=(",", ":")))
    _redis.ltrim("metrics:event_tail", 0, 99)


def _parse_tail_entry(raw: str) -> dict[str, Any]:
    """Accept legacy plain event_type strings and new JSON tail entries."""
    if not raw:
        return {"ts": None, "topic": "events.user", "event_type": "?", "user_id": None, "video_id": None}
    if raw.startswith("{"):
        try:
            data = json.loads(raw)
            return {
                "ts": data.get("ts"),
                "topic": data.get("topic") or "events.user",
                "event_type": data.get("event_type") or "?",
                "user_id": data.get("user_id"),
                "video_id": data.get("video_id"),
            }
        except json.JSONDecodeError:
            pass
    return {"ts": None, "topic": "events.user", "event_type": raw, "user_id": None, "video_id": None}


def _events_in_window(window_sec: int = 60) -> int:
    now_sec = int(datetime.now(timezone.utc).timestamp())
    keys = [f"{_VELOCITY_BUCKET_PREFIX}{now_sec - i}" for i in range(window_sec)]
    values = _redis.mget(keys)
    return sum(int(v) for v in values if v)


def _events_per_sec(window_sec: int = _VELOCITY_WINDOW_SEC) -> float:
    """Average throughput from per-second Redis counters over a sliding window."""
    return round(_events_in_window(window_sec) / float(window_sec), 2)


def get_metrics() -> dict[str, Any]:
    total = int(_redis.get("metrics:events:total") or 0)
    by_type = {k: int(v) for k, v in _redis.hgetall("metrics:events").items()}
    raw_hot_1m = _redis.zrevrange("metrics:hot_tags:1m", 0, 49, withscores=True)
    raw_hot = raw_hot_1m if raw_hot_1m else _redis.zrevrange("metrics:hot_tags", 0, 49, withscores=True)
    hot_tags = [
        (t, s)
        for t, s in raw_hot
        if not is_noise_tag(t) and t.lower() not in ENRICHED_INTERNAL and t.lower() != "general"
    ][:20]
    raw_tail = _redis.lrange("metrics:event_tail", 0, 49)
    event_tail = [_parse_tail_entry(x) for x in raw_tail]
    speed_windows = _redis.hgetall("metrics:speed_windows") or {}
    extract_completed = int(_redis.get("metrics:extract:completed") or 0)
    extract_last_ms = int(_redis.get("metrics:extract:last_ms") or 0)
    return {
        "events_total": total,
        "events_1m": _events_in_window(60),
        "events_by_type": by_type,
        "hot_tags": [{"tag": t, "score": s} for t, s in hot_tags],
        "event_tail": event_tail[:30],
        "events_per_sec": _events_per_sec(),
        "velocity_window_sec": _VELOCITY_WINDOW_SEC,
        "hll_unique_users": hll_unique_users(_redis),
        "hll_active_1m": hll_unique_users_1m(_redis),
        "speed_windows": speed_windows,
        "extract_stats": {
            "completed_total": extract_completed,
            "last_ms": extract_last_ms,
        },
    }


def mark_hot_tags(tags: list[str], weight: float = 1.0) -> None:
    tags = filter_for_hot_tags(tags)
    if not tags:
        return
    pipe = _redis.pipeline()
    for tag in tags:
        pipe.zincrby("metrics:hot_tags", weight, tag.lower())
    pipe.execute()


def followed_creators(db: Session, user_id: str) -> set[str]:
    rows = fetch_all(db, "SELECT creator_id FROM follows WHERE user_id = :u", {"u": user_id})
    return {r["creator_id"] for r in rows if r["creator_id"]}


def _cold_start_boost(v: dict[str, Any], now_utc: datetime) -> float:
    """TikTok-style Cold-start Traffic Pool recency boost for freshly enriched uploads."""
    if v.get("status") != "enriched":
        return 0.0
    created = v.get("created_at")
    if not isinstance(created, datetime):
        return 0.0
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    age_min = (now_utc - created).total_seconds() / 60.0
    if age_min < 0 or age_min > 30.0:
        return 0.0
    return round(0.08 * (1.0 - age_min / 30.0), 4)


def _diversity_rerank(scored: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Greedy diversity re-ranking: avoid back-to-back same creator and >2 consecutive same category."""
    pool = list(scored)
    out: list[dict[str, Any]] = []
    while pool and len(out) < limit:
        picked_idx = 0
        last_creator = out[-1]["creator_id"] if out else None
        last_two_cats = [x.get("category") for x in out[-2:]]
        for i, cand in enumerate(pool):
            same_creator = bool(last_creator and cand.get("creator_id") == last_creator)
            cat = cand.get("category")
            too_many_cat = bool(cat and len(last_two_cats) == 2 and all(c == cat for c in last_two_cats))
            if not same_creator and not too_many_cat:
                picked_idx = i
                break
        out.append(pool.pop(picked_idx))
    return out


def recommend_feed(
    db: Session,
    user_id: str,
    limit: int = 20,
    *,
    following_only: bool = False,
) -> list[dict[str, Any]]:
    weights = get_weights(db)
    followed = followed_creators(db, user_id)
    videos = fetch_all(
        db,
        """
        SELECT video_id, creator_id, title, tags, category, media_key, thumb_key,
               status, duration_sec, enriched_tags, created_at
        FROM videos
        WHERE status IN ('draft', 'extracting', 'enriched', 'published')
          AND media_key IS NOT NULL AND media_key <> ''
        ORDER BY created_at DESC
        LIMIT 1000
        """,
    )
    if following_only:
        videos = [v for v in videos if v["creator_id"] in followed]
    if not videos:
        return []

    seen_set = bloom_contains_many(_redis, user_id, (v["video_id"] for v in videos))
    now_utc = datetime.now(timezone.utc)

    cf_rows = fetch_all(
        db,
        "SELECT video_id, score FROM cf_scores WHERE user_id = :u",
        {"u": user_id},
    )
    cf_map = {r["video_id"]: float(r["score"]) for r in cf_rows}
    if cf_map:
        mx = max(cf_map.values()) or 1.0
        cf_map = {k: v / mx for k, v in cf_map.items()}

    affinity = user_realtime_affinity(user_id)
    user_hist = fetch_all(
        db,
        """
        SELECT v.tags, v.enriched_tags, v.category, i.event_type
        FROM interactions i
        JOIN videos v ON v.video_id = i.video_id
        WHERE i.user_id = :u
        ORDER BY i.created_at DESC
        LIMIT 100
        """,
        {"u": user_id},
    )
    profile_tags: list[str] = []
    for h in user_hist:
        profile_tags.extend(filter_content_tags(h["tags"] or []))
        profile_tags.extend(h["enriched_tags"] or [])
        if h["category"]:
            profile_tags.append(h["category"])
        if h["event_type"] in ("like", "share"):
            profile_tags.extend(filter_content_tags(h["tags"] or []))
    for tag, score in affinity.items():
        if score > 0:
            repeats = max(1, min(10, int(score)))
            profile_tags.extend([tag] * repeats)

    def _item_content_tags(v: dict) -> list[str]:
        tags = filter_content_tags(v["tags"] or [])
        if v.get("category") and not is_noise_tag(str(v["category"])):
            cat = str(v["category"]).lower().replace(" ", "_")
            if cat not in tags and cat != "general":
                tags = [cat, *tags]
        tags.extend(t for t in (v.get("enriched_tags") or []) if t)
        return tags

    vocab = sorted(
        {
            t.lower()
            for v in videos
            for t in _item_content_tags(v)
            if t and not is_noise_tag(t)
        }
    )
    user_vec = _tag_vector([t for t in profile_tags if not is_noise_tag(t)], vocab)

    unseen_scored: list[dict[str, Any]] = []
    seen_scored: list[dict[str, Any]] = []
    for v in videos:
        if not media_file_ok(v["media_key"]):
            continue
        item_tags = _item_content_tags(v)
        content = _cosine(user_vec, _tag_vector(item_tags, vocab))
        if not profile_tags:
            content = 0.15

        cf = cf_map.get(v["video_id"], 0.0)
        rt = 0.0
        if affinity and item_tags:
            pos_aff_sum = sum(val for val in affinity.values() if val > 0)
            hits = [affinity.get(t.lower(), 0.0) for t in item_tags if not is_noise_tag(t)]
            pos_hits = [h for h in hits if h > 0]
            if pos_aff_sum > 0 and pos_hits:
                rt = min(1.0, sum(pos_hits) / (pos_aff_sum + 1e-6) * len(pos_hits))

        cold_boost = _cold_start_boost(v, now_utc)
        final = weights["alpha"] * content + weights["beta"] * cf + weights["gamma"] * rt + cold_boost
        final += 0.02 * math.tanh((hash(v["video_id"]) % 100) / 100.0)

        entry = {
            "video_id": v["video_id"],
            "creator_id": v["creator_id"],
            "title": v["title"],
            "tags": filter_content_tags(v["tags"] or []),
            "enriched_tags": filter_enriched_display(v["enriched_tags"] or []),
            "category": v["category"],
            "status": v["status"],
            "duration_sec": v["duration_sec"],
            "media_url": media_url(v["media_key"]),
            "thumb_url": media_url(v["thumb_key"]),
            "followed": v["creator_id"] in followed,
            "score": round(final, 4),
            "explain": {
                "content": round(content, 4),
                "cf_batch": round(cf, 4),
                "realtime": round(rt, 4),
                "cold_boost": cold_boost,
                "weights": weights,
            },
        }
        if v["video_id"] in seen_set:
            seen_scored.append(entry)
        else:
            unseen_scored.append(entry)

    unseen_scored.sort(key=lambda x: x["score"], reverse=True)
    seen_scored.sort(key=lambda x: x["score"], reverse=True)
    # Prefer unseen (Bloom Filter); fall back to highest-scoring seen only if unseen pool is small.
    candidates = unseen_scored if len(unseen_scored) >= min(limit, 8) else unseen_scored + seen_scored
    return _diversity_rerank(candidates, limit)


def run_batch_cf(db: Session) -> dict[str, Any]:
    """Truncated-SVD collaborative filtering → cf_scores (batch layer)."""
    rows = fetch_all(
        db,
        """
        SELECT user_id, video_id, SUM(weight) AS rating
        FROM interactions
        WHERE weight > 0
        GROUP BY user_id, video_id
        """,
    )
    if not rows:
        result = {"status": "empty", "pairs": 0, "users": 0, "videos": 0}
        _store_batch_cf_status(result)
        return result

    users = sorted({r["user_id"] for r in rows})
    videos = sorted({r["video_id"] for r in rows})
    u_idx = {u: i for i, u in enumerate(users)}
    v_idx = {v: i for i, v in enumerate(videos)}
    R = np.zeros((len(users), len(videos)), dtype=np.float64)
    for r in rows:
        R[u_idx[r["user_id"]], v_idx[r["video_id"]]] = float(r["rating"])

    k = min(8, min(R.shape) - 1) if min(R.shape) > 1 else 1
    try:
        U, S, Vt = np.linalg.svd(R, full_matrices=False)
        Uk, Sk, Vk = U[:, :k], S[:k], Vt[:k, :]
        pred = Uk @ np.diag(Sk) @ Vk
    except np.linalg.LinAlgError:
        result = {"status": "svd_failed", "pairs": 0, "users": len(users), "videos": len(videos)}
        _store_batch_cf_status(result)
        return result

    db.execute(text("DELETE FROM cf_scores"))
    inserted = 0
    top_n = 50
    for ui, user_id in enumerate(users):
        scores = pred[ui].copy()
        # Exclude already-interacted items so CF recommends genuinely NEW videos (not filtered out by Bloom Filter).
        unseen_mask = R[ui] <= 0
        if np.any(unseen_mask):
            scores[~unseen_mask] = -1.0
        top = np.argsort(scores)[::-1][:top_n]
        for vi in top:
            score = float(scores[vi])
            if score <= 0:
                continue
            db.execute(
                text(
                    """
                    INSERT INTO cf_scores (user_id, video_id, score, updated_at)
                    VALUES (:u, :v, :s, NOW())
                    ON CONFLICT (user_id, video_id) DO UPDATE
                      SET score=EXCLUDED.score, updated_at=NOW()
                    """
                ),
                {"u": user_id, "v": videos[int(vi)], "s": score},
            )
            inserted += 1
    db.commit()
    result = {
        "status": "ok",
        "users": len(users),
        "videos": len(videos),
        "pairs": inserted,
    }
    _store_batch_cf_status(result)
    return result


def _store_batch_cf_status(result: dict[str, Any]) -> None:
    payload = {
        **result,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "interval_sec": int(getattr(settings, "batch_cf_interval_sec", 90) or 90),
    }
    _redis.set("metrics:batch_cf:last", json.dumps(payload, separators=(",", ":")))


def get_batch_cf_status() -> dict[str, Any]:
    interval = int(getattr(settings, "batch_cf_interval_sec", 90) or 90)
    raw = _redis.get("metrics:batch_cf:last")
    if not raw:
        return {"interval_sec": interval, "last": None}
    try:
        last = json.loads(raw)
    except json.JSONDecodeError:
        last = None
    return {"interval_sec": interval, "last": last}
