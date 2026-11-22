"""Probabilistic data structures on Redis (CO3137 Ch. 6): Bloom Filter & HyperLogLog."""
from __future__ import annotations

import hashlib
import math
import time
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    import redis

BLOOM_BITS = 8192  # 1 KB per user
BLOOM_HASHES = 5
BLOOM_TTL_SEC = 60 * 60 * 24 * 7  # 7 days
HLL_ACTIVE_USERS_KEY = "hll:active_users"
HLL_1M_PREFIX = "hll:active_10s:"
HLL_BUCKET_SEC = 10
HLL_BUCKETS_1M = 6  # 6 x 10s = 60s sliding window


def _bloom_key(user_id: str) -> str:
    return f"bf:seen:{user_id}"


def _bloom_positions(video_id: str, m: int = BLOOM_BITS, k: int = BLOOM_HASHES) -> list[int]:
    """Compute k independent bit positions using double hashing over SHA-256."""
    digest = hashlib.sha256(video_id.encode("utf-8")).digest()
    h1 = int.from_bytes(digest[:8], "big")
    h2 = int.from_bytes(digest[8:16], "big") or 1
    return [(h1 + i * h2) % m for i in range(k)]


def bloom_add(r: redis.Redis, user_id: str, video_id: str) -> None:
    if not user_id or not video_id:
        return
    key = _bloom_key(user_id)
    pipe = r.pipeline()
    for pos in _bloom_positions(video_id):
        pipe.setbit(key, pos, 1)
    pipe.expire(key, BLOOM_TTL_SEC)
    pipe.execute()


def bloom_contains_many(r: redis.Redis, user_id: str, video_ids: Iterable[str]) -> set[str]:
    vids = [v for v in video_ids if v]
    if not user_id or not vids:
        return set()
    key = _bloom_key(user_id)
    if not r.exists(key):
        return set()
    pipe = r.pipeline()
    for vid in vids:
        for pos in _bloom_positions(vid):
            pipe.getbit(key, pos)
    bits = pipe.execute()
    seen: set[str] = set()
    k = BLOOM_HASHES
    for idx, vid in enumerate(vids):
        chunk = bits[idx * k : (idx + 1) * k]
        if len(chunk) == k and all(chunk):
            seen.add(vid)
    return seen


def bloom_seen_estimate(r: redis.Redis, user_id: str) -> int:
    """Approximate number of items inserted into a user's Bloom Filter from BITCOUNT."""
    if not user_id:
        return 0
    ones = int(r.bitcount(_bloom_key(user_id)) or 0)
    if ones <= 0:
        return 0
    if ones >= BLOOM_BITS:
        return int(BLOOM_BITS / BLOOM_HASHES)
    est = -(BLOOM_BITS / float(BLOOM_HASHES)) * math.log(1.0 - ones / float(BLOOM_BITS))
    return max(1, int(round(est)))


def hll_add_user(r: redis.Redis, user_id: str) -> None:
    if not user_id:
        return
    bucket = int(time.time()) // HLL_BUCKET_SEC
    bucket_key = f"{HLL_1M_PREFIX}{bucket}"
    pipe = r.pipeline()
    pipe.pfadd(HLL_ACTIVE_USERS_KEY, user_id)
    pipe.pfadd(bucket_key, user_id)
    pipe.expire(bucket_key, HLL_BUCKET_SEC * HLL_BUCKETS_1M + 30)
    pipe.execute()


def hll_unique_users(r: redis.Redis) -> int:
    return int(r.pfcount(HLL_ACTIVE_USERS_KEY) or 0)


def hll_unique_users_1m(r: redis.Redis) -> int:
    """Sliding 60s unique active users by merging 6 x 10s HyperLogLog registers."""
    now_bucket = int(time.time()) // HLL_BUCKET_SEC
    keys = [f"{HLL_1M_PREFIX}{now_bucket - i}" for i in range(HLL_BUCKETS_1M)]
    return int(r.pfcount(*keys) or 0)
