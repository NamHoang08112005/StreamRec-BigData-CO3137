from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path

import httpx

API_BASE = os.getenv("API_BASE", "http://localhost:8000").rstrip("/")
POOL_DIR = Path(os.getenv("POOL_DIR", "/data/coldstart/upload_pool"))
META_PATH = Path(os.getenv("POOL_META", "/data/coldstart/pool_meta.json"))
INTERVAL_SEC = max(5, int(os.getenv("UPLOAD_INTERVAL_SEC", "15")))
BATCH_SIZE = max(1, int(os.getenv("UPLOAD_BATCH_SIZE", "5")))
CREATOR_ID = os.getenv("CREATOR_ID", "creator_auto")
CATEGORIES = ["food", "travel", "sports", "music", "tech", "pets", "comedy", "fashion", "nature"]


def wait_api(client: httpx.Client) -> None:
    while True:
        try:
            if client.get(f"{API_BASE}/health", timeout=3).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)


def list_pool() -> list[Path]:
    if not POOL_DIR.is_dir():
        return []
    files = [p for p in POOL_DIR.iterdir() if p.is_file() and p.suffix.lower() in {".mp4", ".webm", ".mov", ".mkv"}]
    files.sort(key=lambda p: p.stat().st_size)
    return files


def load_meta() -> dict[str, dict]:
    if not META_PATH.exists():
        return {}
    try:
        raw = json.loads(META_PATH.read_text(encoding="utf-8"))
        return {str(k): v for k, v in raw.items() if isinstance(v, dict)}
    except Exception:
        return {}


def meta_for(path: Path, catalog: dict[str, dict]) -> tuple[str, str, str]:
    """Return title, tags_csv, category from pool_meta or sensible fallback."""
    m = catalog.get(path.stem) or {}
    category = str(m.get("category") or random.choice(CATEGORIES)).strip() or "general"
    tags = m.get("tags") or []
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",") if t.strip()]
    tags = [str(t).strip() for t in tags if str(t).strip()]
    if category and category.lower() not in {t.lower() for t in tags} and category != "general":
        tags = [category, *tags]
    if not tags:
        tags = [category]
    title = str(m.get("title") or "").strip()
    if not title:
        title = f"{category.replace('_', ' ').title()} · {path.stem}"
    return title[:120], ",".join(tags[:8]), category[:64]


def upload_one(client: httpx.Client, path: Path, catalog: dict[str, dict]) -> dict:
    title, tags, category = meta_for(path, catalog)
    with path.open("rb") as fh:
        files = {"file": (path.name, fh, "video/mp4")}
        data = {
            "title": title,
            "tags": tags,
            "category": category,
            "creator_id": CREATOR_ID,
        }
        resp = client.post(f"{API_BASE}/api/upload", data=data, files=files, timeout=120)
        resp.raise_for_status()
        return resp.json()


def main() -> None:
    print(
        f"Upload-simulator -> {API_BASE} pool={POOL_DIR} meta={META_PATH} "
        f"batch={BATCH_SIZE} every {INTERVAL_SEC}s"
    )
    with httpx.Client() as client:
        wait_api(client)
        idx = 0
        time.sleep(8)
        while True:
            pool = list_pool()
            catalog = load_meta()
            if not pool:
                print(f"No files in {POOL_DIR}; waiting…")
                time.sleep(10)
                continue
            batch = []
            for _ in range(min(BATCH_SIZE, len(pool))):
                batch.append(pool[idx % len(pool)])
                idx += 1
            for path in batch:
                try:
                    result = upload_one(client, path, catalog)
                    print(
                        f"uploaded file={path.name} video_id={result.get('video_id')} "
                        f"tags={result.get('tags')} title={result.get('title')!r}"
                    )
                except Exception as exc:
                    print(f"upload failed file={path.name}: {exc}")
            time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
