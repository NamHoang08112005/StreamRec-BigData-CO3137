# Architecture — StreamRec (Lambda video recommend demo)

## Overview

StreamRec mô phỏng hệ thống recommend short-video kiểu TikTok trên **Lambda architecture**:

- **Batch layer:** collaborative filtering (ALS / truncated SVD) trên lịch sử tương tác.
- **Speed layer:** cửa sổ tumbling trên stream `events.user` → affinity tag realtime (Redis).
- **Serving layer:** FastAPI trộn `α·content + β·cf_batch + γ·realtime`.

Bus sự kiện: **Kafka**. Media: **shared volume** (`/data/media`, serve qua API `/media` — thay MinIO vì registry image hiện không pull anonymous). Catalog/serving: **Postgres + Redis**.

## Components

| Service | Role |
|---------|------|
| `web` | TikTok Feed / Upload / Profile; Ops Dashboard riêng |
| `api` | Ingest events, upload, hybrid serve, SSE/WS metrics |
| `extract-worker` | Async ffprobe/FFmpeg metadata enrichment |
| `speed-worker` | Tumbling-window speed layer (Kafka → Redis) |
| `traffic-simulator` | Bot users tạo throughput |
| `spark-*` (profile) | PySpark Structured Streaming + ALS jobs |
| `kafka`, `postgres`, `redis`, `media_data` volume | Infra |

## Topics

- `events.user` — view/watch/like/skip/share
- `events.content` — upload + draft metadata
- `events.extract` — extract.requested / extract.completed

## Metadata hybrid

1. Creator gửi title/tags/category + file → status `draft`/`extracting`.
2. Worker extract duration/resolution/fps/thumbnail/heuristic tags → `enriched`.
3. Content-based dùng tags draft ∪ enriched.

## Demo narrative

1. Dashboard: simulator bơm events, hot tags đổi.
2. Upload video → pipeline status đổi.
3. Feed: tương tác mạnh 1 category → Refresh rank → feed lệch.
4. Vặn α/β/γ trên Dashboard.
