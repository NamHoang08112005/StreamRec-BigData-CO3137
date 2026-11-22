# Mã Mermaid chuẩn cho Hình 2.1 & Hình 3.1

Bạn có thể copy từng khối Mermaid bên dưới dán trực tiếp vào:
- **Mermaid Live Editor:** https://mermaid.live (chọn theme `neutral` hoặc `base`, xuất PNG độ phân giải cao)
- **Draw.io (diagrams.net):** Menu *Arrange → Insert → Advanced → Mermaid...* rồi tự căn chỉnh màu sắc/icon theo ý thích
- **Eraser.io (DiagramGPT / Diagram as Code):** Dán trực tiếp để render phong cách kiến trúc cloud chuyên nghiệp

Lưu 2 file xuất ra đúng tên:
1. `Báo cáo/latex/graphics/placeholders/architecture.png`
2. `Báo cáo/latex/graphics/placeholders/data-simulation.png`

---

## Hình 2.1 — Sơ đồ kiến trúc Lambda của StreamRec (`architecture.png`)

```mermaid
flowchart LR
  subgraph Sources["1. Clients & Simulators"]
    WEB["TikTok Web UI<br/>(For You · Explore · Studio · Sim · Ops)"]
    TSIM["traffic-simulator<br/>(Replay CSV stream)"]
    USIM["upload-simulator<br/>(5 mp4 / 15s)"]
  end

  subgraph Ingest["2. Ingestion Gateway"]
    API["FastAPI Ingestion<br/>POST /api/events · /api/upload<br/>Completion Rate & Quick-skip"]
  end

  subgraph Bus["3. Apache Kafka (Event Bus)"]
    KU["Topic: events.user"]
    KC["Topics: events.content<br/>& events.extract"]
  end

  subgraph Lambda["4. Lambda Processing"]
    SPEED["Speed Layer<br/>speed-worker<br/>Tumbling 60s · 3s micro-flush"]
    EXT["Extract Worker<br/>FFmpeg / ffprobe<br/>draft → enriched"]
    BATCH["Batch Layer<br/>Spark Batch ALS<br/>spark-master:7077 (180s)"]
  end

  subgraph Stores["5. Storage & Probabilistic DS"]
    REDIS[("Redis 7<br/>user:affinity · hot_tags:1m<br/>Bloom Filter · HyperLogLog 1m")]
    PG[("PostgreSQL 16<br/>videos · interactions<br/>cf_scores (Top-50 unseen)")]
  end

  subgraph Serve["6. Serving Layer"]
    RANK["FastAPI Hybrid Serving<br/>α·Content + β·CF + γ·Realtime<br/>+ Cold-start Boost + Diversity"]
  end

  WEB & TSIM & USIM --> API
  API -->|"Master log"| PG
  API -->|"Bloom SETBIT / HLL PFADD"| REDIS
  API -->|"Publish"| KU
  API -->|"Publish"| KC
  KU --> SPEED --> REDIS
  KC --> EXT --> PG
  PG --> BATCH -->|"Overwrite cf_scores"| PG
  REDIS & PG --> RANK
  RANK -.->|"Batch Prefetch 20 vid + WS /ws/metrics"| WEB
```

---

## Hình 3.1 — Luồng chuẩn bị dữ liệu & Mô phỏng Runtime (`data-simulation.png`)

```mermaid
flowchart LR
  SRC["Tsinghua ShortVideo (CC BY 4.0)<br/>1,000 mp4 (~11.3 GB)<br/>~794,000 interactions CSV"]

  subgraph Prep["Tiền xử lý & Tách 70/30"]
    FILT["Lọc theo 1,000 mp4<br/>interaction_filtered.csv<br/>231,258 dòng<br/>(1,095 users · 851 authors)"]
    VSPLIT["Chia kho Video 70/30<br/>700 mp4 catalog ấm<br/>300 mp4 upload_pool"]
  end

  subgraph Split["Tập dữ liệu tách theo thời gian"]
    HIST["interactions_history.csv<br/>70% sớm (161,886 dòng)<br/>Seed DB + Batch CF"]
    STRM["interactions_stream.csv<br/>30% muộn (69,372 dòng)<br/>Replay stream"]
    POOL["Cold-start Pool<br/>300 mp4 + pool_meta.json<br/>+ 1 user [cold-start]"]
  end

  subgraph Runtime["Mô phỏng Runtime (Docker Compose)"]
    ETL["ETL + Spark Batch ALS<br/>Seed 700 vid + history<br/>Ghi video_id_map.json"]
    TBOT["traffic-simulator<br/>Replay CSV theo timeline<br/>POST /api/events → Kafka"]
    UBOT["upload-simulator<br/>POST 5 vid / 15s → /api/upload<br/>draft → extracting → enriched"]
  end

  SRC --> FILT
  SRC --> VSPLIT
  FILT -->|"70%"| HIST
  FILT -->|"30%"| STRM
  VSPLIT --> POOL
  HIST --> ETL
  STRM --> TBOT
  POOL --> UBOT
  ETL -.->|"pid → v_*"| TBOT
```
