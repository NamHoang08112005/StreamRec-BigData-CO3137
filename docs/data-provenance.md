# Data provenance

## Policy

- **Không crawl** TikTok / YouTube / Kuaishou production.
- Mọi item trên Feed phải có **file video `.mp4` thật** trên media volume.
- Demo chính dùng **Tsinghua ShortVideo** (mp4 + interaction + metadata).
- Không dùng clip màu ffmpeg (`generate_demo_clips.py`) cho buổi bảo vệ.

## Two data folders (catalog vs cold-start)

| Folder | Role |
|--------|------|
| `data/raw/shortvideo/` (+ catalog sau ETL) | Video **đã / sẽ load** vào hệ thống — warm catalog |
| `data/raw/shortvideo/interactions_history.csv` | ~70% interaction sớm → seed / Batch CF |
| `data/raw/shortvideo/interactions_stream.csv` | ~30% interaction muộn → traffic **replay thật** |
| `data/raw/shortvideo/video_id_map.json` | `pid` → `v_…` (ghi sau ETL; bot dùng để map event) |
| `data/coldstart/upload_pool/` | Video **chưa có** — `upload-simulator` POST multipart `/api/upload` mỗi ~90s × 5 vid |

Thêm `.mp4` vào `upload_pool/` là simulator sẽ đăng như Provider Studio (draft → extracting → enriched).

Chi tiết sơ đồ / 28 cột CSV: [`docs/data-simulation.md`](data-simulation.md).

## Primary: Tsinghua ShortVideo dataset

- Repo: https://github.com/tsinghua-fib-lab/ShortVideo_dataset
- Paper: https://arxiv.org/abs/2502.05922
- License: CC BY 4.0
- Sample download (theo README repo):  
  http://fi.ee.tsinghua.edu.cn/datasets/short-video-dataset  
  (Username / Password xem trên GitHub README của dataset)

### Layout sau khi tải / tách

```text
data/raw/shortvideo/
  raw_file/<pid>.mp4               # ~700 catalog ấm (sau split 30% cold)
  interaction_sampled.csv          # CSV gốc (~794k rows, 28 cột) — giữ nguyên
  interaction_filtered.csv         # chỉ row có pid đã tải (~1000 mp4)
  interactions_history.csv         # ~70% sớm trên tập đã lọc
  interactions_stream.csv          # ~30% muộn → traffic replay
  video_id_map.json                # sau ETL
  categories_cn_en.csv             # optional
  title_en/<pid>.txt               # optional
data/coldstart/upload_pool/        # ~300 mp4 cold-start (move từ raw_file)
```

Pipeline disk:

```bash
python scripts/data/download_shortvideo_fast.py --out data/raw/shortvideo --max-videos 1000 --workers 3 --max-mb 20.0
python scripts/data/filter_interactions_by_videos.py --root data/raw/shortvideo
python scripts/data/split_interactions.py --root data/raw/shortvideo --source interaction_filtered.csv
python scripts/data/split_coldstart_pool.py --fraction 0.30 --seed 42
```

### Load vào hệ thống

```bash
# stack đang chạy
pip install httpx
python scripts/data/etl_shortvideo.py --purge --root data/raw/shortvideo --max-videos 700
python scripts/data/reset_coldstart_user.py --api http://localhost:8000
```

Hoặc wrapper:

```bash
python scripts/data/reset_and_load_shortvideo.py --root data/raw/shortvideo --api http://localhost:8000
```

Khuyến nghị demo: **~1000 mp4** (≈70% catalog / 30% `upload_pool`), seed interactions từ history đã lọc.

Script sẽ:

1. (với `--purge`) xóa catalog seed cũ / clip ảo  
2. Chỉ ingest video **có file mp4**  
3. Map title / tag / category từ dataset khi có  
4. Seed interactions từ **history** CSV → Kafka + Postgres  
5. Ghi `video_id_map.json` và chạy batch CF  

Traffic-simulator đọc **stream** CSV + map → `POST /api/events` (không tạo `bot_*` giả).

## Optional fallbacks (không bắt buộc)

| Source | Role |
|--------|------|
| Pexels API | Thêm mp4 license rõ nếu ShortVideo chưa tải kịp |
| KuaiRec | Phân bố tương tác bổ sung — không thay catalog thiếu mp4 |
| `generate_demo_clips.py` | **Deprecated** — chỉ emergency offline |

## Citation (báo cáo)

- ShortVideo dataset (arXiv:2502.05922), CC BY 4.0  
- Architecture: Lambda, Kafka, Spark Streaming (CO3137)  
