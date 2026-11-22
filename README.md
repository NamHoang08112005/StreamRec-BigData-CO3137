# StreamRec — Hệ thống gợi ý video ngắn (Big Data CO3137)

**StreamRec** là hệ thống mô phỏng nền tảng gợi ý video ngắn kiểu TikTok theo mô hình **Providers & Customers** (**Video → Metadata → Hybrid Recommendation**), xây dựng trên **kiến trúc Lambda** với bộ dữ liệu thật **Tsinghua ShortVideo**.

- **Công nghệ chính:** Apache Kafka, Apache Spark (Batch ALS), PostgreSQL, Redis (Speed Layer, Bloom Filter, HyperLogLog), FFmpeg, FastAPI, React (Vite) và Docker Compose.
- **Báo cáo chi tiết:** [Báo cáo/latex/main.pdf](Báo%20cáo/latex/main.pdf)

---

## Yêu cầu hệ thống

- **Docker Desktop** (khuyến nghị cấp ≥ 6 GB RAM)
- **Python 3.10+** (dùng để chạy script tải & nạp dữ liệu)
- Dung lượng ổ đĩa trống khoảng **12 GB** (nếu tải đầy đủ 1.000 video thật)

---

## Hướng dẫn cài đặt và chạy

### Bước 1: Khởi động hệ thống bằng Docker Compose

```bash
git clone https://github.com/NamHoang08112005/StreamRec-BigData-CO3137.git
cd StreamRec-BigData-CO3137
docker compose up --build -d
```

Kiểm tra các container đang chạy:

```bash
docker compose ps
```

### Bước 2: Cài đặt thư viện Python cho script dữ liệu

```bash
pip install -r scripts/data/requirements.txt
```

### Bước 3: Tải và chuẩn bị dữ liệu thật (Tsinghua ShortVideo)

```bash
# 1. Tải tối đa 1.000 video .mp4 (<= 20 MB/video)
python scripts/data/download_shortvideo_fast.py --out data/raw/shortvideo --max-videos 1000 --workers 3 --max-mb 20.0

# 2. Lọc tương tác theo các video đã tải & tách 70/30 theo thời gian
python scripts/data/filter_interactions_by_videos.py --root data/raw/shortvideo
python scripts/data/split_interactions.py --root data/raw/shortvideo --source interaction_filtered.csv

# 3. Tách 30% video làm pool upload cold-start
python scripts/data/split_coldstart_pool.py --fraction 0.30 --seed 42
```

### Bước 4: Nạp dữ liệu vào hệ thống (ETL)

```bash
# Nạp 700 video ấm + lịch sử 70% và tạo video_id_map.json
python scripts/data/etl_shortvideo.py --purge --root data/raw/shortvideo --max-videos 700

# Khởi tạo 1 tài khoản user cold-start (0 lịch sử)
python scripts/data/reset_coldstart_user.py --api http://localhost:8000
```

### Bước 5: Truy cập hệ thống

- **Giao diện Web (TikTok UI & Ops Dashboard):** http://localhost:5173
- **API Healthcheck:** http://localhost:8000/health
- **Spark Master Web UI (Batch Layer):** http://localhost:8080

---

## Chạy đánh giá định lượng Offline (tùy chọn)

```bash
python scripts/data/eval_offline.py
```

---

## Dừng hệ thống

```bash
# Dừng container (giữ nguyên dữ liệu)
docker compose down

# Dừng và xóa toàn bộ volume dữ liệu (nếu muốn reset sạch)
docker compose down -v
```

---

## Nhóm thực hiện (HCMUT — CO3137)

- Nguyễn Thiện Minh — `2312097`
- Phan Nguyễn Hoàng Nam — `2312183`
- Nguyễn Đặng Thế Hoàng — `2311051`
- Phan Đức Nhã — `2312410`
