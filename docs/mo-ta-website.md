# Mô tả website StreamRec

StreamRec là website demo gợi ý video ngắn cho môn Big Data. Người xem mở một feed kiểu TikTok. Phía sau, lượt xem của nhiều người được ghi lại, xử lý, rồi dùng để xếp thứ tự video lần mở feed sau.

Ba địa chỉ khi stack Docker đang chạy:

| Việc | Địa chỉ |
|------|---------|
| Website | http://localhost:5173 |
| API | http://localhost:8000 |
| Spark UI | http://localhost:8080 |

Web (nginx) đứng cổng 5173 và chuyển `/api`, `/media`, `/ws` sang API. Trình duyệt không gọi thẳng cổng 8000.

---

## 1. Người dùng thấy gì

Router hiện gắn ba trang. File `UploadPage`, `ProfilePage`, `StudioPage`, `CreatorPage` còn trong source, API upload và follow cũng còn, nhưng thanh điều hướng không mở các trang đó. Upload video mới do `upload-simulator` gọi API.

### For You — `/`

Trang mặc định. Một video chiếm màn hình, tự phát (bắt đầu ở trạng thái mute).

- **Demo as:** chọn `user_id`. User đánh dấu `[cold-start]` có 0 lịch sử. User khác hiện số event đã có. Lựa chọn lưu trong `localStorage` khóa `streamrec_user`. Lần đầu vào, nếu chưa chọn, web tự tạo id dạng `viewer_xxxxxx`.
- **Reload:** gọi lại feed, xếp hạng từ đầu.
- Vuốt, lăn chuột, hoặc phím lên/xuống để chuyển video.
- Nút **thích**, **bỏ qua**, **chia sẻ**, và **Why**.
- Dưới video: tên creator, tiêu đề, tag. Tag nhiễu của pipeline (`short_form`, `vertical`, …) bị ẩn.
- Mở một video gửi event `view`. Chuyển video gửi event `watch` kèm số giây đã xem. Thích / bỏ qua / chia sẻ gửi `like` / `skip` / `share`.

**Why** hiện ba số đã dùng để chấm video đang phát, kèm trọng số:

- `content` — video có cùng chủ đề với lịch sử của user này không
- `cf_batch` — video có nằm trong top 30 collaborative filtering đã tính sẵn không
- `realtime` — tag của video có trùng với những gì user vừa tương tác không
- `score` — điểm tổng sau khi nhân trọng số

Ba số này tính lúc gọi feed, không tính liên tục trong lúc video đang phát. Muốn thấy thứ tự mới thì bấm Reload.

### Trending — `/trending`

Top 5 video `enriched`, xếp theo độ nóng trên toàn bộ bảng tương tác. Mỗi dòng có hạng, ảnh thumbnail, tiêu đề, creator, heat, số like, số view. Bấm một dòng để phát. Reload lấy lại bảng xếp hạng. Trang này không phải feed cá nhân: mọi người xem cùng một top nếu dữ liệu chưa đổi.

Heat của một video:

| Event | Điểm cộng vào heat |
|-------|-------------------|
| share | +4 |
| like | +3 |
| watch | +1.5 |
| view | +0.5 |
| skip | −0.5 |

### Dashboard — `/dashboard`

Trang vận hành, tách khỏi giao diện xem video. Có năm đoạn, đọc từ trên xuống:

1. **Pipeline.** Upload → Extract (số draft / extracting / enriched) → Catalog (số video, số category) → công thức hybrid → For You.
2. **Streaming.** `events/sec` trên cửa sổ 30 giây, và khoảng 30 event mới nhất (giờ, topic, loại, user, video).
3. **Lambda.** Ba cột Speed (số hot tag), Batch (số cặp CF, lần chạy gần nhất, nút **Run batch CF now**), Serving (α β γ). Thanh trượt chỉnh trọng số rồi **Apply weights**. Đám mây hot tag và số event theo loại.
4. **4V.** Volume (số interaction, số video, tổng event), Velocity (`events/sec`), Variety (số category, số loại event), Veracity (enriched / extracting / draft).
5. **Chứng minh.** Các bước kéo γ lên, sang For You like vài video cùng tag, mở Why, Reload.

Số trên Dashboard tới qua `GET /api/metrics` mỗi 4 giây và qua WebSocket `/ws/metrics`.

---

## 2. Dữ liệu

Nguồn là dataset Tsinghua ShortVideo (CC BY 4.0). Không crawl TikTok hay Kuaishou. Mọi video trên feed phải có file `.mp4` thật.

Số liệu đã chốt trên đĩa:

| Kho | Số lượng | Vai trò |
|-----|----------|---------|
| mp4 đã tải | 1000 (~11.3 GB) | Toàn bộ video vật lý |
| Catalog ấm | 700 | Đã nạp vào Postgres, phát được ngay |
| `upload_pool` | 300 | Chưa có trong catalog, simulator đăng dần |
| CSV gốc | ~794.000 dòng, 28 cột | Giữ nguyên, không sửa |
| CSV đã lọc | 231.258 dòng | Chỉ những `pid` có file mp4 |
| History | 161.886 dòng (70% sớm) | Seed tương tác và batch CF |
| Stream | 69.372 dòng (30% muộn) | Bot phát lại thành event |

Cắt 70/30 theo `exposed_time`. Mốc đã dùng: `1663653505`. Chia video 700/300 bằng `split_coldstart_pool.py`, seed 42.

### File trên đĩa

```text
data/raw/shortvideo/
  raw_file/<pid>.mp4
  interaction_sampled.csv
  interaction_filtered.csv
  interactions_history.csv
  interactions_stream.csv
  video_id_map.json          # pid → v_…, có sau ETL
  categories_cn_en.csv
  title_en/<pid>.txt
data/coldstart/
  upload_pool/*.mp4
  pool_meta.json             # title, category, tag cho video sắp upload
```

### Một dòng CSV thành gì

| Cột dataset | Trong hệ thống |
|-------------|----------------|
| `user_id` | `sv_<user_id>` |
| `pid` | `video_id_map[pid]` → `v_…` |
| `author_id` | `creator_id` |
| `title`, `tag_name`, `category` | tiêu đề, tag, category của video |
| `watch_time` | số giây xem |
| `cvm_like` | event `like` |
| `hate` | event `skip` |
| `forward` hoặc `collect` | event `share` |
| `click` | event `view` |
| không cờ nào ở trên | event `watch` |

`gender`, `age`, `author_fans_count`, thành phố, giá, và các cột ngữ cảnh khác không tham gia chấm điểm.

Tag giả đã bị gỡ khỏi catalog và khỏi UI: `shortvideo`, `auto_upload`, `coldstart`, cùng các token kỹ thuật chỉ dùng nội bộ khi xếp hạng (`short_form`, `vertical`, `landscape`, `hd`, …).

### Nạp vào hệ thống

```bash
python scripts/data/download_shortvideo_fast.py --out data/raw/shortvideo --max-videos 1000 --workers 3 --max-mb 20.0
python scripts/data/filter_interactions_by_videos.py --root data/raw/shortvideo
python scripts/data/split_interactions.py --root data/raw/shortvideo --source interaction_filtered.csv
python scripts/data/split_coldstart_pool.py --fraction 0.30 --seed 42
python scripts/data/etl_shortvideo.py --purge --root data/raw/shortvideo --max-videos 700
python scripts/data/reset_coldstart_user.py --api http://localhost:8000
```

ETL `--purge` xóa catalog cũ, đưa 700 video có mp4 vào Postgres, seed interaction từ history, ghi `video_id_map.json`, rồi chạy batch CF. `reset_coldstart_user.py` xóa interaction, điểm CF và sổ tag Redis của một user demo để For You của người đó bắt đầu từ 0 lịch sử.

---

## 3. Các thành phần đang chạy

`docker compose up --build -d` bật:

| Service | Việc |
|---------|------|
| `postgres` | Catalog, interaction, điểm CF, trọng số. Cổng 5432, user/db `recsys`. |
| `redis` | Sổ tag theo user, hot tag, đếm event, tail, user cold-start. Cổng 6379. |
| `kafka` | Bus sự kiện. Cổng 9092. Một node, plaintext. |
| `kafka-init` | Tạo topic rồi thoát. |
| `api` | FastAPI. Nhận event, xếp feed, phục vụ file media. Cổng 8000. |
| `web` | React build, nginx. Cổng 5173. |
| `extract-worker` | Đọc Kafka, ffprobe/ffmpeg, ghi metadata video. |
| `speed-worker` | Gom event 60 giây, cộng vào Redis. |
| `traffic-simulator` | Phát lại `interactions_stream.csv` vào `/api/events`. |
| `upload-simulator` | Mỗi 90 giây đăng 5 video từ `upload_pool`. |
| `spark-master` / `spark-worker` | Spark UI cổng 8080. Job không tự chạy. |

Profile `spark-jobs` (chạy tay) mới submit:

- `spark-batch` — ALS trong `jobs/spark/batch_cf.py`, ghi `cf_scores`.
- `spark-speed` — Structured Streaming trong `jobs/spark/speed_stream.py`.

Demo hằng ngày dùng batch SVD ngay trong API (mỗi 90 giây) và `speed-worker`, không cần bật profile Spark.

Topic Kafka:

| Topic | Ai ghi | Ai đọc |
|-------|--------|--------|
| `events.user` | API khi có tương tác | speed-worker, và Spark speed nếu bật |
| `events.content` | API khi upload | — |
| `events.extract` | API khi upload | extract-worker |
| `features.user`, `features.item` | Được tạo sẵn | Đường chạy hiện tại không ghi |

Media nằm trên volume Docker `/data/media`, API phục vụ dưới `/media/...`. Không dùng MinIO.

---

## 4. Một lượt tương tác đi những đâu

Người bấm trên web và bot dùng cùng `POST /api/events`:

```json
{
  "user_id": "sv_7313",
  "video_id": "v_…",
  "event_type": "watch",
  "watch_time": 12.5
}
```

API làm bốn việc rồi trả `{"status":"ok"}`. Request này không xếp lại feed.

1. **Ghi Postgres** bảng `interactions`. Trọng số:

   | Event | Weight |
   |-------|--------|
   | view | 0.5 |
   | watch | 1.0, hoặc `min(5, 0.5 + watch_time/10)` nếu có thời lượng |
   | like | 3.0 |
   | skip | −0.5 |
   | share | 4.0 |

2. **Cộng sổ tag trên Redis** `user:affinity:{user_id}`. Like, share, watch, view cộng +1 vào từng tag và category của video. Skip cộng −0.4. Sổ hết hạn sau 24 giờ. Tag được cộng dương còn làm tăng `metrics:hot_tags`.

3. **Đẩy bản sao lên Kafka** `events.user` (user, video, loại, trọng số, tag).

4. **Đếm metric:** tổng event, event theo loại, bucket từng giây để tính `events/sec`, và danh sách 100 event mới nhất để Dashboard vẽ.

`speed-worker` đọc Kafka, gom theo cửa sổ tumbling 60 giây, rồi cộng dồn (user, tag) vào đúng hash Redis ở bước 2. Bước 2 làm feed đổi được ngay ở lần Reload. Bước của worker là nhịp gom theo phút của lớp speed.

Bot (`traffic-simulator`):

- Chỉ gửi dòng có `pid` nằm trong `video_id_map.json`.
- Đi theo `exposed_time`. Khoảng cách thời gian gốc chia cho `SPEEDUP=3600`. Nghỉ tối đa `MAX_GAP_SLEEP=0.05` giây. Trần `EVENTS_PER_SEC=200`. Gửi song song `CONCURRENCY=12`.
- `LOOP_STREAM=1` nên hết file thì phát lại.
- Không tạo user `bot_*`. User trong event là `sv_<id>` của dataset.

---

## 5. Feed được chấm thế nào

`GET /api/feed/{user_id}?limit=40` không đọc Kafka. Hàm `rank_feed` trong `apps/api/app/recommend.py`:

1. Lấy tối đa 500 video còn file media, trạng thái `draft` / `extracting` / `enriched` / `published`.
2. Đọc điểm CF của user từ `cf_scores`, chia cho điểm cao nhất của user đó. Video không có dòng thì điểm CF = 0.
3. Đọc sổ tag Redis của user.
4. Dựng hồ sơ content từ 100 interaction gần nhất (tag, category; like và share được tính thêm một lần) cộng các tag đang có điểm trên Redis.
5. Với từng video tính:

```text
score = α·content + β·cf_batch + γ·realtime + ε
```

Mặc định α = 0.35, β = 0.35, γ = 0.30. Tổng được chuẩn về 1 khi lưu. ε là nhiễu nhỏ khoảng 0.02 theo `video_id`, để hai video sát điểm không dính một thứ tự mãi.

- **Content** là cosine giữa vector tag của user và vector tag của video. User chưa có hồ sơ nhận 0.15.
- **CF** là điểm SVD đã lưu. Xem mục 6.
- **Realtime** = tổng điểm affinity của tag video này, chia cho tổng affinity của user, nhân với số tag trùng và dương, trần 1.

Kết quả sắp giảm dần, cắt theo `limit`, mỗi item kèm `explain` để Why vẽ.

Đổi trọng số: Dashboard gọi `PUT /api/weights`. Lần feed sau dùng trọng số mới. Feed đang mở không tự nhảy.

---

## 6. Batch CF

Mỗi 90 giây (`BATCH_CF_INTERVAL_SEC`), API đọc mọi interaction có `weight > 0`, cộng thành ma trận user × video, phân rã SVD hạng `k ≤ 8`, rồi với mỗi user chỉ giữ 30 video điểm dương cao nhất vào `cf_scores`. Bảng bị xóa và ghi lại mỗi lần chạy. Nút **Run batch CF now** và `POST /api/admin/run-batch-cf` làm cùng việc.

Video ngoài top 30 có `cf_batch = 0` trên Why. Số 0 có nghĩa là video không nằm trong danh sách đã lưu, không có nghĩa là batch đang tắt.

Job Spark ALS là đường khác, cùng bảng `cf_scores`, chỉ chạy khi gọi profile `spark-jobs`.

---

## 7. Video mới và extract

`upload-simulator` mỗi 90 giây lấy 5 file trong `upload_pool`, đọc title/category/tag từ `pool_meta.json`, POST multipart `/api/upload` với `creator_id = creator_auto`.

API lưu file vào media volume, tạo dòng `videos` trạng thái `draft` rồi `extracting`, gửi `events.content` và `events.extract`.

`extract-worker` chạy ffprobe: duration, width, height, fps. ffmpeg cắt một khung ở giây thứ 1 làm thumbnail. Tag kỹ thuật (`short_form` / `mid_form` / `long_form`, `vertical` / `landscape` / `square`, `hd`, `high_fps`) ghi vào `enriched_tags`. Trạng thái chuyển `enriched`. Những tag này tham gia xếp hạng, không hiện trên caption.

---

## 8. Postgres

| Bảng | Nội dung |
|------|----------|
| `users` | `user_id`, tên, `interest_tags` |
| `videos` | id, creator, title, tags, category, đường media, thumbnail, status, duration, kích thước, fps, `enriched_tags` |
| `interactions` | user, video, event, watch_time, weight, thời điểm |
| `cf_scores` | user, video, điểm SVD, thời điểm cập nhật. Khóa (user, video) |
| `serving_weights` | một dòng α β γ |
| `follows` | user theo dõi creator. API có. UI đang mở không có nút follow |
| `pipeline_stats` | thống kê pipeline dạng JSON |

---

## 9. Redis

| Khóa | Nội dung |
|------|----------|
| `user:affinity:{user_id}` | hash tag → điểm, TTL 24 giờ |
| `metrics:hot_tags` | sorted set tag đang được tương tác |
| `metrics:events` | hash đếm theo loại event |
| `metrics:events:total` | tổng event |
| `metrics:event_tail` | list tối đa 100 event gần nhất |
| `metrics:velocity:{giây}` | đếm event trong một giây, dùng cho events/sec |
| `metrics:batch_cf:last` | JSON lần batch gần nhất |
| `metrics:speed_windows` | số lần speed-worker flush cửa sổ |
| `demo:coldstart_user` | user đang được demo ở trạng thái 0 lịch sử |

---

## 10. API

| Method | Đường dẫn | Việc |
|--------|-----------|------|
| GET | `/health` | Sống |
| GET | `/api/feed/{user_id}` | Feed đã xếp hạng, kèm explain |
| GET | `/api/feed/{user_id}/following` | Feed chỉ video của creator user đang follow |
| GET | `/api/trending` | Top video theo heat, mặc định 5 |
| GET | `/api/demo-users` | User có nhiều interaction, gắn cờ cold-start |
| POST | `/api/events` | Ghi một tương tác |
| GET / PUT | `/api/weights` | Đọc hoặc sửa α β γ |
| GET | `/api/metrics` | Số cho Dashboard |
| WS | `/ws/metrics` | Đẩy metric |
| POST | `/api/upload` | Đăng một video |
| GET | `/api/videos` | Danh sách video, lọc theo creator |
| GET | `/api/creators/{id}` | Trang creator |
| GET | `/api/studio/{id}` | Video của một creator |
| POST | `/api/follow`, `/api/unfollow` | Theo dõi |
| POST | `/api/admin/run-batch-cf` | Chạy SVD ngay |
| POST | `/api/admin/coldstart-user` | Xóa lịch sử một user |
| POST | `/api/admin/scrub-tags` | Gỡ tag nhiễu trên catalog và Redis |
| POST | `/api/admin/purge-catalog` | Xóa catalog |
| POST | `/api/admin/seed-videos`, `seed-interactions` | ETL gọi vào |

File tĩnh: `GET /media/...`.

---

## 11. Frontend

React 18, Vite, React Router 6. Ba route trong `apps/web/src/App.jsx`.

| File | Trang |
|------|-------|
| `pages/FeedPage.jsx` | For You |
| `pages/TrendingPage.jsx` | Trending |
| `pages/DashboardPage.jsx` | Dashboard |
| `api.js` | Mọi lời gọi HTTP. `VITE_API_BASE` rỗng khi chạy sau nginx |
| `styles.css` | Giao diện feed tối và dashboard sáng |

`user_id` đang xem nằm ở `App`, truyền xuống For You. Trending và Dashboard không đổi theo user đang chọn, trừ khi người đó tự tạo event trên feed.

---

## 12. Việc website không làm

- Không tự đổi thứ tự video đang phát. Phải Reload hoặc vào lại For You.
- Không hiện hàng trăm feed cùng lúc. Mỗi lần mở là một `user_id`.
- Không huấn luyện mạng neural trên từng event.
- Spark không nằm trên đường bấm Like. Like đi API → Postgres, Redis, Kafka.
- Follow, studio, upload bằng tay có API và có file React, không có trên thanh menu hiện tại.
