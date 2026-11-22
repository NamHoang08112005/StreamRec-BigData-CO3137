# Danh sách hình ảnh cần bổ sung cho Báo cáo LaTeX

Tất cả các hình hiện đang dùng khung tạm `\placeholderfig{...}` trong `Báo cáo/latex/chapters/main/`. Sau khi có ảnh thật (đúng tên file bên dưới), chỉ cần đổi `\placeholderfig` sang `\includegraphics` rồi chạy lại `build_report.ps1`.

---

## Nhóm 1 — 2 Sơ đồ kiến trúc & dataflow (Đã viết sẵn mã Mermaid tại `SO-DO-MERMAID.md`)

Copy mã Mermaid trong `SO-DO-MERMAID.md` dán vào **Draw.io** / **Mermaid Live (`mermaid.live`)** / **Eraser.io** để xuất PNG chất lượng cao:

| # | Đường dẫn file | Vị trí trong báo cáo | Nội dung |
|---|---|---|---|
| 1 | `graphics/placeholders/architecture.png` | Chương 2 (`02-theory.tex`) | Sơ đồ kiến trúc Lambda của StreamRec |
| 2 | `graphics/placeholders/data-simulation.png` | Chương 3 (`03-implementation.tex`) | Sơ đồ luồng chuẩn bị dữ liệu 70/30 & mô phỏng runtime |

---

## Nhóm 2 — Ảnh chụp màn hình hệ thống chạy thật (Cần bạn chạy Docker Compose để chụp)

> **Lưu ý:** 4 hình này phải chụp sau khi hoàn thiện phần Backend Big Data + dựng `docker compose up --build -d` để số liệu, giao diện và job Spark đều là kết quả chạy thật.

| # | Đường dẫn file | Vị trí trong báo cáo | Cách chụp đúng ý báo cáo |
|---|---|---|---|
| 3 | `graphics/placeholders/feed.png` | Chương 3 (`03-implementation.tex`) | Mở `http://localhost:5173/` (trang **For You**), chọn user `[cold-start]`, Like 2–3 video cùng chủ đề, sau đó bấm nút **`Show Big Data`** ở góc trên phải. Chụp toàn màn hình thấy rõ khung video 9:16 kiểu TikTok bên trái và bảng **For You · Recommendation Inspector** (điểm Content/CF/Realtime, thanh $\alpha/\beta/\gamma$, hồ sơ affinity, Bloom Filter seen, log sự kiện) bên phải. |
| 4 | `graphics/placeholders/upload.png` | Chương 3 (`03-implementation.tex`) | Mở `http://localhost:5173/studio` (**TikTok Studio**), chọn 1 video mẫu + điền Caption/Hashtags để khung điện thoại Preview bên phải hiện rõ, bấm **`Show Big Data`** (hoặc bấm Post) để chụp thấy đồng thời form Upload + Phone Preview + bảng **Studio · Extract Pipeline Live** (`draft` / `extracting` / `enriched` và danh sách video qua `ffprobe`). |
| 5 | `graphics/placeholders/dashboard.png` | Chương 3 (`03-implementation.tex`) | Mở `http://localhost:5173/dashboard` (**System Ops**) khi `traffic-simulator` đang chạy ổn định ~1–2 phút. Chụp thấy rõ 4 thẻ KPI (Velocity ev/s, Volume, **HyperLogLog Unique Users**, Batch CF pairs), sơ đồ **Lambda Architecture Live Dataflow**, biểu đồ throughput theo thời gian thực và bảng Event Tail. |
| 6 | `graphics/placeholders/spark-ui.png` | Chương 4 (`04-results.tex`) | Mở **Spark Master UI** tại `http://localhost:8080` sau khi job `recsys-batch-als` chạy xong ít nhất 1 lần. Chụp màn hình thấy rõ mục **Workers (1 alive)** và bảng **Completed Applications** có dòng `recsys-batch-als` trạng thái `FINISHED`. |
