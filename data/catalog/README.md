# Catalog (warm / already-known assets)

Video **đã có trong hệ thống** (load bằng ETL ShortVideo), dùng cho Feed / Batch CF / demo Customer.

Nguồn chính hiện tại:

```text
../raw/shortvideo/raw_file/*.mp4
```

Load vào API:

```bash
python scripts/data/reset_and_load_shortvideo.py --root data/raw/shortvideo --api http://localhost:8000
```

Không dùng folder này cho auto-upload live. Auto-upload đọc `data/coldstart/upload_pool/`.
