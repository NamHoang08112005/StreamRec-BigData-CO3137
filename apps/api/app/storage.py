import shutil
from pathlib import Path

from app.config import settings

MEDIA_ROOT = Path(settings.media_root)


def ensure_bucket() -> None:
    MEDIA_ROOT.mkdir(parents=True, exist_ok=True)
    (MEDIA_ROOT / "uploads").mkdir(parents=True, exist_ok=True)
    (MEDIA_ROOT / "thumbs").mkdir(parents=True, exist_ok=True)
    (MEDIA_ROOT / "seed").mkdir(parents=True, exist_ok=True)


def upload_bytes(object_key: str, data: bytes, content_type: str) -> str:
    path = MEDIA_ROOT / object_key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    if path.stat().st_size != len(data) or path.stat().st_size == 0:
        path.unlink(missing_ok=True)
        raise OSError(f"Incomplete media write for {object_key}")
    return media_url(object_key)


def media_file_ok(object_key: str | None, min_bytes: int = 10_000) -> bool:
    if not object_key:
        return False
    path = MEDIA_ROOT / object_key
    try:
        return path.is_file() and path.stat().st_size >= min_bytes
    except OSError:
        return False


def local_path(object_key: str) -> Path:
    return MEDIA_ROOT / object_key


def copy_media(src_key: str, dest_key: str) -> str:
    src = local_path(src_key)
    dest = local_path(dest_key)
    if not src.is_file() or src.stat().st_size < 10_000:
        raise FileNotFoundError(f"Source media missing/too small: {src_key}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    if dest.stat().st_size < 10_000:
        dest.unlink(missing_ok=True)
        raise OSError(f"Incomplete media copy for {dest_key}")
    return media_url(dest_key)


def delete_media(object_key: str | None) -> None:
    if not object_key:
        return
    path = local_path(object_key)
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def media_url(object_key: str | None) -> str | None:
    if not object_key:
        return None
    base = settings.media_public_base.rstrip("/")
    return f"{base}/{object_key.lstrip('/')}"
