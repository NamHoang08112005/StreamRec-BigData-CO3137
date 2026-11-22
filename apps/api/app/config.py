from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg://recsys:recsys@localhost:5432/recsys"
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap: str = "localhost:9092"
    media_root: str = "/data/media"
    media_public_base: str = "http://localhost:8000/media"
    cors_origins: str = "http://localhost:5173,http://localhost:3000"
    batch_cf_interval_sec: int = 90
    auto_upload_interval_sec: int = 90
    auto_upload_keep: int = 60

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


settings = Settings()
