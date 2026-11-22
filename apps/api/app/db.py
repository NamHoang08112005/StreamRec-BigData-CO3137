from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.config import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def fetch_one(db, sql: str, params: dict | None = None):
    return db.execute(text(sql), params or {}).mappings().first()


def fetch_all(db, sql: str, params: dict | None = None):
    return list(db.execute(text(sql), params or {}).mappings().all())
