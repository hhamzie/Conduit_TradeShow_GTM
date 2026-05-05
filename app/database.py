from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings


settings = get_settings()

connect_args: dict[str, object] = {}
if settings.database_url.startswith("sqlite"):
    connect_args["check_same_thread"] = False

engine = create_engine(settings.database_url, future=True, pool_pre_ping=True, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
def init_db() -> None:
    from app import models  # noqa: F401

    settings.export_dir.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)
    _migrate_existing_schema()


def _migrate_existing_schema() -> None:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if "shows" in existing_tables:
        _ensure_show_columns(inspector)


def _ensure_show_columns(inspector) -> None:
    existing_columns = {column["name"] for column in inspector.get_columns("shows")}
    is_postgres = engine.dialect.name == "postgresql"
    datetime_type = "TIMESTAMP" if is_postgres else "DATETIME"
    required_columns = {
        "enriched_export_path": "TEXT NOT NULL DEFAULT ''",
        "smartlead_ready_export_path": "TEXT NOT NULL DEFAULT ''",
        "clay_table_id": "TEXT NOT NULL DEFAULT ''",
        "clay_table_name": "TEXT NOT NULL DEFAULT ''",
        "clay_table_url": "TEXT NOT NULL DEFAULT ''",
        "clay_last_polled_at": datetime_type,
        "clay_last_imported_at": datetime_type,
        "clay_total_rows": "INTEGER NOT NULL DEFAULT 0",
        "clay_ready_rows": "INTEGER NOT NULL DEFAULT 0",
        "clay_failed_rows": "INTEGER NOT NULL DEFAULT 0",
        "clay_skipped_rows": "INTEGER NOT NULL DEFAULT 0",
        "smartlead_campaign_id": "INTEGER",
        "smartlead_campaign_name": "TEXT NOT NULL DEFAULT ''",
        "smartlead_imported_at": datetime_type,
        "smartlead_imported_rows": "INTEGER NOT NULL DEFAULT 0",
    }
    missing = {
        name: definition
        for name, definition in required_columns.items()
        if name not in existing_columns
    }
    if not missing:
        return

    with engine.begin() as connection:
        for name, definition in missing.items():
            connection.execute(text(f"ALTER TABLE shows ADD COLUMN {name} {definition}"))
