from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
load_dotenv(BASE_DIR / ".env")


def normalize_database_url(raw_url: str) -> str:
    if raw_url.startswith("postgres://"):
        return "postgresql+psycopg://" + raw_url[len("postgres://"):]
    if raw_url.startswith("postgresql://") and not raw_url.startswith("postgresql+psycopg://"):
        return "postgresql+psycopg://" + raw_url[len("postgresql://"):]
    return raw_url


@dataclass(frozen=True)
class Settings:
    app_name: str
    database_url: str
    export_dir: Path
    default_run_offset_days: int
    worker_poll_seconds: int
    default_scraper_workers: int
    default_max_pages: int
    default_sample_size: int
    default_browser_mode: str
    default_browser_timeout_ms: int
    session_secret: str
    dashboard_username: str
    dashboard_password: str
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    smtp_use_tls: bool
    notify_from_email: str
    notify_to_emails: tuple[str, ...]
    clay_api_key: str
    clay_input_table_id: str
    clay_webhook_url: str
    clay_webhook_auth_header: str
    clay_webhook_auth_value: str
    clay_session_cookie: str
    heyreach_api_key: str
    smartlead_api_key: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    database_url = normalize_database_url(
        os.getenv("DATABASE_URL", f"sqlite:///{(DATA_DIR / 'trade_show_app.db').resolve()}")
    )
    export_dir = Path(os.getenv("EXPORT_DIR", str((DATA_DIR / "exports").resolve()))).expanduser()
    notify_to_emails = tuple(
        part.strip()
        for part in os.getenv("NOTIFY_TO_EMAILS", "").split(",")
        if part.strip()
    )
    return Settings(
        app_name=os.getenv("APP_NAME", "Trade Show Outbound"),
        database_url=database_url,
        export_dir=export_dir,
        default_run_offset_days=int(os.getenv("DEFAULT_RUN_OFFSET_DAYS", "14")),
        worker_poll_seconds=int(os.getenv("WORKER_POLL_SECONDS", "30")),
        default_scraper_workers=int(os.getenv("DEFAULT_SCRAPER_WORKERS", "8")),
        default_max_pages=int(os.getenv("DEFAULT_MAX_PAGES", "250")),
        default_sample_size=int(os.getenv("DEFAULT_SAMPLE_SIZE", "3")),
        default_browser_mode=os.getenv("DEFAULT_BROWSER_MODE", "auto"),
        default_browser_timeout_ms=int(os.getenv("DEFAULT_BROWSER_TIMEOUT_MS", "25000")),
        session_secret=os.getenv("SESSION_SECRET", "dev-session-secret-change-me"),
        dashboard_username=os.getenv("DASHBOARD_USERNAME", "admin"),
        dashboard_password=os.getenv("DASHBOARD_PASSWORD", "change-me-now"),
        smtp_host=os.getenv("SMTP_HOST", ""),
        smtp_port=int(os.getenv("SMTP_PORT", "587")),
        smtp_username=os.getenv("SMTP_USERNAME", ""),
        smtp_password=os.getenv("SMTP_PASSWORD", ""),
        smtp_use_tls=os.getenv("SMTP_USE_TLS", "true").lower() not in {"0", "false", "no"},
        notify_from_email=os.getenv("NOTIFY_FROM_EMAIL", ""),
        notify_to_emails=notify_to_emails,
        clay_api_key=os.getenv("CLAY_API_KEY", ""),
        clay_input_table_id=os.getenv("CLAY_INPUT_TABLE_ID", ""),
        clay_webhook_url=os.getenv("CLAY_WEBHOOK_URL", ""),
        clay_webhook_auth_header=os.getenv("CLAY_WEBHOOK_AUTH_HEADER", ""),
        clay_webhook_auth_value=os.getenv("CLAY_WEBHOOK_AUTH_VALUE", ""),
        clay_session_cookie=os.getenv("CLAY_SESSION_COOKIE", ""),
        heyreach_api_key=os.getenv("HEYREACH_API_KEY", ""),
        smartlead_api_key=os.getenv("SMARTLEAD_API_KEY", ""),
    )
