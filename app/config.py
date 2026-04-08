from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
load_dotenv(BASE_DIR / ".env")


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
    twilio_account_sid: str
    twilio_auth_token: str
    twilio_from_number: str
    notify_to_numbers: tuple[str, ...]
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
    database_url = os.getenv("DATABASE_URL", f"sqlite:///{(DATA_DIR / 'trade_show_app.db').resolve()}")
    export_dir = Path(os.getenv("EXPORT_DIR", str((DATA_DIR / "exports").resolve()))).expanduser()
    notify_to_numbers = tuple(
        part.strip()
        for part in os.getenv("NOTIFY_TO_NUMBERS", "").split(",")
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
        twilio_account_sid=os.getenv("TWILIO_ACCOUNT_SID", ""),
        twilio_auth_token=os.getenv("TWILIO_AUTH_TOKEN", ""),
        twilio_from_number=os.getenv("TWILIO_FROM_NUMBER", ""),
        notify_to_numbers=notify_to_numbers,
        clay_api_key=os.getenv("CLAY_API_KEY", ""),
        clay_input_table_id=os.getenv("CLAY_INPUT_TABLE_ID", ""),
        clay_webhook_url=os.getenv("CLAY_WEBHOOK_URL", ""),
        clay_webhook_auth_header=os.getenv("CLAY_WEBHOOK_AUTH_HEADER", ""),
        clay_webhook_auth_value=os.getenv("CLAY_WEBHOOK_AUTH_VALUE", ""),
        clay_session_cookie=os.getenv("CLAY_SESSION_COOKIE", ""),
        heyreach_api_key=os.getenv("HEYREACH_API_KEY", ""),
        smartlead_api_key=os.getenv("SMARTLEAD_API_KEY", ""),
    )
