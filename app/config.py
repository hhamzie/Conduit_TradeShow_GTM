from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
BOOT_DEPLOY_REVISION = f"boot-{uuid4().hex}"
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
    bulk_scraper_workers: int
    default_max_pages: int
    default_sample_size: int
    default_browser_mode: str
    default_browser_timeout_ms: int
    min_scrape_company_count: int
    openai_api_key: str
    trade_show_scan_model: str
    deploy_revision: str
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
    clay_base_url: str
    clay_input_table_id: str
    clay_template_table_id: str
    clay_webhook_url: str
    clay_webhook_auth_header: str
    clay_webhook_auth_value: str
    cultivate_webhook_url: str
    cultivate_webhook_auth_header: str
    cultivate_webhook_auth_value: str
    cultivate_enable_smartlead: bool
    trade_show_ingestion_dir: Path
    pipedrive_api_token: str
    pipedrive_base_url: str
    clay_session_cookie: str
    clay_row_status_column: str
    clay_ready_status_value: str
    clay_failed_status_value: str
    clay_skipped_status_value: str
    trade_show_clay_table_id: str
    trade_show_clay_view_id: str
    trade_show_clay_ready_column: str
    trade_show_clay_ready_value: str
    trade_show_clay_ready_any_value: bool
    trade_show_completed_webhook_secret: str
    heyreach_api_key: str
    smartlead_api_key: str
    smartlead_base_url: str
    smartlead_client_id: str
    smartlead_template_campaign_id: str
    outbound_sender_capacity: int
    outbound_window_weeks: int
    weekly_show_sync_enabled: bool
    weekly_show_sync_source_url: str
    weekly_show_sync_source_path: str
    weekly_show_sync_weekday: int
    weekly_show_sync_hour: int
    weekly_show_sync_timezone: str
    weekly_show_sync_lookahead_days: int
    weekly_show_sync_require_notion: bool
    notion_api_token: str
    notion_database_id: str
    notion_data_source_id: str
    scrape_execution_mode: str
    scrape_due_webhook_url: str
    airtable_token: str
    airtable_base_id: str
    airtable_shows_table_id: str
    airtable_companies_table_id: str
    airtable_contacts_table_id: str
    airtable_campaign_pushes_table_id: str


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
        app_name=os.getenv("APP_NAME", "TradeShowScraper"),
        database_url=database_url,
        export_dir=export_dir,
        default_run_offset_days=int(os.getenv("DEFAULT_RUN_OFFSET_DAYS", "14")),
        worker_poll_seconds=int(os.getenv("WORKER_POLL_SECONDS", "30")),
        default_scraper_workers=int(os.getenv("DEFAULT_SCRAPER_WORKERS", "1")),
        bulk_scraper_workers=int(os.getenv("BULK_SCRAPER_WORKERS", "1")),
        default_max_pages=int(os.getenv("DEFAULT_MAX_PAGES", "250")),
        default_sample_size=int(os.getenv("DEFAULT_SAMPLE_SIZE", "3")),
        default_browser_mode=os.getenv("DEFAULT_BROWSER_MODE", "auto"),
        default_browser_timeout_ms=int(os.getenv("DEFAULT_BROWSER_TIMEOUT_MS", "25000")),
        min_scrape_company_count=max(1, int(os.getenv("MIN_SCRAPE_COMPANY_COUNT", "6"))),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        trade_show_scan_model=os.getenv("TRADE_SHOW_SCAN_MODEL", "gpt-5").strip() or "gpt-5",
        deploy_revision=(
            os.getenv("RENDER_GIT_COMMIT", "").strip()
            or os.getenv("SOURCE_VERSION", "").strip()
            or BOOT_DEPLOY_REVISION
        ),
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
        clay_base_url=os.getenv("CLAY_BASE_URL", "https://api.clay.com/v3").rstrip("/"),
        clay_input_table_id=os.getenv("CLAY_INPUT_TABLE_ID", ""),
        clay_template_table_id=os.getenv("CLAY_TEMPLATE_TABLE_ID", ""),
        clay_webhook_url=os.getenv("CLAY_WEBHOOK_URL", ""),
        clay_webhook_auth_header=os.getenv("CLAY_WEBHOOK_AUTH_HEADER", ""),
        clay_webhook_auth_value=os.getenv("CLAY_WEBHOOK_AUTH_VALUE", ""),
        cultivate_webhook_url=(
            os.getenv("CULTIVATE_WEBHOOK_URL", "").strip()
            or (
                f"{os.getenv('N8N_BASE_URL', '').rstrip('/')}/webhook/cultivate-airtable-loop"
                if os.getenv("N8N_BASE_URL", "").strip()
                else ""
            )
        ),
        cultivate_webhook_auth_header=os.getenv("CULTIVATE_WEBHOOK_AUTH_HEADER", "").strip(),
        cultivate_webhook_auth_value=os.getenv("CULTIVATE_WEBHOOK_AUTH_VALUE", "").strip(),
        cultivate_enable_smartlead=os.getenv("CULTIVATE_ENABLE_SMARTLEAD", "true").lower()
        in {"1", "true", "yes"},
        trade_show_ingestion_dir=Path(
            os.getenv("TRADE_SHOW_INGESTION_DIR", str(Path.home() / "TradeShowIngestion"))
        ).expanduser(),
        pipedrive_api_token=os.getenv("PIPEDRIVE_API_TOKEN", ""),
        pipedrive_base_url=os.getenv("PIPEDRIVE_BASE_URL", "https://api.pipedrive.com/v1").rstrip("/"),
        clay_session_cookie=os.getenv("CLAY_SESSION_COOKIE", ""),
        clay_row_status_column=os.getenv("CLAY_ROW_STATUS_COLUMN", "enriched_status"),
        clay_ready_status_value=os.getenv("CLAY_READY_STATUS_VALUE", "ready"),
        clay_failed_status_value=os.getenv("CLAY_FAILED_STATUS_VALUE", "failed"),
        clay_skipped_status_value=os.getenv("CLAY_SKIPPED_STATUS_VALUE", "skip"),
        trade_show_clay_table_id=os.getenv("TRADE_SHOW_CLAY_TABLE_ID", "").strip(),
        trade_show_clay_view_id=os.getenv("TRADE_SHOW_CLAY_VIEW_ID", "").strip(),
        trade_show_clay_ready_column=os.getenv("TRADE_SHOW_CLAY_READY_COLUMN", "").strip(),
        trade_show_clay_ready_value=os.getenv("TRADE_SHOW_CLAY_READY_VALUE", "").strip(),
        trade_show_clay_ready_any_value=os.getenv("TRADE_SHOW_CLAY_READY_ANY_VALUE", "false").lower()
        in {"1", "true", "yes"},
        trade_show_completed_webhook_secret=os.getenv("TRADE_SHOW_COMPLETED_WEBHOOK_SECRET", "").strip(),
        heyreach_api_key=os.getenv("HEYREACH_API_KEY", ""),
        smartlead_api_key=os.getenv("SMARTLEAD_API_KEY", ""),
        smartlead_base_url=os.getenv("SMARTLEAD_BASE_URL", "https://server.smartlead.ai/api/v1").rstrip("/"),
        smartlead_client_id=os.getenv("SMARTLEAD_CLIENT_ID", ""),
        smartlead_template_campaign_id=os.getenv("SMARTLEAD_TEMPLATE_CAMPAIGN_ID", ""),
        outbound_sender_capacity=max(1, int(os.getenv("OUTBOUND_SENDER_CAPACITY", "1"))),
        outbound_window_weeks=max(1, int(os.getenv("OUTBOUND_WINDOW_WEEKS", "3"))),
        weekly_show_sync_enabled=os.getenv("WEEKLY_SHOW_SYNC_ENABLED", "false").lower() in {"1", "true", "yes"},
        weekly_show_sync_source_url=os.getenv("WEEKLY_SHOW_SYNC_SOURCE_URL", "").strip(),
        weekly_show_sync_source_path=os.getenv("WEEKLY_SHOW_SYNC_SOURCE_PATH", "").strip(),
        weekly_show_sync_weekday=int(os.getenv("WEEKLY_SHOW_SYNC_WEEKDAY", "6")),
        weekly_show_sync_hour=int(os.getenv("WEEKLY_SHOW_SYNC_HOUR", "10")),
        weekly_show_sync_timezone=os.getenv("WEEKLY_SHOW_SYNC_TIMEZONE", "America/New_York").strip() or "America/New_York",
        weekly_show_sync_lookahead_days=max(1, int(os.getenv("WEEKLY_SHOW_SYNC_LOOKAHEAD_DAYS", "100"))),
        weekly_show_sync_require_notion=os.getenv("WEEKLY_SHOW_SYNC_REQUIRE_NOTION", "false").lower()
        in {"1", "true", "yes"},
        notion_api_token=os.getenv("NOTION_API_TOKEN", "").strip(),
        notion_database_id=os.getenv(
            "NOTION_DATABASE_ID",
            "356127477edb804d89e7c406ad08975b",
        ).strip(),
        notion_data_source_id=os.getenv(
            "NOTION_DATA_SOURCE_ID",
            "356127477edb8094b75f000bbd6766d8",
        ).strip(),
        scrape_execution_mode=(os.getenv("SCRAPE_EXECUTION_MODE", "worker").strip().lower() or "worker"),
        scrape_due_webhook_url=os.getenv("SCRAPE_DUE_WEBHOOK_URL", "").strip(),
        airtable_token=os.getenv("AIRTABLE_TOKEN", "").strip(),
        airtable_base_id=os.getenv("AIRTABLE_BASE_ID", "appfBCKnwzWr26p8R").strip(),
        airtable_shows_table_id=os.getenv("AIRTABLE_SHOWS_TABLE_ID", "tblneSplTxPpRcJun").strip(),
        airtable_companies_table_id=os.getenv("AIRTABLE_COMPANIES_TABLE_ID", "tblNQrSab6MmUI98s").strip(),
        airtable_contacts_table_id=os.getenv("AIRTABLE_CONTACTS_TABLE_ID", "tbl3eQqwTAsVAf5vr").strip(),
        airtable_campaign_pushes_table_id=os.getenv(
            "AIRTABLE_CAMPAIGN_PUSHES_TABLE_ID",
            "tbleK9abvfJaetm7o",
        ).strip(),
    )
