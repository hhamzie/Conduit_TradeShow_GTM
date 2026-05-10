from __future__ import annotations

import csv
import gc
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
import re
import zipfile
from urllib.parse import urlparse
from collections.abc import Callable
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.config import get_settings
from app.models import AutomationCheckpoint, CampaignRun, ClaySyncRow, ProviderStatus, RunStatus, Show, ShowStatus
from app.providers import (
    ClayPollResult,
    SmartleadSyncResult,
    ensure_smartlead_campaign,
    import_ready_rows_to_smartlead,
    launch_smartlead_campaign,
    notify_ready_for_review,
    pause_smartlead_campaign,
    poll_clay_table,
    push_to_clay,
    push_to_heyreach,
)
from app.trade_show_feeder import is_b2b_physical_goods_show, scan_upcoming_trade_shows
from scraper import ScrapeOptions, run_scrape


MANUAL_TRADE_SHOW_SCAN_CHECKPOINT_KEY = "manual_trade_show_scan"


HEADER_ALIASES = {
    "show": "show",
    "event": "show",
    "conference": "show",
    "date": "date",
    "start date": "date",
    "place": "place",
    "location": "place",
    "link": "link",
    "url": "link",
    "directory url": "link",
}
ENRICHED_ALIASES = {
    "email": ("email", "email_address", "work_email", "professional_email"),
    "first_name": ("first_name", "firstname", "first"),
    "last_name": ("last_name", "lastname", "last"),
    "company_name": ("company_name", "company", "organization", "organisation"),
    "job_title": ("job_title", "title", "role"),
    "website": ("website", "website_url", "company_url", "domain", "company_domain"),
    "location": ("location", "city", "hq_location"),
    "linkedin_profile": ("linkedin_profile", "linkedin_url", "linkedin"),
    "phone_number": ("phone_number", "phone", "phone_mobile", "mobile_phone"),
}
SMARTLEAD_READY_HEADERS = [
    "email",
    "first_name",
    "last_name",
    "company_name",
    "job_title",
    "website",
    "company_domain",
    "location",
    "linkedin_profile",
    "phone_number",
    "show_name",
    "show_date",
    "show_place",
    "source_url",
    "clay_row_id",
]
CLAY_STATUS_POLLING = "polling"
CLAY_STATUS_COMPLETE = "complete"
SMARTLEAD_STATUS_SYNCING = "syncing"
SMARTLEAD_STATUS_PREPARED = "prepared"
SMARTLEAD_STATUS_READY = "ready_to_launch"
SMARTLEAD_STATUS_ACTIVE = "active"
SMARTLEAD_STATUS_PAUSED = "paused"
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
SLUG_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class ImportSummary:
    created: int
    updated: int
    skipped: int


@dataclass(frozen=True)
class PreparedLead:
    clay_row_id: str
    csv_row: dict[str, str]
    smartlead_row: dict[str, object]


@dataclass(frozen=True)
class DirectScrapeResult:
    output_path: Path
    company_count: int
    failure_count: int
    conference_name: str
    conference_location: str


@dataclass(frozen=True)
class BulkDirectScrapeResult:
    archive_path: Path
    show_count: int
    success_count: int
    failed_count: int
    skipped_count: int


@dataclass(frozen=True)
class QueuedBulkShow:
    show_id: int
    show_name: str
    event_date_raw: str
    place: str
    link: str


@dataclass(frozen=True)
class OutboundPlan:
    email_count: int
    linkedin_count: int
    weeks: int
    sender_capacity: int
    active_campaign_count: int
    available_slots: int
    at_capacity: bool


@dataclass(frozen=True)
class WeeklyShowSyncResult:
    created: int
    updated: int
    skipped: int
    filtered_out: int


def _parse_bulk_csv_payload(payload: bytes) -> tuple[list[dict[str, str]], dict[str, str]]:
    text = payload.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    headers = normalize_headers(reader.fieldnames)
    missing = [field for field in ("show", "date", "place", "link") if field not in headers]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    rows = list(reader)
    return rows, headers


def normalize_show_identity_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.strip().lower()).strip()


def normalize_show_identity_url(value: str) -> str:
    raw_value = value.strip()
    if not raw_value:
        return ""
    candidate = raw_value if "://" in raw_value else f"https://{raw_value}"
    parsed = urlparse(candidate)
    host = (parsed.netloc or parsed.path).strip().lower()
    path = parsed.path.strip().rstrip("/")
    query = parsed.query.strip()
    normalized = f"{host}{path}"
    if query:
        normalized = f"{normalized}?{query}"
    return normalized


def _find_matching_show(
    db: Session,
    *,
    show_name: str,
    event_date: date,
    link: str,
    exclude_show_id: int | None = None,
) -> Show | None:
    normalized_name = normalize_show_identity_name(show_name)
    normalized_url = normalize_show_identity_url(link)
    candidates = list(db.scalars(select(Show).where(Show.event_date == event_date)))
    for candidate in candidates:
        if exclude_show_id is not None and candidate.id == exclude_show_id:
            continue
        if normalize_show_identity_name(candidate.name) == normalized_name:
            return candidate
        if normalized_url and normalize_show_identity_url(candidate.source_url) == normalized_url:
            return candidate
    return None


def upsert_show(
    db: Session,
    *,
    show_name: str,
    event_date_raw: str,
    place: str,
    link: str,
    run_offset_days: int,
) -> tuple[Show, bool]:
    normalized_name = show_name.strip()
    normalized_place = place.strip()
    normalized_link = link.strip()
    if not (normalized_name and event_date_raw.strip() and normalized_place and normalized_link):
        raise ValueError("Show name, date, place, and directory URL are all required.")

    event_date = parse_show_date(event_date_raw)
    run_at = compute_run_at(event_date, run_offset_days)

    existing = _find_matching_show(
        db,
        show_name=normalized_name,
        event_date=event_date,
        link=normalized_link,
    )
    if existing is None:
        show = Show(
            name=normalized_name,
            event_date=event_date,
            place=normalized_place,
            source_url=normalized_link,
            run_offset_days=run_offset_days,
            run_at=run_at,
            status=ShowStatus.waiting.value,
        )
        db.add(show)
        db.flush()
        return show, True

    existing.name = normalized_name
    existing.place = normalized_place
    existing.source_url = normalized_link
    existing.run_offset_days = run_offset_days
    existing.run_at = run_at
    db.flush()
    return existing, False


def slugify(value: str) -> str:
    return SLUG_RE.sub("-", value.lower()).strip("-") or "show"


def parse_show_date(raw_value: str, today: date | None = None) -> date:
    value = raw_value.strip()
    if not value:
        raise ValueError("Missing date value.")

    today = today or date.today()
    for fmt in (
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%b %d %Y",
        "%B %d %Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    for fmt in ("%b %d", "%B %d", "%b %d,", "%B %d,"):
        try:
            parsed = datetime.strptime(value, fmt).date()
        except ValueError:
            continue
        candidate = parsed.replace(year=today.year)
        if candidate < today - timedelta(days=30):
            candidate = candidate.replace(year=today.year + 1)
        return candidate

    raise ValueError(f"Unsupported date format: {raw_value}")


def compute_run_at(event_date: date, run_offset_days: int) -> datetime:
    target_day = event_date - timedelta(days=run_offset_days)
    return datetime.combine(target_day, time(hour=9, minute=0))


def export_path_for_show(show: Show) -> Path:
    settings = get_settings()
    stamped = show.event_date.isoformat()
    filename = f"{slugify(show.name)}_{stamped}.csv"
    return settings.export_dir / filename


def enriched_export_path_for_show(show: Show) -> Path:
    settings = get_settings()
    stamped = show.event_date.isoformat()
    filename = f"{slugify(show.name)}_{stamped}_clay_enriched.csv"
    return settings.export_dir / filename


def smartlead_ready_export_path_for_show(show: Show) -> Path:
    settings = get_settings()
    stamped = show.event_date.isoformat()
    filename = f"{slugify(show.name)}_{stamped}_smartlead_ready.csv"
    return settings.export_dir / filename


def direct_single_export_path(show_name: str, event_date: date | None = None) -> Path:
    settings = get_settings()
    timestamp_part = event_date.isoformat() if event_date else datetime.now().strftime("%Y%m%d_%H%M%S")
    return settings.export_dir / "direct" / f"{slugify(show_name)}_{timestamp_part}.csv"


def direct_bulk_archive_path() -> Path:
    settings = get_settings()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return settings.export_dir / "bulk" / f"trade_show_scrapes_{timestamp}.zip"


def normalize_headers(fieldnames: list[str] | None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for name in fieldnames or []:
        key = HEADER_ALIASES.get(name.strip().lower())
        if key:
            normalized[key] = name
    return normalized


def create_or_update_show(
    db: Session,
    *,
    show_name: str,
    event_date_raw: str,
    place: str,
    link: str,
    run_offset_days: int,
) -> bool:
    _, created = upsert_show(
        db,
        show_name=show_name,
        event_date_raw=event_date_raw,
        place=place,
        link=link,
        run_offset_days=run_offset_days,
    )
    return created


def update_show(
    db: Session,
    *,
    show: Show,
    show_name: str,
    event_date_raw: str,
    place: str,
    link: str,
    run_offset_days: int,
) -> None:
    normalized_name = show_name.strip()
    normalized_place = place.strip()
    normalized_link = link.strip()
    if not (normalized_name and event_date_raw.strip() and normalized_place and normalized_link):
        raise ValueError("Show name, date, place, and directory URL are all required.")

    event_date = parse_show_date(event_date_raw)
    existing = _find_matching_show(
        db,
        show_name=normalized_name,
        event_date=event_date,
        link=normalized_link,
        exclude_show_id=show.id,
    )
    if existing is not None:
        raise ValueError("Another show with the same date, name, or directory link already exists.")

    show.name = normalized_name
    show.place = normalized_place
    show.source_url = normalized_link
    show.event_date = event_date
    show.run_offset_days = run_offset_days
    show.run_at = compute_run_at(event_date, run_offset_days)
    db.commit()


def _run_direct_scrape(
    *,
    show_name: str,
    place: str,
    link: str,
    output_path: Path,
    require_website: bool = True,
    browser_mode: str = "auto",
    workers: int | None = None,
) -> DirectScrapeResult:
    settings = get_settings()
    direct_scrape_workers = workers or min(settings.default_scraper_workers, 4)
    result = run_scrape(
        ScrapeOptions(
            directory_url=link.strip(),
            output_path=output_path,
            workers=direct_scrape_workers,
            max_pages=settings.default_max_pages,
            sample_size=settings.default_sample_size,
            browser_mode=browser_mode,
            browser_timeout_ms=settings.default_browser_timeout_ms,
            conference_name=show_name.strip(),
            conference_location=place.strip(),
            require_website=require_website,
        )
    )
    return DirectScrapeResult(
        output_path=result.output_path,
        company_count=result.company_count,
        failure_count=result.failures,
        conference_name=result.conference_name,
        conference_location=result.conference_location,
    )


def run_single_show_scrape(
    *,
    show_name: str,
    place: str,
    link: str,
    event_date_raw: str = "",
) -> DirectScrapeResult:
    normalized_name = show_name.strip()
    normalized_place = place.strip()
    normalized_link = link.strip()
    if not (normalized_name and normalized_place and normalized_link):
        raise ValueError("Show name, place, and directory URL are required.")

    event_date = parse_show_date(event_date_raw) if event_date_raw.strip() else None
    output_path = direct_single_export_path(normalized_name, event_date)
    return _run_direct_scrape(
        show_name=normalized_name,
        place=normalized_place,
        link=normalized_link,
        output_path=output_path,
        require_website=True,
        browser_mode="auto",
    )


def run_show_scrape(db: Session, show: Show, *, workers: int | None = None) -> DirectScrapeResult:
    campaign_run = CampaignRun(
        show=show,
        status=RunStatus.running.value,
        started_at=datetime.now(),
    )
    show.status = ShowStatus.scraping.value
    show.last_error = ""
    db.add(campaign_run)
    db.commit()

    try:
        result = _run_direct_scrape(
            show_name=show.name,
            place=show.place,
            link=show.source_url,
            output_path=export_path_for_show(show),
            require_website=True,
            browser_mode="auto",
            workers=workers,
        )
    except Exception as exc:  # noqa: BLE001
        campaign_run.status = RunStatus.failed.value
        campaign_run.error_message = str(exc)
        campaign_run.finished_at = datetime.now()
        show.status = ShowStatus.failed.value
        show.last_error = str(exc)
        db.commit()
        raise

    campaign_run.status = RunStatus.success.value
    campaign_run.output_path = str(result.output_path)
    campaign_run.company_count = result.company_count
    campaign_run.failure_count = result.failure_count
    campaign_run.finished_at = datetime.now()
    show.latest_export_path = str(result.output_path)
    show.company_count = result.company_count
    show.failure_count = result.failure_count
    show.status = ShowStatus.ready_for_review.value
    show.last_error = ""
    db.commit()
    return result


def run_bulk_direct_scrape(
    payload: bytes,
    progress_callback: Callable[[int, int, str, str], None] | None = None,
    *,
    db: Session | None = None,
    run_offset_days: int | None = None,
    queued_shows: list[QueuedBulkShow] | None = None,
) -> BulkDirectScrapeResult:
    settings = get_settings()
    if db is not None and queued_shows is not None:
        return _run_bulk_direct_scrape_queue(
            queued_shows,
            settings,
            progress_callback=progress_callback,
            db=db,
        )
    rows, headers = _parse_bulk_csv_payload(payload)
    return _run_bulk_direct_scrape_rows(
        rows,
        headers,
        settings,
        progress_callback,
    )


def _run_bulk_direct_scrape_rows(
    rows: list[dict[str, str]],
    headers: dict[str, str],
    settings,
    progress_callback: Callable[[int, int, str, str], None] | None = None,
) -> BulkDirectScrapeResult:
    total_rows = len(rows)

    archive_path = direct_bulk_archive_path()
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_rows: list[dict[str, str | int]] = []
    success_count = 0
    failed_count = 0
    skipped_count = 0

    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for row_number, row in enumerate(rows, start=2):
            show_name = (row.get(headers["show"]) or "").strip()
            event_date_raw = (row.get(headers["date"]) or "").strip()
            place = (row.get(headers["place"]) or "").strip()
            link = (row.get(headers["link"]) or "").strip()
            progress_name = show_name or f"Row {row_number}"

            if not (show_name and event_date_raw and place and link):
                failed_count += 1
                manifest_rows.append(
                    {
                        "show_name": show_name or f"row_{row_number}",
                        "event_date": event_date_raw,
                        "place": place,
                        "source_url": link,
                        "status": "failed",
                        "company_count": 0,
                        "failure_count": 0,
                        "csv_file": "",
                        "error": "Missing one or more required fields.",
                    }
                )
                if progress_callback is not None:
                    progress_callback(
                        success_count + failed_count,
                        total_rows,
                        progress_name,
                        "Missing one or more required fields.",
                    )
                continue

            try:
                event_date = parse_show_date(event_date_raw)
                output_path = direct_single_export_path(show_name, event_date)
                if progress_callback is not None:
                    progress_callback(
                        success_count + failed_count,
                        total_rows,
                        show_name,
                        f"Scraping {show_name} ({success_count + failed_count + 1}/{total_rows})",
                    )
                result = _run_direct_scrape(
                    show_name=show_name,
                    place=place,
                    link=link,
                    output_path=output_path,
                    require_website=True,
                    browser_mode="auto",
                    workers=max(1, min(settings.bulk_scraper_workers, 2)),
                )
                relative_name = output_path.name
                archive.write(result.output_path, arcname=relative_name)
                manifest_rows.append(
                    {
                        "show_name": show_name,
                        "event_date": event_date.isoformat(),
                        "place": place,
                        "source_url": link,
                        "status": "success",
                        "company_count": result.company_count,
                        "failure_count": result.failure_count,
                        "csv_file": relative_name,
                        "error": "",
                    }
                )
                success_count += 1
                if progress_callback is not None:
                    progress_callback(
                        success_count + failed_count,
                        total_rows,
                        show_name,
                        f"Finished {show_name} ({success_count + failed_count}/{total_rows})",
                    )
            except Exception as exc:  # noqa: BLE001
                failed_count += 1
                manifest_rows.append(
                    {
                        "show_name": show_name,
                        "event_date": event_date_raw,
                        "place": place,
                        "source_url": link,
                        "status": "failed",
                        "company_count": 0,
                        "failure_count": 0,
                        "csv_file": "",
                        "error": str(exc),
                    }
                )
                if progress_callback is not None:
                    progress_callback(
                        success_count + failed_count,
                        total_rows,
                        show_name,
                        f"Failed {show_name}: {exc}",
                    )
            finally:
                gc.collect()

        manifest_buffer = io.StringIO()
        writer = csv.DictWriter(
            manifest_buffer,
            fieldnames=[
                "show_name",
                "event_date",
                "place",
                "source_url",
                "status",
                "company_count",
                "failure_count",
                "csv_file",
                "error",
            ],
        )
        writer.writeheader()
        for manifest_row in manifest_rows:
            writer.writerow(manifest_row)
        archive.writestr("manifest.csv", manifest_buffer.getvalue().encode("utf-8"))

    return BulkDirectScrapeResult(
        archive_path=archive_path,
        show_count=len(manifest_rows),
        success_count=success_count,
        failed_count=failed_count,
        skipped_count=skipped_count,
    )


def _run_bulk_direct_scrape_queue(
    queued_shows: list[QueuedBulkShow],
    settings,
    *,
    progress_callback: Callable[[int, int, str, str], None] | None = None,
    db: Session,
) -> BulkDirectScrapeResult:
    total_rows = len(queued_shows)

    archive_path = direct_bulk_archive_path()
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_rows: list[dict[str, str | int]] = []
    success_count = 0
    failed_count = 0
    skipped_count = 0

    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in queued_shows:
            if progress_callback is not None:
                progress_callback(
                    success_count + failed_count + skipped_count,
                    total_rows,
                    item.show_name,
                    f"Checking queue for {item.show_name} ({success_count + failed_count + skipped_count + 1}/{total_rows})",
                )

            show = db.get(Show, item.show_id)
            if show is None:
                skipped_count += 1
                manifest_rows.append(
                    {
                        "show_name": item.show_name,
                        "event_date": item.event_date_raw,
                        "place": item.place,
                        "source_url": item.link,
                        "status": "skipped",
                        "company_count": 0,
                        "failure_count": 0,
                        "csv_file": "",
                        "error": "Removed from dashboard before scrape started.",
                    }
                )
                if progress_callback is not None:
                    progress_callback(
                        success_count + failed_count + skipped_count,
                        total_rows,
                        item.show_name,
                        f"Skipped {item.show_name} because it was removed from the dashboard.",
                    )
                continue

            try:
                if progress_callback is not None:
                    progress_callback(
                        success_count + failed_count + skipped_count,
                        total_rows,
                        show.name,
                        f"Scraping {show.name} ({success_count + failed_count + skipped_count + 1}/{total_rows})",
                    )
                result = run_show_scrape(
                    db,
                    show,
                    workers=max(1, min(settings.bulk_scraper_workers, 2)),
                )
                relative_name = Path(show.latest_export_path or result.output_path).name
                archive.write(result.output_path, arcname=relative_name)
                manifest_rows.append(
                    {
                        "show_name": show.name,
                        "event_date": show.event_date.isoformat(),
                        "place": show.place,
                        "source_url": show.source_url,
                        "status": "success",
                        "company_count": result.company_count,
                        "failure_count": result.failure_count,
                        "csv_file": relative_name,
                        "error": "",
                    }
                )
                success_count += 1
                if progress_callback is not None:
                    progress_callback(
                        success_count + failed_count + skipped_count,
                        total_rows,
                        show.name,
                        f"Finished {show.name} ({success_count + failed_count + skipped_count}/{total_rows})",
                    )
            except Exception as exc:  # noqa: BLE001
                failed_count += 1
                manifest_rows.append(
                    {
                        "show_name": show.name,
                        "event_date": show.event_date.isoformat(),
                        "place": show.place,
                        "source_url": show.source_url,
                        "status": "failed",
                        "company_count": 0,
                        "failure_count": 0,
                        "csv_file": "",
                        "error": str(exc),
                    }
                )
                if progress_callback is not None:
                    progress_callback(
                        success_count + failed_count + skipped_count,
                        total_rows,
                        show.name,
                        f"Failed {show.name}: {exc}",
                    )
            finally:
                gc.collect()

        manifest_buffer = io.StringIO()
        writer = csv.DictWriter(
            manifest_buffer,
            fieldnames=[
                "show_name",
                "event_date",
                "place",
                "source_url",
                "status",
                "company_count",
                "failure_count",
                "csv_file",
                "error",
            ],
        )
        writer.writeheader()
        for manifest_row in manifest_rows:
            writer.writerow(manifest_row)
        archive.writestr("manifest.csv", manifest_buffer.getvalue().encode("utf-8"))

    return BulkDirectScrapeResult(
        archive_path=archive_path,
        show_count=len(manifest_rows),
        success_count=success_count,
        failed_count=failed_count,
        skipped_count=skipped_count,
    )


def register_bulk_shows(
    db: Session,
    payload: bytes,
    run_offset_days: int,
) -> tuple[ImportSummary, list[QueuedBulkShow]]:
    rows, headers = _parse_bulk_csv_payload(payload)

    created = 0
    updated = 0
    skipped = 0
    queued_shows: list[QueuedBulkShow] = []
    seen_upload_keys: set[tuple[str, str, str]] = set()

    for row in rows:
        show_name = (row.get(headers["show"]) or "").strip()
        event_date_raw = (row.get(headers["date"]) or "").strip()
        place = (row.get(headers["place"]) or "").strip()
        link = (row.get(headers["link"]) or "").strip()

        if not (show_name and event_date_raw and place and link):
            skipped += 1
            continue

        event_date = parse_show_date(event_date_raw)
        dedupe_key = (
            event_date.isoformat(),
            normalize_show_identity_name(show_name),
            normalize_show_identity_url(link),
        )
        if dedupe_key in seen_upload_keys:
            skipped += 1
            continue
        seen_upload_keys.add(dedupe_key)

        show, created_now = upsert_show(
            db,
            show_name=show_name,
            event_date_raw=event_date_raw,
            place=place,
            link=link,
            run_offset_days=run_offset_days,
        )
        if created_now:
            created += 1
        else:
            updated += 1
        queued_shows.append(
            QueuedBulkShow(
                show_id=show.id,
                show_name=show_name,
                event_date_raw=event_date_raw,
                place=place,
                link=link,
            )
        )

    db.commit()
    return ImportSummary(created=created, updated=updated, skipped=skipped), queued_shows


def import_shows_from_csv(db: Session, payload: bytes, run_offset_days: int) -> ImportSummary:
    summary, _ = register_bulk_shows(db, payload, run_offset_days)
    return summary


def _estimate_outbound_counts(show: Show) -> tuple[int, int]:
    raw_path = show.smartlead_ready_export_path or show.enriched_export_path
    if raw_path:
        path = Path(raw_path).expanduser()
        if path.exists():
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)
            if rows:
                linkedin_count = 0
                for row in rows:
                    normalized = {_normalize_enriched_key(key): (value or "").strip() for key, value in row.items() if key}
                    if _pick_enriched_value(normalized, "linkedin_profile"):
                        linkedin_count += 1
                return len(rows), linkedin_count

    fallback_email_count = max(show.smartlead_imported_rows, show.clay_ready_rows, 0)
    return fallback_email_count, 0


def build_outbound_plan(db: Session, show: Show) -> OutboundPlan:
    settings = get_settings()
    email_count, linkedin_count = _estimate_outbound_counts(show)
    active_campaign_count = int(
        db.scalar(
            select(func.count())
            .select_from(Show)
            .where(
                Show.smartlead_status == SMARTLEAD_STATUS_ACTIVE,
                Show.id != show.id,
            )
        )
        or 0
    )
    sender_capacity = max(1, settings.outbound_sender_capacity)
    available_slots = max(sender_capacity - active_campaign_count, 0)
    at_capacity = show.smartlead_status != SMARTLEAD_STATUS_ACTIVE and available_slots == 0
    return OutboundPlan(
        email_count=email_count,
        linkedin_count=linkedin_count,
        weeks=settings.outbound_window_weeks,
        sender_capacity=sender_capacity,
        active_campaign_count=active_campaign_count,
        available_slots=available_slots,
        at_capacity=at_capacity,
    )


def start_outbound_campaign(db: Session, show: Show) -> OutboundPlan:
    plan = build_outbound_plan(db, show)
    if plan.email_count <= 0:
        raise ValueError("Outbound is not ready yet. No email-ready leads are loaded for this show.")
    if plan.at_capacity:
        raise ValueError(
            f"Email senders are at capacity. {plan.active_campaign_count} campaign(s) are already running across "
            f"{plan.sender_capacity} sender slot(s)."
        )

    if not show.smartlead_campaign_id:
        campaign_result = ensure_smartlead_campaign(show)
        if campaign_result.status != ProviderStatus.success.value:
            raise ValueError(campaign_result.message)
        show.smartlead_campaign_id = campaign_result.campaign_id
        show.smartlead_campaign_name = campaign_result.campaign_name

    if show.status != ShowStatus.approved.value and show.status != ShowStatus.live.value:
        if show.clay_total_rows == 0:
            raise ValueError("Outbound is not ready yet. Clay has not prepared lead rows for this show.")
        terminal_rows = show.clay_ready_rows + show.clay_failed_rows + show.clay_skipped_rows
        if terminal_rows < show.clay_total_rows:
            raise ValueError("Outbound is not ready yet. Clay is still enriching this show.")
        show.status = ShowStatus.approved.value
        show.approved_at = datetime.now()
        db.commit()

    if show.smartlead_status == SMARTLEAD_STATUS_ACTIVE and show.status == ShowStatus.live.value:
        return plan

    launch_show(db, show)
    return plan


def _load_weekly_show_sync_payload(settings) -> bytes:
    if settings.weekly_show_sync_source_url:
        response = httpx.get(settings.weekly_show_sync_source_url, timeout=45.0, follow_redirects=True)
        response.raise_for_status()
        return response.content
    if settings.weekly_show_sync_source_path:
        return Path(settings.weekly_show_sync_source_path).expanduser().read_bytes()
    raise ValueError("Weekly trade show sync is enabled, but no source URL or source path is configured.")


def _get_automation_checkpoint(db: Session, key: str) -> AutomationCheckpoint:
    checkpoint = db.scalar(select(AutomationCheckpoint).where(AutomationCheckpoint.key == key))
    if checkpoint is None:
        checkpoint = AutomationCheckpoint(key=key)
        db.add(checkpoint)
        db.flush()
    return checkpoint


def _localize_automation_timestamp(moment: datetime, timezone_name: str) -> datetime:
    timezone = ZoneInfo(timezone_name)
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone)
    return moment.astimezone(timezone)


def manual_trade_show_scan_already_ran_today(db: Session, now: datetime | None = None) -> bool:
    settings = get_settings()
    current_local = _localize_automation_timestamp(now or datetime.now(), settings.weekly_show_sync_timezone)
    checkpoint = _get_automation_checkpoint(db, MANUAL_TRADE_SHOW_SCAN_CHECKPOINT_KEY)
    if checkpoint.last_run_at is None:
        return False
    checkpoint_meta = _load_checkpoint_meta(checkpoint)
    checkpoint_revision = str(checkpoint_meta.get("deploy_revision") or "").strip()
    current_revision = settings.deploy_revision.strip()
    if current_revision and checkpoint_revision != current_revision:
        return False
    previous_local = _localize_automation_timestamp(checkpoint.last_run_at, settings.weekly_show_sync_timezone)
    return previous_local.date() >= current_local.date()


def record_manual_trade_show_scan(db: Session, now: datetime | None = None) -> None:
    settings = get_settings()
    current_local = _localize_automation_timestamp(now or datetime.now(), settings.weekly_show_sync_timezone)
    checkpoint = _get_automation_checkpoint(db, MANUAL_TRADE_SHOW_SCAN_CHECKPOINT_KEY)
    checkpoint.last_run_at = current_local.replace(tzinfo=None)
    checkpoint.meta_json = json.dumps(
        {
            "lookahead_days": settings.weekly_show_sync_lookahead_days,
            "recorded_on": current_local.date().isoformat(),
            "source": "manual_scan",
            "deploy_revision": settings.deploy_revision.strip(),
        },
        sort_keys=True,
    )


def _load_checkpoint_meta(checkpoint: AutomationCheckpoint) -> dict[str, object]:
    raw = (checkpoint.meta_json or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _current_weekly_sync_window(now: datetime, settings) -> datetime:
    timezone = ZoneInfo(settings.weekly_show_sync_timezone)
    local_now = now.astimezone(timezone) if now.tzinfo else now.replace(tzinfo=timezone)
    days_since_target = (local_now.weekday() - settings.weekly_show_sync_weekday) % 7
    scheduled_date = local_now.date() - timedelta(days=days_since_target)
    scheduled_at = datetime.combine(
        scheduled_date,
        time(hour=settings.weekly_show_sync_hour, minute=0),
        tzinfo=timezone,
    )
    if local_now < scheduled_at:
        scheduled_at -= timedelta(days=7)
    return scheduled_at


def run_weekly_show_sync(db: Session, now: datetime | None = None) -> WeeklyShowSyncResult | None:
    settings = get_settings()
    if not settings.weekly_show_sync_enabled:
        return None

    now = now or datetime.now(ZoneInfo(settings.weekly_show_sync_timezone))
    scheduled_window = _current_weekly_sync_window(now, settings)
    if now.tzinfo is None:
        timezone = ZoneInfo(settings.weekly_show_sync_timezone)
        current_local = now.replace(tzinfo=timezone)
    else:
        current_local = now.astimezone(ZoneInfo(settings.weekly_show_sync_timezone))
    if current_local < scheduled_window:
        return None

    checkpoint = _get_automation_checkpoint(db, "weekly_trade_show_sync")
    if checkpoint.last_run_at is not None:
        previous_run = checkpoint.last_run_at
        previous_local = (
            previous_run.astimezone(ZoneInfo(settings.weekly_show_sync_timezone))
            if previous_run.tzinfo
            else previous_run.replace(tzinfo=ZoneInfo(settings.weekly_show_sync_timezone))
        )
        if previous_local >= scheduled_window:
            return None

    created = 0
    updated = 0
    skipped = 0
    filtered_out = 0
    seen_upload_keys: set[tuple[str, str, str]] = set()
    start_date = current_local.date()
    end_date = start_date + timedelta(days=settings.weekly_show_sync_lookahead_days)

    if settings.weekly_show_sync_source_url or settings.weekly_show_sync_source_path:
        payload = _load_weekly_show_sync_payload(settings)
        rows, headers = _parse_bulk_csv_payload(payload)
        candidate_rows = [
            {
                "show_name": (row.get(headers["show"]) or "").strip(),
                "event_date_raw": (row.get(headers["date"]) or "").strip(),
                "place": (row.get(headers["place"]) or "").strip(),
                "link": (row.get(headers["link"]) or "").strip(),
            }
            for row in rows
        ]
    else:
        candidate_rows = [
            {
                "show_name": candidate.show_name,
                "event_date_raw": candidate.event_date_raw,
                "place": candidate.place,
                "link": candidate.link,
            }
            for candidate in scan_upcoming_trade_shows(
                today=start_date,
                lookahead_days=settings.weekly_show_sync_lookahead_days,
            )
        ]

    for row in candidate_rows:
        show_name = row["show_name"]
        event_date_raw = row["event_date_raw"]
        place = row["place"]
        link = row["link"]
        if not (show_name and event_date_raw and place and link):
            skipped += 1
            continue

        event_date = parse_show_date(event_date_raw, today=start_date)
        if event_date < start_date or event_date > end_date:
            filtered_out += 1
            continue
        if not is_b2b_physical_goods_show(show_name, link):
            filtered_out += 1
            continue

        dedupe_key = (
            event_date.isoformat(),
            normalize_show_identity_name(show_name),
            normalize_show_identity_url(link),
        )
        if dedupe_key in seen_upload_keys:
            skipped += 1
            continue
        seen_upload_keys.add(dedupe_key)

        show, created_now = upsert_show(
            db,
            show_name=show_name,
            event_date_raw=event_date.isoformat(),
            place=place,
            link=link,
            run_offset_days=settings.default_run_offset_days,
        )
        if created_now:
            created += 1
        else:
            updated += 1

    checkpoint.last_run_at = current_local.replace(tzinfo=None)
    checkpoint.meta_json = json.dumps(
        {
            "lookahead_days": settings.weekly_show_sync_lookahead_days,
            "source": settings.weekly_show_sync_source_url or settings.weekly_show_sync_source_path,
            "scheduled_window": scheduled_window.isoformat(),
        },
        sort_keys=True,
    )
    db.commit()
    return WeeklyShowSyncResult(created=created, updated=updated, skipped=skipped, filtered_out=filtered_out)


def list_shows(db: Session) -> list[Show]:
    return list(
        db.scalars(
            select(Show)
            .options(selectinload(Show.runs), selectinload(Show.clay_rows), selectinload(Show.guide_rows))
            .order_by(Show.event_date.asc(), Show.created_at.desc())
        )
    )


def get_show(db: Session, show_id: int) -> Show | None:
    return db.scalar(
        select(Show)
        .options(selectinload(Show.runs), selectinload(Show.clay_rows), selectinload(Show.guide_rows))
        .where(Show.id == show_id)
    )


def queue_due_shows(db: Session, now: datetime | None = None) -> int:
    now = now or datetime.now()
    due_shows = list(
        db.scalars(
            select(Show).where(
                Show.status == ShowStatus.waiting.value,
                Show.run_at <= now,
            )
        )
    )

    for show in due_shows:
        show.status = ShowStatus.queued.value
        db.add(CampaignRun(show=show, status=RunStatus.queued.value))

    if due_shows:
        db.commit()
    return len(due_shows)


def queue_show_now(db: Session, show: Show) -> None:
    if show.status in {ShowStatus.queued.value, ShowStatus.scraping.value}:
        return

    previous_status = show.status
    for clay_row in list(show.clay_rows):
        db.delete(clay_row)

    show.run_at = datetime.now()
    show.status = ShowStatus.queued.value
    show.last_error = ""
    show.failure_count = 0
    show.clay_status = ProviderStatus.pending.value
    show.notification_status = ProviderStatus.pending.value
    show.smartlead_status = ProviderStatus.pending.value
    show.heyreach_status = ProviderStatus.pending.value
    show.clay_last_polled_at = None
    show.clay_last_imported_at = None
    show.clay_total_rows = 0
    show.clay_ready_rows = 0
    show.clay_failed_rows = 0
    show.clay_skipped_rows = 0
    show.smartlead_imported_rows = 0
    show.smartlead_imported_at = None
    show.enriched_export_path = ""
    show.smartlead_ready_export_path = ""
    if previous_status != ShowStatus.live.value:
        show.approved_at = None
    db.add(CampaignRun(show=show, status=RunStatus.queued.value))
    db.commit()


def _normalize_enriched_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _pick_enriched_value(cells: dict[str, str], canonical_field: str) -> str:
    for alias in ENRICHED_ALIASES[canonical_field]:
        value = cells.get(_normalize_enriched_key(alias), "").strip()
        if value:
            return value
    return ""


def _normalize_email(raw_email: str) -> str:
    return raw_email.strip().lower()


def _extract_company_domain(raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        return ""
    candidate = value
    if "://" not in candidate:
        candidate = "https://" + candidate
    parsed = urlparse(candidate)
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _normalize_website(raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        return ""
    if "://" in value:
        return value
    return f"https://{value}"


def _build_prepared_lead(show: Show, clay_row_id: str, cells: dict[str, str]) -> PreparedLead | None:
    email = _normalize_email(_pick_enriched_value(cells, "email"))
    if not email or not EMAIL_RE.match(email):
        return None

    website_value = _pick_enriched_value(cells, "website")
    company_domain = _extract_company_domain(website_value)
    if not company_domain:
        return None

    first_name = _pick_enriched_value(cells, "first_name")
    last_name = _pick_enriched_value(cells, "last_name")
    company_name = _pick_enriched_value(cells, "company_name")
    job_title = _pick_enriched_value(cells, "job_title")
    location = _pick_enriched_value(cells, "location")
    linkedin_profile = _pick_enriched_value(cells, "linkedin_profile")
    phone_number = _pick_enriched_value(cells, "phone_number")
    website = _normalize_website(website_value)

    csv_row = {
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "company_name": company_name,
        "job_title": job_title,
        "website": website,
        "company_domain": company_domain,
        "location": location,
        "linkedin_profile": linkedin_profile,
        "phone_number": phone_number,
        "show_name": show.name,
        "show_date": show.event_date.isoformat(),
        "show_place": show.place,
        "source_url": show.source_url,
        "clay_row_id": clay_row_id,
    }
    smartlead_row: dict[str, object] = {
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "company_name": company_name,
        "phone_number": phone_number,
        "website": website,
        "location": location,
        "linkedin_profile": linkedin_profile,
        "company_url": website,
    }
    custom_fields = {
        "job_title": job_title,
        "company_domain": company_domain,
        "show_name": show.name,
        "show_date": show.event_date.isoformat(),
        "show_place": show.place,
        "source_url": show.source_url,
        "clay_row_id": clay_row_id,
    }
    custom_fields = {key: value for key, value in custom_fields.items() if value}
    if custom_fields:
        smartlead_row["custom_fields"] = custom_fields
    smartlead_row = {key: value for key, value in smartlead_row.items() if value not in ("", None, {})}
    return PreparedLead(clay_row_id=clay_row_id, csv_row=csv_row, smartlead_row=smartlead_row)


def _write_csv(path: Path, rows: list[dict[str, str]], base_headers: list[str]) -> None:
    fieldnames = list(base_headers)
    seen = set(fieldnames)
    for row in rows:
        for key in row:
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _row_hash(cells: dict[str, str]) -> str:
    return hashlib.sha1(json.dumps(cells, sort_keys=True).encode("utf-8")).hexdigest()


def _update_smartlead_status(show: Show, poll_result: ClayPollResult) -> None:
    settings = get_settings()
    if not settings.smartlead_api_key:
        show.smartlead_status = ProviderStatus.skipped.value
        return
    if show.status == ShowStatus.live.value:
        show.smartlead_status = SMARTLEAD_STATUS_ACTIVE
        return
    if show.smartlead_campaign_id and poll_result.all_terminal:
        show.smartlead_status = SMARTLEAD_STATUS_READY if show.status == ShowStatus.approved.value else SMARTLEAD_STATUS_PREPARED
        return
    if show.smartlead_campaign_id or show.smartlead_imported_rows > 0 or poll_result.ready_rows > 0:
        show.smartlead_status = SMARTLEAD_STATUS_SYNCING
        return
    show.smartlead_status = ProviderStatus.pending.value


def sync_show_from_clay(db: Session, show: Show) -> str:
    poll_result = poll_clay_table(show)
    show.clay_last_polled_at = datetime.now()
    if poll_result.status != "success":
        show.clay_status = poll_result.status
        show.last_error = poll_result.message if poll_result.status == ProviderStatus.failed.value else show.last_error
        db.commit()
        return poll_result.status

    show.clay_total_rows = poll_result.total_rows
    show.clay_ready_rows = poll_result.ready_rows
    show.clay_failed_rows = poll_result.failed_rows
    show.clay_skipped_rows = poll_result.skipped_rows
    show.clay_status = CLAY_STATUS_COMPLETE if poll_result.all_terminal else CLAY_STATUS_POLLING

    existing_rows = {row.clay_row_id: row for row in show.clay_rows}
    now = datetime.now()
    ready_value = get_settings().clay_ready_status_value.strip().lower()
    raw_rows: list[dict[str, str]] = []
    smartlead_ready_rows: list[dict[str, str]] = []
    ready_payloads: list[dict[str, object]] = []
    ready_sync_rows: list[ClaySyncRow] = []

    for record in poll_result.records:
        raw_rows.append(
            {
                "clay_row_id": record.clay_row_id,
                "row_status": record.row_status,
                **record.cells,
            }
        )
        sync_row = existing_rows.get(record.clay_row_id)
        if sync_row is None:
            sync_row = ClaySyncRow(show=show, clay_row_id=record.clay_row_id)
            existing_rows[record.clay_row_id] = sync_row
        sync_row.row_status = record.row_status
        sync_row.row_hash = _row_hash(record.cells)
        sync_row.last_seen_at = now

        if record.row_status == ready_value:
            prepared = _build_prepared_lead(show, record.clay_row_id, record.cells)
            if prepared is not None:
                smartlead_ready_rows.append(prepared.csv_row)
                sync_row.email = prepared.csv_row["email"]
                if not sync_row.imported_to_smartlead:
                    ready_payloads.append(prepared.smartlead_row)
                    ready_sync_rows.append(sync_row)
        db.add(sync_row)

    raw_path = enriched_export_path_for_show(show)
    ready_path = smartlead_ready_export_path_for_show(show)
    _write_csv(raw_path, raw_rows, ["clay_row_id", "row_status"])
    _write_csv(ready_path, smartlead_ready_rows, SMARTLEAD_READY_HEADERS)
    show.enriched_export_path = str(raw_path)
    show.smartlead_ready_export_path = str(ready_path)
    show.clay_last_imported_at = now

    if ready_payloads:
        import_result = import_ready_rows_to_smartlead(show, ready_payloads)
        if import_result.status != "success":
            show.smartlead_status = import_result.status
            show.last_error = import_result.message
            db.commit()
            return import_result.status
        for sync_row in ready_sync_rows:
            sync_row.imported_to_smartlead = True
            sync_row.imported_at = now
            db.add(sync_row)
        show.smartlead_imported_at = now
        show.smartlead_campaign_id = import_result.campaign_id
        show.smartlead_campaign_name = import_result.campaign_name
    elif show.smartlead_campaign_id is None and poll_result.ready_rows > 0:
        campaign_result = ensure_smartlead_campaign(show)
        if campaign_result.status == "success":
            show.smartlead_campaign_id = campaign_result.campaign_id
            show.smartlead_campaign_name = campaign_result.campaign_name
        elif campaign_result.status == ProviderStatus.failed.value:
            show.smartlead_status = campaign_result.status
            show.last_error = campaign_result.message
            db.commit()
            return campaign_result.status

    show.smartlead_imported_rows = sum(1 for row in show.clay_rows if row.imported_to_smartlead)
    _update_smartlead_status(show, poll_result)
    db.commit()
    return ProviderStatus.success.value


def _shows_for_background_sync(db: Session) -> list[Show]:
    return list(
        db.scalars(
            select(Show)
            .options(selectinload(Show.clay_rows))
            .where(Show.status.in_([
                ShowStatus.ready_for_review.value,
                ShowStatus.approved.value,
                ShowStatus.live.value,
            ]))
            .order_by(Show.updated_at.asc(), Show.id.asc())
        )
    )


def run_next_campaign(db: Session) -> CampaignRun | None:
    campaign_run = db.scalar(
        select(CampaignRun)
        .options(selectinload(CampaignRun.show))
        .where(CampaignRun.status == RunStatus.queued.value)
        .order_by(CampaignRun.created_at.asc())
    )
    if campaign_run is None:
        return None

    show = campaign_run.show
    show.status = ShowStatus.scraping.value
    show.last_error = ""
    campaign_run.status = RunStatus.running.value
    campaign_run.started_at = datetime.now()
    db.commit()

    try:
        result = run_scrape(
            ScrapeOptions(
                directory_url=show.source_url,
                output_path=export_path_for_show(show),
                workers=get_settings().default_scraper_workers,
                max_pages=get_settings().default_max_pages,
                sample_size=get_settings().default_sample_size,
                browser_mode=get_settings().default_browser_mode,
                browser_timeout_ms=get_settings().default_browser_timeout_ms,
                conference_name=show.name,
                conference_location=show.place,
                require_website=True,
            )
        )
    except Exception as exc:  # noqa: BLE001
        campaign_run.status = RunStatus.failed.value
        campaign_run.error_message = str(exc)
        campaign_run.finished_at = datetime.now()
        show.status = ShowStatus.failed.value
        show.last_error = str(exc)
        db.commit()
        return campaign_run

    campaign_run.status = RunStatus.success.value
    campaign_run.output_path = str(result.output_path)
    campaign_run.company_count = result.company_count
    campaign_run.failure_count = result.failures
    campaign_run.finished_at = datetime.now()
    show.latest_export_path = str(result.output_path)
    show.company_count = result.company_count
    show.failure_count = result.failures
    show.status = ShowStatus.ready_for_review.value
    show.last_error = ""

    clay_result = push_to_clay(show)
    notify_result = notify_ready_for_review(show)

    if clay_result.status == ProviderStatus.success.value:
        show.clay_status = CLAY_STATUS_POLLING if show.clay_table_id else ProviderStatus.success.value
    else:
        show.clay_status = clay_result.status
        if clay_result.status == ProviderStatus.failed.value:
            show.last_error = clay_result.message
    show.heyreach_status = ProviderStatus.pending.value
    show.smartlead_status = ProviderStatus.pending.value
    show.notification_status = notify_result.status
    db.commit()
    return campaign_run


def approve_show(db: Session, show: Show) -> None:
    show.status = ShowStatus.approved.value
    show.approved_at = datetime.now()
    if show.smartlead_campaign_id and show.smartlead_status not in {SMARTLEAD_STATUS_ACTIVE, SMARTLEAD_STATUS_PAUSED}:
        terminal = show.clay_total_rows > 0 and show.clay_total_rows == (
            show.clay_ready_rows + show.clay_failed_rows + show.clay_skipped_rows
        )
        show.smartlead_status = SMARTLEAD_STATUS_READY if terminal else SMARTLEAD_STATUS_SYNCING
    db.commit()


def launch_show(db: Session, show: Show) -> None:
    if show.status != ShowStatus.approved.value:
        raise ValueError("Approve the show before launching its Smartlead campaign.")
    if not show.smartlead_campaign_id:
        raise ValueError("This show does not have a Smartlead campaign yet.")
    if show.clay_total_rows == 0:
        raise ValueError("Clay has not returned any rows for this show yet.")
    terminal_rows = show.clay_ready_rows + show.clay_failed_rows + show.clay_skipped_rows
    if terminal_rows < show.clay_total_rows:
        raise ValueError("Clay is still enriching rows for this show.")

    result = launch_smartlead_campaign(show)
    if result.status != ProviderStatus.success.value:
        raise ValueError(result.message)

    show.status = ShowStatus.live.value
    show.smartlead_status = SMARTLEAD_STATUS_ACTIVE
    db.commit()


def pause_show(db: Session, show: Show) -> None:
    if not show.smartlead_campaign_id:
        raise ValueError("This show does not have a Smartlead campaign yet.")

    result = pause_smartlead_campaign(show)
    if result.status != ProviderStatus.success.value:
        raise ValueError(result.message)

    if show.status == ShowStatus.live.value:
        show.status = ShowStatus.approved.value
    show.smartlead_status = SMARTLEAD_STATUS_PAUSED
    db.commit()


def sync_approved_shows(db: Session) -> int:
    touched = 0
    for show in _shows_for_background_sync(db):
        if show.latest_export_path and not show.clay_table_id and show.clay_status == ProviderStatus.pending.value:
            clay_result = push_to_clay(show)
            if clay_result.status == ProviderStatus.success.value:
                show.clay_status = CLAY_STATUS_POLLING if show.clay_table_id else ProviderStatus.success.value
                db.commit()
            else:
                show.clay_status = clay_result.status
                if clay_result.status == ProviderStatus.failed.value:
                    show.last_error = clay_result.message
                db.commit()

        if show.clay_table_id:
            sync_show_from_clay(db, show)
            touched += 1
            continue

        if show.smartlead_campaign_id is None and show.smartlead_ready_export_path:
            campaign_result = ensure_smartlead_campaign(show)
            if campaign_result.status == ProviderStatus.success.value:
                show.smartlead_campaign_id = campaign_result.campaign_id
                show.smartlead_campaign_name = campaign_result.campaign_name
                show.smartlead_status = SMARTLEAD_STATUS_SYNCING
                db.commit()
            touched += 1

        if show.status == ShowStatus.approved.value:
            heyreach_result = push_to_heyreach(show)
            show.heyreach_status = heyreach_result.status
            db.commit()
    return touched
