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

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.config import get_settings
from app.models import CampaignRun, ClaySyncRow, ProviderStatus, RunStatus, Show, ShowStatus
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
from scraper import ScrapeOptions, run_scrape


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
    normalized_name = show_name.strip()
    normalized_place = place.strip()
    normalized_link = link.strip()
    if not (normalized_name and event_date_raw.strip() and normalized_place and normalized_link):
        raise ValueError("Show name, date, place, and directory URL are all required.")

    event_date = parse_show_date(event_date_raw)
    run_at = compute_run_at(event_date, run_offset_days)

    existing = db.scalar(
        select(Show).where(Show.source_url == normalized_link, Show.event_date == event_date)
    )
    if existing is None:
        db.add(
            Show(
                name=normalized_name,
                event_date=event_date,
                place=normalized_place,
                source_url=normalized_link,
                run_offset_days=run_offset_days,
                run_at=run_at,
                status=ShowStatus.waiting.value,
            )
        )
        return True

    existing.name = normalized_name
    existing.place = normalized_place
    existing.run_offset_days = run_offset_days
    existing.run_at = run_at
    return False


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
    existing = db.scalar(
        select(Show).where(
            Show.id != show.id,
            Show.source_url == normalized_link,
            Show.event_date == event_date,
        )
    )
    if existing is not None:
        raise ValueError("Another show already uses that date and source URL.")

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


def run_bulk_direct_scrape(
    payload: bytes,
    progress_callback: Callable[[int, int, str, str], None] | None = None,
) -> BulkDirectScrapeResult:
    settings = get_settings()
    text = payload.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    headers = normalize_headers(reader.fieldnames)
    missing = [field for field in ("show", "date", "place", "link") if field not in headers]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    rows = list(reader)

    return _run_bulk_direct_scrape_rows(rows, headers, settings, progress_callback)


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
    )


def import_shows_from_csv(db: Session, payload: bytes, run_offset_days: int) -> ImportSummary:
    text = payload.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    headers = normalize_headers(reader.fieldnames)
    missing = [field for field in ("show", "date", "place", "link") if field not in headers]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    created = 0
    updated = 0
    skipped = 0

    for row in reader:
        show_name = (row.get(headers["show"]) or "").strip()
        event_date_raw = (row.get(headers["date"]) or "").strip()
        place = (row.get(headers["place"]) or "").strip()
        link = (row.get(headers["link"]) or "").strip()

        if not (show_name and event_date_raw and place and link):
            skipped += 1
            continue

        if create_or_update_show(
            db,
            show_name=show_name,
            event_date_raw=event_date_raw,
            place=place,
            link=link,
            run_offset_days=run_offset_days,
        ):
            created += 1
        else:
            updated += 1

    db.commit()
    return ImportSummary(created=created, updated=updated, skipped=skipped)


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
