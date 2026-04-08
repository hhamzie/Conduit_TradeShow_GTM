from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
import re

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.config import get_settings
from app.models import CampaignRun, ProviderStatus, RunStatus, Show, ShowStatus
from app.providers import notify_ready_for_review, push_to_clay, push_to_heyreach, push_to_smartlead
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
SLUG_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class ImportSummary:
    created: int
    updated: int
    skipped: int


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


def normalize_headers(fieldnames: list[str] | None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for name in fieldnames or []:
        key = HEADER_ALIASES.get(name.strip().lower())
        if key:
            normalized[key] = name
    return normalized


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

        event_date = parse_show_date(event_date_raw)
        run_at = compute_run_at(event_date, run_offset_days)

        existing = db.scalar(
            select(Show).where(Show.source_url == link, Show.event_date == event_date)
        )
        if existing is None:
            db.add(
                Show(
                    name=show_name,
                    event_date=event_date,
                    place=place,
                    source_url=link,
                    run_offset_days=run_offset_days,
                    run_at=run_at,
                    status=ShowStatus.waiting.value,
                )
            )
            created += 1
            continue

        existing.name = show_name
        existing.place = place
        existing.run_offset_days = run_offset_days
        existing.run_at = run_at
        updated += 1

    db.commit()
    return ImportSummary(created=created, updated=updated, skipped=skipped)


def list_shows(db: Session) -> list[Show]:
    return list(
        db.scalars(
            select(Show)
            .options(selectinload(Show.runs))
            .order_by(Show.event_date.asc(), Show.created_at.desc())
        )
    )


def get_show(db: Session, show_id: int) -> Show | None:
    return db.scalar(
        select(Show)
        .options(selectinload(Show.runs))
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
    show.run_at = datetime.now()
    show.status = ShowStatus.queued.value
    db.add(CampaignRun(show=show, status=RunStatus.queued.value))
    db.commit()


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

    clay_result = push_to_clay(show)

    show.status = ShowStatus.ready_for_review.value
    show.latest_export_path = str(result.output_path)
    show.company_count = result.company_count
    show.failure_count = result.failures
    show.clay_status = clay_result.status
    show.heyreach_status = ProviderStatus.pending.value
    show.smartlead_status = ProviderStatus.pending.value
    show.notification_status = notify_ready_for_review(show).status
    if clay_result.status == ProviderStatus.failed.value:
        show.last_error = clay_result.message
    db.commit()
    return campaign_run


def approve_show(db: Session, show: Show) -> None:
    show.status = ShowStatus.approved.value
    show.approved_at = datetime.now()
    show.clay_status = ProviderStatus.pending.value
    show.heyreach_status = ProviderStatus.pending.value
    show.smartlead_status = ProviderStatus.pending.value
    db.commit()


def sync_approved_shows(db: Session) -> int:
    shows = list(
        db.scalars(
            select(Show).where(
                Show.status == ShowStatus.approved.value,
                (
                    (Show.heyreach_status == ProviderStatus.pending.value)
                    | (Show.smartlead_status == ProviderStatus.pending.value)
                ),
            )
        )
    )
    synced = 0
    for show in shows:
        clay_result = push_to_clay(show)
        heyreach_result = push_to_heyreach(show)
        smartlead_result = push_to_smartlead(show)

        show.clay_status = clay_result.status
        show.heyreach_status = heyreach_result.status
        show.smartlead_status = smartlead_result.status

        if all(result.status == ProviderStatus.success.value for result in (clay_result, heyreach_result, smartlead_result)):
            show.status = ShowStatus.live.value
        synced += 1

    if shows:
        db.commit()
    return synced
