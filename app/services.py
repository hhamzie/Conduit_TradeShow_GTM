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
from app.show_intelligence import _company_row_key
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
DUPLICATE_SHOW_DATE_WINDOW_DAYS = 5


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


SHOW_NAME_NOISE_WORDS = {
    "the",
    "and",
    "expo",
    "show",
    "fair",
    "market",
    "conference",
    "event",
    "events",
    "annual",
}

SHOW_NAME_SMALL_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "or",
    "the",
    "to",
    "vs",
    "via",
    "with",
}

SHOW_NAME_PHRASE_OVERRIDES = {
    "Asd": "ASD",
    "Avixa": "AVIXA",
    "B2B": "B2B",
    "Icff": "ICFF",
    "Infocomm": "InfoComm",
    "Nacs": "NACS",
    "Nra": "NRA",
    "Usa": "USA",
}


def normalize_show_identity_tokens(value: str) -> tuple[str, ...]:
    normalized = normalize_show_identity_name(value)
    if not normalized:
        return ()
    tokens = [
        token
        for token in normalized.split()
        if token and token not in SHOW_NAME_NOISE_WORDS
    ]
    return tuple(dict.fromkeys(tokens))


def _normalize_show_display_token(token: str, *, is_first: bool, is_last: bool) -> str:
    segments = re.split(r"([-/])", token)
    normalized_segments: list[str] = []
    for segment in segments:
        if segment in {"-", "/"}:
            normalized_segments.append(segment)
            continue
        lowered = segment.lower()
        if lowered in SHOW_NAME_SMALL_WORDS and not is_first and not is_last:
            normalized_segments.append(lowered)
            continue
        normalized_segments.append(lowered[:1].upper() + lowered[1:])
    return "".join(normalized_segments)


def normalize_show_display_name(value: str) -> str:
    collapsed = re.sub(r"\s+", " ", value.strip())
    if not collapsed:
        return ""

    tokens = collapsed.split(" ")
    normalized_tokens = [
        _normalize_show_display_token(token, is_first=index == 0, is_last=index == len(tokens) - 1)
        for index, token in enumerate(tokens)
    ]
    normalized = " ".join(normalized_tokens)
    for source, target in SHOW_NAME_PHRASE_OVERRIDES.items():
        normalized = re.sub(rf"\b{re.escape(source)}\b", target, normalized)
    return normalized


def shows_have_matching_identity_name(left: str, right: str) -> bool:
    normalized_left = normalize_show_identity_name(left)
    normalized_right = normalize_show_identity_name(right)
    if not normalized_left or not normalized_right:
        return False
    if normalized_left == normalized_right:
        return True

    left_tokens = set(normalize_show_identity_tokens(left))
    right_tokens = set(normalize_show_identity_tokens(right))
    if not left_tokens or not right_tokens:
        return False
    if left_tokens == right_tokens:
        return True

    shared_tokens = left_tokens & right_tokens
    smaller_token_count = min(len(left_tokens), len(right_tokens))
    return smaller_token_count >= 2 and len(shared_tokens) == smaller_token_count


def show_identity_name_key(value: str) -> str:
    tokens = normalize_show_identity_tokens(value)
    if tokens:
        return " ".join(tokens)
    return normalize_show_identity_name(value)


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


def _date_window_bounds(event_date: date, *, days: int = DUPLICATE_SHOW_DATE_WINDOW_DAYS) -> tuple[date, date]:
    return event_date - timedelta(days=days), event_date + timedelta(days=days)


def _shows_represent_same_event(left: Show, right: Show) -> bool:
    if abs((left.event_date - right.event_date).days) > DUPLICATE_SHOW_DATE_WINDOW_DAYS:
        return False
    if shows_have_matching_identity_name(left.name, right.name):
        return True
    left_url = normalize_show_identity_url(left.source_url)
    right_url = normalize_show_identity_url(right.source_url)
    return bool(left_url and right_url and left_url == right_url)


def _show_status_rank(status: str) -> int:
    ranking = {
        ShowStatus.waiting.value: 0,
        ShowStatus.queued.value: 1,
        ShowStatus.scraping.value: 2,
        ShowStatus.failed.value: 3,
        ShowStatus.ready_for_review.value: 4,
        ShowStatus.approved.value: 5,
        ShowStatus.live.value: 6,
    }
    return ranking.get(status, -1)


def _show_quality_score(show: Show) -> tuple[int, int, int, int, int, int, int]:
    return (
        _show_status_rank(show.status),
        int(show.company_count or 0),
        int(bool(show.latest_export_path.strip())),
        int(bool(show.smartlead_campaign_id)),
        int(bool(show.clay_table_id.strip())),
        len(show.guide_rows),
        len(show.runs),
    )


def _merge_show_into_primary(primary: Show, duplicate: Show) -> None:
    primary_clay_rows = {row.clay_row_id: row for row in primary.clay_rows}
    for row in list(duplicate.clay_rows):
        existing = primary_clay_rows.get(row.clay_row_id)
        if existing is None:
            row.show = primary
            primary_clay_rows[row.clay_row_id] = row
            continue
        if row.imported_to_smartlead and not existing.imported_to_smartlead:
            existing.imported_to_smartlead = True
            existing.imported_at = row.imported_at or existing.imported_at
        existing.row_status = existing.row_status or row.row_status
        existing.email = existing.email or row.email
        existing.row_hash = existing.row_hash or row.row_hash
        existing.last_seen_at = max(existing.last_seen_at or datetime.min, row.last_seen_at or datetime.min)

    for row in list(duplicate.guide_rows):
        row.show = primary
    for run in list(duplicate.runs):
        run.show = primary

    if _show_quality_score(duplicate) > _show_quality_score(primary):
        primary.name = duplicate.name
        primary.place = duplicate.place
        primary.source_url = duplicate.source_url
        primary.event_date = duplicate.event_date
        primary.run_offset_days = duplicate.run_offset_days
        primary.run_at = duplicate.run_at
        primary.status = duplicate.status

    if duplicate.company_count > primary.company_count:
        primary.company_count = duplicate.company_count
        primary.failure_count = duplicate.failure_count
        if duplicate.latest_export_path.strip():
            primary.latest_export_path = duplicate.latest_export_path
    if not primary.latest_export_path.strip() and duplicate.latest_export_path.strip():
        primary.latest_export_path = duplicate.latest_export_path
    if not primary.enriched_export_path.strip() and duplicate.enriched_export_path.strip():
        primary.enriched_export_path = duplicate.enriched_export_path
    if not primary.smartlead_ready_export_path.strip() and duplicate.smartlead_ready_export_path.strip():
        primary.smartlead_ready_export_path = duplicate.smartlead_ready_export_path
    if not primary.smartlead_campaign_id and duplicate.smartlead_campaign_id:
        primary.smartlead_campaign_id = duplicate.smartlead_campaign_id
        primary.smartlead_campaign_name = duplicate.smartlead_campaign_name
    if not primary.clay_table_id.strip() and duplicate.clay_table_id.strip():
        primary.clay_table_id = duplicate.clay_table_id
        primary.clay_table_name = duplicate.clay_table_name
        primary.clay_table_url = duplicate.clay_table_url
    primary.clay_total_rows = max(primary.clay_total_rows, duplicate.clay_total_rows)
    primary.clay_ready_rows = max(primary.clay_ready_rows, duplicate.clay_ready_rows)
    primary.clay_failed_rows = max(primary.clay_failed_rows, duplicate.clay_failed_rows)
    primary.clay_skipped_rows = max(primary.clay_skipped_rows, duplicate.clay_skipped_rows)
    primary.smartlead_imported_rows = max(primary.smartlead_imported_rows, duplicate.smartlead_imported_rows)
    if not primary.last_error.strip() and duplicate.last_error.strip():
        primary.last_error = duplicate.last_error


def _remove_show_files(show: Show, *, keep_paths: set[str] | None = None) -> None:
    keep = {path.strip() for path in (keep_paths or set()) if path.strip()}
    raw_paths = {
        show.latest_export_path.strip(),
        show.enriched_export_path.strip(),
        show.smartlead_ready_export_path.strip(),
    }
    for raw_path in raw_paths:
        if not raw_path or raw_path in keep:
            continue
        try:
            Path(raw_path).expanduser().unlink(missing_ok=True)
        except OSError:
            continue


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
    window_start, window_end = _date_window_bounds(event_date)
    candidates = list(
        db.scalars(
            select(Show).where(
                Show.event_date >= window_start,
                Show.event_date <= window_end,
            )
        )
    )
    best_match: Show | None = None
    best_score: tuple[int, int] | None = None
    for candidate in candidates:
        if exclude_show_id is not None and candidate.id == exclude_show_id:
            continue
        matches_name = shows_have_matching_identity_name(candidate.name, normalized_name)
        matches_url = normalized_url and normalize_show_identity_url(candidate.source_url) == normalized_url
        if not matches_name and not matches_url:
            continue
        score = (
            0 if candidate.event_date == event_date else abs((candidate.event_date - event_date).days),
            0 if matches_url else 1,
        )
        if best_score is None or score < best_score:
            best_match = candidate
            best_score = score
    return best_match


def find_matching_show(
    db: Session,
    *,
    show_name: str,
    event_date_raw: str,
    link: str,
    exclude_show_id: int | None = None,
) -> Show | None:
    event_date = parse_show_date(event_date_raw)
    return _find_matching_show(
        db,
        show_name=show_name,
        event_date=event_date,
        link=link,
        exclude_show_id=exclude_show_id,
    )


def upsert_show(
    db: Session,
    *,
    show_name: str,
    event_date_raw: str,
    place: str,
    link: str,
    run_offset_days: int,
) -> tuple[Show, bool]:
    normalized_name = normalize_show_display_name(show_name)
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
    normalized_name = normalize_show_display_name(show_name)
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
    agent_mode: str = "fallback",
    workers: int | None = None,
) -> DirectScrapeResult:
    settings = get_settings()
    minimum_company_count = settings.min_scrape_company_count
    attempt_specs = (
        {
            "browser_mode": browser_mode,
            "agent_mode": agent_mode,
            "require_website": require_website,
            "label": "default",
        },
        {
            "browser_mode": "prefer",
            "agent_mode": "fallback",
            "require_website": True,
            "label": "browser_retry",
        },
        {
            "browser_mode": "prefer",
            "agent_mode": "always",
            "require_website": True,
            "label": "agent_retry",
        },
        {
            "browser_mode": "prefer",
            "agent_mode": "always",
            "require_website": False,
            "label": "agent_retry_relaxed",
        },
    )
    best_result: DirectScrapeResult | None = None
    failure_messages: list[str] = []
    seen_attempts: set[tuple[str, str, bool]] = set()
    for attempt in attempt_specs:
        attempt_key = (
            str(attempt["browser_mode"]),
            str(attempt["agent_mode"]),
            bool(attempt["require_website"]),
        )
        if attempt_key in seen_attempts:
            continue
        seen_attempts.add(attempt_key)
        try:
            result = _run_direct_scrape_once(
                show_name=show_name,
                place=place,
                link=link,
                output_path=output_path,
                require_website=bool(attempt["require_website"]),
                browser_mode=str(attempt["browser_mode"]),
                agent_mode=str(attempt["agent_mode"]),
                workers=workers,
            )
        except Exception as exc:  # noqa: BLE001
            failure_messages.append(f"{attempt['label']}: {exc}")
            continue
        if best_result is None or result.company_count > best_result.company_count:
            best_result = result
        if result.company_count >= minimum_company_count:
            return result
        failure_messages.append(
            f"{attempt['label']}: only found {result.company_count} exhibitors"
        )

    if best_result is not None:
        raise RuntimeError(
            f"Scrape quality gate failed for {show_name}. "
            f"Best attempt found {best_result.company_count} exhibitors; need at least {minimum_company_count}. "
            f"Attempts: {'; '.join(failure_messages)}"
        )
    raise RuntimeError(
        f"Scrape failed for {show_name}. Attempts: {'; '.join(failure_messages) or 'no successful scrape attempt'}"
    )


def _run_direct_scrape_once(
    *,
    show_name: str,
    place: str,
    link: str,
    output_path: Path,
    require_website: bool = True,
    browser_mode: str = "auto",
    agent_mode: str = "fallback",
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
            agent_mode=agent_mode,
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
            show_identity_name_key(show_name),
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
            show_identity_name_key(show_name),
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
    collapse_duplicate_shows(db)
    shows = list(
        db.scalars(
            select(Show)
            .options(selectinload(Show.runs), selectinload(Show.clay_rows), selectinload(Show.guide_rows))
            .order_by(Show.event_date.asc(), Show.created_at.desc())
        )
    )
    now = datetime.now()
    names_updated = False
    status_updated = False
    for show in shows:
        normalized_name = normalize_show_display_name(show.name)
        if normalized_name and normalized_name != show.name:
            show.name = normalized_name
            names_updated = True
        if reconcile_show_runtime_state(show, now=now):
            status_updated = True
    if names_updated or status_updated:
        db.commit()
    return shows


def purge_show(db: Session, show: Show) -> None:
    _remove_show_files(show)
    for row in list(show.guide_rows):
        db.delete(row)
    for row in list(show.clay_rows):
        db.delete(row)
    for run in list(show.runs):
        db.delete(run)
    db.delete(show)


def collapse_duplicate_shows(db: Session) -> int:
    shows = list(
        db.scalars(
            select(Show)
            .options(selectinload(Show.runs), selectinload(Show.clay_rows), selectinload(Show.guide_rows))
            .order_by(Show.event_date.asc(), Show.created_at.asc(), Show.id.asc())
        )
    )
    merged_count = 0
    for index, show in enumerate(shows):
        if db.get(Show, show.id) is None:
            continue
        for other in shows[index + 1 :]:
            if db.get(Show, other.id) is None:
                continue
            if not _shows_represent_same_event(show, other):
                continue
            primary = show
            duplicate = other
            if _show_quality_score(other) > _show_quality_score(show):
                primary = other
                duplicate = show
            keep_paths = {
                primary.latest_export_path.strip(),
                primary.enriched_export_path.strip(),
                primary.smartlead_ready_export_path.strip(),
            }
            _merge_show_into_primary(primary, duplicate)
            _remove_show_files(duplicate, keep_paths=keep_paths)
            db.delete(duplicate)
            show = primary
            merged_count += 1
    if merged_count:
        db.commit()
    return merged_count


def show_needs_scrape(show: Show, *, minimum_company_count: int | None = None) -> bool:
    threshold = minimum_company_count or get_settings().min_scrape_company_count
    if show.status in {ShowStatus.queued.value, ShowStatus.scraping.value}:
        return False
    if not show.source_url.strip():
        return False

    export_exists = False
    if show.latest_export_path.strip():
        export_exists = Path(show.latest_export_path).expanduser().exists()
    return not export_exists or int(show.company_count or 0) < threshold


def build_pending_scrape_queue(db: Session) -> list[QueuedBulkShow]:
    queued_shows: list[QueuedBulkShow] = []
    for show in list_shows(db):
        if not show_needs_scrape(show):
            continue
        queued_shows.append(
            QueuedBulkShow(
                show_id=show.id,
                show_name=show.name,
                event_date_raw=show.event_date.isoformat(),
                place=show.place,
                link=show.source_url,
            )
        )
    return queued_shows


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


def remove_show_from_queue(db: Session, show: Show, *, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    reconcile_show_runtime_state(show, now=now)
    if show.status == ShowStatus.scraping.value:
        return False

    queued_or_running_runs = [
        run
        for run in list(show.runs)
        if run.status in {RunStatus.queued.value, RunStatus.running.value}
    ]
    for run in queued_or_running_runs:
        if run in show.runs:
            show.runs.remove(run)
        db.delete(run)

    show.status = ShowStatus.waiting.value
    baseline_run_at = show.run_at or now
    deferred_run_at = now + timedelta(days=1)
    show.run_at = baseline_run_at if baseline_run_at > deferred_run_at else deferred_run_at
    return True


def reconcile_show_runtime_state(show: Show, *, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    running_runs = [run for run in show.runs if run.status == RunStatus.running.value]
    queued_runs = [run for run in show.runs if run.status == RunStatus.queued.value]

    original_status = show.status

    if show.status == ShowStatus.scraping.value and not running_runs:
        if queued_runs:
            show.status = ShowStatus.queued.value
        elif show.company_count > 0 and show.latest_export_path.strip():
            show.status = ShowStatus.ready_for_review.value
        else:
            show.status = ShowStatus.failed.value if show.last_error.strip() else ShowStatus.waiting.value

    return show.status != original_status


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


def delete_show_export_rows(show: Show, row_keys: list[str]) -> int:
    export_path_raw = (show.latest_export_path or "").strip()
    if not export_path_raw:
        raise ValueError("This show does not have an export yet.")

    export_path = Path(export_path_raw).expanduser()
    if not export_path.exists():
        raise ValueError("The lead CSV file no longer exists.")

    unique_keys = {key.strip() for key in row_keys if key.strip()}
    if not unique_keys:
        return show.company_count or 0

    with export_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        kept_rows: list[dict[str, str]] = []
        for raw_row in reader:
            normalized = {
                re.sub(r"[^a-z0-9]+", "_", key.strip().lower()).strip("_"): (value or "").strip()
                for key, value in raw_row.items()
                if key is not None
            }
            if _company_row_key(normalized) in unique_keys:
                continue
            kept_rows.append({key: value or "" for key, value in raw_row.items() if key is not None})

    _write_csv(export_path, kept_rows, fieldnames)
    show.company_count = len(kept_rows)
    return len(kept_rows)


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
        result = _run_direct_scrape(
            show_name=show.name,
            place=show.place,
            link=show.source_url,
            output_path=export_path_for_show(show),
            require_website=True,
            browser_mode=get_settings().default_browser_mode,
            workers=get_settings().default_scraper_workers,
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
    campaign_run.failure_count = result.failure_count
    campaign_run.finished_at = datetime.now()
    show.latest_export_path = str(result.output_path)
    show.company_count = result.company_count
    show.failure_count = result.failure_count
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
