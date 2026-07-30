from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from sqlalchemy import Date, DateTime, Integer, String, Text, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from app.config import get_settings
from app.database import Base


DEFAULT_TIMEZONE = "America/New_York"
DEFAULT_REFRESH_HOUR = 6
LOOKBACK_DAYS = 30
MINIMUM_SAMPLE = 10
PAGE_LIMIT = 500
LEADERBOARD_OWNER_LIMIT = 10
DEAL_INCLUDE_FIELDS = (
    "activities_count",
    "done_activities_count",
    "undone_activities_count",
    "next_activity_id",
)

_DAY_LABELS = ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")


class PipedriveAnalyticsError(RuntimeError):
    """Raised when a safe, complete analytics snapshot cannot be produced."""


class PipedriveAnalyticsSnapshot(Base):
    __tablename__ = "pipedrive_analytics_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True)
    report_date: Mapped[date] = mapped_column(Date(), unique=True, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    timezone_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source_count: Mapped[int] = mapped_column(Integer(), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text(), nullable=False)

    def payload(self) -> dict[str, Any]:
        value = json.loads(self.payload_json)
        if not isinstance(value, dict):
            raise PipedriveAnalyticsError("Stored Pipedrive analytics payload is not an object")
        return value


class _PipedriveReadApi:
    def __init__(
        self,
        *,
        api_token: str,
        base_url: str,
        client: Any | None,
    ) -> None:
        self.base_url = _api_root(base_url)
        self._owns_client = client is None
        self.client = client or httpx.Client(
            headers={"x-api-token": api_token},
            timeout=45.0,
            follow_redirects=True,
        )

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def get(self, path: str, *, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        try:
            response = self.client.request(
                "GET",
                f"{self.base_url}{path}",
                params=dict(params or {}),
            )
            if isinstance(response, dict):
                body: Any = response
            else:
                response.raise_for_status()
                body = response.json()
        except PipedriveAnalyticsError:
            raise
        except Exception as exc:
            raise PipedriveAnalyticsError(f"Pipedrive GET {path} failed") from exc

        if not isinstance(body, dict):
            raise PipedriveAnalyticsError(
                f"Pipedrive returned a non-object response for GET {path}"
            )
        if body.get("success") is False:
            raise PipedriveAnalyticsError(f"Pipedrive rejected GET {path}")
        return body


def build_payload(
    deals: Sequence[Mapping[str, Any]],
    owner_names: Mapping[int, str] | Sequence[Mapping[str, Any]] | None = None,
    *,
    now: datetime | None = None,
    timezone_name: str | None = None,
    lookback_days: int = LOOKBACK_DAYS,
    minimum_sample: int = MINIMUM_SAMPLE,
) -> dict[str, Any]:
    """Build a privacy-minimized, screenshot-shaped analytics payload.

    ``deals`` may be raw Pipedrive deal records or the sanitized records
    returned by this module's reader. Only add time, owner ID, and activity
    counts are used. ``owner_names`` should contain active users only; when
    raw user records are supplied, inactive users and all fields except ID and
    display name are discarded.
    """

    if lookback_days < 1:
        raise ValueError("lookback_days must be at least 1")
    if minimum_sample < 1:
        raise ValueError("minimum_sample must be at least 1")

    tz_name, tz = _resolve_timezone(timezone_name)
    local_now = _coerce_now(now, tz)
    report_date = local_now.date()
    window_start = report_date - timedelta(days=lookback_days - 1)
    cutoff = datetime.combine(window_start, time.min, tzinfo=tz)
    active_owner_names = _normalize_owner_names(owner_names or {})

    events: list[tuple[datetime, int | None, bool]] = []
    for raw_deal in deals:
        if not isinstance(raw_deal, Mapping):
            raise PipedriveAnalyticsError("Pipedrive deal data contains a non-object record")
        added_at = _parse_add_time(raw_deal.get("add_time")).astimezone(tz)
        if added_at < cutoff or added_at > local_now:
            continue
        activities_count = _nonnegative_int(
            raw_deal.get("activities_count"),
            default=0,
            field="activities_count",
        )
        events.append(
            (
                added_at,
                _optional_id(
                    raw_deal.get("owner_id", raw_deal.get("user_id")),
                    field="owner_id",
                ),
                activities_count > 0,
            )
        )

    total_deals = len(events)
    total_followed_up = sum(1 for _added, _owner, followed in events if followed)

    hour_totals: Counter[int] = Counter()
    hour_followed: Counter[int] = Counter()
    weekday_totals: Counter[int] = Counter()
    weekday_followed: Counter[int] = Counter()
    cell_totals: Counter[tuple[int, int]] = Counter()
    cell_followed: Counter[tuple[int, int]] = Counter()
    owner_totals: Counter[int] = Counter()
    owner_followed: Counter[int] = Counter()
    owner_day_totals: Counter[tuple[int, date]] = Counter()
    owner_day_followed: Counter[tuple[int, date]] = Counter()

    for added_at, owner_id, followed_up in events:
        hour = added_at.hour
        day_index = _sunday_day_index(added_at)
        hour_totals[hour] += 1
        weekday_totals[day_index] += 1
        cell_totals[(hour, day_index)] += 1
        if followed_up:
            hour_followed[hour] += 1
            weekday_followed[day_index] += 1
            cell_followed[(hour, day_index)] += 1

        if owner_id is not None and owner_id in active_owner_names:
            owner_totals[owner_id] += 1
            owner_day_totals[(owner_id, added_at.date())] += 1
            if followed_up:
                owner_followed[owner_id] += 1
                owner_day_followed[(owner_id, added_at.date())] += 1

    week_start = report_date - timedelta(days=report_date.weekday())
    leaderboard_dates = [
        week_start + timedelta(days=offset)
        for offset in range(5)
        if week_start + timedelta(days=offset) <= report_date
    ]
    leaderboard_days = [
        {"date": day.isoformat(), "label": f"{_DAY_LABELS[_sunday_day_index(day)]} {day.month}/{day.day}"}
        for day in leaderboard_dates
    ]

    ranked_owner_ids = sorted(
        owner_totals,
        key=lambda owner_id: (
            -owner_totals[owner_id],
            -(_coverage(owner_followed[owner_id], owner_totals[owner_id]) or 0.0),
            active_owner_names[owner_id].casefold(),
            owner_id,
        ),
    )[:LEADERBOARD_OWNER_LIMIT]
    leaderboard_rows: list[dict[str, Any]] = []
    for owner_id in ranked_owner_ids:
        day_cells = []
        for day in leaderboard_dates:
            deals_count = owner_day_totals[(owner_id, day)]
            followed_count = owner_day_followed[(owner_id, day)]
            day_cells.append(
                {
                    "date": day.isoformat(),
                    "deals": deals_count,
                    "followed_up": followed_count,
                    "coverage": _coverage(followed_count, deals_count),
                }
            )
        leaderboard_rows.append(
            {
                "owner_id": owner_id,
                "owner": active_owner_names[owner_id],
                "days": day_cells,
                "period_deals": owner_totals[owner_id],
                "period_coverage": _coverage(
                    owner_followed[owner_id],
                    owner_totals[owner_id],
                ),
            }
        )

    weekday_blended = []
    for day_index in range(1, 6):
        dates_in_window = _weekday_occurrences(
            window_start,
            report_date,
            day_index,
        )
        deals_count = weekday_totals[day_index]
        followed_count = weekday_followed[day_index]
        weekday_blended.append(
            {
                "day_index": day_index,
                "label": _DAY_LABELS[day_index],
                "deals": deals_count,
                "followed_up": followed_count,
                "coverage": _coverage(followed_count, deals_count),
                "avg_deals_per_day": round(deals_count / dates_in_window, 1)
                if dates_in_window
                else 0.0,
            }
        )

    hourly = [
        {
            "hour": hour,
            "label": _hour_label(hour),
            "deals": hour_totals[hour],
            "followed_up": hour_followed[hour],
            "coverage": _coverage(hour_followed[hour], hour_totals[hour]),
        }
        for hour in range(24)
    ]
    weekdays = [
        {
            "day_index": day_index,
            "label": _DAY_LABELS[day_index],
            "deals": weekday_totals[day_index],
            "followed_up": weekday_followed[day_index],
            "coverage": _coverage(
                weekday_followed[day_index],
                weekday_totals[day_index],
            ),
        }
        for day_index in range(7)
    ]
    heatmap = [
        {
            "hour": hour,
            "hour_label": _hour_label(hour),
            "day_index": day_index,
            "day_label": _DAY_LABELS[day_index],
            "deals": cell_totals[(hour, day_index)],
            "followed_up": cell_followed[(hour, day_index)],
            "coverage": _coverage(
                cell_followed[(hour, day_index)],
                cell_totals[(hour, day_index)],
            ),
        }
        for hour in range(24)
        for day_index in range(7)
    ]

    best_hour = _best_bucket(
        (
            (hour, hour_totals[hour], hour_followed[hour])
            for hour in range(24)
        ),
        minimum_sample=minimum_sample,
        label_for=lambda hour: _hour_label(int(hour)),
    )
    best_day = _best_bucket(
        (
            (
                day_index,
                weekday_totals[day_index],
                weekday_followed[day_index],
            )
            for day_index in range(7)
        ),
        minimum_sample=minimum_sample,
        label_for=lambda day_index: _DAY_LABELS[int(day_index)],
    )
    top_owner = _best_bucket(
        (
            (owner_id, owner_totals[owner_id], owner_followed[owner_id])
            for owner_id in owner_totals
        ),
        minimum_sample=minimum_sample,
        label_for=lambda owner_id: active_owner_names[int(owner_id)],
    )

    return {
        "report": {
            "date": report_date.isoformat(),
            "generated_at": local_now.isoformat(),
            "generated_at_display": _format_datetime(local_now),
            "timezone": tz_name,
            "lookback_days": lookback_days,
            "minimum_sample": minimum_sample,
            "window_start": window_start.isoformat(),
            "window_end": report_date.isoformat(),
            "definition": (
                "Coverage is the percentage of Pipedrive deals with one or more "
                "activities (activities_count > 0), based on deal add_time. "
                "Best-hour, best-day, and top-owner KPIs require the stated "
                "minimum sample. Owner names are active Pipedrive users only."
            ),
            "source_count": total_deals,
        },
        "kpis": {
            "total_deals": {
                "value": total_deals,
                "display": _format_count(total_deals),
                "followed_up": total_followed_up,
            },
            "coverage": {
                "value": _coverage(total_followed_up, total_deals),
                "display": _format_percent(
                    _coverage(total_followed_up, total_deals)
                ),
            },
            "best_hour": best_hour,
            "best_day": best_day,
            "top_owner": top_owner,
        },
        "leaderboard": {
            "days": leaderboard_days,
            "rows": leaderboard_rows,
        },
        "weekday_blended": weekday_blended,
        "hourly": hourly,
        "weekdays": weekdays,
        "heatmap": heatmap,
    }


def refresh_pipedrive_analytics(
    db: Session,
    *,
    client: Any | None = None,
    now: datetime | None = None,
    timezone_name: str | None = None,
    api_token: str | None = None,
    base_url: str | None = None,
    lookback_days: int | None = None,
    minimum_sample: int | None = None,
) -> dict[str, Any]:
    """Fetch and persist one immutable snapshot for the local report date."""

    settings = get_settings()
    token = settings.pipedrive_api_token if api_token is None else api_token.strip()
    if not token:
        raise PipedriveAnalyticsError(
            "PIPEDRIVE_API_TOKEN is required to refresh analytics"
        )
    resolved_base_url = (
        settings.pipedrive_base_url if base_url is None else base_url
    )
    tz_name, tz = _resolve_timezone(timezone_name)
    local_now = _coerce_now(now, tz)
    report_date = local_now.date()
    resolved_lookback_days = _resolve_positive_setting(
        lookback_days,
        env_name="PIPEDRIVE_ANALYTICS_LOOKBACK_DAYS",
        default=LOOKBACK_DAYS,
    )
    resolved_minimum_sample = _resolve_positive_setting(
        minimum_sample,
        env_name="PIPEDRIVE_ANALYTICS_MIN_SAMPLE",
        default=MINIMUM_SAMPLE,
    )

    existing = _get_snapshot_for_date(db, report_date)
    if existing is not None:
        return existing.payload()

    window_start = report_date - timedelta(days=resolved_lookback_days - 1)
    cutoff = datetime.combine(window_start, time.min, tzinfo=tz)
    api = _PipedriveReadApi(
        api_token=token,
        base_url=resolved_base_url,
        client=client,
    )
    try:
        owner_names = _fetch_active_owner_names(api)
        deals = _fetch_recent_deals(api, cutoff=cutoff)
    finally:
        api.close()

    payload = build_payload(
        deals,
        owner_names,
        now=local_now,
        timezone_name=tz_name,
        lookback_days=resolved_lookback_days,
        minimum_sample=resolved_minimum_sample,
    )
    snapshot = PipedriveAnalyticsSnapshot(
        report_date=report_date,
        generated_at=local_now.astimezone(timezone.utc),
        timezone_name=tz_name,
        source_count=int(payload["report"]["source_count"]),
        payload_json=json.dumps(payload, separators=(",", ":"), sort_keys=True),
    )
    db.add(snapshot)
    try:
        db.commit()
    except IntegrityError:
        # Another web/worker process may have won the unique report-date race.
        db.rollback()
        winner = _get_snapshot_for_date(db, report_date)
        if winner is not None:
            return winner.payload()
        raise
    db.refresh(snapshot)
    return payload


def get_latest_pipedrive_analytics(
    db: Session,
) -> dict[str, Any] | None:
    snapshot = db.execute(
        select(PipedriveAnalyticsSnapshot).order_by(
            PipedriveAnalyticsSnapshot.report_date.desc(),
            PipedriveAnalyticsSnapshot.generated_at.desc(),
        )
    ).scalars().first()
    return snapshot.payload() if snapshot is not None else None


def refresh_pipedrive_analytics_if_due(
    db: Session,
    *,
    client: Any | None = None,
    now: datetime | None = None,
    timezone_name: str | None = None,
    refresh_hour: int | None = None,
    api_token: str | None = None,
    base_url: str | None = None,
    lookback_days: int | None = None,
    minimum_sample: int | None = None,
) -> dict[str, Any] | None:
    """Refresh after the configured local hour, at most once per local date."""

    settings = get_settings()
    token = settings.pipedrive_api_token if api_token is None else api_token.strip()
    if not token:
        return None

    _tz_name, tz = _resolve_timezone(timezone_name)
    local_now = _coerce_now(now, tz)
    if _get_snapshot_for_date(db, local_now.date()) is not None:
        return None

    due_hour = _resolve_refresh_hour(refresh_hour)
    if local_now.hour < due_hour:
        return None
    resolved_lookback_days = _resolve_positive_setting(
        lookback_days,
        env_name="PIPEDRIVE_ANALYTICS_LOOKBACK_DAYS",
        default=LOOKBACK_DAYS,
    )
    resolved_minimum_sample = _resolve_positive_setting(
        minimum_sample,
        env_name="PIPEDRIVE_ANALYTICS_MIN_SAMPLE",
        default=MINIMUM_SAMPLE,
    )

    return refresh_pipedrive_analytics(
        db,
        client=client,
        now=local_now,
        timezone_name=_tz_name,
        api_token=token,
        base_url=base_url,
        lookback_days=resolved_lookback_days,
        minimum_sample=resolved_minimum_sample,
    )


def _fetch_recent_deals(
    api: _PipedriveReadApi,
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    while True:
        params: dict[str, Any] = {
            "sort_by": "add_time",
            "sort_direction": "desc",
            "include_fields": ",".join(DEAL_INCLUDE_FIELDS),
            "limit": PAGE_LIMIT,
        }
        if cursor is not None:
            params["cursor"] = cursor
        body = api.get("/api/v2/deals", params=params)
        data = body.get("data")
        if not isinstance(data, list):
            raise PipedriveAnalyticsError(
                "Pipedrive deals response is missing a data list"
            )

        reached_cutoff = False
        for raw_deal in data:
            if not isinstance(raw_deal, Mapping):
                raise PipedriveAnalyticsError(
                    "Pipedrive deals response contains a non-object record"
                )
            added_at = _parse_add_time(raw_deal.get("add_time"))
            if added_at.astimezone(cutoff.tzinfo) < cutoff:
                reached_cutoff = True
                break
            records.append(
                {
                    "add_time": raw_deal.get("add_time"),
                    "owner_id": raw_deal.get(
                        "owner_id",
                        raw_deal.get("user_id"),
                    ),
                    "activities_count": raw_deal.get("activities_count"),
                }
            )
        if reached_cutoff:
            return records

        cursor = _next_cursor(body)
        if cursor is None:
            return records
        if cursor in seen_cursors:
            raise PipedriveAnalyticsError(
                "Pipedrive deals response repeated a pagination cursor"
            )
        seen_cursors.add(cursor)


def _fetch_active_owner_names(api: _PipedriveReadApi) -> dict[int, str]:
    users: list[Mapping[str, Any]] = []
    start = 0
    while True:
        body = api.get(
            "/v1/users",
            params={"start": start, "limit": PAGE_LIMIT},
        )
        data = body.get("data")
        if not isinstance(data, list):
            raise PipedriveAnalyticsError(
                "Pipedrive users response is missing a data list"
            )
        for raw_user in data:
            if not isinstance(raw_user, Mapping):
                raise PipedriveAnalyticsError(
                    "Pipedrive users response contains a non-object record"
                )
            users.append(raw_user)

        pagination = _v1_pagination(body)
        if not pagination.get("more_items_in_collection"):
            break
        next_start = _optional_id(
            pagination.get("next_start"),
            field="next_start",
        )
        if next_start is None or next_start <= start:
            raise PipedriveAnalyticsError(
                "Pipedrive users response contains invalid pagination"
            )
        start = next_start
    return _normalize_owner_names(users)


def _normalize_owner_names(
    owners: Mapping[int, str] | Sequence[Mapping[str, Any]],
) -> dict[int, str]:
    normalized: dict[int, str] = {}
    if isinstance(owners, Mapping):
        for raw_id, raw_name in owners.items():
            owner_id = _optional_id(raw_id, field="owner_id")
            name = str(raw_name or "").strip()
            if owner_id is not None and name:
                normalized[owner_id] = name
        return normalized

    for raw_owner in owners:
        if not isinstance(raw_owner, Mapping):
            raise PipedriveAnalyticsError(
                "Pipedrive users response contains a non-object record"
            )
        if not _is_active(raw_owner.get("active_flag")):
            continue
        owner_id = _optional_id(raw_owner.get("id"), field="owner_id")
        name = str(raw_owner.get("name") or "").strip()
        if owner_id is not None and name:
            normalized[owner_id] = name
    return normalized


def _best_bucket(
    buckets: Any,
    *,
    minimum_sample: int,
    label_for: Any,
) -> dict[str, Any]:
    eligible = [
        (key, count, followed)
        for key, count, followed in buckets
        if count >= minimum_sample
    ]
    if not eligible:
        return {"label": None, "coverage": None, "count": 0}
    key, count, followed = max(
        eligible,
        key=lambda item: (
            _coverage(item[2], item[1]) or 0.0,
            item[1],
            -int(item[0]),
        ),
    )
    return {
        "label": label_for(key),
        "coverage": _coverage(followed, count),
        "count": count,
    }


def _api_root(base_url: str) -> str:
    root = str(base_url or "").strip().rstrip("/")
    if not root:
        raise PipedriveAnalyticsError("Pipedrive base URL is empty")
    lowered = root.casefold()
    for suffix in ("/api/v2", "/api/v1", "/v2", "/v1"):
        if lowered.endswith(suffix):
            return root[: -len(suffix)]
    return root


def _parse_add_time(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise PipedriveAnalyticsError(
            "Pipedrive deal record is missing a valid add_time"
        )
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise PipedriveAnalyticsError(
            "Pipedrive deal record contains an invalid add_time"
        ) from exc
    # Pipedrive's offset-less add_time values are UTC.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _resolve_timezone(
    timezone_name: str | None,
) -> tuple[str, ZoneInfo]:
    name = (
        timezone_name
        if timezone_name is not None
        else os.getenv("PIPEDRIVE_ANALYTICS_TIMEZONE", DEFAULT_TIMEZONE)
    )
    name = str(name).strip() or DEFAULT_TIMEZONE
    try:
        return name, ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise PipedriveAnalyticsError(
            f"Unknown Pipedrive analytics timezone: {name}"
        ) from exc


def _resolve_refresh_hour(refresh_hour: int | None) -> int:
    raw: Any = (
        refresh_hour
        if refresh_hour is not None
        else os.getenv(
            "PIPEDRIVE_ANALYTICS_REFRESH_HOUR",
            str(DEFAULT_REFRESH_HOUR),
        )
    )
    try:
        hour = int(raw)
    except (TypeError, ValueError) as exc:
        raise PipedriveAnalyticsError(
            "PIPEDRIVE_ANALYTICS_REFRESH_HOUR must be an integer from 0 to 23"
        ) from exc
    if hour < 0 or hour > 23:
        raise PipedriveAnalyticsError(
            "PIPEDRIVE_ANALYTICS_REFRESH_HOUR must be from 0 to 23"
        )
    return hour


def _resolve_positive_setting(
    value: int | None,
    *,
    env_name: str,
    default: int,
) -> int:
    raw: Any = value if value is not None else os.getenv(env_name, str(default))
    if isinstance(raw, bool):
        raise PipedriveAnalyticsError(f"{env_name} must be a positive integer")
    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise PipedriveAnalyticsError(
            f"{env_name} must be a positive integer"
        ) from exc
    if parsed < 1:
        raise PipedriveAnalyticsError(f"{env_name} must be a positive integer")
    return parsed


def _coerce_now(value: datetime | None, tz: ZoneInfo) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=tz)
    return current.astimezone(tz)


def _get_snapshot_for_date(
    db: Session,
    report_date: date,
) -> PipedriveAnalyticsSnapshot | None:
    return db.execute(
        select(PipedriveAnalyticsSnapshot).where(
            PipedriveAnalyticsSnapshot.report_date == report_date
        )
    ).scalar_one_or_none()


def _next_cursor(body: Mapping[str, Any]) -> str | None:
    additional = body.get("additional_data")
    if not isinstance(additional, Mapping):
        return None
    raw_cursor = additional.get("next_cursor")
    if raw_cursor in (None, ""):
        pagination = additional.get("pagination")
        if isinstance(pagination, Mapping):
            raw_cursor = pagination.get("next_cursor")
    if raw_cursor in (None, ""):
        return None
    if not isinstance(raw_cursor, (str, int)):
        raise PipedriveAnalyticsError(
            "Pipedrive deals response contains an invalid pagination cursor"
        )
    return str(raw_cursor)


def _v1_pagination(body: Mapping[str, Any]) -> Mapping[str, Any]:
    additional = body.get("additional_data")
    if not isinstance(additional, Mapping):
        return {}
    pagination = additional.get("pagination")
    return pagination if isinstance(pagination, Mapping) else {}


def _optional_id(value: Any, *, field: str) -> int | None:
    if isinstance(value, Mapping):
        value = value.get("id")
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise PipedriveAnalyticsError(f"Pipedrive {field} is not a valid integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise PipedriveAnalyticsError(
            f"Pipedrive {field} is not a valid integer"
        ) from exc


def _nonnegative_int(value: Any, *, default: int, field: str) -> int:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        raise PipedriveAnalyticsError(f"Pipedrive {field} is not a valid count")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise PipedriveAnalyticsError(
            f"Pipedrive {field} is not a valid count"
        ) from exc
    if parsed < 0:
        raise PipedriveAnalyticsError(
            f"Pipedrive {field} is not a valid count"
        )
    return parsed


def _is_active(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes"}
    return value is True or value == 1


def _sunday_day_index(value: datetime | date) -> int:
    return (value.weekday() + 1) % 7


def _weekday_occurrences(
    start: date,
    end: date,
    sunday_day_index: int,
) -> int:
    return sum(
        1
        for offset in range((end - start).days + 1)
        if _sunday_day_index(start + timedelta(days=offset))
        == sunday_day_index
    )


def _coverage(followed_up: int, total: int) -> float | None:
    if total <= 0:
        return None
    return round((followed_up / total) * 100.0, 1)


def _hour_label(hour: int) -> str:
    suffix = "am" if hour < 12 else "pm"
    display_hour = hour % 12 or 12
    return f"{display_hour}{suffix}"


def _format_count(value: int) -> str:
    if value < 1_000:
        return str(value)
    compact = round(value / 1_000, 1)
    return f"{compact:g}K"


def _format_percent(value: float | None) -> str:
    return "—" if value is None else f"{value:g}%"


def _format_datetime(value: datetime) -> str:
    clock = value.strftime("%I:%M %p").lstrip("0")
    return f"{value.strftime('%b')} {value.day}, {value.year} at {clock}"
