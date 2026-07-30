from __future__ import annotations

import json
import logging
import os
import time as time_module
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from sqlalchemy import Date, DateTime, Integer, String, Text, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

from app.database import Base


DEFAULT_TIMEZONE = "America/New_York"
DEFAULT_REFRESH_HOURS = (9, 13, 17)
DEFAULT_OPENPHONE_BASE_URL = "https://api.openphone.com"
LOOKBACK_DAYS = 30
BLENDED_LOOKBACK_DAYS = 28
TOP_REP_LOOKBACK_DAYS = 7
CONNECTED_DURATION_SECONDS = 90
MINIMUM_SAMPLE = 15
USER_PAGE_LIMIT = 50
API_PAGE_LIMIT = 100
LEADERBOARD_REP_LIMIT = 10
REQUEST_INTERVAL_SECONDS = 0.125
MAX_REQUEST_ATTEMPTS = 4
PAYLOAD_SCHEMA_VERSION = "openphone-calls-v1"

_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
_OUTBOUND_DIRECTIONS = frozenset({"outgoing", "outbound"})
_INBOUND_DIRECTIONS = frozenset({"incoming", "inbound"})
_DAY_LABELS = ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")

# HTTPX logs full query URLs at INFO, including the participant number required
# by OpenPhone's calls endpoint. Keep those transient lookup values out of
# application and Render logs.
logging.getLogger("httpx").setLevel(logging.WARNING)


class PipedriveAnalyticsError(RuntimeError):
    """Raised when a safe, complete call-analytics snapshot cannot be produced."""


class PipedriveAnalyticsSnapshot(Base):
    """Legacy table name retained so the deployed database needs no migration."""

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
            raise PipedriveAnalyticsError("Stored call analytics payload is not an object")
        return value


class _OpenPhoneReadApi:
    """Small read-only API client with bounded retries and sub-10/sec pacing."""

    def __init__(
        self,
        *,
        api_token: str,
        base_url: str,
        client: Any | None,
        sleep_fn: Callable[[float], None] | None = None,
        monotonic_fn: Callable[[], float] | None = None,
        request_interval_seconds: float = REQUEST_INTERVAL_SECONDS,
    ) -> None:
        self.base_url = _api_root(base_url)
        self._owns_client = client is None
        self.client = client or httpx.Client(
            headers={"Authorization": api_token},
            timeout=45.0,
            follow_redirects=True,
        )
        self._sleep = sleep_fn or time_module.sleep
        self._monotonic = monotonic_fn or time_module.monotonic
        self._request_interval_seconds = max(
            REQUEST_INTERVAL_SECONDS,
            float(request_interval_seconds),
        )
        self._last_request_at: float | None = None

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def get(self, path: str, *, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        for attempt in range(MAX_REQUEST_ATTEMPTS):
            self._wait_for_request_slot()
            try:
                response = self.client.request(
                    "GET",
                    f"{self.base_url}{path}",
                    params=dict(params or {}),
                )
            except Exception as exc:
                if attempt + 1 >= MAX_REQUEST_ATTEMPTS:
                    raise PipedriveAnalyticsError(
                        f"OpenPhone GET {path} failed after retries"
                    ) from None
                self._sleep(_retry_delay(attempt, None))
                continue

            if isinstance(response, dict):
                body: Any = response
                status_code = 200
            else:
                status_code = int(getattr(response, "status_code", 0) or 0)
                if (
                    status_code in _RETRYABLE_STATUS_CODES
                    and attempt + 1 < MAX_REQUEST_ATTEMPTS
                ):
                    self._sleep(
                        _retry_delay(
                            attempt,
                            _retry_after_seconds(getattr(response, "headers", None)),
                        )
                    )
                    continue
                if status_code >= 400:
                    raise PipedriveAnalyticsError(
                        f"OpenPhone rejected GET {path} (HTTP {status_code})"
                    )
                try:
                    body = response.json()
                except Exception as exc:
                    raise PipedriveAnalyticsError(
                        f"OpenPhone returned invalid JSON for GET {path}"
                    ) from exc

            if not isinstance(body, dict):
                raise PipedriveAnalyticsError(
                    f"OpenPhone returned a non-object response for GET {path}"
                )
            return body

        raise PipedriveAnalyticsError(f"OpenPhone GET {path} failed after retries")

    def _wait_for_request_slot(self) -> None:
        now = self._monotonic()
        if self._last_request_at is not None:
            wait_seconds = (
                self._last_request_at + self._request_interval_seconds - now
            )
            if wait_seconds > 0:
                self._sleep(wait_seconds)
                now = self._monotonic()
        self._last_request_at = now


def build_payload(
    calls: Sequence[Mapping[str, Any]],
    user_names: Mapping[str, str] | Sequence[Mapping[str, Any]] | None = None,
    *,
    now: datetime | None = None,
    timezone_name: str | None = None,
    lookback_days: int = LOOKBACK_DAYS,
    minimum_sample: int = MINIMUM_SAMPLE,
) -> dict[str, Any]:
    """Aggregate privacy-minimized OpenPhone call records for the dashboard.

    Only outbound calls are counted. A call is connected exactly when its
    status is ``completed`` and its duration is at least 90 seconds. Windows
    contain full local calendar days and end at the report date's midnight.
    """

    if lookback_days < 1:
        raise ValueError("lookback_days must be at least 1")
    if minimum_sample < 1:
        raise ValueError("minimum_sample must be at least 1")

    tz_name, tz = _resolve_timezone(timezone_name)
    local_now = _coerce_now(now, tz)
    report_date = local_now.date()
    window_end = datetime.combine(report_date, time.min, tzinfo=tz)
    window_start = window_end - timedelta(days=lookback_days)
    blended_start = window_end - timedelta(days=BLENDED_LOOKBACK_DAYS)
    rolling_start = window_end - timedelta(days=TOP_REP_LOOKBACK_DAYS)
    active_user_names = _normalize_user_names(user_names or {})

    events: list[tuple[datetime, str | None, bool]] = []
    for raw_call in calls:
        if not isinstance(raw_call, Mapping):
            raise PipedriveAnalyticsError(
                "OpenPhone call data contains a non-object record"
            )
        raw_direction = raw_call.get("direction")
        if not isinstance(raw_direction, str) or not raw_direction.strip():
            raise PipedriveAnalyticsError(
                "OpenPhone call record is missing a valid direction"
            )
        direction = raw_direction.strip().casefold()
        if direction not in _OUTBOUND_DIRECTIONS | _INBOUND_DIRECTIONS:
            raise PipedriveAnalyticsError(
                "OpenPhone call record contains an invalid direction"
            )
        if direction in _INBOUND_DIRECTIONS:
            continue
        created_at = _parse_datetime(
            raw_call.get("createdAt", raw_call.get("created_at")),
            field="createdAt",
        ).astimezone(tz)
        if created_at < window_start or created_at >= window_end:
            continue
        duration = _duration_seconds(raw_call.get("duration"))
        raw_status = raw_call.get("status")
        if not isinstance(raw_status, str) or not raw_status.strip():
            raise PipedriveAnalyticsError(
                "OpenPhone call record is missing a valid status"
            )
        status = raw_status.strip().casefold()
        connected = (
            status == "completed" and duration >= CONNECTED_DURATION_SECONDS
        )
        user_id = _optional_string_id(
            raw_call.get("initiatedBy")
            or raw_call.get("userId", raw_call.get("user_id")),
            field="userId",
        )
        events.append((created_at, user_id, connected))

    total_calls = len(events)
    total_connected = sum(1 for _created, _user, connected in events if connected)

    hour_totals: Counter[int] = Counter()
    hour_connected: Counter[int] = Counter()
    weekday_totals: Counter[int] = Counter()
    weekday_connected: Counter[int] = Counter()
    blended_weekday_totals: Counter[int] = Counter()
    blended_weekday_connected: Counter[int] = Counter()
    cell_totals: Counter[tuple[int, int]] = Counter()
    cell_connected: Counter[tuple[int, int]] = Counter()
    user_totals: Counter[str] = Counter()
    user_connected: Counter[str] = Counter()
    user_day_totals: Counter[tuple[str, date]] = Counter()
    user_day_connected: Counter[tuple[str, date]] = Counter()

    for created_at, user_id, connected in events:
        hour = created_at.hour
        day_index = _sunday_day_index(created_at)
        hour_totals[hour] += 1
        weekday_totals[day_index] += 1
        if connected:
            hour_connected[hour] += 1
            weekday_connected[day_index] += 1

        if created_at >= blended_start:
            blended_weekday_totals[day_index] += 1
            cell_totals[(hour, day_index)] += 1
            if connected:
                blended_weekday_connected[day_index] += 1
                cell_connected[(hour, day_index)] += 1

        if (
            created_at >= rolling_start
            and user_id is not None
            and user_id in active_user_names
        ):
            user_totals[user_id] += 1
            user_day_totals[(user_id, created_at.date())] += 1
            if connected:
                user_connected[user_id] += 1
                user_day_connected[(user_id, created_at.date())] += 1

    week_start = report_date - timedelta(days=report_date.weekday())
    leaderboard_dates = [
        week_start + timedelta(days=offset)
        for offset in range((report_date - week_start).days)
    ]
    leaderboard_days = [
        {
            "date": day.isoformat(),
            "label": f"{_DAY_LABELS[_sunday_day_index(day)]} {day.month}/{day.day}",
        }
        for day in leaderboard_dates
    ]

    ranked_user_ids = sorted(
        user_totals,
        key=lambda user_id: (
            -user_totals[user_id],
            -(_connect_rate(user_connected[user_id], user_totals[user_id]) or 0.0),
            active_user_names[user_id].casefold(),
            user_id,
        ),
    )[:LEADERBOARD_REP_LIMIT]
    leaderboard_rows: list[dict[str, Any]] = []
    for user_id in ranked_user_ids:
        day_cells: list[dict[str, Any]] = []
        for day in leaderboard_dates:
            call_count = user_day_totals[(user_id, day)]
            connected_count = user_day_connected[(user_id, day)]
            day_cells.append(
                {
                    "date": day.isoformat(),
                    "calls": call_count,
                    "connected": connected_count,
                    "connect_rate": (
                        _connect_rate(connected_count, call_count)
                        if call_count
                        else 0.0
                    ),
                }
            )
        period_calls = user_totals[user_id]
        period_connected = user_connected[user_id]
        leaderboard_rows.append(
            {
                "rep": active_user_names[user_id],
                "days": day_cells,
                "period_calls": period_calls,
                "period_connected": period_connected,
                "period_connect_rate": _connect_rate(
                    period_connected,
                    period_calls,
                ),
            }
        )

    blended_window_end_date = report_date - timedelta(days=1)
    weekday_blended = []
    for day_index in range(1, 6):
        dates_in_window = _weekday_occurrences(
            blended_start.date(),
            blended_window_end_date,
            day_index,
        )
        call_count = blended_weekday_totals[day_index]
        connected_count = blended_weekday_connected[day_index]
        weekday_blended.append(
            {
                "day_index": day_index,
                "label": _DAY_LABELS[day_index],
                "calls": call_count,
                "connected": connected_count,
                "connect_rate": _connect_rate(connected_count, call_count),
                "avg_calls_per_day": (
                    round(call_count / dates_in_window, 1)
                    if dates_in_window
                    else 0.0
                ),
            }
        )

    hourly = [
        {
            "hour": hour,
            "label": _hour_label(hour),
            "calls": hour_totals[hour],
            "connected": hour_connected[hour],
            "connect_rate": _connect_rate(
                hour_connected[hour],
                hour_totals[hour],
            ),
        }
        for hour in range(24)
    ]
    weekdays = [
        {
            "day_index": day_index,
            "label": _DAY_LABELS[day_index],
            "calls": weekday_totals[day_index],
            "connected": weekday_connected[day_index],
            "connect_rate": _connect_rate(
                weekday_connected[day_index],
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
            "calls": cell_totals[(hour, day_index)],
            "connected": cell_connected[(hour, day_index)],
            "connect_rate": _connect_rate(
                cell_connected[(hour, day_index)],
                cell_totals[(hour, day_index)],
            ),
        }
        for hour in range(24)
        for day_index in range(7)
    ]

    best_hour_key, best_hour = _best_bucket(
        (
            (hour, hour_totals[hour], hour_connected[hour])
            for hour in range(24)
        ),
        minimum_sample=minimum_sample,
        label_for=lambda hour: _hour_label(int(hour)),
    )
    best_hour["hour"] = best_hour_key

    best_day_key, best_day = _best_bucket(
        (
            (
                day_index,
                weekday_totals[day_index],
                weekday_connected[day_index],
            )
            for day_index in range(7)
        ),
        minimum_sample=minimum_sample,
        label_for=lambda day_index: _DAY_LABELS[int(day_index)],
    )
    best_day["day_index"] = best_day_key

    _, top_rep = _best_bucket(
        (
            (user_id, user_totals[user_id], user_connected[user_id])
            for user_id in sorted(
                user_totals,
                key=lambda value: (
                    active_user_names[value].casefold(),
                    value,
                ),
            )
        ),
        minimum_sample=minimum_sample,
        label_for=lambda user_id: active_user_names[str(user_id)],
    )
    top_rep["name"] = top_rep["label"]

    return {
        "report": {
            "schema_version": PAYLOAD_SCHEMA_VERSION,
            "source": "openphone_calls",
            "date": report_date.isoformat(),
            "generated_at": local_now.isoformat(),
            "generated_at_display": _format_datetime(local_now),
            "timezone": tz_name,
            "lookback_days": lookback_days,
            "blended_lookback_days": BLENDED_LOOKBACK_DAYS,
            "top_rep_lookback_days": TOP_REP_LOOKBACK_DAYS,
            "minimum_sample": minimum_sample,
            "connected_duration_seconds": CONNECTED_DURATION_SECONDS,
            "window_start": window_start.date().isoformat(),
            "window_end": (report_date - timedelta(days=1)).isoformat(),
            "window_end_exclusive": report_date.isoformat(),
            "definition": (
                "Outbound calls only. Connected means status=completed and "
                "duration is at least 90 seconds. The main window contains 30 "
                "full local days ending at report-date midnight; blended charts "
                "use 28 days and rep rates use 7 days."
            ),
            "source_count": total_calls,
        },
        "kpis": {
            "total_calls": {
                "value": total_calls,
                "display": _format_count(total_calls),
                "connected": total_connected,
            },
            "connect_rate": {
                "value": _connect_rate(total_connected, total_calls),
                "display": _format_percent(
                    _connect_rate(total_connected, total_calls)
                ),
            },
            "best_hour": best_hour,
            "best_day": best_day,
            "top_rep": top_rep,
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
    replace_existing: bool = False,
) -> dict[str, Any]:
    """Fetch and persist a schema-current snapshot for the local report date."""

    token = (
        os.getenv("OPENPHONE_API_KEY", "").strip()
        if api_token is None
        else api_token.strip()
    )
    if not token:
        raise PipedriveAnalyticsError(
            "OPENPHONE_API_KEY is required to refresh analytics"
        )
    resolved_base_url = (
        os.getenv("OPENPHONE_BASE_URL", DEFAULT_OPENPHONE_BASE_URL)
        if base_url is None
        else base_url
    )
    tz_name, tz = _resolve_timezone(timezone_name)
    local_now = _coerce_now(now, tz)
    report_date = local_now.date()
    resolved_lookback_days = _resolve_positive_setting(
        lookback_days,
        env_name="OPENPHONE_ANALYTICS_LOOKBACK_DAYS",
        default=LOOKBACK_DAYS,
    )
    resolved_minimum_sample = _resolve_positive_setting(
        minimum_sample,
        env_name="OPENPHONE_ANALYTICS_MIN_SAMPLE",
        default=MINIMUM_SAMPLE,
    )

    existing = _get_snapshot_for_date(db, report_date)
    if (
        existing is not None
        and _snapshot_has_current_schema(existing)
        and not replace_existing
    ):
        return existing.payload()

    window_end = datetime.combine(report_date, time.min, tzinfo=tz)
    window_start = window_end - timedelta(days=resolved_lookback_days)
    api_window_start = window_start - timedelta(milliseconds=1)
    api = _OpenPhoneReadApi(
        api_token=token,
        base_url=str(resolved_base_url),
        client=client,
    )
    try:
        user_names = _fetch_user_names(api)
        conversation_pairs = _fetch_conversation_pairs(
            api,
            updated_after=api_window_start,
        )
        calls = _fetch_calls(
            api,
            conversation_pairs,
            created_after=api_window_start,
            created_before=window_end,
        )
    finally:
        api.close()

    payload = build_payload(
        calls,
        user_names,
        now=local_now,
        timezone_name=tz_name,
        lookback_days=resolved_lookback_days,
        minimum_sample=resolved_minimum_sample,
    )
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    if existing is None:
        snapshot = PipedriveAnalyticsSnapshot(report_date=report_date)
        db.add(snapshot)
    else:
        snapshot = existing
    snapshot.generated_at = local_now.astimezone(timezone.utc)
    snapshot.timezone_name = tz_name
    snapshot.source_count = int(payload["report"]["source_count"])
    snapshot.payload_json = payload_json

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        winner = _get_snapshot_for_date(db, report_date)
        if winner is not None and _snapshot_has_current_schema(winner):
            return winner.payload()
        raise
    db.refresh(snapshot)
    return payload


def get_latest_pipedrive_analytics(
    db: Session,
) -> dict[str, Any] | None:
    snapshot = _get_latest_current_schema_snapshot(db)
    return snapshot.payload() if snapshot is not None else None


def refresh_pipedrive_analytics_if_due(
    db: Session,
    *,
    client: Any | None = None,
    now: datetime | None = None,
    timezone_name: str | None = None,
    refresh_hours: Sequence[int] | str | None = None,
    api_token: str | None = None,
    base_url: str | None = None,
    lookback_days: int | None = None,
    minimum_sample: int | None = None,
) -> dict[str, Any] | None:
    """Refresh once after each configured local-time slot."""

    token = (
        os.getenv("OPENPHONE_API_KEY", "").strip()
        if api_token is None
        else api_token.strip()
    )
    if not token:
        return None

    tz_name, tz = _resolve_timezone(timezone_name)
    local_now = _coerce_now(now, tz)
    resolved_refresh_hours = _resolve_refresh_hours(refresh_hours)
    existing = _get_latest_current_schema_snapshot(db)
    first_slot_today = datetime.combine(
        local_now.date(),
        time(hour=resolved_refresh_hours[0]),
        tzinfo=tz,
    )
    if existing is None and local_now < first_slot_today:
        return None
    scheduled_at = _latest_due_refresh(
        local_now,
        resolved_refresh_hours,
        tz,
    )
    if (
        existing is not None
        and _snapshot_generated_at(existing, tz) >= scheduled_at
    ):
        return None

    return refresh_pipedrive_analytics(
        db,
        client=client,
        now=local_now,
        timezone_name=tz_name,
        api_token=token,
        base_url=base_url,
        lookback_days=lookback_days,
        minimum_sample=minimum_sample,
        replace_existing=True,
    )


def _fetch_user_names(api: _OpenPhoneReadApi) -> dict[str, str]:
    users: list[Mapping[str, Any]] = []
    page_token: str | None = None
    seen_tokens: set[str] = set()
    while True:
        params: dict[str, Any] = {"maxResults": USER_PAGE_LIMIT}
        if page_token is not None:
            params["pageToken"] = page_token
        body = api.get("/v1/users", params=params)
        data = _response_data_list(body, resource="users")
        users.extend(data)
        page_token = _next_page_token(body, resource="users")
        if page_token is None:
            break
        if page_token in seen_tokens:
            raise PipedriveAnalyticsError(
                "OpenPhone users response repeated a pagination token"
            )
        seen_tokens.add(page_token)
    return _normalize_user_names(users)


def _fetch_conversation_pairs(
    api: _OpenPhoneReadApi,
    *,
    updated_after: datetime,
) -> list[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    page_token: str | None = None
    seen_tokens: set[str] = set()
    while True:
        params: dict[str, Any] = {
            "updatedAfter": _format_api_datetime(updated_after),
            "maxResults": API_PAGE_LIMIT,
        }
        if page_token is not None:
            params["pageToken"] = page_token
        body = api.get("/v1/conversations", params=params)
        data = _response_data_list(body, resource="conversations")
        for raw_conversation in data:
            phone_number_id = _required_string_id(
                raw_conversation.get("phoneNumberId"),
                field="conversation phoneNumberId",
            )
            raw_participants = raw_conversation.get("participants")
            if not isinstance(raw_participants, list):
                raise PipedriveAnalyticsError(
                    "OpenPhone conversation is missing a participants list"
                )
            for raw_participant in raw_participants:
                participant = _required_string_id(
                    raw_participant,
                    field="conversation participant",
                )
                pairs.add((phone_number_id, participant))

        page_token = _next_page_token(body, resource="conversations")
        if page_token is None:
            break
        if page_token in seen_tokens:
            raise PipedriveAnalyticsError(
                "OpenPhone conversations response repeated a pagination token"
            )
        seen_tokens.add(page_token)
    return sorted(pairs)


def _fetch_calls(
    api: _OpenPhoneReadApi,
    conversation_pairs: Sequence[tuple[str, str]],
    *,
    created_after: datetime,
    created_before: datetime,
) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    seen_call_ids: set[str] = set()
    after_text = _format_api_datetime(created_after)
    before_text = _format_api_datetime(created_before)

    for phone_number_id, participant in conversation_pairs:
        page_token: str | None = None
        seen_tokens: set[str] = set()
        while True:
            params: dict[str, Any] = {
                "phoneNumberId": phone_number_id,
                "participants": [participant],
                "createdAfter": after_text,
                "createdBefore": before_text,
                "maxResults": API_PAGE_LIMIT,
            }
            if page_token is not None:
                params["pageToken"] = page_token
            body = api.get("/v1/calls", params=params)
            data = _response_data_list(body, resource="calls")
            for raw_call in data:
                call_id = _required_string_id(
                    raw_call.get("id"),
                    field="call id",
                )
                if call_id in seen_call_ids:
                    continue
                seen_call_ids.add(call_id)
                calls.append(
                    {
                        "createdAt": raw_call.get("createdAt"),
                        "direction": raw_call.get("direction"),
                        "status": raw_call.get("status"),
                        "duration": raw_call.get("duration"),
                        "userId": raw_call.get("initiatedBy")
                        or raw_call.get("userId"),
                    }
                )

            page_token = _next_page_token(body, resource="calls")
            if page_token is None:
                break
            if page_token in seen_tokens:
                raise PipedriveAnalyticsError(
                    "OpenPhone calls response repeated a pagination token"
                )
            seen_tokens.add(page_token)
    return calls


def _normalize_user_names(
    users: Mapping[str, str] | Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    normalized: dict[str, str] = {}
    if isinstance(users, Mapping):
        for raw_id, raw_name in users.items():
            user_id = _optional_string_id(raw_id, field="userId")
            name = str(raw_name or "").strip()
            if user_id is not None and name:
                normalized[user_id] = name
        return normalized

    for raw_user in users:
        if not isinstance(raw_user, Mapping):
            raise PipedriveAnalyticsError(
                "OpenPhone users response contains a non-object record"
            )
        user_id = _required_string_id(raw_user.get("id"), field="user id")
        name = " ".join(
            part
            for part in (
                str(raw_user.get("firstName") or "").strip(),
                str(raw_user.get("lastName") or "").strip(),
            )
            if part
        )
        normalized[user_id] = name or "Unknown rep"
    return normalized


def _best_bucket(
    buckets: Any,
    *,
    minimum_sample: int,
    label_for: Callable[[Any], str],
) -> tuple[Any | None, dict[str, Any]]:
    eligible = [
        (index, key, count, connected)
        for index, (key, count, connected) in enumerate(buckets)
        if count >= minimum_sample
    ]
    if not eligible:
        return None, {
            "label": None,
            "connect_rate": None,
            "count": 0,
            "calls": 0,
        }
    _index, key, count, connected = max(
        eligible,
        key=lambda item: (
            _connect_rate(item[3], item[2]) or 0.0,
            item[2],
            -item[0],
        ),
    )
    return key, {
        "label": label_for(key),
        "connect_rate": _connect_rate(connected, count),
        "count": count,
        "calls": count,
    }


def _response_data_list(
    body: Mapping[str, Any],
    *,
    resource: str,
) -> list[Mapping[str, Any]]:
    data = body.get("data")
    if not isinstance(data, list):
        raise PipedriveAnalyticsError(
            f"OpenPhone {resource} response is missing a data list"
        )
    records: list[Mapping[str, Any]] = []
    for record in data:
        if not isinstance(record, Mapping):
            raise PipedriveAnalyticsError(
                f"OpenPhone {resource} response contains a non-object record"
            )
        records.append(record)
    return records


def _next_page_token(
    body: Mapping[str, Any],
    *,
    resource: str,
) -> str | None:
    raw_token = body.get("nextPageToken")
    if raw_token in (None, ""):
        return None
    if not isinstance(raw_token, (str, int)):
        raise PipedriveAnalyticsError(
            f"OpenPhone {resource} response contains an invalid pagination token"
        )
    return str(raw_token)


def _api_root(base_url: str) -> str:
    root = str(base_url or "").strip().rstrip("/")
    if not root:
        raise PipedriveAnalyticsError("OpenPhone base URL is empty")
    if root.casefold().endswith("/v1"):
        return root[:-3]
    return root


def _retry_delay(attempt: int, retry_after: float | None) -> float:
    exponential = min(8.0, 0.5 * (2**attempt))
    return max(exponential, retry_after or 0.0)


def _retry_after_seconds(headers: Any) -> float | None:
    if not isinstance(headers, Mapping):
        return None
    raw_value = headers.get("Retry-After") or headers.get("retry-after")
    if raw_value in (None, ""):
        return None
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    return max(0.0, value)


def _parse_datetime(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise PipedriveAnalyticsError(
            f"OpenPhone call record is missing a valid {field}"
        )
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise PipedriveAnalyticsError(
            f"OpenPhone call record contains an invalid {field}"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _duration_seconds(value: Any) -> float:
    if value in (None, "") or isinstance(value, bool):
        raise PipedriveAnalyticsError(
            "OpenPhone call record is missing a valid duration"
        )
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise PipedriveAnalyticsError(
            "OpenPhone call record contains an invalid duration"
        ) from exc
    if parsed < 0:
        raise PipedriveAnalyticsError(
            "OpenPhone call record contains an invalid duration"
        )
    return parsed


def _required_string_id(value: Any, *, field: str) -> str:
    parsed = _optional_string_id(value, field=field)
    if parsed is None:
        raise PipedriveAnalyticsError(f"OpenPhone {field} is missing")
    return parsed


def _optional_string_id(value: Any, *, field: str) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise PipedriveAnalyticsError(f"OpenPhone {field} is invalid")
    parsed = str(value).strip()
    if not parsed:
        return None
    return parsed


def _resolve_timezone(
    timezone_name: str | None,
) -> tuple[str, ZoneInfo]:
    name = (
        timezone_name
        if timezone_name is not None
        else os.getenv("OPENPHONE_ANALYTICS_TIMEZONE", DEFAULT_TIMEZONE)
    )
    name = str(name).strip() or DEFAULT_TIMEZONE
    try:
        return name, ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise PipedriveAnalyticsError(
            f"Unknown OpenPhone analytics timezone: {name}"
        ) from exc


def _resolve_refresh_hours(
    refresh_hours: Sequence[int] | str | None,
) -> tuple[int, ...]:
    raw_values: Sequence[Any]
    if refresh_hours is None:
        raw_values = os.getenv(
            "OPENPHONE_ANALYTICS_REFRESH_HOURS",
            ",".join(str(hour) for hour in DEFAULT_REFRESH_HOURS),
        ).split(",")
    elif isinstance(refresh_hours, str):
        raw_values = refresh_hours.split(",")
    else:
        raw_values = refresh_hours

    parsed: list[int] = []
    for raw in raw_values:
        if isinstance(raw, bool):
            raise PipedriveAnalyticsError(
                "OPENPHONE_ANALYTICS_REFRESH_HOURS must contain hours from 0 to 23"
            )
        try:
            hour = int(str(raw).strip())
        except (TypeError, ValueError) as exc:
            raise PipedriveAnalyticsError(
                "OPENPHONE_ANALYTICS_REFRESH_HOURS must be a comma-separated "
                "list of hours from 0 to 23"
            ) from exc
        if hour < 0 or hour > 23:
            raise PipedriveAnalyticsError(
                "OPENPHONE_ANALYTICS_REFRESH_HOURS must contain hours from 0 to 23"
            )
        parsed.append(hour)

    if not parsed:
        raise PipedriveAnalyticsError(
            "OPENPHONE_ANALYTICS_REFRESH_HOURS must contain at least one hour"
        )
    return tuple(sorted(set(parsed)))


def _latest_due_refresh(
    local_now: datetime,
    refresh_hours: Sequence[int],
    tz: ZoneInfo,
) -> datetime:
    for hour in reversed(refresh_hours):
        candidate = datetime.combine(
            local_now.date(),
            time(hour=hour),
            tzinfo=tz,
        )
        if candidate <= local_now:
            return candidate
    return datetime.combine(
        local_now.date() - timedelta(days=1),
        time(hour=refresh_hours[-1]),
        tzinfo=tz,
    )


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


def _get_latest_current_schema_snapshot(
    db: Session,
) -> PipedriveAnalyticsSnapshot | None:
    snapshots = db.execute(
        select(PipedriveAnalyticsSnapshot)
        .order_by(
            PipedriveAnalyticsSnapshot.report_date.desc(),
            PipedriveAnalyticsSnapshot.generated_at.desc(),
        )
    ).scalars()
    for snapshot in snapshots:
        if _snapshot_has_current_schema(snapshot):
            return snapshot
    return None


def _snapshot_generated_at(
    snapshot: PipedriveAnalyticsSnapshot,
    tz: ZoneInfo,
) -> datetime:
    generated_at = snapshot.generated_at
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    return generated_at.astimezone(tz)


def _snapshot_has_current_schema(
    snapshot: PipedriveAnalyticsSnapshot,
) -> bool:
    try:
        report = snapshot.payload().get("report")
    except (PipedriveAnalyticsError, json.JSONDecodeError, TypeError, ValueError):
        return False
    return (
        isinstance(report, Mapping)
        and report.get("schema_version") == PAYLOAD_SCHEMA_VERSION
        and report.get("source") == "openphone_calls"
    )


def _format_api_datetime(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _sunday_day_index(value: datetime | date) -> int:
    return (value.weekday() + 1) % 7


def _weekday_occurrences(
    start: date,
    end: date,
    sunday_day_index: int,
) -> int:
    if end < start:
        return 0
    return sum(
        1
        for offset in range((end - start).days + 1)
        if _sunday_day_index(start + timedelta(days=offset))
        == sunday_day_index
    )


def _connect_rate(connected: int, total: int) -> float | None:
    if total <= 0:
        return None
    return round((connected / total) * 100.0, 1)


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
    return "—" if value is None else f"{int(value + 0.5)}%"


def _format_datetime(value: datetime) -> str:
    clock = value.strftime("%I:%M %p").lstrip("0")
    return f"{value.strftime('%b')} {value.day}, {value.year} at {clock}"
