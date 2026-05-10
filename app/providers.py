from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.message import EmailMessage
import hashlib
import logging
import smtplib
import time
from pathlib import Path
import re
from urllib.parse import urlparse

import httpx

from app.config import get_settings
from app.models import Show


logger = logging.getLogger(__name__)
SMARTLEAD_BATCH_SIZE = 400
CLAY_PUSH_BATCH_SIZE = 100
CLAY_PULL_PAGE_SIZE = 250
CLAY_META_KEYS = {
    "id",
    "record_id",
    "recordId",
    "created_at",
    "updated_at",
    "createdAt",
    "updatedAt",
    "cells",
    "status",
}
WEBSITE_RE = re.compile(r"^https?://", re.IGNORECASE)


@dataclass(frozen=True)
class ProviderResult:
    name: str
    status: str
    message: str


@dataclass(frozen=True)
class ClayRecord:
    clay_row_id: str
    row_status: str
    cells: dict[str, str]


@dataclass(frozen=True)
class ClayPollResult:
    name: str
    status: str
    message: str
    records: list[ClayRecord] = field(default_factory=list)
    total_rows: int = 0
    ready_rows: int = 0
    failed_rows: int = 0
    skipped_rows: int = 0
    all_terminal: bool = False


@dataclass(frozen=True)
class SmartleadSyncResult:
    name: str
    status: str
    message: str
    campaign_id: int | None = None
    campaign_name: str = ""
    imported_count: int = 0


def _load_export_rows(export_path: str) -> list[dict[str, str]]:
    path = Path(export_path)
    if not path.exists():
        raise FileNotFoundError(f"Clay export file is missing: {path}")

    with path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        rows: list[dict[str, str]] = []
        for row in reader:
            rows.append(
                {
                    (key or "").strip(): (value or "").strip()
                    for key, value in row.items()
                    if key
                }
            )
        return rows


def _safe_json(response: httpx.Response) -> object:
    try:
        return response.json()
    except ValueError:
        return response.text


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, object] | None = None,
    payload: object | None = None,
    timeout: float = 45.0,
) -> tuple[int, object]:
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.request(method, url, headers=headers, params=params, json=payload)
        body = _safe_json(response)
        response.raise_for_status()
        return response.status_code, body


def _ordinal_day(day: int) -> str:
    if 10 <= day % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def _pretty_event_day(show: Show) -> str:
    return show.event_date.strftime("%B ") + _ordinal_day(show.event_date.day)


def _build_source_row_id(show: Show, row: dict[str, str], index: int) -> str:
    seed = "|".join(
        [
            show.source_url,
            show.event_date.isoformat(),
            row.get("company_name", ""),
            row.get("website_url", ""),
            str(index),
        ]
    )
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:18]
    return f"show-{show.id}-{digest}"


def _show_payload_fields(show: Show, scraped_at: str, source_row_id: str) -> dict[str, str]:
    pretty_day = _pretty_event_day(show)
    return {
        "show_id": str(show.id),
        "show_name": show.name,
        "show_date": show.event_date.isoformat(),
        "show_day": pretty_day,
        "show_year": str(show.event_date.year),
        "show_date_pretty": f"{pretty_day}, {show.event_date.year}",
        "show_place": show.place,
        "scraped_at": scraped_at,
        "source_url": show.source_url,
        "source_row_id": source_row_id,
    }


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _stringify_cell_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return ", ".join(part for part in (_stringify_cell_value(item) for item in value) if part)
    if isinstance(value, dict):
        for key in ("displayValue", "display_value", "value", "text", "name", "label"):
            candidate = value.get(key)
            if candidate not in (None, ""):
                return _stringify_cell_value(candidate)
        return ", ".join(
            part
            for part in (
                _stringify_cell_value(item)
                for item in value.values()
            )
            if part
        )
    return str(value).strip()


def _flatten_clay_cells(record: dict[str, object]) -> dict[str, str]:
    cells = record.get("cells")
    source = cells if isinstance(cells, dict) else record
    flattened: dict[str, str] = {}
    for key, value in source.items():
        if key in CLAY_META_KEYS:
            continue
        if key is None:
            continue
        flattened[str(key)] = _stringify_cell_value(value)
    return flattened


def _extract_table_id(payload: object) -> str:
    if isinstance(payload, dict):
        for key in ("id", "table_id", "tableId"):
            value = payload.get(key)
            if value:
                return str(value)
        for key in ("data", "table", "result"):
            nested = payload.get(key)
            nested_id = _extract_table_id(nested)
            if nested_id:
                return nested_id
    if isinstance(payload, list):
        for item in payload:
            nested_id = _extract_table_id(item)
            if nested_id:
                return nested_id
    return ""


def _extract_table_name(payload: object) -> str:
    if isinstance(payload, dict):
        for key in ("name", "table_name", "tableName", "title"):
            value = payload.get(key)
            if value:
                return str(value)
        for key in ("data", "table", "result"):
            nested = payload.get(key)
            nested_name = _extract_table_name(nested)
            if nested_name:
                return nested_name
    return ""


def _extract_table_url(payload: object) -> str:
    if isinstance(payload, dict):
        for key in ("url", "table_url", "tableUrl", "href", "webUrl"):
            value = payload.get(key)
            if value:
                return str(value)
        for key in ("data", "table", "result"):
            nested = payload.get(key)
            nested_url = _extract_table_url(nested)
            if nested_url:
                return nested_url
    return ""


def _extract_records(payload: object) -> list[dict[str, object]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("records", "data", "items", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                for nested_key in ("records", "items", "results", "data"):
                    nested_value = value.get(nested_key)
                    if isinstance(nested_value, list):
                        return [item for item in nested_value if isinstance(item, dict)]
    return []


def _extract_count(payload: object) -> int | None:
    if isinstance(payload, dict):
        for key in ("count", "total", "total_count", "row_count"):
            value = payload.get(key)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
        for key in ("data", "result"):
            nested = payload.get(key)
            nested_count = _extract_count(nested)
            if nested_count is not None:
                return nested_count
    return None


def _derive_company_domain(raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        return ""
    candidate = value
    if not WEBSITE_RE.search(candidate):
        candidate = "https://" + candidate
    parsed = urlparse(candidate)
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _clay_headers() -> dict[str, str]:
    settings = get_settings()
    headers: dict[str, str] = {"Accept": "application/json"}
    if settings.clay_session_cookie:
        headers["Cookie"] = settings.clay_session_cookie
    if settings.clay_api_key:
        headers["Authorization"] = f"Bearer {settings.clay_api_key}"
    return headers


def _clay_url(path: str) -> str:
    return f"{get_settings().clay_base_url}{path}"


def _clay_request(
    method: str,
    path: str,
    *,
    params: dict[str, object] | None = None,
    payload: object | None = None,
    timeout: float = 45.0,
) -> tuple[int, object]:
    return _request_json(
        method,
        _clay_url(path),
        headers=_clay_headers(),
        params=params,
        payload=payload,
        timeout=timeout,
    )


def _smartlead_url(path: str) -> str:
    return f"{get_settings().smartlead_base_url}{path}"


def _smartlead_request(
    method: str,
    path: str,
    *,
    params: dict[str, object] | None = None,
    payload: object | None = None,
    timeout: float = 45.0,
) -> tuple[int, object]:
    settings = get_settings()
    query = dict(params or {})
    query["api_key"] = settings.smartlead_api_key
    return _request_json(method, _smartlead_url(path), params=query, payload=payload, timeout=timeout)


def _push_rows_to_clay_webhook(show: Show) -> ProviderResult:
    settings = get_settings()
    if not settings.clay_webhook_url:
        return ProviderResult(
            name="clay",
            status="skipped",
            message=(
                "Clay webhook URL is not configured. Clay's live table endpoints rejected "
                "API-key auth in our probe, so webhook is the supported path here."
            ),
        )

    if not show.latest_export_path:
        return ProviderResult("clay", "failed", "No export file is available to send to Clay.")

    rows = _load_export_rows(show.latest_export_path)
    headers: dict[str, str] = {}
    if settings.clay_webhook_auth_header and settings.clay_webhook_auth_value:
        headers[settings.clay_webhook_auth_header] = settings.clay_webhook_auth_value

    sent = 0
    scraped_at = datetime.now(timezone.utc).isoformat()
    try:
        for index, row in enumerate(rows, start=1):
            source_row_id = _build_source_row_id(show, row, index)
            payload = {
                **row,
                **_show_payload_fields(show, scraped_at, source_row_id),
            }
            status_code, _body = _request_json(
                "POST",
                settings.clay_webhook_url,
                headers=headers,
                payload=payload,
                timeout=30.0,
            )
            if status_code < 200 or status_code >= 300:
                return ProviderResult(
                    name="clay",
                    status="failed",
                    message=f"Clay webhook returned non-success status {status_code}.",
                )
            sent += 1
            time.sleep(0.08)
    except FileNotFoundError as exc:
        return ProviderResult(name="clay", status="failed", message=str(exc))
    except httpx.HTTPStatusError as exc:
        body = exc.response.text[:300]
        return ProviderResult("clay", "failed", f"Clay webhook HTTP {exc.response.status_code}: {body}")
    except httpx.HTTPError as exc:
        return ProviderResult("clay", "failed", f"Clay webhook network error: {exc}")

    return ProviderResult(
        name="clay",
        status="success",
        message=f"Sent {sent} row(s) from the scraper export to Clay via webhook.",
    )


def _duplicate_clay_table(show: Show) -> ProviderResult:
    settings = get_settings()
    if not settings.clay_session_cookie:
        return ProviderResult("clay", "failed", "Clay table automation needs CLAY_SESSION_COOKIE.")
    if not settings.clay_template_table_id:
        return ProviderResult("clay", "failed", "Clay table automation needs CLAY_TEMPLATE_TABLE_ID.")

    desired_name = f"{show.name} - {show.event_date.isoformat()}"
    candidates = [
        (f"/tables/{settings.clay_template_table_id}/duplicate", {"name": desired_name}),
        (f"/tables/{settings.clay_template_table_id}/duplicate", {"table_name": desired_name}),
        (f"/tables/{settings.clay_template_table_id}/copy", {"name": desired_name}),
        (f"/tables/{settings.clay_template_table_id}/copy", {"table_name": desired_name}),
    ]
    last_error = "Clay did not accept any known duplicate-table request shape."
    for path, payload in candidates:
        try:
            _status_code, body = _clay_request("POST", path, payload=payload, timeout=60.0)
            table_id = _extract_table_id(body)
            if not table_id:
                last_error = f"Clay duplicate response did not include a table id: {body!r}"
                continue
            show.clay_table_id = table_id
            show.clay_table_name = _extract_table_name(body) or desired_name
            show.clay_table_url = _extract_table_url(body)
            return ProviderResult("clay", "success", f"Created Clay table {show.clay_table_name}.")
        except httpx.HTTPStatusError as exc:
            last_error = f"Clay duplicate HTTP {exc.response.status_code}: {exc.response.text[:240]}"
        except httpx.HTTPError as exc:
            last_error = f"Clay duplicate network error: {exc}"
    return ProviderResult("clay", "failed", last_error)


def _push_rows_to_clay_table_v3(show: Show, table_id: str) -> ProviderResult:
    if not table_id:
        return ProviderResult("clay", "failed", "No Clay table id is available for this show.")
    if not show.latest_export_path:
        return ProviderResult("clay", "failed", "No export file is available to send to Clay.")

    rows = _load_export_rows(show.latest_export_path)
    scraped_at = datetime.now(timezone.utc).isoformat()
    all_records: list[dict[str, object]] = []
    for index, row in enumerate(rows, start=1):
        source_row_id = _build_source_row_id(show, row, index)
        website_domain = _derive_company_domain(row.get("website_url", "") or row.get("website", ""))
        all_records.append(
            {
                "id": source_row_id,
                "cells": {
                    **row,
                    **_show_payload_fields(show, scraped_at, source_row_id),
                    "company_domain": website_domain,
                },
            }
        )

    try:
        for offset in range(0, len(all_records), CLAY_PUSH_BATCH_SIZE):
            batch = all_records[offset : offset + CLAY_PUSH_BATCH_SIZE]
            _status_code, _body = _clay_request(
                "POST",
                f"/tables/{table_id}/records",
                payload={"records": batch},
                timeout=60.0,
            )
    except FileNotFoundError as exc:
        return ProviderResult(name="clay", status="failed", message=str(exc))
    except httpx.HTTPStatusError as exc:
        return ProviderResult(
            name="clay",
            status="failed",
            message=f"Clay direct table HTTP {exc.response.status_code}: {exc.response.text[:240]}",
        )
    except httpx.HTTPError as exc:
        return ProviderResult(
            name="clay",
            status="failed",
            message=f"Clay direct table network error: {exc}",
        )

    return ProviderResult(
        name="clay",
        status="success",
        message=f"Sent {len(all_records)} row(s) to Clay table {table_id}.",
    )


def notify_ready_for_review(show: Show) -> ProviderResult:
    settings = get_settings()
    if not settings.notify_to_emails:
        return ProviderResult(
            name="notification",
            status="skipped",
            message="No email recipients configured.",
        )

    if not (settings.smtp_host and settings.notify_from_email):
        logger.info(
            "Ready-for-review notification requested for show %s, but SMTP is not configured.",
            show.id,
        )
        return ProviderResult(
            name="notification",
            status="skipped",
            message="Recipients are configured, but SMTP host/from-email are missing.",
        )

    message = EmailMessage()
    message["Subject"] = f"[Trade Show Outbound] {show.name} is ready for review"
    message["From"] = settings.notify_from_email
    message["To"] = ", ".join(settings.notify_to_emails)
    message.set_content(
        "\n".join(
            [
                f"Show: {show.name}",
                f"Date: {show.event_date}",
                f"Place: {show.place}",
                f"Source URL: {show.source_url}",
                f"Rows exported: {show.company_count}",
                f"Profile failures: {show.failure_count}",
                f"Export path: {show.latest_export_path or 'not available'}",
                "",
                "The show has finished scraping and is ready for review in the dashboard.",
            ]
        )
    )

    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=30) as smtp:
            if settings.smtp_use_tls:
                smtp.starttls()
            if settings.smtp_username:
                smtp.login(settings.smtp_username, settings.smtp_password)
            smtp.send_message(message)
    except Exception as exc:  # noqa: BLE001
        logger.exception("SMTP notification failed for show %s.", show.id)
        return ProviderResult(
            name="notification",
            status="failed",
            message=f"SMTP notification failed: {exc}",
        )

    return ProviderResult(
        name="notification",
        status="success",
        message=f"Sent email notification to {', '.join(settings.notify_to_emails)}.",
    )


def push_to_clay(show: Show) -> ProviderResult:
    settings = get_settings()
    if settings.clay_template_table_id and settings.clay_session_cookie:
        if not show.clay_table_id:
            create_result = _duplicate_clay_table(show)
            if create_result.status != "success":
                return create_result
        return _push_rows_to_clay_table_v3(show, show.clay_table_id)
    if settings.clay_webhook_url:
        return _push_rows_to_clay_webhook(show)
    if settings.clay_session_cookie and settings.clay_input_table_id:
        show.clay_table_id = settings.clay_input_table_id
        show.clay_table_name = show.clay_table_name or "Clay Input Table"
        return _push_rows_to_clay_table_v3(show, settings.clay_input_table_id)
    if settings.clay_api_key:
        return ProviderResult(
            "clay",
            "skipped",
            (
                "Clay API key is configured, but Clay's live table endpoints still require a "
                "logged-in session in our probe. Configure CLAY_TEMPLATE_TABLE_ID with "
                "CLAY_SESSION_COOKIE or use CLAY_WEBHOOK_URL."
            ),
        )
    return ProviderResult("clay", "skipped", "No Clay integration method is configured.")


def poll_clay_table(show: Show) -> ClayPollResult:
    settings = get_settings()
    if not settings.clay_session_cookie:
        return ClayPollResult("clay", "skipped", "Clay polling needs CLAY_SESSION_COOKIE.")
    if not show.clay_table_id:
        return ClayPollResult("clay", "skipped", "This show does not have a Clay table yet.")

    try:
        _status_code, count_payload = _clay_request("GET", f"/tables/{show.clay_table_id}/count", timeout=30.0)
        expected_total = _extract_count(count_payload)
    except httpx.HTTPError:
        expected_total = None

    records: list[dict[str, object]] = []
    page = 1
    while True:
        try:
            _status_code, body = _clay_request(
                "GET",
                f"/tables/{show.clay_table_id}/records",
                params={"page": page, "pageSize": CLAY_PULL_PAGE_SIZE},
                timeout=60.0,
            )
        except httpx.HTTPStatusError as exc:
            return ClayPollResult(
                "clay",
                "failed",
                f"Clay table poll HTTP {exc.response.status_code}: {exc.response.text[:240]}",
            )
        except httpx.HTTPError as exc:
            return ClayPollResult("clay", "failed", f"Clay table poll network error: {exc}")

        batch = _extract_records(body)
        if not batch:
            break
        records.extend(batch)
        if len(batch) < CLAY_PULL_PAGE_SIZE:
            break
        if expected_total is not None and len(records) >= expected_total:
            break
        page += 1
        if page > 40:
            break

    normalized_records: list[ClayRecord] = []
    ready_value = settings.clay_ready_status_value.strip().lower()
    failed_value = settings.clay_failed_status_value.strip().lower()
    skipped_value = settings.clay_skipped_status_value.strip().lower()
    terminal_values = {ready_value, failed_value, skipped_value}
    status_column = _normalize_key(settings.clay_row_status_column)

    for record in records:
        clay_row_id = str(
            record.get("id")
            or record.get("record_id")
            or record.get("recordId")
            or ""
        )
        if not clay_row_id:
            continue
        cells = _flatten_clay_cells(record)
        normalized_cells = {_normalize_key(key): value for key, value in cells.items()}
        row_status = normalized_cells.get(status_column, "").strip().lower()
        normalized_records.append(
            ClayRecord(
                clay_row_id=clay_row_id,
                row_status=row_status,
                cells=normalized_cells,
            )
        )

    total_rows = len(normalized_records)
    ready_rows = sum(1 for record in normalized_records if record.row_status == ready_value)
    failed_rows = sum(1 for record in normalized_records if record.row_status == failed_value)
    skipped_rows = sum(1 for record in normalized_records if record.row_status == skipped_value)
    all_terminal = total_rows > 0 and all(record.row_status in terminal_values for record in normalized_records)

    return ClayPollResult(
        name="clay",
        status="success",
        message=f"Fetched {total_rows} row(s) from Clay table {show.clay_table_id}.",
        records=normalized_records,
        total_rows=total_rows,
        ready_rows=ready_rows,
        failed_rows=failed_rows,
        skipped_rows=skipped_rows,
        all_terminal=all_terminal,
    )


def _smartlead_client_id() -> int | None:
    raw_value = get_settings().smartlead_client_id.strip()
    if not raw_value:
        return None
    if not raw_value.isdigit():
        return None
    return int(raw_value)


def _show_campaign_name(show: Show) -> str:
    return f"{show.name} - {_pretty_event_day(show)} {show.event_date.year}"


def _extract_smartlead_data(payload: object) -> object:
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload


def _list_smartlead_campaigns() -> list[dict[str, object]]:
    _status_code, body = _smartlead_request("GET", "/campaigns/")
    data = _extract_smartlead_data(body)
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def _get_smartlead_campaign(campaign_id: int) -> dict[str, object]:
    _status_code, body = _smartlead_request("GET", f"/campaigns/{campaign_id}")
    data = _extract_smartlead_data(body)
    return data if isinstance(data, dict) else {}


def _get_smartlead_sequences(campaign_id: int) -> list[dict[str, object]]:
    _status_code, body = _smartlead_request("GET", f"/campaigns/{campaign_id}/sequences")
    data = _extract_smartlead_data(body)
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def _get_smartlead_email_accounts(campaign_id: int) -> list[dict[str, object]]:
    _status_code, body = _smartlead_request("GET", f"/campaigns/{campaign_id}/email-accounts")
    data = _extract_smartlead_data(body)
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def _extract_delay_in_days(sequence: dict[str, object]) -> int:
    if isinstance(sequence.get("seq_delay_details"), dict):
        details = sequence["seq_delay_details"]
        if isinstance(details.get("delay_in_days"), int):
            return int(details["delay_in_days"])
    for key in ("delay_in_days", "delayInDays"):
        value = sequence.get(key)
        if isinstance(value, int):
            return int(value)
    return 0


def _apply_show_placeholders(value: str, show: Show) -> str:
    return (
        value.replace("{{show_name}}", show.name)
        .replace("{{show_name_lower}}", show.name.lower())
    )


def _clone_template_settings(target_campaign_id: int, template_campaign_id: int, show: Show) -> None:
    template = _get_smartlead_campaign(template_campaign_id)
    if not template:
        raise ValueError(f"Smartlead template campaign {template_campaign_id} could not be loaded.")
    template_sequences = _get_smartlead_sequences(template_campaign_id)
    if not template_sequences:
        raise ValueError(f"Smartlead template campaign {template_campaign_id} has no sequence steps to clone.")
    template_accounts = _get_smartlead_email_accounts(template_campaign_id)

    settings_payload = {}
    for key in (
        "track_settings",
        "stop_lead_settings",
        "unsubscribe_text",
        "send_as_plain_text",
        "force_plain_text",
        "follow_up_percentage",
        "enable_ai_esp_matching",
        "auto_pause_domain_leads_on_reply",
        "ignore_ss_mailbox_sending_limit",
        "bounce_autopause_threshold",
        "domain_level_rate_limit",
    ):
        if template.get(key) not in (None, "", []):
            settings_payload[key] = template.get(key)

    client_id = _smartlead_client_id()
    if client_id is not None:
        settings_payload["client_id"] = client_id

    if settings_payload:
        _smartlead_request("POST", f"/campaigns/{target_campaign_id}/settings", payload=settings_payload)

    if template_sequences:
        sequence_payload = {
            "sequences": [
                {
                    "id": None,
                    "seq_number": sequence.get("seq_number") or (index + 1),
                    "subject": _apply_show_placeholders(str(sequence.get("subject", "")), show),
                    "email_body": _apply_show_placeholders(str(sequence.get("email_body", "")), show),
                    "seq_delay_details": {"delay_in_days": _extract_delay_in_days(sequence)},
                }
                for index, sequence in enumerate(template_sequences)
            ]
        }
        _smartlead_request("POST", f"/campaigns/{target_campaign_id}/sequences", payload=sequence_payload)

    account_ids = [
        int(account["id"])
        for account in template_accounts
        if account.get("id") is not None
    ]
    if not account_ids:
        raise ValueError(f"Smartlead template campaign {template_campaign_id} has no sender accounts to clone.")

    _smartlead_request(
        "POST",
        f"/campaigns/{target_campaign_id}/email-accounts",
        payload={"email_account_ids": account_ids},
    )


def ensure_smartlead_campaign(show: Show, *, force_rebuild: bool = False) -> SmartleadSyncResult:
    settings = get_settings()
    if not settings.smartlead_api_key:
        return SmartleadSyncResult("smartlead", "skipped", "No Smartlead API key configured.")

    desired_name = _show_campaign_name(show)
    try:
        if show.smartlead_campaign_id and not force_rebuild:
            show.smartlead_campaign_name = show.smartlead_campaign_name or desired_name
            return SmartleadSyncResult(
                "smartlead",
                "success",
                f"Using existing Smartlead campaign {show.smartlead_campaign_id}.",
                campaign_id=show.smartlead_campaign_id,
                campaign_name=show.smartlead_campaign_name,
            )

        if not force_rebuild:
            for campaign in _list_smartlead_campaigns():
                if str(campaign.get("name", "")).strip() != desired_name:
                    continue
                campaign_id = campaign.get("id")
                if campaign_id is None:
                    continue
                show.smartlead_campaign_id = int(campaign_id)
                show.smartlead_campaign_name = desired_name
                return SmartleadSyncResult(
                    "smartlead",
                    "success",
                    f"Reused existing Smartlead campaign {campaign_id}.",
                    campaign_id=show.smartlead_campaign_id,
                    campaign_name=show.smartlead_campaign_name,
                )

        payload: dict[str, object] = {"name": desired_name}
        client_id = _smartlead_client_id()
        if client_id is not None:
            payload["client_id"] = client_id
        _status_code, body = _smartlead_request("POST", "/campaigns/create", payload=payload)
        campaign_id_raw = _extract_table_id(body)
        if not campaign_id_raw or not campaign_id_raw.isdigit():
            return SmartleadSyncResult(
                "smartlead",
                "failed",
                f"Smartlead create campaign response did not include an id: {body!r}",
            )

        campaign_id = int(campaign_id_raw)
        if settings.smartlead_template_campaign_id and settings.smartlead_template_campaign_id != str(campaign_id):
            _clone_template_settings(campaign_id, int(settings.smartlead_template_campaign_id), show)
        show.smartlead_campaign_id = campaign_id
        show.smartlead_campaign_name = desired_name

        return SmartleadSyncResult(
            "smartlead",
            "success",
            f"{'Rebuilt' if force_rebuild else 'Created'} Smartlead campaign {campaign_id}.",
            campaign_id=campaign_id,
            campaign_name=desired_name,
        )
    except ValueError as exc:
        return SmartleadSyncResult("smartlead", "failed", str(exc))
    except httpx.HTTPStatusError as exc:
        return SmartleadSyncResult(
            "smartlead",
            "failed",
            f"Smartlead HTTP {exc.response.status_code}: {exc.response.text[:240]}",
        )
    except httpx.HTTPError as exc:
        return SmartleadSyncResult("smartlead", "failed", f"Smartlead network error: {exc}")


def list_smartlead_campaign_options() -> list[dict[str, object]]:
    settings = get_settings()
    if not settings.smartlead_api_key:
        return []

    try:
        campaigns = _list_smartlead_campaigns()
    except httpx.HTTPError:
        return []

    normalized: list[dict[str, object]] = []
    for campaign in campaigns:
        campaign_id = campaign.get("id")
        if campaign_id is None:
            continue
        normalized.append(
            {
                "id": int(campaign_id),
                "name": str(campaign.get("name", "")).strip() or f"Campaign {campaign_id}",
                "status": str(campaign.get("status", "")).strip(),
            }
        )
    return sorted(normalized, key=lambda item: str(item["name"]).lower())


def fetch_smartlead_campaign_option(campaign_id: int) -> dict[str, object] | None:
    settings = get_settings()
    if not settings.smartlead_api_key:
        return None

    try:
        campaign = _get_smartlead_campaign(campaign_id)
    except httpx.HTTPError:
        return None
    if not campaign:
        return None
    return {
        "id": campaign_id,
        "name": str(campaign.get("name", "")).strip() or f"Campaign {campaign_id}",
        "status": str(campaign.get("status", "")).strip(),
    }


def import_ready_rows_to_smartlead(show: Show, lead_rows: list[dict[str, object]]) -> SmartleadSyncResult:
    campaign_result = ensure_smartlead_campaign(show)
    if campaign_result.status != "success":
        return campaign_result
    if not lead_rows:
        return SmartleadSyncResult(
            "smartlead",
            "success",
            "No new Smartlead-ready rows were waiting to import.",
            campaign_id=show.smartlead_campaign_id,
            campaign_name=show.smartlead_campaign_name,
        )

    imported_count = 0
    try:
        for offset in range(0, len(lead_rows), SMARTLEAD_BATCH_SIZE):
            batch = lead_rows[offset : offset + SMARTLEAD_BATCH_SIZE]
            payload = {
                "lead_list": batch,
                "settings": {
                    "ignore_duplicate_leads_in_other_campaign": False,
                    "return_lead_ids": True,
                },
            }
            _status_code, body = _smartlead_request(
                "POST",
                f"/campaigns/{show.smartlead_campaign_id}/leads",
                payload=payload,
                timeout=60.0,
            )
            data = _extract_smartlead_data(body)
            if isinstance(data, dict):
                imported_count += int(data.get("added_count") or 0)
            elif isinstance(body, dict):
                imported_count += int(body.get("added_count") or 0)
            else:
                imported_count += len(batch)
    except httpx.HTTPStatusError as exc:
        return SmartleadSyncResult(
            "smartlead",
            "failed",
            f"Smartlead lead import HTTP {exc.response.status_code}: {exc.response.text[:240]}",
            campaign_id=show.smartlead_campaign_id,
            campaign_name=show.smartlead_campaign_name,
        )
    except httpx.HTTPError as exc:
        return SmartleadSyncResult(
            "smartlead",
            "failed",
            f"Smartlead lead import network error: {exc}",
            campaign_id=show.smartlead_campaign_id,
            campaign_name=show.smartlead_campaign_name,
        )

    return SmartleadSyncResult(
        "smartlead",
        "success",
        f"Imported {imported_count} Smartlead-ready lead(s).",
        campaign_id=show.smartlead_campaign_id,
        campaign_name=show.smartlead_campaign_name,
        imported_count=imported_count,
    )


def _update_smartlead_campaign_status(campaign_id: int, status_value: str) -> None:
    _smartlead_request("POST", f"/campaigns/{campaign_id}/status", payload={"status": status_value})


def launch_smartlead_campaign(show: Show) -> ProviderResult:
    if not get_settings().smartlead_api_key:
        return ProviderResult("smartlead", "skipped", "No Smartlead API key configured.")
    if not show.smartlead_campaign_id:
        return ProviderResult("smartlead", "failed", "No Smartlead campaign is linked to this show yet.")

    try:
        for campaign in _list_smartlead_campaigns():
            campaign_id = campaign.get("id")
            status_value = str(campaign.get("status", "")).upper()
            if campaign_id is None or int(campaign_id) == int(show.smartlead_campaign_id):
                continue
            if status_value == "ACTIVE":
                _update_smartlead_campaign_status(int(campaign_id), "PAUSED")
        _update_smartlead_campaign_status(int(show.smartlead_campaign_id), "START")
    except httpx.HTTPStatusError as exc:
        return ProviderResult(
            "smartlead",
            "failed",
            f"Smartlead launch HTTP {exc.response.status_code}: {exc.response.text[:240]}",
        )
    except httpx.HTTPError as exc:
        return ProviderResult("smartlead", "failed", f"Smartlead launch network error: {exc}")

    return ProviderResult(
        "smartlead",
        "success",
        f"Started Smartlead campaign {show.smartlead_campaign_id} and paused the other active campaigns.",
    )


def pause_smartlead_campaign(show: Show) -> ProviderResult:
    if not get_settings().smartlead_api_key:
        return ProviderResult("smartlead", "skipped", "No Smartlead API key configured.")
    if not show.smartlead_campaign_id:
        return ProviderResult("smartlead", "failed", "No Smartlead campaign is linked to this show yet.")

    try:
        _update_smartlead_campaign_status(int(show.smartlead_campaign_id), "PAUSED")
    except httpx.HTTPStatusError as exc:
        return ProviderResult(
            "smartlead",
            "failed",
            f"Smartlead pause HTTP {exc.response.status_code}: {exc.response.text[:240]}",
        )
    except httpx.HTTPError as exc:
        return ProviderResult("smartlead", "failed", f"Smartlead pause network error: {exc}")

    return ProviderResult("smartlead", "success", f"Paused Smartlead campaign {show.smartlead_campaign_id}.")


def push_to_heyreach(show: Show) -> ProviderResult:
    settings = get_settings()
    if not settings.heyreach_api_key:
        return ProviderResult("heyreach", "skipped", "No HeyReach API key configured.")
    logger.info("HeyReach sync placeholder for show %s.", show.id)
    return ProviderResult("heyreach", "skipped", "HeyReach API contract still needs final payload mapping.")


def push_to_smartlead(show: Show) -> ProviderResult:
    campaign_result = ensure_smartlead_campaign(show)
    return ProviderResult(campaign_result.name, campaign_result.status, campaign_result.message)
