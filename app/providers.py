from __future__ import annotations

import csv
from datetime import datetime, timezone
import logging
from dataclasses import dataclass
from email.message import EmailMessage
import json
from pathlib import Path
import smtplib
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.config import get_settings
from app.models import Show


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProviderResult:
    name: str
    status: str
    message: str


def _load_export_rows(export_path: str) -> list[dict[str, str]]:
    path = Path(export_path)
    if not path.exists():
        raise FileNotFoundError(f"Clay export file is missing: {path}")

    with path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        return [
            {key: value for key, value in row.items() if key}
            for row in reader
        ]


def _post_json(url: str, payload: object, headers: dict[str, str] | None = None) -> tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    request = Request(url, data=data, method="POST")
    request.add_header("Content-Type", "application/json")
    request.add_header("Accept", "application/json")
    for key, value in (headers or {}).items():
        if value:
            request.add_header(key, value)

    with urlopen(request, timeout=30) as response:
        return response.status, response.read().decode("utf-8", errors="replace")


def _ordinal_day(day: int) -> str:
    if 10 <= day % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def _pretty_event_day(show: Show) -> str:
    return show.event_date.strftime("%B ") + _ordinal_day(show.event_date.day)


def _show_payload_fields(show: Show, scraped_at: str) -> dict[str, str]:
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
    }


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
        for row in rows:
            payload = {
                **row,
                **_show_payload_fields(show, scraped_at),
            }
            attempt = 0
            while True:
                try:
                    status_code, _body = _post_json(settings.clay_webhook_url, payload, headers=headers)
                    if status_code < 200 or status_code >= 300:
                        return ProviderResult(
                            name="clay",
                            status="failed",
                            message=f"Clay webhook returned non-success status {status_code}.",
                        )
                    break
                except HTTPError as exc:
                    body = exc.read().decode("utf-8", errors="replace")
                    if exc.code == 429 and attempt < 5:
                        time.sleep(1.5 * (attempt + 1))
                        attempt += 1
                        continue
                    return ProviderResult(
                        name="clay",
                        status="failed",
                        message=f"Clay webhook HTTP {exc.code}: {body[:200]}",
                    )
            sent += 1
            time.sleep(0.08)
    except URLError as exc:
        return ProviderResult(
            name="clay",
            status="failed",
            message=f"Clay webhook network error: {exc}",
        )
    except FileNotFoundError as exc:
        return ProviderResult(name="clay", status="failed", message=str(exc))

    return ProviderResult(
        name="clay",
        status="success",
        message=f"Sent {sent} row(s) from the scraper export to Clay via webhook.",
    )


def _push_rows_to_clay_table_v3(show: Show) -> ProviderResult:
    settings = get_settings()
    if not settings.clay_input_table_id or not settings.clay_session_cookie:
        return ProviderResult(
            name="clay",
            status="skipped",
            message="Clay direct table mode needs both CLAY_INPUT_TABLE_ID and CLAY_SESSION_COOKIE.",
        )

    if not show.latest_export_path:
        return ProviderResult("clay", "failed", "No export file is available to send to Clay.")

    rows = _load_export_rows(show.latest_export_path)
    url = f"https://api.clay.com/v3/tables/{settings.clay_input_table_id}/records"
    scraped_at = datetime.now(timezone.utc).isoformat()
    records = [
        {
            "id": f"show-{show.id}-{uuid.uuid4().hex[:12]}",
            "cells": {
                **row,
                **_show_payload_fields(show, scraped_at),
            },
        }
        for row in rows
    ]
    try:
        status_code, _body = _post_json(
            url,
            {"records": records},
            headers={"Cookie": settings.clay_session_cookie},
        )
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return ProviderResult(
            name="clay",
            status="failed",
            message=f"Clay direct table HTTP {exc.code}: {body[:200]}",
        )
    except URLError as exc:
        return ProviderResult(
            name="clay",
            status="failed",
            message=f"Clay direct table network error: {exc}",
        )
    except FileNotFoundError as exc:
        return ProviderResult(name="clay", status="failed", message=str(exc))

    if status_code < 200 or status_code >= 300:
        return ProviderResult(
            name="clay",
            status="failed",
            message=f"Clay direct table returned non-success status {status_code}.",
        )
    return ProviderResult(
        name="clay",
        status="success",
        message=f"Sent {len(records)} row(s) to Clay table {settings.clay_input_table_id}.",
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
    if settings.clay_webhook_url:
        return _push_rows_to_clay_webhook(show)
    if settings.clay_session_cookie and settings.clay_input_table_id:
        return _push_rows_to_clay_table_v3(show)
    if settings.clay_api_key:
        return ProviderResult(
            "clay",
            "skipped",
            (
                "Clay API key is configured, but Clay's live table endpoints still require a "
                "logged-in session in our probe. Configure CLAY_WEBHOOK_URL or CLAY_SESSION_COOKIE."
            ),
        )
    return ProviderResult("clay", "skipped", "No Clay integration method is configured.")


def push_to_heyreach(show: Show) -> ProviderResult:
    settings = get_settings()
    if not settings.heyreach_api_key:
        return ProviderResult("heyreach", "skipped", "No HeyReach API key configured.")
    logger.info("HeyReach sync placeholder for show %s.", show.id)
    return ProviderResult("heyreach", "skipped", "HeyReach API contract still needs final payload mapping.")


def push_to_smartlead(show: Show) -> ProviderResult:
    settings = get_settings()
    if not settings.smartlead_api_key:
        return ProviderResult("smartlead", "skipped", "No Smartlead API key configured.")
    logger.info("Smartlead sync placeholder for show %s.", show.id)
    return ProviderResult("smartlead", "skipped", "Smartlead API contract still needs final payload mapping.")
