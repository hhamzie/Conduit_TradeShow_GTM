from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from urllib.parse import urlparse

import httpx

from app.trade_show_feeder import (
    _extract_start_dates_from_text,
    _find_curated_trade_show_source,
    _strip_html_to_text,
)


DATE_MATCH_WINDOW_DAYS = 45


@dataclass(frozen=True)
class TradeShowDateVerification:
    tracker_start_date: date
    official_start_date: date | None
    effective_start_date: date
    status: str
    official_url: str
    message: str


def official_page_url_for_show(show_name: str, fallback_url: str) -> str:
    """Return a deterministic organizer URL without doing any web search."""

    curated = _find_curated_trade_show_source(show_name)
    if curated is not None:
        return curated.official_url
    parsed = urlparse(fallback_url.strip())
    if parsed.scheme.casefold() in {"http", "https"} and parsed.netloc:
        return fallback_url.strip()
    return ""


def verify_trade_show_date(
    *,
    show_name: str,
    tracker_start_date: date,
    fallback_url: str,
    http_client: httpx.Client | None = None,
    timeout_seconds: float = 25.0,
) -> TradeShowDateVerification:
    """Triangulate a tracker date against the exact organizer page.

    The official page is authoritative when it contains a date reasonably
    close to the tracker row.  A failed or ambiguous fetch keeps the tracker
    date and records an unverified state; it never triggers a search.
    """

    official_url = official_page_url_for_show(show_name, fallback_url)
    if not official_url:
        return TradeShowDateVerification(
            tracker_start_date=tracker_start_date,
            official_start_date=None,
            effective_start_date=tracker_start_date,
            status="unverified",
            official_url="",
            message="No deterministic official organizer URL was available.",
        )

    owns_client = http_client is None
    client = http_client or httpx.Client()
    try:
        response = client.get(
            official_url,
            timeout=timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": "ConduitTradeShowMonitor/1.0"},
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        return TradeShowDateVerification(
            tracker_start_date=tracker_start_date,
            official_start_date=None,
            effective_start_date=tracker_start_date,
            status="unverified",
            official_url=official_url,
            message=f"Official organizer page could not be verified: {exc}",
        )
    finally:
        if owns_client:
            client.close()

    page_text = _strip_html_to_text(response.text[:500_000])
    nearby_dates = [
        candidate
        for candidate in _extract_start_dates_from_text(page_text)
        if abs((candidate - tracker_start_date).days) <= DATE_MATCH_WINDOW_DAYS
    ]
    if not nearby_dates:
        return TradeShowDateVerification(
            tracker_start_date=tracker_start_date,
            official_start_date=None,
            effective_start_date=tracker_start_date,
            status="unverified",
            official_url=official_url,
            message="The official organizer page did not expose a nearby parseable date.",
        )

    official_start = min(nearby_dates, key=lambda candidate: abs((candidate - tracker_start_date).days))
    if official_start == tracker_start_date:
        return TradeShowDateVerification(
            tracker_start_date=tracker_start_date,
            official_start_date=official_start,
            effective_start_date=official_start,
            status="matched",
            official_url=official_url,
            message="Tracker and official organizer dates match.",
        )
    delta = (official_start - tracker_start_date).days
    return TradeShowDateVerification(
        tracker_start_date=tracker_start_date,
        official_start_date=official_start,
        effective_start_date=official_start,
        status="mismatch",
        official_url=official_url,
        message=(
            f"Official organizer date is {official_start.isoformat()} "
            f"({delta:+d} day(s) versus the tracker); official date used for scheduling."
        ),
    )
