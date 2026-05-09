from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import json

import httpx

from app.config import get_settings
from scraper import OPENAI_RESPONSES_URL, extract_text_from_openai_response


PHYSICAL_GOODS_INCLUDE_TERMS = (
    "furniture",
    "home",
    "gift",
    "decor",
    "design",
    "market",
    "packaging",
    "housewares",
    "kitchen",
    "hardware",
    "industrial",
    "manufacturing",
    "supplier",
    "sourcing",
    "apparel",
    "textile",
    "materials",
    "pet",
    "foodservice",
    "restaurant",
    "building",
    "construction",
    "fabric",
    "furnishings",
)
PHYSICAL_GOODS_EXCLUDE_TERMS = (
    "saas",
    "software",
    "crypto",
    "web3",
    "gaming",
    "media",
    "influencer",
    "creator",
    "adtech",
    "fintech",
)


@dataclass(frozen=True)
class TradeShowScanCandidate:
    show_name: str
    event_date_raw: str
    place: str
    link: str
    summary: str


class TradeShowScanError(Exception):
    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


SCAN_MODEL_FALLBACKS = ("gpt-4.1-mini", "gpt-4.1")


def is_b2b_physical_goods_show(show_name: str, source_url: str = "") -> bool:
    haystack = f"{show_name} {source_url}".strip().lower()
    if not haystack:
        return False
    if any(term in haystack for term in PHYSICAL_GOODS_EXCLUDE_TERMS):
        return False
    return any(term in haystack for term in PHYSICAL_GOODS_INCLUDE_TERMS)


def scan_upcoming_trade_shows(
    *,
    query_hint: str = "",
    today: date | None = None,
    lookahead_days: int | None = None,
    limit: int = 8,
) -> list[TradeShowScanCandidate]:
    settings = get_settings()
    api_key = settings.openai_api_key
    if not api_key:
        raise ValueError("Set OPENAI_API_KEY before scanning for upcoming trade shows.")

    model = settings.trade_show_scan_model
    start_date = today or date.today()
    days_ahead = lookahead_days or settings.weekly_show_sync_lookahead_days
    end_date = start_date + timedelta(days=days_ahead)
    normalized_hint = query_hint.strip()

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "shows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "show_name": {"type": "string"},
                        "event_date": {"type": "string"},
                        "place": {"type": "string"},
                        "link": {"type": "string"},
                        "summary": {"type": "string"},
                    },
                    "required": ["show_name", "event_date", "place", "link", "summary"],
                },
            }
        },
        "required": ["shows"],
    }

    prompt = (
        f"Find upcoming B2B physical-goods trade shows between {start_date.isoformat()} and {end_date.isoformat()}. "
        "Only include shows where exhibitors are likely manufacturers, wholesalers, suppliers, or brands selling physical goods. "
        "Exclude software, creator, media, fintech, or purely digital events. "
        "Prefer official exhibitor directory URLs. If there is no public directory, use the best official show page. "
        "Focus on North American shows. Keep summaries short and direct."
    )
    if normalized_hint:
        prompt = f"{prompt} Extra focus: {normalized_hint}."

    request_payload = {
        "tools": [
            {
                "type": "web_search",
            }
        ],
        "tool_choice": "auto",
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You are a trade show feeder for outbound sales operations. "
                            "Find real upcoming trade shows, not blog posts or recap articles. "
                            "Return only events that fit B2B physical-goods supplier outreach."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": prompt,
                    }
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "upcoming_trade_show_scan",
                "strict": True,
                "schema": schema,
            }
        },
        "max_output_tokens": 1800,
    }

    candidate_models: list[str] = [model]
    candidate_models.extend(
        fallback_model
        for fallback_model in SCAN_MODEL_FALLBACKS
        if fallback_model != model
    )

    body: dict[str, object] | None = None
    last_error: httpx.HTTPStatusError | None = None
    for candidate_model in candidate_models:
        payload = dict(request_payload)
        payload["model"] = candidate_model
        response = httpx.post(
            OPENAI_RESPONSES_URL,
            json=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            timeout=90.0,
            follow_redirects=True,
        )
        try:
            response.raise_for_status()
            body = response.json()
            break
        except httpx.HTTPStatusError as exc:
            last_error = exc
            if not _should_retry_scan_with_fallback(exc):
                raise _build_trade_show_scan_error(exc) from exc
            continue

    if body is None:
        if last_error is not None:
            raise _build_trade_show_scan_error(last_error) from last_error
        return []

    text = extract_text_from_openai_response(body)
    if not text:
        return []
    parsed = json.loads(text)
    raw_shows = parsed.get("shows", []) if isinstance(parsed, dict) else []
    if not isinstance(raw_shows, list):
        return []

    candidates: list[TradeShowScanCandidate] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for item in raw_shows:
        if not isinstance(item, dict):
            continue
        show_name = str(item.get("show_name") or "").strip()
        event_date_raw = str(item.get("event_date") or "").strip()
        place = str(item.get("place") or "").strip()
        link = str(item.get("link") or "").strip()
        summary = str(item.get("summary") or "").strip()
        if not (show_name and event_date_raw and place and link):
            continue
        if not is_b2b_physical_goods_show(show_name, link):
            continue
        dedupe_key = (show_name.lower(), event_date_raw, link.lower())
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        candidates.append(
            TradeShowScanCandidate(
                show_name=show_name,
                event_date_raw=event_date_raw,
                place=place,
                link=link,
                summary=summary,
            )
        )
        if len(candidates) >= max(1, limit):
            break

    return candidates


def _should_retry_scan_with_fallback(error: httpx.HTTPStatusError) -> bool:
    response = error.response
    if response.status_code != 404:
        return False
    try:
        payload = response.json()
    except ValueError:
        return False
    if not isinstance(payload, dict):
        return False
    details = payload.get("error")
    if not isinstance(details, dict):
        return False
    code = str(details.get("code") or "").strip().lower()
    message = str(details.get("message") or "").strip().lower()
    return code == "model_not_found" or "must be verified to use the model" in message


def _build_trade_show_scan_error(error: httpx.HTTPStatusError) -> TradeShowScanError:
    response = error.response
    status_code = response.status_code
    payload_message = _extract_openai_error_message(response)

    if status_code == 429:
        message = "Trade show scan is rate limited right now. Try again later."
        if payload_message:
            message = f"{message} {payload_message}"
        return TradeShowScanError(message, status_code=429)

    if status_code == 401:
        return TradeShowScanError("Trade show scan key is invalid. Update OPENAI_API_KEY.", status_code=401)

    if status_code == 403:
        return TradeShowScanError("Trade show scan is blocked for this project right now.", status_code=403)

    if status_code == 404 and payload_message:
        return TradeShowScanError(payload_message, status_code=400)

    if payload_message:
        return TradeShowScanError(f"Trade show scan failed. {payload_message}", status_code=502)

    return TradeShowScanError("Trade show scan failed right now. Try again later.", status_code=502)


def _extract_openai_error_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return ""
    if not isinstance(payload, dict):
        return ""
    details = payload.get("error")
    if not isinstance(details, dict):
        return ""
    return str(details.get("message") or "").strip()
