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
    "icff",
    "interior design",
    "interiors",
    "architecture",
    "architectural",
    "real estate",
    "property",
    "hospitality design",
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


@dataclass(frozen=True)
class CuratedTradeShowSource:
    aliases: tuple[str, ...]
    official_url: str
    directory_url: str = ""


SCAN_MODEL_FALLBACKS = ("gpt-4.1-mini", "gpt-4.1")
TRADE_SHOW_SCAN_DISCOVERY_DOMAINS = (
    "tsnn.com",
    "eventsinamerica.com",
    "tradefairdates.com",
    "10times.com",
    "tradefest.io",
    "expodatabase.com",
)
TRADE_SHOW_SCAN_DIRECTORY_DOMAINS = (
    "mapyourshow.com",
    "expofp.com",
    "eventscribe.net",
    "personifycloud.com",
    "bulletin.co",
)
TRADE_SHOW_SCAN_OFFICIAL_DOMAINS = (
    "highpointmarket.org",
    "hpmkt.highpointmarket.org",
    "andmore.com",
    "atlantamarket.com",
    "americasmart.com",
    "lasvegasmarket.com",
    "dallasmarketcenter.com",
    "infocommshow.org",
    "avixa.org",
    "nationalrestaurantshow.com",
    "restaurant.org",
    "sweetsandsnacks.com",
    "specialtyfood.com",
    "thecarwashshow.com",
    "luxepack.com",
    "packexpo.com",
    "globalpetexpo.org",
    "asdonline.com",
    "asdmarketweek.com",
    "nacsshow.com",
)
TRADE_SHOW_SCAN_ALLOWED_DOMAINS = (
    *TRADE_SHOW_SCAN_DISCOVERY_DOMAINS,
    *TRADE_SHOW_SCAN_DIRECTORY_DOMAINS,
    *TRADE_SHOW_SCAN_OFFICIAL_DOMAINS,
)
CURATED_TRADE_SHOW_SOURCES = (
    CuratedTradeShowSource(
        aliases=("high point market",),
        official_url="https://www.highpointmarket.org/",
        directory_url="https://www.highpointmarket.org/ExhibitorDirectory?alpha=U",
    ),
    CuratedTradeShowSource(
        aliases=("atlanta market",),
        official_url="https://www.atlantamarket.com/",
        directory_url="https://www.atlantamarket.com/exhibitor/exhibitor-directory",
    ),
    CuratedTradeShowSource(
        aliases=("las vegas market", "vegas market"),
        official_url="https://www.lasvegasmarket.com/en/Visit/Market-Dates-and-Hours",
        directory_url="https://www.lasvegasmarket.com/en/exhibitor/exhibitor-directory",
    ),
    CuratedTradeShowSource(
        aliases=("lightovation",),
        official_url="https://www.dallasmarketcenter.com/lightovation",
    ),
    CuratedTradeShowSource(
        aliases=("national restaurant association show", "national restaurant show"),
        official_url="https://www.nationalrestaurantshow.com/",
        directory_url="https://www.nationalrestaurantshow.com/home/search/",
    ),
    CuratedTradeShowSource(
        aliases=("sweets & snacks", "sweets and snacks", "sweets & snacks expo"),
        official_url="https://sweetsandsnacks.com/",
        directory_url="https://sse26.mapyourshow.com/",
    ),
    CuratedTradeShowSource(
        aliases=("the car wash show",),
        official_url="https://thecarwashshow.com/",
    ),
    CuratedTradeShowSource(
        aliases=("infocomm", "infocomm las vegas"),
        official_url="https://www.infocommshow.org/",
    ),
    CuratedTradeShowSource(
        aliases=("luxe pack", "luxe pack new york", "luxe pack los angeles"),
        official_url="https://www.luxepack.com/",
    ),
    CuratedTradeShowSource(
        aliases=("pack expo", "pack expo las vegas", "pack expo international"),
        official_url="https://www.packexpo.com/",
    ),
    CuratedTradeShowSource(
        aliases=("global pet expo",),
        official_url="https://globalpetexpo.org/",
    ),
    CuratedTradeShowSource(
        aliases=("asd market week", "asd", "asd las vegas"),
        official_url="https://www.asdonline.com/",
    ),
    CuratedTradeShowSource(
        aliases=("nacs show",),
        official_url="https://www.nacsshow.com/About",
    ),
)


def _normalize_source_domain(url: str) -> str:
    normalized = url.strip().lower()
    if not normalized:
        return ""
    normalized = normalized.split("://", 1)[-1]
    normalized = normalized.split("/", 1)[0]
    return normalized.removeprefix("www.")


def _domain_matches_allowed_host(domain: str, allowed_hosts: tuple[str, ...]) -> bool:
    return any(domain == host or domain.endswith(f".{host}") for host in allowed_hosts)


def _normalize_show_name(value: str) -> str:
    return " ".join(value.strip().lower().replace("&", " and ").split())


def _find_curated_trade_show_source(show_name: str) -> CuratedTradeShowSource | None:
    normalized_name = _normalize_show_name(show_name)
    for source in CURATED_TRADE_SHOW_SOURCES:
        if any(alias in normalized_name for alias in source.aliases):
            return source
    return None


def resolve_trade_show_scan_source_url(show_name: str, url: str) -> str:
    curated_source = _find_curated_trade_show_source(show_name)
    if curated_source is not None:
        return curated_source.directory_url or curated_source.official_url
    return url.strip()


def is_trade_show_scan_final_source_url(url: str) -> bool:
    domain = _normalize_source_domain(url)
    if not domain:
        return False
    return _domain_matches_allowed_host(domain, TRADE_SHOW_SCAN_OFFICIAL_DOMAINS) or _domain_matches_allowed_host(
        domain,
        TRADE_SHOW_SCAN_DIRECTORY_DOMAINS,
    )


def is_b2b_physical_goods_show(show_name: str, source_url: str = "") -> bool:
    haystack = f"{show_name} {source_url}".strip().lower()
    if not haystack:
        return False
    if any(term in haystack for term in PHYSICAL_GOODS_EXCLUDE_TERMS):
        return False
    return any(term in haystack for term in PHYSICAL_GOODS_INCLUDE_TERMS)


def _build_scan_web_search_tool(model: str) -> dict[str, object]:
    tool: dict[str, object] = {
        "type": "web_search",
        "user_location": {
            "type": "approximate",
            "country": "US",
            "timezone": "America/New_York",
        },
    }
    if model.startswith("gpt-5"):
        tool["filters"] = {
            "allowed_domains": list(TRADE_SHOW_SCAN_ALLOWED_DOMAINS),
        }
    return tool


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

    base_prompt = (
        f"Find upcoming B2B physical-goods trade shows between {start_date.isoformat()} and {end_date.isoformat()}. "
        "Only include shows where exhibitors are likely manufacturers, wholesalers, suppliers, or brands selling physical goods. "
        "Exclude software, creator, media, fintech, and digital-only events. "
        "Exclude interior-design, architecture, hospitality-design, property, and real-estate-adjacent events such as ICFF. "
        "Use discovery sites only to find candidates, then verify them against official organizer pages or official exhibitor directories. "
        "The final link you return must be an official show page or official exhibitor directory URL, never a listing site. "
        "Prioritize this source stack: High Point Market, Atlanta Market, Las Vegas Market, Dallas Market Center / Lightovation, "
        "National Restaurant Association Show, Sweets & Snacks Expo, The Car Wash Show, InfoComm, LUXE PACK, PACK EXPO, "
        "Global Pet Expo, ASD Market Week, and NACS Show. "
        "Focus on North American shows. Keep summaries short and direct."
    )
    if normalized_hint:
        base_prompt = f"{base_prompt} Extra focus: {normalized_hint}."

    prompts = (
        base_prompt,
        (
            f"{base_prompt} Prioritize official wholesale market sources first: High Point Market, Atlanta Market, "
            "Las Vegas Market, Dallas Market Center, and Lightovation. Use their exhibitor directories when available. "
            "Look for upcoming shows in the next 100 days that are not already listed."
        ),
        (
            f"{base_prompt} Prioritize foodservice, packaging, restaurant supply, snacks, housewares, gifting, pet, "
            "manufacturing, sourcing, and hardware supplier shows such as National Restaurant Association Show, Sweets & Snacks Expo, "
            "PACK EXPO, Global Pet Expo, ASD Market Week, NACS Show, and The Car Wash Show. Search discovery sites, then verify on official pages."
        ),
    )

    request_payload = {
        "tool_choice": "auto",
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

    candidates: list[TradeShowScanCandidate] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for prompt in prompts:
        raw_shows = _run_trade_show_scan_pass(
            api_key=api_key,
            candidate_models=candidate_models,
            request_payload=request_payload,
            prompt=prompt,
        )
        for item in raw_shows:
            if not isinstance(item, dict):
                continue
            show_name = str(item.get("show_name") or "").strip()
            event_date_raw = str(item.get("event_date") or "").strip()
            place = str(item.get("place") or "").strip()
            link = resolve_trade_show_scan_source_url(show_name, str(item.get("link") or "").strip())
            summary = str(item.get("summary") or "").strip()
            if not (show_name and event_date_raw and place and link):
                continue
            if not is_b2b_physical_goods_show(show_name, link):
                continue
            if not is_trade_show_scan_final_source_url(link):
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
                return candidates
        if candidates:
            return candidates

    return candidates


def _run_trade_show_scan_pass(
    *,
    api_key: str,
    candidate_models: list[str],
    request_payload: dict[str, object],
    prompt: str,
) -> list[dict[str, object]]:
    payload_input = [
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
    ]

    body: dict[str, object] | None = None
    last_error: httpx.HTTPStatusError | None = None
    for candidate_model in candidate_models:
        payload = dict(request_payload)
        payload["model"] = candidate_model
        payload["tools"] = [_build_scan_web_search_tool(candidate_model)]
        payload["input"] = payload_input
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
    return raw_shows if isinstance(raw_shows, list) else []


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
