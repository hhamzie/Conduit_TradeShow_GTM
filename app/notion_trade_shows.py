from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
import json
import re
from typing import Any
from urllib.parse import quote, urlparse

import httpx

from app.trade_show_feeder import resolve_trade_show_scan_source_url


NOTION_API_BASE_URL = "https://api.notion.com/v1"
NOTION_API_VERSION = "2025-09-03"
NOTION_PAGE_SIZE = 100
NOTION_ID_RE = re.compile(r"(?i)([0-9a-f]{32})")


@dataclass(frozen=True)
class NotionTradeShowCandidate:
    """A normalized trade-show row from the Conduit Notion tracker."""

    notion_page_id: str
    show_name: str
    event_date: date
    event_end_date: date | None
    event_date_raw: str
    place: str
    link: str
    registered: bool
    notion_page_url: str = ""


class NotionTradeShowError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 502, error_code: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code


def normalize_notion_id(value: str) -> str:
    """Accept a Notion UUID, compact ID, or a copied Notion URL."""

    raw = value.strip()
    if not raw:
        raise ValueError("A Notion database ID is required.")
    compact = raw.replace("-", "")
    if re.fullmatch(r"(?i)[0-9a-f]{32}", compact):
        return compact.lower()
    match = NOTION_ID_RE.search(raw.replace("-", ""))
    if match:
        return match.group(1).lower()
    raise ValueError("The Notion database ID must be a UUID, compact 32-character ID, or Notion URL.")


def _property(properties: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    direct = properties.get(name)
    if isinstance(direct, Mapping):
        return direct
    normalized_name = " ".join(name.casefold().split())
    for key, value in properties.items():
        if " ".join(str(key).casefold().split()) == normalized_name and isinstance(value, Mapping):
            return value
    return {}


def _plain_text(items: object) -> str:
    if not isinstance(items, list):
        return ""
    chunks: list[str] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        plain_text = item.get("plain_text")
        if isinstance(plain_text, str):
            chunks.append(plain_text)
            continue
        text = item.get("text")
        if isinstance(text, Mapping) and isinstance(text.get("content"), str):
            chunks.append(str(text["content"]))
    return "".join(chunks).strip()


def _property_text(prop: Mapping[str, Any]) -> str:
    prop_type = prop.get("type")
    if prop_type in {"title", "rich_text"}:
        return _plain_text(prop.get(str(prop_type)))
    if prop_type in {"select", "status"}:
        selected = prop.get(str(prop_type))
        if isinstance(selected, Mapping) and isinstance(selected.get("name"), str):
            return str(selected["name"]).strip()
    if prop_type == "multi_select":
        selected = prop.get("multi_select")
        if isinstance(selected, list):
            return ", ".join(
                str(item["name"]).strip()
                for item in selected
                if isinstance(item, Mapping) and isinstance(item.get("name"), str) and str(item["name"]).strip()
            )
    if prop_type == "url" and isinstance(prop.get("url"), str):
        return str(prop["url"]).strip()
    if prop_type == "email" and isinstance(prop.get("email"), str):
        return str(prop["email"]).strip()
    if prop_type == "phone_number" and isinstance(prop.get("phone_number"), str):
        return str(prop["phone_number"]).strip()
    if prop_type == "formula":
        formula = prop.get("formula")
        if isinstance(formula, Mapping):
            value_type = formula.get("type")
            value = formula.get(str(value_type))
            if isinstance(value, (str, int, float, bool)):
                return str(value).strip()
    if prop_type == "rollup":
        rollup = prop.get("rollup")
        if isinstance(rollup, Mapping):
            value_type = rollup.get("type")
            value = rollup.get(str(value_type))
            if isinstance(value, (str, int, float, bool)):
                return str(value).strip()
    return ""


def _property_url(prop: Mapping[str, Any]) -> str:
    value = _property_text(prop)
    if value:
        return value
    files = prop.get("files")
    if not isinstance(files, list):
        return ""
    for item in files:
        if not isinstance(item, Mapping):
            continue
        item_type = item.get("type")
        payload = item.get(str(item_type))
        if isinstance(payload, Mapping) and isinstance(payload.get("url"), str):
            return str(payload["url"]).strip()
    return ""


def _parse_iso_date(value: object) -> date | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        if len(raw) == 10:
            return date.fromisoformat(raw)
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _property_date(prop: Mapping[str, Any]) -> tuple[date | None, date | None, str]:
    payload: object = None
    prop_type = prop.get("type")
    if prop_type == "date":
        payload = prop.get("date")
    elif prop_type in {"formula", "rollup"}:
        wrapper = prop.get(str(prop_type))
        if isinstance(wrapper, Mapping) and wrapper.get("type") == "date":
            payload = wrapper.get("date")
    if isinstance(payload, Mapping):
        raw_start = payload.get("start")
        return _parse_iso_date(raw_start), _parse_iso_date(payload.get("end")), str(raw_start or "").strip()

    raw = _property_text(prop)
    return _parse_iso_date(raw), None, raw


def _property_boolean(prop: Mapping[str, Any]) -> bool:
    prop_type = prop.get("type")
    if prop_type == "checkbox":
        return prop.get("checkbox") is True
    if prop_type == "formula":
        formula = prop.get("formula")
        if isinstance(formula, Mapping) and formula.get("type") == "boolean":
            return formula.get("boolean") is True
    value = _property_text(prop).casefold().strip()
    if value in {"true", "yes", "y", "1", "registered", "attending", "done", "complete"}:
        return True
    if value in {"false", "no", "n", "0", "not registered", "not attending", ""}:
        return False
    return False


def _normalize_http_url(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme.casefold() in {"http", "https"} and parsed.netloc:
        return raw
    return ""


def candidate_from_notion_page(page: Mapping[str, Any]) -> NotionTradeShowCandidate | None:
    """Map one Notion page, ignoring incomplete tracker rows without a name or date."""

    properties = page.get("properties")
    if not isinstance(properties, Mapping):
        return None

    show_name = _property_text(_property(properties, "Tradeshow Name")).strip()
    event_date, event_end_date, event_date_raw = _property_date(_property(properties, "Event Date"))
    if not show_name or event_date is None:
        return None

    place = _property_text(_property(properties, "Place")).strip()
    notion_url = _normalize_http_url(_property_url(_property(properties, "URL")))
    resolved_url = _normalize_http_url(resolve_trade_show_scan_source_url(show_name, notion_url))
    registered = _property_boolean(_property(properties, "Registered"))

    return NotionTradeShowCandidate(
        notion_page_id=str(page.get("id") or "").strip(),
        show_name=show_name,
        event_date=event_date,
        event_end_date=event_end_date,
        event_date_raw=event_date_raw,
        place=place,
        link=resolved_url,
        registered=registered,
        notion_page_url=_normalize_http_url(str(page.get("url") or "")),
    )


class NotionTradeShowClient:
    def __init__(
        self,
        *,
        token: str,
        database_id: str,
        data_source_id: str = "",
        http_client: httpx.Client | None = None,
        base_url: str = NOTION_API_BASE_URL,
        timeout_seconds: float = 30.0,
    ) -> None:
        if not token.strip():
            raise ValueError("A Notion integration token is required.")
        self.database_id = normalize_notion_id(database_id)
        self.data_source_id = normalize_notion_id(data_source_id) if data_source_id.strip() else ""
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.Client()
        self._headers = {
            "Authorization": f"Bearer {token.strip()}",
            "Content-Type": "application/json",
            "Notion-Version": NOTION_API_VERSION,
        }

    def close(self) -> None:
        if self._owns_http_client:
            self._http_client.close()

    def __enter__(self) -> NotionTradeShowClient:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def fetch_candidates(self) -> list[NotionTradeShowCandidate]:
        candidates: list[NotionTradeShowCandidate] = []
        seen_page_ids: set[str] = set()

        data_source_ids = [self.data_source_id] if self.data_source_id else self._discover_data_source_ids()
        for data_source_id in data_source_ids:
            cursor = ""
            seen_cursors: set[str] = set()

            while True:
                payload: dict[str, object] = {"page_size": NOTION_PAGE_SIZE}
                if cursor:
                    payload["start_cursor"] = cursor
                response_payload = self._query_data_source(data_source_id, payload)
                results = response_payload.get("results")
                if not isinstance(results, list):
                    raise NotionTradeShowError("Notion returned an invalid response: 'results' was not a list.")

                for result in results:
                    if not isinstance(result, Mapping):
                        continue
                    candidate = candidate_from_notion_page(result)
                    if candidate is None:
                        continue
                    if candidate.notion_page_id and candidate.notion_page_id in seen_page_ids:
                        continue
                    if candidate.notion_page_id:
                        seen_page_ids.add(candidate.notion_page_id)
                    candidates.append(candidate)

                has_more = response_payload.get("has_more") is True
                if not has_more:
                    break
                next_cursor = response_payload.get("next_cursor")
                if not isinstance(next_cursor, str) or not next_cursor.strip():
                    raise NotionTradeShowError("Notion indicated more results but did not return a next cursor.")
                cursor = next_cursor.strip()
                if cursor in seen_cursors:
                    raise NotionTradeShowError("Notion returned a repeated pagination cursor.")
                seen_cursors.add(cursor)

        return candidates

    def _discover_data_source_ids(self) -> list[str]:
        database_url = f"{self.base_url}/databases/{quote(self.database_id, safe='')}"
        database = self._request("GET", database_url)
        raw_data_sources = database.get("data_sources")
        if not isinstance(raw_data_sources, list):
            raise NotionTradeShowError("Notion returned an invalid database response: 'data_sources' was not a list.")
        data_source_ids: list[str] = []
        for raw_data_source in raw_data_sources:
            if not isinstance(raw_data_source, Mapping) or not isinstance(raw_data_source.get("id"), str):
                continue
            try:
                data_source_ids.append(normalize_notion_id(str(raw_data_source["id"])))
            except ValueError:
                continue
        if not data_source_ids:
            raise NotionTradeShowError("The Notion database has no accessible data sources.", status_code=422)
        return list(dict.fromkeys(data_source_ids))

    def _query_data_source(self, data_source_id: str, payload: Mapping[str, object]) -> Mapping[str, Any]:
        query_url = f"{self.base_url}/data_sources/{quote(data_source_id, safe='')}/query"
        return self._request("POST", query_url, payload=payload)

    def _request(
        self,
        method: str,
        url: str,
        *,
        payload: Mapping[str, object] | None = None,
    ) -> Mapping[str, Any]:
        try:
            response = self._http_client.request(
                method,
                url,
                headers=self._headers,
                json=dict(payload) if payload is not None else None,
                timeout=self.timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            raise NotionTradeShowError("The Notion API request timed out.", status_code=504) from exc
        except httpx.RequestError as exc:
            raise NotionTradeShowError(f"Could not reach the Notion API: {exc}", status_code=502) from exc

        try:
            decoded = response.json()
        except json.JSONDecodeError as exc:
            raise NotionTradeShowError(
                f"Notion returned a non-JSON response (HTTP {response.status_code}).",
                status_code=response.status_code,
            ) from exc

        if response.is_error:
            message = "Notion API request failed."
            error_code = ""
            if isinstance(decoded, Mapping):
                if isinstance(decoded.get("message"), str):
                    message = str(decoded["message"])
                if isinstance(decoded.get("code"), str):
                    error_code = str(decoded["code"])
            raise NotionTradeShowError(
                f"Notion API request failed (HTTP {response.status_code}): {message}",
                status_code=response.status_code,
                error_code=error_code,
            )
        if not isinstance(decoded, Mapping):
            raise NotionTradeShowError("Notion returned an invalid JSON response.")
        return decoded


def fetch_notion_trade_shows(
    *,
    token: str,
    database_id: str,
    data_source_id: str = "",
    http_client: httpx.Client | None = None,
) -> list[NotionTradeShowCandidate]:
    """Fetch and normalize every complete row from a Notion trade-show tracker."""

    with NotionTradeShowClient(
        token=token,
        database_id=database_id,
        data_source_id=data_source_id,
        http_client=http_client,
    ) as client:
        return client.fetch_candidates()
