from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any, TypeVar
from urllib.parse import quote

import httpx


AIRTABLE_API_BASE_URL = "https://api.airtable.com/v0"
AIRTABLE_MAX_RECORDS_PER_REQUEST = 10

SHOW_MERGE_FIELDS = ("Dashboard Show ID",)
COMPANY_MERGE_FIELDS = ("Show Name", "Source Row ID")
CONTACT_MERGE_FIELDS = ("Contact Key",)
CAMPAIGN_PUSH_MERGE_FIELDS = ("Push Key",)

T = TypeVar("T")


class AirtableSyncError(RuntimeError):
    """A token-safe error raised when an Airtable sync request cannot complete."""

    def __init__(self, message: str, *, status_code: int = 502, error_type: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_type = error_type


@dataclass(frozen=True)
class AirtableUpsertResult:
    """The combined result of one or more Airtable upsert batches."""

    records: tuple[Mapping[str, Any], ...] = ()
    created_record_ids: tuple[str, ...] = ()
    updated_record_ids: tuple[str, ...] = ()
    request_count: int = 0


def deterministic_company_source_row_id(
    *,
    show_identifier: object,
    source_identifier: object,
    company_name: object = "",
    website: object = "",
    booth_number: object = "",
) -> str:
    """Build a stable source-row identity for a scraped exhibitor.

    The upstream show and source identifiers define the namespace. Company,
    website, and booth data distinguish multiple exhibitors within that source.
    Whitespace and casing differences do not change the result.
    """

    show = _identity_part(show_identifier)
    source = _identity_part(source_identifier)
    company = _identity_part(company_name)
    site = _identity_part(website)
    booth = _identity_part(booth_number)
    if not show:
        raise ValueError("A show identifier is required to build a company source row ID.")
    if not source:
        raise ValueError("A source identifier is required to build a company source row ID.")
    if not any((company, site, booth)):
        raise ValueError("Company name, website, or booth number is required to identify the source row.")
    digest = hashlib.sha256("|".join((show, source, company, site, booth)).encode("utf-8")).hexdigest()[:24]
    return f"company-{digest}"


def _identity_part(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().casefold())


def _record_ids(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    ids: list[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            ids.append(item.strip())
        elif isinstance(item, Mapping) and isinstance(item.get("id"), str) and str(item["id"]).strip():
            ids.append(str(item["id"]).strip())
    return ids


def _chunks(values: Sequence[T], size: int) -> Iterable[Sequence[T]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


class AirtableSyncClient:
    """Small Airtable REST client for idempotent, batched record synchronization."""

    def __init__(
        self,
        *,
        token: str,
        base_id: str,
        http_client: httpx.Client | None = None,
        api_base_url: str = AIRTABLE_API_BASE_URL,
        timeout_seconds: float = 30.0,
        batch_size: int = AIRTABLE_MAX_RECORDS_PER_REQUEST,
    ) -> None:
        clean_token = token.strip()
        clean_base_id = base_id.strip()
        if not clean_token:
            raise ValueError("An Airtable personal access token is required.")
        if not clean_base_id:
            raise ValueError("An Airtable base ID is required.")
        if not 1 <= batch_size <= AIRTABLE_MAX_RECORDS_PER_REQUEST:
            raise ValueError(f"Airtable batch size must be between 1 and {AIRTABLE_MAX_RECORDS_PER_REQUEST}.")

        self.base_id = clean_base_id
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.batch_size = batch_size
        self._token = clean_token
        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.Client()
        self._headers = {
            "Authorization": f"Bearer {clean_token}",
            "Content-Type": "application/json",
        }

    def close(self) -> None:
        if self._owns_http_client:
            self._http_client.close()

    def __enter__(self) -> AirtableSyncClient:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def upsert_records(
        self,
        *,
        table_id: str,
        records: Iterable[Mapping[str, Any]],
        merge_fields: Sequence[str],
        typecast: bool = False,
    ) -> AirtableUpsertResult:
        """Upsert field dictionaries with Airtable's ``performUpsert`` option.

        Airtable's REST API accepts at most ten records per request. This method
        batches larger inputs and combines the response metadata.
        """

        clean_table_id = table_id.strip()
        if not clean_table_id:
            raise ValueError("An Airtable table ID or name is required.")
        clean_merge_fields = tuple(str(field).strip() for field in merge_fields if str(field).strip())
        if not clean_merge_fields:
            raise ValueError("At least one Airtable merge field is required.")
        if len(set(clean_merge_fields)) != len(clean_merge_fields):
            raise ValueError("Airtable merge fields must be unique.")

        field_records = [self._validate_fields(record, clean_merge_fields) for record in records]
        if not field_records:
            return AirtableUpsertResult()

        response_records: list[Mapping[str, Any]] = []
        created_ids: list[str] = []
        updated_ids: list[str] = []
        request_count = 0

        for batch in _chunks(field_records, self.batch_size):
            payload = {
                "performUpsert": {"fieldsToMergeOn": list(clean_merge_fields)},
                "records": [{"fields": dict(fields)} for fields in batch],
                "typecast": typecast,
            }
            decoded = self._request("PATCH", clean_table_id, payload)
            raw_records = decoded.get("records")
            if not isinstance(raw_records, list):
                raise AirtableSyncError("Airtable returned an invalid upsert response: 'records' was not a list.")
            response_records.extend(record for record in raw_records if isinstance(record, Mapping))
            created_ids.extend(_record_ids(decoded.get("createdRecords")))
            updated_ids.extend(_record_ids(decoded.get("updatedRecords")))
            request_count += 1

        return AirtableUpsertResult(
            records=tuple(response_records),
            created_record_ids=tuple(created_ids),
            updated_record_ids=tuple(updated_ids),
            request_count=request_count,
        )

    def upsert_shows(
        self,
        *,
        table_id: str,
        records: Iterable[Mapping[str, Any]],
        merge_fields: Sequence[str] = SHOW_MERGE_FIELDS,
        typecast: bool = True,
    ) -> AirtableUpsertResult:
        return self.upsert_records(
            table_id=table_id,
            records=records,
            merge_fields=merge_fields,
            typecast=typecast,
        )

    def upsert_companies(
        self,
        *,
        table_id: str,
        records: Iterable[Mapping[str, Any]],
        merge_fields: Sequence[str] = COMPANY_MERGE_FIELDS,
        typecast: bool = True,
    ) -> AirtableUpsertResult:
        return self.upsert_records(
            table_id=table_id,
            records=records,
            merge_fields=merge_fields,
            typecast=typecast,
        )

    def upsert_contacts(
        self,
        *,
        table_id: str,
        records: Iterable[Mapping[str, Any]],
        merge_fields: Sequence[str] = CONTACT_MERGE_FIELDS,
        typecast: bool = True,
    ) -> AirtableUpsertResult:
        return self.upsert_records(
            table_id=table_id,
            records=records,
            merge_fields=merge_fields,
            typecast=typecast,
        )

    def upsert_campaign_pushes(
        self,
        *,
        table_id: str,
        records: Iterable[Mapping[str, Any]],
        merge_fields: Sequence[str] = CAMPAIGN_PUSH_MERGE_FIELDS,
        typecast: bool = True,
    ) -> AirtableUpsertResult:
        return self.upsert_records(
            table_id=table_id,
            records=records,
            merge_fields=merge_fields,
            typecast=typecast,
        )

    @staticmethod
    def _validate_fields(record: Mapping[str, Any], merge_fields: Sequence[str]) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise TypeError("Each Airtable record must be a mapping of field names to values.")
        fields = dict(record)
        for field in merge_fields:
            value = fields.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"Airtable merge field '{field}' must be present and non-empty on every record.")
        return fields

    def _request(
        self,
        method: str,
        table_id: str,
        payload: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        url = (
            f"{self.api_base_url}/{quote(self.base_id, safe='')}"
            f"/{quote(table_id, safe='')}"
        )
        try:
            response = self._http_client.request(
                method,
                url,
                headers=self._headers,
                json=dict(payload),
                timeout=self.timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            raise AirtableSyncError("The Airtable API request timed out.", status_code=504) from exc
        except httpx.RequestError as exc:
            raise AirtableSyncError("Could not reach the Airtable API.", status_code=502) from exc

        try:
            decoded = response.json()
        except (json.JSONDecodeError, ValueError) as exc:
            if response.is_error:
                raise AirtableSyncError(
                    f"Airtable API request failed (HTTP {response.status_code}) with a non-JSON response.",
                    status_code=response.status_code,
                ) from exc
            raise AirtableSyncError("Airtable returned an invalid non-JSON response.") from exc

        if response.is_error:
            error_type, message = self._extract_error(decoded)
            safe_message = self._redact(message)[:500]
            raise AirtableSyncError(
                f"Airtable API request failed (HTTP {response.status_code}): {safe_message}",
                status_code=response.status_code,
                error_type=error_type,
            )
        if not isinstance(decoded, Mapping):
            raise AirtableSyncError("Airtable returned an invalid JSON response.")
        return decoded

    @staticmethod
    def _extract_error(decoded: object) -> tuple[str, str]:
        error: object = decoded.get("error") if isinstance(decoded, Mapping) else None
        if isinstance(error, Mapping):
            error_type = str(error.get("type") or "").strip()
            message = str(error.get("message") or error_type or "Airtable rejected the request.").strip()
            return error_type, message
        if isinstance(error, str) and error.strip():
            return "", error.strip()
        return "", "Airtable rejected the request."

    def _redact(self, message: str) -> str:
        safe = message.replace(self._token, "[REDACTED]")
        safe = re.sub(r"(?i)\bbearer\s+[a-z0-9._~+/=-]+", "Bearer [REDACTED]", safe)
        safe = re.sub(r"(?i)\bpat[a-z0-9._-]{8,}", "[REDACTED]", safe)
        return safe
