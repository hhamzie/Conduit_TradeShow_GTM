#!/usr/bin/env python3
"""Revalidate existing Vegas contacts and reconcile them into the paused campaign.

This is intentionally scoped to the already-scraped Las Vegas Market records. It
does not discover new people, create a campaign, or activate outreach.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
BASE_ID = "appfBCKnwzWr26p8R"
LEADS = "tblhzByGqquL0czNF"
CONTACT_TABLES = {
    "Sales": "tblh8mxFnH3xYuDkK",
    "Ops": "tblwk9D9Ve1RxwfNa",
    "CS": "tblRtquRALrH3ZXEk",
}
CAMPAIGN_ID = "3628683"


def clean_email(value: Any) -> str:
    value = str(value or "").strip().lower()
    return value if "@" in value and "." in value.rsplit("@", 1)[-1] else ""


def usable_first_name(value: Any) -> str:
    candidate = str(value or "").strip().split()[0] if str(value or "").strip() else ""
    candidate = re.sub(r"[^A-Za-z'-]", "", candidate)
    if len(candidate) < 2 or candidate.lower() in {"unknown", "contact", "sales", "team", "info", "admin", "support", "n/a"}:
        return ""
    return candidate


def contact_name_and_title(contact: dict[str, Any]) -> tuple[str, str, str]:
    fields = contact["fields"]
    persona = contact["persona"]
    if persona == "Sales":
        name = fields.get("Sales Full Name") or fields.get("First Name") or fields.get("First Name (2)")
        title = fields.get("Sales Job Title")
    elif persona == "Ops":
        name = fields.get("Ops Full Name") or fields.get("First Name")
        title = fields.get("Ops Job Title")
    else:
        name = fields.get("CS Full Name") or fields.get("First_Name") or fields.get("First Name")
        title = fields.get("CS Job Title")
    return str(name or "").strip(), str(title or "").strip(), usable_first_name(name)


def valid_persona(persona: str, title: str) -> bool:
    text = title.lower()
    if not text or any(flag in text for flag in ("not found", "unknown", "n/a", "placeholder")):
        return False
    patterns = {
        "Sales": r"\b(sales|revenue|commercial|growth|business development|partnership|account executive|account manager|cro|chief revenue)\b",
        "Ops": r"\b(operations|ops|coo|supply chain|logistics|manufacturing|production|procurement|fulfillment|warehouse)\b",
        "CS": r"\b(customer success|customer support|customer service|client success|client service|support manager|support director|service manager|service director|customer care)\b",
    }
    return bool(re.search(patterns[persona], text, flags=re.I))


def vegas_record(fields: dict[str, Any], vegas_source_ids: set[str], persona: str) -> bool:
    if persona == "Sales":
        return str(fields.get("Rows from: las_vegas_market_exhibitors", "")) in vegas_source_ids
    return "las vegas market" in str(fields.get("Conference", "")).lower()


async def airtable_rows(client: httpx.AsyncClient, headers: dict[str, str], table: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset: str | None = None
    while True:
        params: dict[str, Any] = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        response = await client.get(f"https://api.airtable.com/v0/{BASE_ID}/{table}", headers=headers, params=params)
        response.raise_for_status()
        payload = response.json()
        rows.extend(payload["records"])
        offset = payload.get("offset")
        if not offset:
            return rows


async def validate(client: httpx.AsyncClient, email: str, key: str, semaphore: asyncio.Semaphore) -> tuple[str, dict[str, Any]]:
    async with semaphore:
        for attempt in range(3):
            try:
                response = await client.post(
                    "https://api.enrichley.io/api/v1/validate-single-email",
                    headers={"X-Api-Key": key},
                    json={"email": email},
                )
                response.raise_for_status()
                return email, response.json()
            except (httpx.HTTPError, ValueError) as error:
                if attempt == 2:
                    return email, {"valid": False, "result": "unknown", "validator_error": str(error)[:160]}
                await asyncio.sleep(2 ** attempt)


async def patch_airtable(client: httpx.AsyncClient, headers: dict[str, str], table: str, record_id: str, fields: dict[str, Any]) -> None:
    response = await client.patch(
        f"https://api.airtable.com/v0/{BASE_ID}/{table}/{record_id}", headers=headers, json={"fields": fields, "typecast": True}
    )
    response.raise_for_status()


async def patch_airtable_batch(
    client: httpx.AsyncClient, headers: dict[str, str], table: str, records: list[dict[str, Any]]
) -> None:
    response = await client.patch(
        f"https://api.airtable.com/v0/{BASE_ID}/{table}", headers=headers, json={"records": records, "typecast": True}
    )
    response.raise_for_status()


async def campaign_emails(client: httpx.AsyncClient, key: str) -> set[str]:
    emails: set[str] = set()
    offset = 0
    while True:
        response = await client.get(
            f"https://server.smartlead.ai/api/v1/campaigns/{CAMPAIGN_ID}/leads",
            params={"api_key": key, "offset": offset, "limit": 100},
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("data", payload) if isinstance(payload, dict) else payload
        if not isinstance(batch, list):
            raise RuntimeError("Unexpected Smartlead campaign-leads response.")
        emails.update(
            clean_email((row.get("lead") or {}).get("email") if isinstance(row, dict) else "")
            for row in batch
        )
        if len(batch) < 100:
            return emails - {""}
        offset += len(batch)


async def add_to_campaign(client: httpx.AsyncClient, key: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "lead_list": rows,
        "settings": {
            "ignore_duplicate_leads_in_other_campaign": False,
            "ignore_global_block_list": False,
            "ignore_unsubscribe_list": False,
            "ignore_community_bounce_list": False,
        },
    }
    response = await client.post(
        f"https://server.smartlead.ai/api/v1/campaigns/{CAMPAIGN_ID}/leads", params={"api_key": key}, json=payload
    )
    response.raise_for_status()
    return response.json()


async def main() -> None:
    load_dotenv(ROOT / ".env")
    airtable_token = os.environ["AIRTABLE_TOKEN"]
    enrichley_key = os.environ["ENRICHLEY_API_KEY"]
    smartlead_key = os.environ["SMARTLEAD_API_KEY"]
    airtable_headers = {"Authorization": f"Bearer {airtable_token}", "Content-Type": "application/json"}
    timeout = httpx.Timeout(60.0)
    async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
        leads = await airtable_rows(client, airtable_headers, LEADS)
        vegas_source_ids = {
            str(row["fields"].get("Source Row ID", ""))
            for row in leads
            if "las vegas market" in str(row["fields"].get("Conference", "")).lower()
        }
        contacts: list[dict[str, Any]] = []
        for persona, table in CONTACT_TABLES.items():
            for row in await airtable_rows(client, airtable_headers, table):
                if vegas_record(row["fields"], vegas_source_ids, persona):
                    contacts.append({"persona": persona, "table": table, "id": row["id"], "fields": row["fields"]})

        by_email: dict[str, list[dict[str, Any]]] = {}
        for contact in contacts:
            email = clean_email(contact["fields"].get("Final Work Email"))
            if email:
                by_email.setdefault(email, []).append(contact)

        cached_verdicts: dict[str, dict[str, Any]] = {}
        for email, matching_contacts in by_email.items():
            for contact in matching_contacts:
                try:
                    cached = json.loads(str(contact["fields"].get("Validate Email", ""))).get("final", {})
                except json.JSONDecodeError:
                    cached = {}
                if isinstance(cached, dict) and ("valid" in cached or "result" in cached):
                    cached_verdicts[email] = cached
                    break

        semaphore = asyncio.Semaphore(10)
        # Enrichley permits 10 requests per second. Pacing full batches keeps the
        # reconciliation reliable rather than spending credits on rate-limit errors.
        results: list[tuple[str, dict[str, Any]]] = []
        emails = [email for email in by_email if email not in cached_verdicts]
        for start in range(0, len(emails), 10):
            batch = emails[start:start + 10]
            results.extend(await asyncio.gather(*(validate(client, email, enrichley_key, semaphore) for email in batch)))
            print(f"validated {min(start + 10, len(emails))}/{len(emails)}", flush=True)
            if start + 10 < len(emails):
                await asyncio.sleep(1.05)
        print(f"validated {len(results)} new final emails; reused {len(cached_verdicts)} completed results", flush=True)
        verdicts = {**cached_verdicts, **dict(results)}
        status = lambda verdict: "valid" if verdict.get("valid") is True else "invalid" if verdict.get("result") in {"invalid", "catch_all"} else "unknown"

        airtable_updates: dict[str, list[dict[str, Any]]] = {table: [] for table in CONTACT_TABLES.values()}
        for email, matching_contacts in by_email.items():
            verdict = verdicts[email]
            payload = json.dumps({"final": verdict, "finalStatus": status(verdict)}, separators=(",", ":"))
            for contact in matching_contacts:
                airtable_updates[contact["table"]].append({"id": contact["id"], "fields": {"Validate Email": payload}})
        for table, updates in airtable_updates.items():
            for start in range(0, len(updates), 10):
                await patch_airtable_batch(client, airtable_headers, table, updates[start:start + 10])
                await asyncio.sleep(0.22)
        print("wrote email-validation results to Airtable", flush=True)

        existing = await campaign_emails(client, smartlead_key)
        print(f"found {len(existing)} existing Smartlead campaign leads", flush=True)
        eligible: dict[str, dict[str, Any]] = {}
        smartlead_holds: dict[str, list[dict[str, Any]]] = {table: [] for table in CONTACT_TABLES.values()}
        for email, matching_contacts in by_email.items():
            if status(verdicts[email]) != "valid" or email in existing:
                continue
            contact = matching_contacts[0]
            fields = contact["fields"]
            full_name, title, first_name = contact_name_and_title(contact)
            if not first_name:
                smartlead_holds[contact["table"]].append({"id": contact["id"], "fields": {"Smartlead Status": "held_missing_usable_first_name"}})
                continue
            if not valid_persona(contact["persona"], title):
                smartlead_holds[contact["table"]].append({"id": contact["id"], "fields": {"Smartlead Status": "held_invalid_persona"}})
                continue
            eligible[email] = {
                "email": email,
                "first_name": first_name,
                "company_name": str(fields.get("brand_name", "")).strip(),
                "custom_fields": {"show_name": "Las Vegas Market", "persona": contact["persona"]},
            }

        for table, updates in smartlead_holds.items():
            for start in range(0, len(updates), 10):
                await patch_airtable_batch(client, airtable_headers, table, updates[start:start + 10])
                await asyncio.sleep(0.22)
        print(f"held {sum(len(value) for value in smartlead_holds.values())} contacts on name/persona gates", flush=True)

        added = 0
        for chunk_start in range(0, len(eligible), 100):
            chunk = list(eligible.values())[chunk_start:chunk_start + 100]
            await add_to_campaign(client, smartlead_key, chunk)
            added += len(chunk)

        output = {
            "vegas_leads": len(vegas_source_ids),
            "vegas_contacts": len(contacts),
            "unique_final_emails_validated": len(by_email),
            "validation": dict(Counter(status(value) for value in verdicts.values())),
            "campaign_existing_before": len(existing),
            "campaign_added": added,
            "campaign_already_present": len([email for email in by_email if email in existing]),
            "smartlead_held_missing_first_name": sum(
                1 for updates in smartlead_holds.values() for row in updates if row["fields"]["Smartlead Status"] == "held_missing_usable_first_name"
            ),
            "smartlead_held_invalid_persona": sum(
                1 for updates in smartlead_holds.values() for row in updates if row["fields"]["Smartlead Status"] == "held_invalid_persona"
            ),
            "pipedrive_with_lead_id": sum(bool(contact["fields"].get("Pipedrive Lead ID")) for contact in contacts),
            "pipedrive_without_lead_id": sum(not bool(contact["fields"].get("Pipedrive Lead ID")) for contact in contacts),
        }
        print(json.dumps(output, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
