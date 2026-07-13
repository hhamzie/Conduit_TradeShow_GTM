from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.models import OutboundAccount, OutboundContact, OutboundEnrichmentJob, OutboundSyncEvent, Show


ACCOUNT_HEADER_ALIASES = {
    "company": "company_name",
    "company name": "company_name",
    "company_name": "company_name",
    "brand": "company_name",
    "brand name": "company_name",
    "brand_name": "company_name",
    "exhibitor": "company_name",
    "exhibitor name": "company_name",
    "name": "company_name",
    "booth": "booth_number",
    "booth number": "booth_number",
    "booth_number": "booth_number",
    "website": "website_url",
    "website url": "website_url",
    "website_url": "website_url",
    "company url": "website_url",
    "company_url": "website_url",
    "exhibitor url": "source_url",
    "exhibitor_url": "source_url",
    "source url": "source_url",
    "source_url": "source_url",
    "domain": "domain",
    "company domain": "domain",
    "company_domain": "domain",
    "official company domain": "domain",
    "official company domain 2": "domain",
    "official company domain (2)": "domain",
    "contact name": "original_contact_name",
    "person name": "original_contact_name",
    "full name": "original_contact_name",
    "name of contact": "original_contact_name",
    "contact title": "original_contact_title",
    "job title": "original_contact_title",
    "title": "original_contact_title",
    "contact email": "original_contact_email",
    "email": "original_contact_email",
    "contact linkedin": "original_contact_linkedin",
    "linkedin": "original_contact_linkedin",
    "linkedin url": "original_contact_linkedin",
    "linkedin_url": "original_contact_linkedin",
}

BUILD_SOURCE_HEADERS = [
    "brand_name",
    "company_email",
    "exhibitor_url",
    "showroom_contact",
    "Official Company Domain (2)",
    "Normalized Company Name",
    "Normalized Name",
    "Conference",
    "Show Date",
    "Show Date Pretty",
    "Booth Number",
    "source_row_id",
]


@dataclass(frozen=True)
class OutboundLeadImportSummary:
    created_accounts: int
    updated_accounts: int
    skipped_rows: int
    created_contacts: int
    total_accounts: int
    domain_ready_accounts: int
    needs_domain_accounts: int


@dataclass(frozen=True)
class OutboundPipelineSummary:
    account_count: int
    domain_ready_count: int
    needs_domain_count: int
    included_count: int
    excluded_count: int
    pending_audit_count: int
    contact_count: int
    qualified_contact_count: int
    clay_seeded_count: int
    pipedrive_synced_count: int
    linkedin_task_count: int


def normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.strip().lower()).strip()


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def normalize_domain(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        return ""
    candidate = value if "://" in value else f"https://{value}"
    parsed = urlparse(candidate)
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host.strip("/")


def normalize_website(raw_value: str, domain: str = "") -> str:
    value = (raw_value or "").strip()
    if value:
        return value if "://" in value else f"https://{value}"
    return f"https://{domain}" if domain else ""


def _canonicalize_row(raw_row: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for raw_key, raw_value in raw_row.items():
        if raw_key is None:
            continue
        canonical_key = ACCOUNT_HEADER_ALIASES.get(normalize_header(raw_key))
        if not canonical_key:
            continue
        normalized[canonical_key] = (raw_value or "").strip()
    return normalized


def _account_row_key(show: Show, row: dict[str, str], row_number: int) -> str:
    domain = normalize_domain(row.get("domain") or row.get("website_url", ""))
    company = normalize_header(row.get("company_name", ""))
    booth = normalize_header(row.get("booth_number", ""))
    if domain:
        stable_key = "|".join((show.event_date.isoformat(), domain))
    else:
        stable_key = "|".join(part for part in (show.event_date.isoformat(), company, booth) if part)
    if not stable_key:
        stable_key = f"row-{row_number}"
    return hashlib.sha1(stable_key.encode("utf-8")).hexdigest()[:24]


def _load_csv_rows(payload: bytes) -> list[dict[str, str]]:
    text = payload.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("Uploaded lead CSV is missing headers.")
    return [
        {key: value or "" for key, value in raw_row.items() if key is not None}
        for raw_row in reader
        if any(str(value or "").strip() for value in raw_row.values())
    ]


def _record_sync_event(
    db: Session,
    *,
    show: Show,
    provider: str,
    action: str,
    status: str,
    message: str,
    account: OutboundAccount | None = None,
    contact: OutboundContact | None = None,
    payload: dict[str, object] | None = None,
) -> None:
    db.add(
        OutboundSyncEvent(
            show=show,
            account=account,
            contact=contact,
            provider=provider,
            action=action,
            status=status,
            message=message,
            payload_json=json.dumps(payload or {}, sort_keys=True),
        )
    )


def import_outbound_leads_from_csv(
    db: Session,
    show: Show,
    *,
    payload: bytes,
    source_label: str = "uploaded lead list",
) -> OutboundLeadImportSummary:
    rows = _load_csv_rows(payload)
    existing_accounts = {account.row_key: account for account in show.outbound_accounts}
    created_accounts = 0
    updated_accounts = 0
    skipped_rows = 0
    created_contacts = 0

    for row_number, raw_row in enumerate(rows, start=2):
        row = _canonicalize_row(raw_row)
        company_name = normalize_name(row.get("company_name", ""))
        if not company_name:
            skipped_rows += 1
            continue

        domain = normalize_domain(row.get("domain") or row.get("website_url", ""))
        website_url = normalize_website(row.get("website_url", ""), domain)
        row_key = _account_row_key(show, {**row, "domain": domain, "website_url": website_url}, row_number)
        account = existing_accounts.get(row_key)
        lifecycle_status = "domain_ready" if domain else "needs_domain"

        if account is None:
            account = OutboundAccount(show=show, row_key=row_key)
            existing_accounts[row_key] = account
            db.add(account)
            created_accounts += 1
        else:
            updated_accounts += 1

        account.company_name = company_name
        account.booth_number = row.get("booth_number", "").strip()
        account.website_url = website_url
        account.domain = domain
        account.source_url = row.get("source_url", "").strip()
        account.source_label = source_label.strip() or "uploaded lead list"
        account.source_payload_json = json.dumps(raw_row, sort_keys=True)
        if account.audit_status in {None, "", "pending"}:
            account.audit_status = "pending"
        if account.lifecycle_status in {None, "", "needs_domain", "domain_ready"}:
            account.lifecycle_status = lifecycle_status
        account.last_error = ""

        original_contact_name = row.get("original_contact_name", "").strip()
        original_contact_email = row.get("original_contact_email", "").strip().lower()
        original_contact_linkedin = row.get("original_contact_linkedin", "").strip()
        original_contact_title = row.get("original_contact_title", "").strip()
        if original_contact_name or original_contact_email or original_contact_linkedin:
            existing_original = next(
                (
                    contact
                    for contact in account.contacts
                    if contact.contact_group == "Original"
                    and (
                        (original_contact_email and contact.email == original_contact_email)
                        or (original_contact_linkedin and contact.linkedin_url == original_contact_linkedin)
                        or (original_contact_name and contact.person_name == original_contact_name)
                    )
                ),
                None,
            )
            if existing_original is not None:
                existing_original.person_name = original_contact_name or existing_original.person_name
                existing_original.job_title = original_contact_title or existing_original.job_title
                existing_original.email = original_contact_email or existing_original.email
                existing_original.linkedin_url = original_contact_linkedin or existing_original.linkedin_url
                continue
            contact = OutboundContact(
                show=show,
                account=account,
                contact_group="Original",
                person_name=original_contact_name,
                job_title=original_contact_title,
                email=original_contact_email,
                linkedin_url=original_contact_linkedin,
                qualification_status="source_contact",
            )
            db.add(contact)
            created_contacts += 1

    db.flush()
    total_accounts = int(
        db.scalar(select(func.count()).select_from(OutboundAccount).where(OutboundAccount.show_id == show.id))
        or 0
    )
    domain_ready_accounts = int(
        db.scalar(
            select(func.count())
            .select_from(OutboundAccount)
            .where(OutboundAccount.show_id == show.id, OutboundAccount.domain != "")
        )
        or 0
    )
    needs_domain_accounts = total_accounts - domain_ready_accounts
    _record_sync_event(
        db,
        show=show,
        provider="dashboard",
        action="lead_list_upload",
        status="success",
        message=f"Imported {created_accounts} new account(s), updated {updated_accounts}, skipped {skipped_rows}.",
        payload={
            "source_label": source_label,
            "created_accounts": created_accounts,
            "updated_accounts": updated_accounts,
            "skipped_rows": skipped_rows,
            "created_contacts": created_contacts,
        },
    )
    db.commit()
    return OutboundLeadImportSummary(
        created_accounts=created_accounts,
        updated_accounts=updated_accounts,
        skipped_rows=skipped_rows,
        created_contacts=created_contacts,
        total_accounts=total_accounts,
        domain_ready_accounts=domain_ready_accounts,
        needs_domain_accounts=needs_domain_accounts,
    )


def build_outbound_pipeline_summary(show: Show) -> OutboundPipelineSummary:
    accounts = list(show.outbound_accounts)
    contacts = list(show.outbound_contacts)
    return OutboundPipelineSummary(
        account_count=len(accounts),
        domain_ready_count=sum(1 for account in accounts if account.domain),
        needs_domain_count=sum(1 for account in accounts if not account.domain),
        included_count=sum(1 for account in accounts if account.audit_status == "included"),
        excluded_count=sum(1 for account in accounts if account.audit_status == "excluded"),
        pending_audit_count=sum(1 for account in accounts if account.audit_status in {"", "pending"}),
        contact_count=len(contacts),
        qualified_contact_count=sum(1 for contact in contacts if contact.qualification_status == "qualified"),
        clay_seeded_count=sum(1 for account in accounts if account.clay_source_row_id),
        pipedrive_synced_count=sum(1 for contact in contacts if contact.pipedrive_lead_id),
        linkedin_task_count=sum(1 for contact in contacts if contact.linkedin_activity_id),
    )


def build_clay_source_rows_for_show(show: Show) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for account in show.outbound_accounts:
        if account.audit_status == "excluded":
            continue
        rows.append(
            {
                "brand_name": account.company_name,
                "company_email": "",
                "exhibitor_url": account.website_url or account.source_url,
                "showroom_contact": "",
                "Official Company Domain (2)": account.domain,
                "Normalized Company Name": account.company_name,
                "Normalized Name": account.company_name,
                "Conference": show.name,
                "Show Date": show.event_date.isoformat(),
                "Show Date Pretty": show.event_date.strftime("%B %d, %Y"),
                "Booth Number": account.booth_number,
                "source_row_id": account.row_key,
            }
        )
    return rows


def write_build_source_csv(show: Show) -> bytes:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=BUILD_SOURCE_HEADERS)
    writer.writeheader()
    for row in build_clay_source_rows_for_show(show):
        writer.writerow({key: row.get(key, "") for key in BUILD_SOURCE_HEADERS})
    return output.getvalue().encode("utf-8")


def ensure_build_flow_job(db: Session, show: Show) -> OutboundEnrichmentJob:
    job = db.scalar(
        select(OutboundEnrichmentJob)
        .where(
            OutboundEnrichmentJob.show_id == show.id,
            OutboundEnrichmentJob.provider == "clay",
            OutboundEnrichmentJob.job_type == "build_show_flow",
            OutboundEnrichmentJob.status.in_(["queued", "running"]),
        )
        .order_by(OutboundEnrichmentJob.created_at.desc())
        .limit(1)
    )
    if job is None:
        job = OutboundEnrichmentJob(show=show, provider="clay", job_type="build_show_flow", status="queued")
        db.add(job)
    job.record_count = len(build_clay_source_rows_for_show(show))
    job.meta_json = json.dumps(
        {
            "description": "Build-style flow: enrich domains, find sales leaders, validate LinkedIn, then sync qualified contacts.",
            "queued_at": datetime.now().isoformat(),
        },
        sort_keys=True,
    )
    _record_sync_event(
        db,
        show=show,
        provider="clay",
        action="build_flow_queued",
        status="queued",
        message=f"Queued Build-style outbound enrichment for {job.record_count} account(s).",
    )
    db.commit()
    return job


def get_show_with_outbound(db: Session, show_id: int) -> Show | None:
    return db.scalar(
        select(Show)
        .options(
            selectinload(Show.outbound_accounts).selectinload(OutboundAccount.contacts),
            selectinload(Show.outbound_contacts),
            selectinload(Show.outbound_jobs),
            selectinload(Show.outbound_sync_events),
        )
        .where(Show.id == show_id)
    )
