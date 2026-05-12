from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlparse

from app.models import Show
from app.show_guides import parse_guide_row_values


HEADER_NORMALIZER = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class ThemePlaybook:
    key: str
    label: str
    summary: str
    roles: tuple[str, ...]
    questions: tuple[str, ...]
    capture: tuple[str, ...]


@dataclass(frozen=True)
class CompanyProfile:
    row_key: str
    name: str
    website_url: str
    domain: str
    booth_number: str
    priority_score: int
    priority_label: str
    priority_slug: str
    rationale: str
    who_to_meet: tuple[str, ...]
    questions: tuple[str, ...]
    capture: tuple[str, ...]


@dataclass(frozen=True)
class ShowVisitAnalysis:
    show: Show
    theme_label: str
    theme_summary: str
    priority_score: int
    priority_label: str
    priority_slug: str
    recommendation: str
    reasons: tuple[str, ...]
    risks: tuple[str, ...]
    next_steps: tuple[str, ...]
    who_to_meet: tuple[str, ...]
    questions_to_ask: tuple[str, ...]
    capture_checklist: tuple[str, ...]
    company_profiles: tuple[CompanyProfile, ...]
    sample_company_names: tuple[str, ...]
    guide_score: int
    guide_score_label: str
    guide_score_slug: str
    guide_company_count: int
    guide_sales_total: int
    guide_support_total: int
    guide_people_total: int
    guide_average_people_per_company: float
    relevant_company_count: int
    average_complexity_score: float
    guide_context: tuple[str, ...]
    exhibitor_count: int
    export_company_count: int
    website_count: int
    booth_count: int
    website_coverage_percent: int
    booth_coverage_percent: int
    days_until_event: int
    export_ready: bool
    outbound_email_count: int
    outbound_linkedin_count: int
    has_running_campaign: bool
    is_past_event: bool


THEME_PLAYBOOKS: dict[str, ThemePlaybook] = {
    "packaging": ThemePlaybook(
        key="packaging",
        label="Packaging",
        summary="Packaging: source suppliers and qualify execution fast.",
        roles=("Packaging lead", "Procurement owner", "Brand operations", "Product development"),
        questions=(
            "What is your fastest production lead time?",
            "What minimum order quantity do you work with?",
            "Which materials or finishes are new this season?",
        ),
        capture=("Sample photos", "MOQ and lead times", "Best contact for follow-up"),
    ),
    "beauty": ThemePlaybook(
        key="beauty",
        label="Beauty",
        summary="Beauty: check traction, channel fit, and hero products.",
        roles=("Retail partnerships", "Brand founder", "Sales director", "Distribution lead"),
        questions=(
            "Which retailers or channels are driving growth right now?",
            "What hero SKUs are you leading with at this show?",
            "How quickly can you support new account onboarding?",
        ),
        capture=("Hero products", "Retail proof points", "Launch timing"),
    ),
    "food_beverage": ThemePlaybook(
        key="food_beverage",
        label="Food & Beverage",
        summary="Food & Beverage: validate distribution, compliance, and shelf pull.",
        roles=("Sales lead", "Distributor partnerships", "Category manager", "Operations lead"),
        questions=(
            "What distribution footprint do you already have?",
            "Which certifications or compliance requirements are covered?",
            "What margin story do you tell retail buyers?",
        ),
        capture=("Certifications", "Distribution notes", "Case pack and shelf details"),
    ),
    "furniture_design": ThemePlaybook(
        key="furniture_design",
        label="Furniture & Design",
        summary="Furniture & Design: qualify collections, delivery, and trade support.",
        roles=("Sales rep", "Designer channel lead", "Showroom manager", "Founder"),
        questions=(
            "Which collections are getting the strongest response this season?",
            "How do you support trade partners after the show?",
            "What is your standard production and delivery window?",
        ),
        capture=("Collection standouts", "Trade program details", "Delivery commitments"),
    ),
    "pet": ThemePlaybook(
        key="pet",
        label="Pet",
        summary="Pet: check sell-through, retail fit, and wholesale options.",
        roles=("Retail sales", "Founder", "Wholesale partnerships", "Operations"),
        questions=(
            "Which products are winning with retailers right now?",
            "Do you support private label or exclusive programs?",
            "What repeat-purchase signals are you seeing?",
        ),
        capture=("Best sellers", "Wholesale terms", "Retail channel fit"),
    ),
    "science_industrial": ThemePlaybook(
        key="science_industrial",
        label="Science & Industrial",
        summary="Science & Industrial: confirm technical fit and rollout risk.",
        roles=("Technical sales", "Product specialist", "Procurement", "Implementation lead"),
        questions=(
            "Which use cases or industries are you built for?",
            "What integration or deployment work is required?",
            "How do you handle compliance, documentation, or validation?",
        ),
        capture=("Technical fit", "Implementation notes", "Procurement path"),
    ),
    "generic": ThemePlaybook(
        key="generic",
        label="General Trade Show",
        summary="General: rank targets, meet owners, and leave with clear next steps.",
        roles=("Sales lead", "Partnerships", "Operations owner", "Founder"),
        questions=(
            "Who are you hoping to meet at this event?",
            "What problem are you solving better than nearby booths?",
            "What is the fastest next step after a good conversation here?",
        ),
        capture=("Booth notes", "Primary contact", "Follow-up action"),
    ),
}


def build_show_analyses(
    shows: list[Show],
    *,
    today: date | None = None,
    company_limit: int = 10,
) -> list[ShowVisitAnalysis]:
    analyses = [build_show_analysis(show, today=today, company_limit=company_limit) for show in shows]
    return sorted(
        analyses,
        key=lambda analysis: (
            analysis.days_until_event < 0,
            analysis.show.event_date,
            analysis.show.name.lower(),
        ),
    )


def build_show_analysis(
    show: Show,
    *,
    today: date | None = None,
    company_limit: int = 18,
) -> ShowVisitAnalysis:
    today = today or date.today()
    playbook = infer_theme_playbook(show.name)
    company_rows = _load_company_rows(show.latest_export_path)
    exhibitor_count = show.company_count or len(company_rows)
    export_company_count = len(company_rows)
    website_count = sum(1 for row in company_rows if row["website_url"])
    booth_count = sum(1 for row in company_rows if row["booth_number"])
    website_coverage_percent = _safe_percent(website_count, export_company_count)
    booth_coverage_percent = _safe_percent(booth_count, export_company_count)
    days_until_event = (show.event_date - today).days
    export_ready = bool(company_rows)
    guide_score = _compute_guide_score(show=show, exhibitor_count=exhibitor_count)
    outbound_email_count, outbound_linkedin_count = _estimate_outbound_counts(show)

    priority_score = _compute_priority_score(
        exhibitor_count=exhibitor_count,
        website_coverage_percent=website_coverage_percent,
        booth_coverage_percent=booth_coverage_percent,
        days_until_event=days_until_event,
        export_ready=export_ready,
        show_status=show.status,
    )
    priority_label, priority_slug = _priority_badge(priority_score)
    reasons = _build_reasons(
        exhibitor_count=exhibitor_count,
        export_company_count=export_company_count,
        website_coverage_percent=website_coverage_percent,
        booth_coverage_percent=booth_coverage_percent,
        days_until_event=days_until_event,
        export_ready=export_ready,
    )
    risks = _build_risks(
        export_company_count=export_company_count,
        website_coverage_percent=website_coverage_percent,
        booth_coverage_percent=booth_coverage_percent,
        days_until_event=days_until_event,
        show=show,
    )
    company_profiles = tuple(
        _build_company_profile(row, playbook) for row in _rank_company_rows(company_rows)[:company_limit]
    )
    sample_company_names = tuple(profile.name for profile in company_profiles[:4])
    recommendation = _build_recommendation(
        priority_label=priority_label,
        show_name=show.name,
        playbook=playbook,
        export_ready=export_ready,
    )
    next_steps = (
        f"Shortlist top {min(max(exhibitor_count, 6), 12)} accounts.",
        f"Meet {playbook.roles[0].lower()} and {playbook.roles[1].lower()} first.",
        "Capture owner, need, next step.",
    )

    return ShowVisitAnalysis(
        show=show,
        theme_label=playbook.label,
        theme_summary=playbook.summary,
        priority_score=priority_score,
        priority_label=priority_label,
        priority_slug=priority_slug,
        recommendation=recommendation,
        reasons=reasons,
        risks=risks,
        next_steps=next_steps,
        who_to_meet=playbook.roles,
        questions_to_ask=playbook.questions,
        capture_checklist=playbook.capture,
        company_profiles=company_profiles,
        sample_company_names=sample_company_names,
        guide_score=guide_score["score"],
        guide_score_label=guide_score["label"],
        guide_score_slug=guide_score["slug"],
        guide_company_count=guide_score["guide_company_count"],
        guide_sales_total=guide_score["sales_total"],
        guide_support_total=guide_score["support_total"],
        guide_people_total=guide_score["people_total"],
        guide_average_people_per_company=guide_score["average_people_per_company"],
        relevant_company_count=guide_score["relevant_company_count"],
        average_complexity_score=guide_score["average_complexity_score"],
        guide_context=guide_score["context"],
        exhibitor_count=exhibitor_count,
        export_company_count=export_company_count,
        website_count=website_count,
        booth_count=booth_count,
        website_coverage_percent=website_coverage_percent,
        booth_coverage_percent=booth_coverage_percent,
        days_until_event=days_until_event,
        export_ready=export_ready,
        outbound_email_count=outbound_email_count,
        outbound_linkedin_count=outbound_linkedin_count,
        has_running_campaign=show.smartlead_status == "active",
        is_past_event=days_until_event < 0,
    )


def infer_theme_playbook(show_name: str) -> ThemePlaybook:
    normalized = show_name.lower()
    keyword_map = (
        ("pack", "packaging"),
        ("beaut", "beauty"),
        ("food", "food_beverage"),
        ("bev", "food_beverage"),
        ("restaurant", "food_beverage"),
        ("pet", "pet"),
        ("design", "furniture_design"),
        ("furniture", "furniture_design"),
        ("home", "furniture_design"),
        ("market", "furniture_design"),
        ("science", "science_industrial"),
        ("tech", "science_industrial"),
        ("operational", "science_industrial"),
        ("lab", "science_industrial"),
    )
    for keyword, key in keyword_map:
        if keyword in normalized:
            return THEME_PLAYBOOKS[key]
    return THEME_PLAYBOOKS["generic"]


def _build_company_profile(row: dict[str, str], playbook: ThemePlaybook) -> CompanyProfile:
    score = 35
    if row["website_url"]:
        score += 30
    if row["domain"]:
        score += 10
    if row["booth_number"]:
        score += 20

    if row["website_url"] and row["booth_number"]:
        rationale = "Website and booth found. Easy to prep and find."
    elif row["website_url"]:
        rationale = "Website found. Prep is possible."
    elif row["booth_number"]:
        rationale = "Booth found. Research still thin."
    else:
        rationale = "Thin profile. Treat as walk-up."

    label, slug = _priority_badge(score)
    return CompanyProfile(
        row_key=row["row_key"],
        name=row["company_name"],
        website_url=row["website_url"],
        domain=row["domain"],
        booth_number=row["booth_number"],
        priority_score=score,
        priority_label=label,
        priority_slug=slug,
        rationale=rationale,
        who_to_meet=playbook.roles,
        questions=playbook.questions,
        capture=playbook.capture,
    )


def _build_recommendation(
    *,
    priority_label: str,
    show_name: str,
    playbook: ThemePlaybook,
    export_ready: bool,
) -> str:
    if not export_ready:
        return f"Scrape first. {show_name} has no exhibitor list yet."
    if priority_label == "High Priority":
        return f"Visit. Strong list, usable data, good timing for {playbook.label.lower()} outreach."
    if priority_label == "Medium Priority":
        return f"Maybe. Check top accounts before you commit."
    return f"Skip for now. Signal is too weak."


def _build_reasons(
    *,
    exhibitor_count: int,
    export_company_count: int,
    website_coverage_percent: int,
    booth_coverage_percent: int,
    days_until_event: int,
    export_ready: bool,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if exhibitor_count >= 150:
        reasons.append(f"{exhibitor_count} exhibitors.")
    elif exhibitor_count >= 60:
        reasons.append(f"{exhibitor_count} exhibitors. Enough scale.")
    elif exhibitor_count > 0:
        reasons.append(f"{exhibitor_count} exhibitors. Manageable list.")

    if export_ready:
        reasons.append(f"{export_company_count} rows loaded.")
    if website_coverage_percent >= 60:
        reasons.append(f"{website_coverage_percent}% website coverage.")
    elif website_coverage_percent >= 35:
        reasons.append(f"{website_coverage_percent}% website coverage. Usable.")

    if booth_coverage_percent >= 20:
        reasons.append(f"{booth_coverage_percent}% booth coverage.")

    if 0 <= days_until_event <= 90:
        reasons.append("In the next 90 days.")
    elif days_until_event > 90:
        reasons.append("Plenty of lead time.")

    return tuple(reasons[:4]) or ("Track it.",)


def _build_risks(
    *,
    export_company_count: int,
    website_coverage_percent: int,
    booth_coverage_percent: int,
    days_until_event: int,
    show: Show,
) -> tuple[str, ...]:
    risks: list[str] = []
    if export_company_count == 0:
        risks.append("No exhibitor export yet.")
    if export_company_count and website_coverage_percent < 35:
        risks.append("Low website coverage.")
    if export_company_count and booth_coverage_percent == 0:
        risks.append("No booth numbers.")
    if days_until_event < 0:
        risks.append("Event already passed.")
    if show.last_error:
        risks.append("Last scrape had errors.")
    return tuple(risks[:4]) or ("No major blockers.",)


def _compute_priority_score(
    *,
    exhibitor_count: int,
    website_coverage_percent: int,
    booth_coverage_percent: int,
    days_until_event: int,
    export_ready: bool,
    show_status: str,
) -> int:
    scale_score = min(exhibitor_count, 250) / 250 * 42
    website_score = website_coverage_percent / 100 * 20
    booth_score = booth_coverage_percent / 100 * 10

    if 0 <= days_until_event <= 90:
        timing_score = 18
    elif 91 <= days_until_event <= 180:
        timing_score = 12
    elif days_until_event > 180:
        timing_score = 8
    elif -30 <= days_until_event < 0:
        timing_score = 4
    else:
        timing_score = 0

    readiness_score = 0
    if export_ready:
        readiness_score += 6
    if show_status in {"ready_for_review", "approved", "live"}:
        readiness_score += 4
    elif show_status in {"queued", "scraping"}:
        readiness_score += 2

    return max(0, min(100, round(scale_score + website_score + booth_score + timing_score + readiness_score)))


def _priority_badge(score: int) -> tuple[str, str]:
    if score >= 75:
        return "High Priority", "high"
    if score >= 55:
        return "Medium Priority", "medium"
    return "Low Priority", "low"


def _rank_company_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(
        rows,
        key=lambda row: (
            not bool(row["website_url"]),
            not bool(row["booth_number"]),
            row["company_name"].lower(),
        ),
    )


def _parse_int(value: str) -> int:
    digits = re.sub(r"[^0-9-]+", "", value.strip())
    if not digits or digits == "-":
        return 0
    try:
        return max(0, int(digits))
    except ValueError:
        return 0


def _parse_float(value: str) -> float:
    normalized = value.strip()
    if not normalized:
        return 0.0
    normalized = re.sub(r"[^0-9.]+", "", normalized)
    if not normalized:
        return 0.0
    try:
        return max(0.0, float(normalized))
    except ValueError:
        return 0.0


def _guide_company_rows(show: Show) -> list[dict[str, str]]:
    return [
        parse_guide_row_values(row)
        for row in show.guide_rows
        if row.sheet_key == "company_summary" and row.source == "workbook"
    ]


def _compute_guide_score(show: Show, *, exhibitor_count: int) -> dict[str, object]:
    guide_rows = _guide_company_rows(show)
    guide_company_count = len(guide_rows)
    sales_total = sum(_parse_int(row.get("sales_team_size", "")) for row in guide_rows)
    support_total = sum(_parse_int(row.get("customer_service_team_size", "")) for row in guide_rows)
    people_total = sales_total + support_total
    relevant_company_count = sum(
        1
        for row in guide_rows
        if (_parse_int(row.get("sales_team_size", "")) + _parse_int(row.get("customer_service_team_size", ""))) > 10
    )
    complexity_values = [
        value
        for value in (_parse_float(row.get("catalog_complexity", "")) for row in guide_rows)
        if value > 0
    ]
    average_complexity_score = round(sum(complexity_values) / len(complexity_values), 1) if complexity_values else 0.0
    base_count = max(exhibitor_count, guide_company_count, 1)
    coverage_ratio = min(1.0, guide_company_count / base_count)
    people_per_company = people_total / guide_company_count if guide_company_count else 0.0
    density_ratio = min(1.0, people_per_company / 20.0)
    score = int(round((density_ratio * 70) + (coverage_ratio * 30)))

    if score >= 70:
        label = "Good"
        slug = "good"
    elif score >= 45:
        label = "Medium"
        slug = "medium"
    else:
        label = "Bad"
        slug = "bad"

    if guide_company_count == 0:
        context = (
            "No guide rows yet.",
            "Build the guide first.",
        )
    elif people_total == 0:
        context = (
            f"{guide_company_count} guide companies.",
            "Team sizes still empty.",
        )
    else:
        coverage_percent = int(round(coverage_ratio * 100))
        context = (
            f"{people_total} sales + support reps.",
            f"{people_per_company:.1f} reps per company.",
            f"{coverage_percent}% company coverage.",
        )

    return {
        "score": max(0, min(100, score)),
        "label": label,
        "slug": slug,
        "guide_company_count": guide_company_count,
        "sales_total": sales_total,
        "support_total": support_total,
        "people_total": people_total,
        "average_people_per_company": round(people_per_company, 1),
        "relevant_company_count": relevant_company_count,
        "average_complexity_score": average_complexity_score,
        "context": context,
    }


def _estimate_outbound_counts(show: Show) -> tuple[int, int]:
    raw_path = show.smartlead_ready_export_path or show.enriched_export_path
    if raw_path:
        path = Path(raw_path)
        if path.exists():
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)
            if rows:
                linkedin_count = 0
                for row in rows:
                    normalized = {_normalize_header(key): (value or "").strip() for key, value in row.items() if key}
                    if _first_value(normalized, "linkedin_profile", "linkedin_url", "linkedin"):
                        linkedin_count += 1
                return len(rows), linkedin_count

    fallback_email_count = max(show.smartlead_imported_rows or 0, show.clay_ready_rows or 0, 0)
    return fallback_email_count, 0


def _load_company_rows(raw_path: str) -> list[dict[str, str]]:
    if not raw_path:
        return []

    path = Path(raw_path)
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for raw_row in reader:
            normalized = {
                _normalize_header(key): (value or "").strip()
                for key, value in raw_row.items()
                if key is not None
            }
            company_name = _first_value(normalized, "company_name", "company", "company_table_data")
            if not company_name:
                continue
            company_name = company_name.strip()
            if not _looks_plausible_company_name(company_name):
                continue
            website_url = _normalize_website(
                _first_value(normalized, "website_url", "website", "company_url", "company_domain", "domain")
            )
            booth_number = _first_value(normalized, "booth_number", "booth", "booth_")
            if not (website_url or booth_number) and len(company_name.split()) >= 5:
                continue
            rows.append(
                {
                    "row_key": _company_row_key(normalized),
                    "company_name": company_name,
                    "website_url": website_url,
                    "domain": _extract_domain(website_url),
                    "booth_number": booth_number,
                }
            )
    return rows


def _company_row_key(cells: dict[str, str]) -> str:
    return hashlib.sha1(json.dumps(cells, sort_keys=True).encode("utf-8")).hexdigest()


def _normalize_header(value: str) -> str:
    return HEADER_NORMALIZER.sub("_", value.strip().lower()).strip("_")


def _first_value(cells: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = cells.get(key, "").strip()
        if value:
            return value
    return ""


def _normalize_website(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        return ""
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    return candidate


def _extract_domain(website_url: str) -> str:
    if not website_url:
        return ""
    parsed = urlparse(website_url)
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _looks_plausible_company_name(company_name: str) -> bool:
    normalized = re.sub(r"\s+", " ", company_name.strip())
    if len(normalized) < 2 or len(normalized) > 140:
        return False
    if re.search(r"[<>|\\{}[\]^~`*]", normalized):
        return False
    if not re.search(r"[A-Za-z]", normalized):
        return False

    words = re.findall(r"[A-Za-z0-9&+'/-]+", normalized)
    if not words:
        return False
    if all(len(word) <= 2 for word in words):
        return False
    short_alpha_words = [
        word
        for word in words
        if len(word) <= 2 and any(character.isalpha() for character in word)
    ]
    if len(words) >= 4 and len(short_alpha_words) >= max(3, len(words) // 2):
        return False
    compact = normalized.replace(" ", "")
    if sum(character.isdigit() for character in compact) >= 6 and len(compact) >= 12:
        return False
    return True


def _safe_percent(part: int, whole: int) -> int:
    if whole <= 0:
        return 0
    return round(part / whole * 100)
