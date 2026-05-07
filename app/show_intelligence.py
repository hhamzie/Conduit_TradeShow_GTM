from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import re
from urllib.parse import urlparse

from app.models import Show


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
    exhibitor_count: int
    export_company_count: int
    website_count: int
    booth_count: int
    website_coverage_percent: int
    booth_coverage_percent: int
    days_until_event: int
    export_ready: bool


THEME_PLAYBOOKS: dict[str, ThemePlaybook] = {
    "packaging": ThemePlaybook(
        key="packaging",
        label="Packaging",
        summary="Prioritize supplier discovery, packaging innovation, and practical sourcing conversations.",
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
        summary="Focus on buyer demand, retail traction, and differentiated product positioning.",
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
        summary="Look for products with strong distribution readiness, certifications, and shelf pull.",
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
        summary="Use the floor to identify differentiated collections, showroom readiness, and trade relationships.",
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
        summary="Evaluate category momentum, retail fit, and private-label or distribution opportunities.",
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
        summary="Prioritize technical depth, procurement alignment, and implementation readiness.",
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
        summary="Use the event to identify the densest account clusters and qualify follow-up quickly.",
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
            -analysis.priority_score,
            analysis.days_until_event < 0,
            abs(analysis.days_until_event),
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
        show_name=show.name,
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
        exhibitor_count=exhibitor_count,
        website_coverage_percent=website_coverage_percent,
        days_until_event=days_until_event,
        playbook=playbook,
        export_ready=export_ready,
    )
    next_steps = (
        f"Shortlist the top {min(max(exhibitor_count, 6), 12)} exhibitors before travel.",
        f"Use the floor to meet {playbook.roles[0].lower()} and {playbook.roles[1].lower()} contacts first.",
        "Capture booth notes, contact owners, and clear next steps while the context is fresh.",
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
        exhibitor_count=exhibitor_count,
        export_company_count=export_company_count,
        website_count=website_count,
        booth_count=booth_count,
        website_coverage_percent=website_coverage_percent,
        booth_coverage_percent=booth_coverage_percent,
        days_until_event=days_until_event,
        export_ready=export_ready,
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
        rationale = "Has both a website and booth reference, so this company is easy to research and easy to find on the floor."
    elif row["website_url"]:
        rationale = "Has a website, so there is enough context to prep before the event even without booth routing."
    elif row["booth_number"]:
        rationale = "Booth information exists, but account research will need to happen manually because a website is missing."
    else:
        rationale = "Minimal profile data is available; treat this as a walk-up prospect until more research is attached."

    label, slug = _priority_badge(score)
    return CompanyProfile(
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
    exhibitor_count: int,
    website_coverage_percent: int,
    days_until_event: int,
    playbook: ThemePlaybook,
    export_ready: bool,
) -> str:
    if not export_ready:
        return (
            f"{show_name} is still a watchlist event. Run the export first so the visit call is based on real exhibitors, not just the show name."
        )

    timing = (
        f"{days_until_event} day(s) away"
        if days_until_event >= 0
        else f"{abs(days_until_event)} day(s) in the past"
    )
    if priority_label == "High Priority":
        return (
            f"{show_name} looks worth visiting. The exhibitor base is large enough to support focused meetings, and the exported data is strong enough to prep around {playbook.label.lower()} conversations."
        )
    if priority_label == "Medium Priority":
        return (
            f"{show_name} looks potentially worth visiting, but validate the top accounts first. Right now the signal is moderate: {exhibitor_count} exhibitors, {website_coverage_percent}% website coverage, and timing {timing}."
        )
    return (
        f"{show_name} should stay on the monitor list for now. The event may still matter, but the current data is too thin to justify a confident visit plan around {playbook.label.lower()} targets."
    )


def _build_reasons(
    *,
    exhibitor_count: int,
    export_company_count: int,
    website_coverage_percent: int,
    booth_coverage_percent: int,
    days_until_event: int,
    export_ready: bool,
    show_name: str,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if exhibitor_count >= 150:
        reasons.append(f"{exhibitor_count} exhibitors makes this a dense meeting environment.")
    elif exhibitor_count >= 60:
        reasons.append(f"{exhibitor_count} exhibitors is enough scale to support a targeted floor plan.")
    elif exhibitor_count > 0:
        reasons.append(f"{exhibitor_count} exhibitors gives you a manageable shortlist instead of a giant sprawl.")

    if export_ready:
        reasons.append(f"{export_company_count} companies are already attached to the show profile.")
    if website_coverage_percent >= 60:
        reasons.append(f"{website_coverage_percent}% website coverage means prep work is practical before travel.")
    elif website_coverage_percent >= 35:
        reasons.append(f"{website_coverage_percent}% website coverage is workable for a tighter pre-show shortlist.")

    if booth_coverage_percent >= 20:
        reasons.append(f"{booth_coverage_percent}% booth coverage improves floor routing.")

    if 0 <= days_until_event <= 90:
        reasons.append(f"{show_name} is close enough to start booking meetings now.")
    elif days_until_event > 90:
        reasons.append("There is still enough time to build a meeting plan without rushing.")

    return tuple(reasons[:4]) or ("This show has enough basic structure to keep tracking.",)


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
        risks.append("No exhibitor export is attached yet, so the visit call is still mostly speculative.")
    if export_company_count and website_coverage_percent < 35:
        risks.append("Website coverage is thin, so account research will be slower than it should be.")
    if export_company_count and booth_coverage_percent == 0:
        risks.append("No booth numbers were found in the export, which weakens on-floor routing.")
    if days_until_event < 0:
        risks.append("The event date has already passed, so use this profile as a benchmark rather than a travel decision.")
    if show.last_error:
        risks.append("The last scrape recorded an error, so the exhibitor list may still be incomplete.")
    return tuple(risks[:4]) or ("No major blockers are visible from the current export.",)


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
            website_url = _normalize_website(
                _first_value(normalized, "website_url", "website", "company_url", "company_domain", "domain")
            )
            rows.append(
                {
                    "company_name": company_name,
                    "website_url": website_url,
                    "domain": _extract_domain(website_url),
                    "booth_number": _first_value(normalized, "booth_number", "booth", "booth_"),
                }
            )
    return rows


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


def _safe_percent(part: int, whole: int) -> int:
    if whole <= 0:
        return 0
    return round(part / whole * 100)
