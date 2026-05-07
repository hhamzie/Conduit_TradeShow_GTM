from __future__ import annotations

import json
from dataclasses import dataclass

from app.models import Show, ShowGuideRow


@dataclass(frozen=True)
class GuideField:
    key: str
    label: str
    input_type: str = "text"
    placeholder: str = ""


@dataclass(frozen=True)
class GuideSheetDefinition:
    key: str
    label: str
    description: str
    fields: tuple[GuideField, ...]


@dataclass(frozen=True)
class GuideRowView:
    id: int
    position: int
    values: dict[str, str]


@dataclass(frozen=True)
class GuideSheetView:
    definition: GuideSheetDefinition
    rows: tuple[GuideRowView, ...]


GUIDE_SHEETS: dict[str, GuideSheetDefinition] = {
    "company_summary": GuideSheetDefinition(
        key="company_summary",
        label="Company Summary",
        description="Primary target companies and leadership context for the show.",
        fields=(
            GuideField("company_name", "Company Name", placeholder="Fiserv"),
            GuideField("booth_number", "Booth Number", placeholder="3254"),
            GuideField("booth_category", "Booth Category", placeholder="3200s"),
            GuideField("sales_team_size", "Sales Team Size", input_type="number", placeholder="932"),
            GuideField("customer_service_team_size", "Customer Service Team Size", input_type="number", placeholder="2009"),
            GuideField("total_team_size", "Total Team Size", input_type="number", placeholder="2941"),
            GuideField("catalog_complexity", "Catalog Complexity (1-5)", input_type="number", placeholder="2"),
            GuideField("sales_leader_name", "Sales Leader Name", placeholder="Robert Clarkson"),
            GuideField("sales_leader_role", "Sales Leader Role", placeholder="Chief Revenue Officer"),
            GuideField("sales_leader_email", "Sales Leader Email", input_type="email", placeholder="name@company.com"),
            GuideField("sales_leader_linkedin", "Sales Leader LinkedIn", input_type="url", placeholder="https://linkedin.com/in/example"),
            GuideField("source_url", "Source URL", input_type="url", placeholder="https://example.com/booth"),
        ),
    ),
    "booth_category_groups": GuideSheetDefinition(
        key="booth_category_groups",
        label="Booth Category Groups",
        description="Cluster exhibitors by booth area and compare category-level team density.",
        fields=(
            GuideField("booth_category", "Booth Category", placeholder="100s"),
            GuideField("category_total_team_size", "Category Total Team Size", input_type="number", placeholder="20"),
            GuideField("company_name", "Company Name", placeholder="Special-Lite"),
            GuideField("booth_number", "Booth Number", placeholder="135"),
            GuideField("sales_team_size", "Sales Team Size", input_type="number", placeholder="7"),
            GuideField("customer_service_team_size", "Customer Service Team Size", input_type="number", placeholder="5"),
            GuideField("total_team_size", "Total Team Size", input_type="number", placeholder="12"),
            GuideField("catalog_complexity", "Catalog Complexity (1-5)", input_type="number", placeholder="4"),
            GuideField("sales_leader_name", "Sales Leader Name", placeholder="Gary Wolf"),
            GuideField("sales_leader_role", "Sales Leader Role", placeholder="Business Development Manager"),
            GuideField("sales_leader_email", "Sales Leader Email", input_type="email", placeholder="name@company.com"),
            GuideField("sales_leader_linkedin", "Sales Leader LinkedIn", input_type="url", placeholder="https://linkedin.com/in/example"),
            GuideField("source_url", "Source URL", input_type="url", placeholder="https://example.com/booth"),
        ),
    ),
}


def get_guide_sheet(sheet_key: str) -> GuideSheetDefinition:
    definition = GUIDE_SHEETS.get(sheet_key)
    if definition is None:
        raise ValueError(f"Unknown guide sheet: {sheet_key}")
    return definition


def normalize_guide_values(sheet_key: str, payload: dict[str, object]) -> dict[str, str]:
    definition = get_guide_sheet(sheet_key)
    return {
        field.key: str(payload.get(field.key, "") or "").strip()
        for field in definition.fields
    }


def parse_guide_row_values(row: ShowGuideRow) -> dict[str, str]:
    try:
        payload = json.loads(row.values_json or "{}")
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return normalize_guide_values(row.sheet_key, payload)


def serialize_guide_values(values: dict[str, str]) -> str:
    return json.dumps(values, sort_keys=True)


def build_guide_sheet_views(show: Show) -> tuple[GuideSheetView, ...]:
    grouped: dict[str, list[GuideRowView]] = {key: [] for key in GUIDE_SHEETS}
    for row in show.guide_rows:
        if row.sheet_key not in grouped:
            continue
        grouped[row.sheet_key].append(
            GuideRowView(
                id=row.id,
                position=row.position,
                values=parse_guide_row_values(row),
            )
        )
    return tuple(
        GuideSheetView(definition=definition, rows=tuple(grouped[definition.key]))
        for definition in GUIDE_SHEETS.values()
    )
