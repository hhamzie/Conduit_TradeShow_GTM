from __future__ import annotations

from collections import Counter

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Show, ShowGuideRow
from app.show_guides import normalize_guide_values, serialize_guide_values
from app.show_intelligence import _load_company_rows


def create_guide_row(
    db: Session,
    *,
    show: Show,
    sheet_key: str,
    payload: dict[str, object],
) -> ShowGuideRow:
    values = normalize_guide_values(sheet_key, payload)
    next_position = db.scalar(
        select(func.coalesce(func.max(ShowGuideRow.position), -1) + 1).where(
            ShowGuideRow.show_id == show.id,
            ShowGuideRow.sheet_key == sheet_key,
        )
    )
    row = ShowGuideRow(
        show=show,
        sheet_key=sheet_key,
        position=int(next_position or 0),
        values_json=serialize_guide_values(values),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def update_guide_row(
    db: Session,
    *,
    row: ShowGuideRow,
    payload: dict[str, object],
) -> None:
    values = normalize_guide_values(row.sheet_key, payload)
    row.values_json = serialize_guide_values(values)
    db.commit()


def delete_guide_row(db: Session, *, row: ShowGuideRow) -> None:
    db.delete(row)
    db.commit()


def _booth_category(booth_number: str) -> str:
    digits = "".join(character for character in booth_number if character.isdigit())
    if len(digits) >= 2:
        return f"{digits[:-2]}00s"
    if digits:
        return f"{digits}00s"
    return "Unassigned"


def rebuild_trade_show_guides(db: Session, *, show: Show) -> tuple[int, int]:
    company_rows = _load_company_rows(show.latest_export_path)
    if not company_rows:
        raise ValueError("Run the show scrape first so there is export data to build the guide from.")

    for row in list(show.guide_rows):
        db.delete(row)
    db.flush()

    category_counts = Counter(_booth_category(row["booth_number"]) for row in company_rows)
    company_summary_rows: list[ShowGuideRow] = []
    booth_group_rows: list[ShowGuideRow] = []
    for index, company in enumerate(company_rows):
        booth_category = _booth_category(company["booth_number"])
        shared_values = {
            "company_name": company["company_name"],
            "booth_number": company["booth_number"],
            "booth_category": booth_category,
            "sales_team_size": "",
            "customer_service_team_size": "",
            "total_team_size": "",
            "catalog_complexity": "",
            "sales_leader_name": "",
            "sales_leader_role": "",
            "sales_leader_email": "",
            "sales_leader_linkedin": "",
            "source_url": company["website_url"] or show.source_url,
        }
        company_summary_rows.append(
            ShowGuideRow(
                show=show,
                sheet_key="company_summary",
                position=index,
                values_json=serialize_guide_values(shared_values),
            )
        )
        booth_group_rows.append(
            ShowGuideRow(
                show=show,
                sheet_key="booth_category_groups",
                position=index,
                values_json=serialize_guide_values(
                    {
                        **shared_values,
                        "category_total_team_size": str(category_counts[booth_category]),
                    }
                ),
            )
        )

    for row in company_summary_rows + booth_group_rows:
        db.add(row)
    db.commit()
    return len(company_summary_rows), len(booth_group_rows)
