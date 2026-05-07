from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Show, ShowGuideRow
from app.show_guides import normalize_guide_values, serialize_guide_values


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
