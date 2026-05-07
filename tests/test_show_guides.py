from __future__ import annotations

from datetime import date, datetime
import unittest

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.guide_services import create_guide_row, delete_guide_row, update_guide_row
from app.models import Show, ShowGuideRow, ShowStatus
from app.show_guides import build_guide_sheet_views


def make_show(**overrides) -> Show:
    payload = {
        "name": "ICA Car Wash Show",
        "event_date": date(2026, 5, 20),
        "place": "Las Vegas, NV",
        "source_url": "https://example.com/exhibitors",
        "run_offset_days": 14,
        "run_at": datetime(2026, 5, 6, 9, 0),
        "status": ShowStatus.ready_for_review.value,
        "latest_export_path": "",
        "company_count": 0,
        "failure_count": 0,
        "last_error": "",
    }
    payload.update(overrides)
    return Show(**payload)


class ShowGuideTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_create_update_delete_guide_row(self) -> None:
        with self.Session() as session:
            show = make_show()
            session.add(show)
            session.commit()

            row = create_guide_row(
                session,
                show=show,
                sheet_key="company_summary",
                payload={"company_name": "Fiserv", "booth_number": "3254"},
            )
            self.assertEqual(row.position, 0)

            update_guide_row(
                session,
                row=row,
                payload={"company_name": "Fiserv Updated", "booth_number": "3254"},
            )
            refreshed = session.scalar(select(ShowGuideRow).where(ShowGuideRow.id == row.id))
            assert refreshed is not None
            self.assertIn("Fiserv Updated", refreshed.values_json)

            delete_guide_row(session, row=row)
            deleted = session.scalar(select(ShowGuideRow).where(ShowGuideRow.id == row.id))
            self.assertIsNone(deleted)

    def test_build_guide_sheet_views_groups_rows_by_sheet(self) -> None:
        with self.Session() as session:
            show = make_show()
            session.add(show)
            session.commit()
            create_guide_row(session, show=show, sheet_key="company_summary", payload={"company_name": "Fiserv"})
            create_guide_row(session, show=show, sheet_key="booth_category_groups", payload={"booth_category": "100s"})
            session.refresh(show)

            views = build_guide_sheet_views(show)

            self.assertEqual(len(views), 2)
            self.assertEqual(views[0].definition.key, "company_summary")
            self.assertEqual(views[0].rows[0].values["company_name"], "Fiserv")
            self.assertEqual(views[1].rows[0].values["booth_category"], "100s")


if __name__ == "__main__":
    unittest.main()
