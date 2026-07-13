from __future__ import annotations

from datetime import date, datetime
import os
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.config import get_settings
from app.database import Base
from app.models import CampaignRun, Show, ShowStatus
from app.notion_trade_shows import NotionTradeShowCandidate
from app.providers import ProviderResult
from app.services import compute_run_at, run_weekly_show_sync, upsert_show
from app.trade_show_verification import TradeShowDateVerification


class SchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self) -> None:
        get_settings.cache_clear()
        self.engine.dispose()

    def test_compute_run_at_is_exactly_offset_days_before_show(self) -> None:
        self.assertEqual(compute_run_at(date(2026, 7, 26), 14), datetime(2026, 7, 12, 0, 0))

    def test_local_show_outside_window_waits_without_worker_job(self) -> None:
        with self.Session() as db, patch.dict(os.environ, {"SCRAPE_EXECUTION_MODE": "local"}):
            get_settings.cache_clear()
            show, _created = upsert_show(
                db,
                show_name="Future Hardware Market",
                event_date_raw="2026-12-01",
                place="Chicago, IL",
                link="https://example.com/exhibitors",
                run_offset_days=14,
            )
            db.commit()

            self.assertEqual(show.status, ShowStatus.waiting.value)
            self.assertEqual(db.scalars(select(CampaignRun)).all(), [])

    def test_notion_mismatch_uses_official_date_and_queues_local_alert(self) -> None:
        candidate = NotionTradeShowCandidate(
            notion_page_id="page-vegas",
            show_name="Vegas Market",
            event_date=date(2026, 7, 27),
            event_end_date=date(2026, 7, 28),
            event_date_raw="2026-07-27",
            place="Las Vegas, NV",
            link="https://www.lasvegasmarket.com/en/exhibitor/exhibitor-directory",
            registered=False,
            notion_page_url="https://notion.so/page-vegas",
        )
        verification = TradeShowDateVerification(
            tracker_start_date=date(2026, 7, 27),
            official_start_date=date(2026, 7, 26),
            effective_start_date=date(2026, 7, 26),
            status="mismatch",
            official_url="https://www.lasvegasmarket.com/en/Visit/Market-Dates-and-Hours",
            message="Official organizer date is one day earlier.",
        )
        env = {
            "WEEKLY_SHOW_SYNC_ENABLED": "true",
            "WEEKLY_SHOW_SYNC_WEEKDAY": "6",
            "WEEKLY_SHOW_SYNC_HOUR": "10",
            "WEEKLY_SHOW_SYNC_TIMEZONE": "America/New_York",
            "WEEKLY_SHOW_SYNC_LOOKAHEAD_DAYS": "100",
            "WEEKLY_SHOW_SYNC_SOURCE_PATH": "",
            "WEEKLY_SHOW_SYNC_SOURCE_URL": "",
            "NOTION_API_TOKEN": "secret-notion-test-token",
            "NOTION_DATABASE_ID": "356127477edb804d89e7c406ad08975b",
            "NOTION_DATA_SOURCE_ID": "356127477edb8094b75f000bbd6766d8",
            "SCRAPE_EXECUTION_MODE": "local",
        }
        with self.Session() as db, patch.dict(os.environ, env, clear=False):
            get_settings.cache_clear()
            with (
                patch("app.services.fetch_notion_trade_shows", return_value=[candidate]),
                patch("app.services.verify_trade_show_date", return_value=verification),
                patch(
                    "app.services.sync_show_to_airtable",
                    return_value=ProviderResult("airtable", "success", "Synced."),
                ) as airtable_mock,
                patch(
                    "app.services.notify_scrape_due",
                    return_value=ProviderResult("notification", "success", "Alerted."),
                ) as alert_mock,
            ):
                result = run_weekly_show_sync(
                    db,
                    now=datetime(2026, 7, 12, 10, 5, tzinfo=ZoneInfo("America/New_York")),
                )

            self.assertIsNotNone(result)
            show = db.scalar(select(Show))
            self.assertIsNotNone(show)
            assert show is not None
            self.assertEqual(show.event_date, date(2026, 7, 26))
            self.assertEqual(show.tracker_event_date, date(2026, 7, 27))
            self.assertEqual(show.run_at, datetime(2026, 7, 12, 0, 0))
            self.assertEqual(show.status, ShowStatus.queued.value)
            self.assertEqual(show.scrape_execution_mode, "local")
            self.assertEqual(show.date_verification_status, "mismatch")
            self.assertEqual(db.scalars(select(CampaignRun)).all(), [])
            alert_mock.assert_called_once()
            airtable_mock.assert_called_once_with(show)

    def test_failed_empty_local_show_is_requeued_when_due(self) -> None:
        with self.Session() as db, patch.dict(os.environ, {"SCRAPE_EXECUTION_MODE": "local"}):
            get_settings.cache_clear()
            failed_show = Show(
                name="Las Vegas Market",
                event_date=date(2026, 7, 26),
                place="Las Vegas, NV",
                source_url="https://www.lasvegasmarket.com/en/exhibitor/exhibitor-directory",
                run_offset_days=14,
                run_at=datetime(2026, 7, 12),
                status=ShowStatus.failed.value,
                scrape_execution_mode="local",
                company_count=0,
            )
            db.add(failed_show)
            db.commit()

            with patch(
                "app.services.notify_scrape_due",
                return_value=ProviderResult("notification", "success", "Alerted."),
            ) as alert_mock:
                show, created = upsert_show(
                    db,
                    show_name="Las Vegas Market",
                    event_date_raw="2026-07-26",
                    place="Las Vegas, NV",
                    link="https://www.lasvegasmarket.com/en/exhibitor/exhibitor-directory",
                    run_offset_days=14,
                    scrape_execution_mode="local",
                )
                db.commit()

            self.assertFalse(created)
            self.assertEqual(show.status, ShowStatus.queued.value)
            self.assertEqual(db.scalars(select(CampaignRun)).all(), [])
            alert_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
