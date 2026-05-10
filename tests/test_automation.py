from __future__ import annotations

from datetime import date, datetime
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
import zipfile
from unittest.mock import patch
from zoneinfo import ZoneInfo

import httpx
from openpyxl import Workbook
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.config import get_settings
from app.database import Base
from app.models import AutomationCheckpoint, CampaignRun, ClaySyncRow, RunStatus, Show, ShowGuideRow, ShowStatus
from app.providers import ClayPollResult, ClayRecord, ProviderResult, SmartleadSyncResult, ensure_smartlead_campaign
from app.services import _build_prepared_lead, BulkDirectScrapeResult, DirectScrapeResult, launch_show, list_shows, register_bulk_shows, run_bulk_direct_scrape, run_next_campaign, run_show_scrape, run_weekly_show_sync, start_outbound_campaign, sync_show_from_clay, upsert_show
from app.trade_show_feeder import (
    TradeShowScanCandidate,
    TradeShowScanDebug,
    TradeShowScanError,
    TradeShowScanPassDebug,
    TradeShowScanRunResult,
    is_b2b_physical_goods_show,
    is_trade_show_scan_final_source_url,
    resolve_trade_show_scan_source_url,
    scan_upcoming_trade_shows,
    scan_upcoming_trade_shows_with_debug,
)


def make_show(**overrides) -> Show:
    payload = {
        "name": "Luxe Pack",
        "event_date": date(2026, 5, 6),
        "place": "New York City, NY",
        "source_url": "https://example.com/exhibitors",
        "run_offset_days": 14,
        "run_at": datetime(2026, 5, 1, 9, 0),
        "status": ShowStatus.ready_for_review.value,
        "latest_export_path": "",
        "clay_table_id": "tbl_123",
    }
    payload.update(overrides)
    return Show(**payload)


def build_guide_workbook_file() -> bytes:
    workbook = Workbook()
    company_sheet = workbook.active
    company_sheet.title = "Company Summary"
    company_sheet.append(
        [
            "Company Name",
            "Booth Number",
            "Booth Category",
            "Sales Team Size",
            "Customer Service Team Size",
            "Total Team Size",
            "Catalog Complexity (1-5)",
            "Sales Leader Name",
            "Sales Leader Role",
            "Sales Leader Email",
            "Sales Leader LinkedIn",
            "Source URL",
        ]
    )
    company_sheet.append(
        [
            "Fiserv",
            "3254",
            "3200s",
            932,
            2009,
            2941,
            2,
            "Robert Clarkson",
            "Chief Revenue Officer",
            "robert.clarkson@fiserv.com",
            "https://linkedin.com/in/robert",
            "https://example.com/fiserv",
        ]
    )
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def make_scan_run_result(*candidates: TradeShowScanCandidate) -> TradeShowScanRunResult:
    return TradeShowScanRunResult(
        candidates=list(candidates),
        debug=TradeShowScanDebug(
            start_date="2026-05-09",
            end_date="2026-08-17",
            lookahead_days=100,
            candidate_count=len(candidates),
            pass_reports=(
                TradeShowScanPassDebug(
                    pass_label="broad_scan",
                    model_used="gpt-4.1-mini",
                    raw_count=len(candidates),
                    source_count=1 if candidates else 0,
                    accepted_count=len(candidates),
                    filtered_missing_fields=0,
                    filtered_non_physical=0,
                    filtered_non_official_source=0,
                    filtered_duplicate=0,
                    remapped_to_curated_source=0,
                    sample_links=tuple(candidate.link for candidate in candidates[:3]),
                    sample_sources=tuple(candidate.link for candidate in candidates[:3]),
                    error_message="",
                ),
            ),
        ),
    )


class AutomationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_build_prepared_lead_uses_aliases_and_requires_domain(self) -> None:
        show = make_show()
        cells = {
            "email_address": "Founder@Acme.com",
            "first_name": "Ada",
            "last_name": "Lovelace",
            "company": "Acme",
            "title": "Founder",
            "company_url": "acme.com",
            "linkedin_url": "https://linkedin.com/in/ada",
            "phone": "123-456-7890",
        }

        prepared = _build_prepared_lead(show, "row-1", cells)

        self.assertIsNotNone(prepared)
        assert prepared is not None
        self.assertEqual(prepared.csv_row["email"], "founder@acme.com")
        self.assertEqual(prepared.csv_row["company_domain"], "acme.com")
        self.assertEqual(prepared.csv_row["website"], "https://acme.com")
        self.assertEqual(prepared.smartlead_row["company_name"], "Acme")
        self.assertEqual(prepared.smartlead_row["custom_fields"]["job_title"], "Founder")

        missing_domain = _build_prepared_lead(
            show,
            "row-2",
            {"email": "hi@example.com", "company": "No Domain Inc"},
        )
        self.assertIsNone(missing_domain)

    def test_sync_show_from_clay_imports_ready_rows_only_once(self) -> None:
        with self.Session() as session, tempfile.TemporaryDirectory() as tmp_dir:
            show = make_show(status=ShowStatus.ready_for_review.value)
            session.add(show)
            session.commit()

            poll_result = ClayPollResult(
                name="clay",
                status="success",
                message="ok",
                records=[
                    ClayRecord(
                        clay_row_id="row-ready",
                        row_status="ready",
                        cells={
                            "email": "ada@acme.com",
                            "first_name": "Ada",
                            "last_name": "Lovelace",
                            "company_name": "Acme",
                            "website": "acme.com",
                        },
                    ),
                    ClayRecord(
                        clay_row_id="row-failed",
                        row_status="failed",
                        cells={"company_name": "Broken Co"},
                    ),
                ],
                total_rows=2,
                ready_rows=1,
                failed_rows=1,
                skipped_rows=0,
                all_terminal=True,
            )

            import_result = SmartleadSyncResult(
                name="smartlead",
                status="success",
                message="imported",
                campaign_id=999,
                campaign_name="Luxe Pack - 2026-05-06",
                imported_count=1,
            )

            raw_path = Path(tmp_dir) / "raw.csv"
            ready_path = Path(tmp_dir) / "ready.csv"
            with patch.dict(os.environ, {"SMARTLEAD_API_KEY": "test-smartlead-key"}):
                get_settings.cache_clear()
                try:
                    with (
                        patch("app.services.poll_clay_table", return_value=poll_result),
                        patch("app.services.import_ready_rows_to_smartlead", return_value=import_result) as import_mock,
                        patch("app.services.enriched_export_path_for_show", return_value=raw_path),
                        patch("app.services.smartlead_ready_export_path_for_show", return_value=ready_path),
                    ):
                        status = sync_show_from_clay(session, show)

                    self.assertEqual(status, "success")
                    self.assertEqual(import_mock.call_count, 1)
                    imported_payload = import_mock.call_args.args[1]
                    self.assertEqual(len(imported_payload), 1)
                    self.assertEqual(show.smartlead_campaign_id, 999)
                    self.assertEqual(show.clay_status, "complete")
                    self.assertEqual(show.smartlead_status, "prepared")
                    self.assertTrue(raw_path.exists())
                    self.assertTrue(ready_path.exists())

                    sync_rows = session.scalars(select(ClaySyncRow).order_by(ClaySyncRow.clay_row_id.asc())).all()
                    self.assertEqual(len(sync_rows), 2)
                    self.assertTrue(sync_rows[0].imported_to_smartlead or sync_rows[1].imported_to_smartlead)

                    with (
                        patch("app.services.poll_clay_table", return_value=poll_result),
                        patch("app.services.import_ready_rows_to_smartlead", return_value=import_result) as import_mock,
                        patch("app.services.enriched_export_path_for_show", return_value=raw_path),
                        patch("app.services.smartlead_ready_export_path_for_show", return_value=ready_path),
                    ):
                        status = sync_show_from_clay(session, show)

                    self.assertEqual(status, "success")
                    self.assertEqual(import_mock.call_count, 0)
                finally:
                    get_settings.cache_clear()

    def test_launch_show_requires_terminal_rows_then_activates(self) -> None:
        with self.Session() as session:
            show = make_show(
                status=ShowStatus.approved.value,
                smartlead_campaign_id=777,
                smartlead_campaign_name="Luxe Pack - 2026-05-06",
                clay_total_rows=5,
                clay_ready_rows=4,
                clay_failed_rows=0,
                clay_skipped_rows=0,
            )
            session.add(show)
            session.commit()

            with self.assertRaisesRegex(ValueError, "Clay is still enriching"):
                launch_show(session, show)

            show.clay_skipped_rows = 1
            session.commit()

            with patch(
                "app.services.launch_smartlead_campaign",
                return_value=ProviderResult("smartlead", "success", "started"),
            ) as launch_mock:
                launch_show(session, show)

            self.assertEqual(launch_mock.call_count, 1)
            self.assertEqual(show.status, ShowStatus.live.value)
            self.assertEqual(show.smartlead_status, "active")

    def test_upsert_show_creates_record_before_scrape_and_run_show_scrape_fills_it(self) -> None:
        with self.Session() as session, tempfile.TemporaryDirectory() as tmp_dir:
            show, created = upsert_show(
                session,
                show_name="ICFF",
                event_date_raw="2026-05-17",
                place="New York, NY",
                link="https://example.com/icff",
                run_offset_days=14,
            )
            session.commit()

            self.assertTrue(created)
            self.assertIsNotNone(show.id)
            self.assertEqual(show.status, ShowStatus.waiting.value)

            output_path = Path(tmp_dir) / "icff.csv"
            output_path.write_text("company_name,website_url\nAcme,https://acme.com\n", encoding="utf-8")

            with patch(
                "app.services._run_direct_scrape",
                return_value=DirectScrapeResult(
                    output_path=output_path,
                    company_count=22,
                    failure_count=1,
                    conference_name="ICFF",
                    conference_location="New York, NY",
                ),
            ):
                result = run_show_scrape(session, show)

            self.assertEqual(result.output_path, output_path)
            self.assertEqual(show.status, ShowStatus.ready_for_review.value)
            self.assertEqual(show.latest_export_path, str(output_path))
            self.assertEqual(show.company_count, 22)
            self.assertEqual(show.failure_count, 1)
            self.assertEqual(len(show.runs), 1)
            self.assertEqual(show.runs[0].status, RunStatus.success.value)

    def test_upsert_show_reuses_existing_record_for_same_name_and_date(self) -> None:
        with self.Session() as session:
            first_show, first_created = upsert_show(
                session,
                show_name="International Contemporary Furniture Fair",
                event_date_raw="2026-05-17",
                place="New York, NY",
                link="https://example.com/icff",
                run_offset_days=14,
            )
            session.commit()

            second_show, second_created = upsert_show(
                session,
                show_name="International Contemporary Furniture Fair",
                event_date_raw="2026-05-17",
                place="Javits Center, New York, NY",
                link="https://another.example.com/icff",
                run_offset_days=21,
            )
            session.commit()

            self.assertTrue(first_created)
            self.assertFalse(second_created)
            self.assertEqual(first_show.id, second_show.id)
            self.assertEqual(second_show.place, "Javits Center, New York, NY")
            self.assertEqual(second_show.run_offset_days, 21)
            shows = session.scalars(select(Show)).all()
            self.assertEqual(len(shows), 1)

    def test_upsert_show_normalizes_display_name_before_storing(self) -> None:
        with self.Session() as session:
            show, created = upsert_show(
                session,
                show_name="NATIONAL RESTAURANT ASSOCIATION SHOW",
                event_date_raw="2026-05-16",
                place="Chicago, IL",
                link="https://example.com/nra",
                run_offset_days=14,
            )
            session.commit()

            self.assertTrue(created)
            self.assertEqual(show.name, "National Restaurant Association Show")

    def test_upsert_show_normalizes_pack_expo_display_name_before_storing(self) -> None:
        with self.Session() as session:
            show, created = upsert_show(
                session,
                show_name="PACK EXPO",
                event_date_raw="2026-06-02",
                place="Chicago, IL",
                link="https://www.packexpo.com/show-directory",
                run_offset_days=14,
            )
            session.commit()

            self.assertTrue(created)
            self.assertEqual(show.name, "Pack Expo")

    def test_upsert_show_reuses_existing_record_for_similar_name_within_five_day_window(self) -> None:
        with self.Session() as session:
            first_show, first_created = upsert_show(
                session,
                show_name="Sweets & Snacks",
                event_date_raw="2026-05-18",
                place="Las Vegas, NV",
                link="https://sweetsandsnacks.com/",
                run_offset_days=14,
            )
            session.commit()

            second_show, second_created = upsert_show(
                session,
                show_name="Sweets & Snacks Expo",
                event_date_raw="2026-05-19",
                place="Las Vegas Convention Center",
                link="https://sse26.mapyourshow.com/",
                run_offset_days=21,
            )
            session.commit()

            self.assertTrue(first_created)
            self.assertFalse(second_created)
            self.assertEqual(first_show.id, second_show.id)

    def test_list_shows_collapses_existing_near_duplicate_shows(self) -> None:
        with self.Session() as session:
            first = make_show(
                name="Sweets & Snacks",
                event_date=date(2026, 5, 18),
                source_url="https://sweetsandsnacks.com/",
                company_count=1054,
                latest_export_path="/tmp/first.csv",
            )
            second = make_show(
                name="Sweets & Snacks Expo",
                event_date=date(2026, 5, 19),
                source_url="https://sse26.mapyourshow.com/",
                company_count=1056,
                latest_export_path="/tmp/second.csv",
            )
            session.add_all([first, second])
            session.commit()

            shows = list_shows(session)

            self.assertEqual(len(shows), 1)
            self.assertEqual(shows[0].company_count, 1056)

    def test_list_shows_normalizes_legacy_display_names(self) -> None:
        with self.Session() as session:
            legacy = make_show(
                name="PACK EXPO",
                event_date=date(2026, 6, 2),
                source_url="https://www.packexpo.com/show-directory",
            )
            session.add(legacy)
            session.commit()

            shows = list_shows(session)

            self.assertEqual(shows[0].name, "Pack Expo")
            refreshed = session.get(Show, legacy.id)
            assert refreshed is not None
            self.assertEqual(refreshed.name, "Pack Expo")

    def test_upsert_show_reuses_existing_record_for_similar_name_and_same_date(self) -> None:
        with self.Session() as session:
            first_show, first_created = upsert_show(
                session,
                show_name="Sweets & Snacks",
                event_date_raw="2026-05-19",
                place="Las Vegas, NV",
                link="https://sweetsandsnacks.com/",
                run_offset_days=14,
            )
            session.commit()

            second_show, second_created = upsert_show(
                session,
                show_name="Sweets and Snacks Expo",
                event_date_raw="2026-05-19",
                place="Las Vegas Convention Center",
                link="https://sse26.mapyourshow.com/",
                run_offset_days=21,
            )
            session.commit()

            self.assertTrue(first_created)
            self.assertFalse(second_created)
            self.assertEqual(first_show.id, second_show.id)
            self.assertEqual(second_show.place, "Las Vegas Convention Center")
            shows = session.scalars(select(Show)).all()
            self.assertEqual(len(shows), 1)

    def test_run_show_scrape_marks_show_failed_when_scrape_raises(self) -> None:
        with self.Session() as session:
            show, _ = upsert_show(
                session,
                show_name="ICFF",
                event_date_raw="2026-05-17",
                place="New York, NY",
                link="https://example.com/icff",
                run_offset_days=14,
            )
            session.commit()

            with patch("app.services._run_direct_scrape", side_effect=RuntimeError("scrape exploded")):
                with self.assertRaisesRegex(RuntimeError, "scrape exploded"):
                    run_show_scrape(session, show)

            self.assertEqual(show.status, ShowStatus.failed.value)
            self.assertEqual(show.last_error, "scrape exploded")
            self.assertEqual(len(show.runs), 1)
            self.assertEqual(show.runs[0].status, RunStatus.failed.value)

    def test_run_bulk_direct_scrape_returns_zip_with_manifest(self) -> None:
        payload = "\n".join(
            [
                "Show,Date,Place,Link",
                "Luxe Pack,2026-05-06,New York City,https://example.com/luxe",
                "High Point,2026-10-24,High Point,https://example.com/highpoint",
            ]
        ).encode("utf-8")

        with tempfile.TemporaryDirectory() as tmp_dir:
            archive_path = Path(tmp_dir) / "bulk.zip"
            created_files = [
                Path(tmp_dir) / "luxe.csv",
                Path(tmp_dir) / "highpoint.csv",
            ]
            for index, path in enumerate(created_files, start=1):
                path.write_text("company_name,website_url\nAcme,https://acme.com\n", encoding="utf-8")

            results = [
                DirectScrapeResult(
                    output_path=created_files[0],
                    company_count=10,
                    failure_count=0,
                    conference_name="Luxe Pack",
                    conference_location="New York City",
                ),
                DirectScrapeResult(
                    output_path=created_files[1],
                    company_count=12,
                    failure_count=1,
                    conference_name="High Point",
                    conference_location="High Point",
                ),
            ]

            with (
                patch("app.services.direct_bulk_archive_path", return_value=archive_path),
                patch("app.services._run_direct_scrape", side_effect=results),
            ):
                result = run_bulk_direct_scrape(payload)

            self.assertIsInstance(result, BulkDirectScrapeResult)
            self.assertEqual(result.success_count, 2)
            self.assertEqual(result.skipped_count, 0)
            self.assertTrue(archive_path.exists())
            with zipfile.ZipFile(archive_path) as archive:
                names = set(archive.namelist())
                self.assertIn("manifest.csv", names)
                self.assertIn("luxe-pack_2026-05-06.csv", names)
                self.assertIn("high-point_2026-10-24.csv", names)
                manifest_text = archive.read("manifest.csv").decode("utf-8")
                self.assertIn("Luxe Pack", manifest_text)
                self.assertIn("High Point", manifest_text)

    def test_run_bulk_direct_scrape_updates_dashboard_shows_when_db_is_provided(self) -> None:
        payload = "\n".join(
            [
                "Show,Date,Place,Link",
                "Luxe Pack,2026-05-06,New York City,https://example.com/luxe",
                "High Point,2026-10-24,High Point,https://example.com/highpoint",
            ]
        ).encode("utf-8")

        with self.Session() as session, tempfile.TemporaryDirectory() as tmp_dir:
            archive_path = Path(tmp_dir) / "bulk.zip"
            created_files = [
                Path(tmp_dir) / "luxe-pack_2026-05-06.csv",
                Path(tmp_dir) / "high-point_2026-10-24.csv",
            ]
            for path in created_files:
                path.write_text("company_name,website_url\nAcme,https://acme.com\n", encoding="utf-8")

            results = [
                DirectScrapeResult(
                    output_path=created_files[0],
                    company_count=10,
                    failure_count=0,
                    conference_name="Luxe Pack",
                    conference_location="New York City",
                ),
                DirectScrapeResult(
                    output_path=created_files[1],
                    company_count=12,
                    failure_count=1,
                    conference_name="High Point",
                    conference_location="High Point",
                ),
            ]

            with (
                patch("app.services.direct_bulk_archive_path", return_value=archive_path),
                patch("app.services._run_direct_scrape", side_effect=results),
            ):
                summary, queued_shows = register_bulk_shows(session, payload, 14)
                result = run_bulk_direct_scrape(payload, db=session, run_offset_days=14, queued_shows=queued_shows)

            self.assertEqual(summary.created, 2)
            self.assertEqual(result.success_count, 2)
            self.assertEqual(result.skipped_count, 0)
            shows = session.scalars(select(Show).order_by(Show.event_date.asc())).all()
            self.assertEqual(len(shows), 2)
            self.assertEqual(shows[0].status, ShowStatus.ready_for_review.value)
            self.assertEqual(shows[0].company_count, 10)
            self.assertTrue(shows[0].latest_export_path.endswith("luxe-pack_2026-05-06.csv"))
            self.assertEqual(shows[1].status, ShowStatus.ready_for_review.value)
            self.assertEqual(shows[1].company_count, 12)
            self.assertTrue(shows[1].latest_export_path.endswith("high-point_2026-10-24.csv"))

    def test_run_bulk_direct_scrape_skips_deleted_queued_show(self) -> None:
        payload = "\n".join(
            [
                "Show,Date,Place,Link",
                "Luxe Pack,2026-05-06,New York City,https://example.com/luxe",
                "High Point,2026-10-24,High Point,https://example.com/highpoint",
            ]
        ).encode("utf-8")

        with self.Session() as session, tempfile.TemporaryDirectory() as tmp_dir:
            archive_path = Path(tmp_dir) / "bulk.zip"
            created_file = Path(tmp_dir) / "luxe-pack_2026-05-06.csv"
            created_file.write_text("company_name,website_url\nAcme,https://acme.com\n", encoding="utf-8")

            summary, queued_shows = register_bulk_shows(session, payload, 14)
            self.assertEqual(summary.created, 2)

            show_to_delete = session.get(Show, queued_shows[1].show_id)
            assert show_to_delete is not None
            session.delete(show_to_delete)
            session.commit()

            with (
                patch("app.services.direct_bulk_archive_path", return_value=archive_path),
                patch(
                    "app.services._run_direct_scrape",
                    return_value=DirectScrapeResult(
                        output_path=created_file,
                        company_count=10,
                        failure_count=0,
                        conference_name="Luxe Pack",
                        conference_location="New York City",
                    ),
                ),
            ):
                result = run_bulk_direct_scrape(payload, db=session, run_offset_days=14, queued_shows=queued_shows)

            self.assertEqual(result.success_count, 1)
            self.assertEqual(result.failed_count, 0)
            self.assertEqual(result.skipped_count, 1)
            remaining_shows = session.scalars(select(Show).order_by(Show.event_date.asc())).all()
            self.assertEqual(len(remaining_shows), 1)
            self.assertEqual(remaining_shows[0].name, "Luxe Pack")

    def test_start_outbound_campaign_blocks_when_sender_capacity_is_full(self) -> None:
        with self.Session() as session:
            active_show = make_show(
                name="Running Show",
                status=ShowStatus.live.value,
                smartlead_status="active",
                smartlead_campaign_id=111,
                source_url="https://example.com/running-show",
            )
            target_show = make_show(
                name="Target Show",
                status=ShowStatus.approved.value,
                smartlead_status="ready_to_launch",
                smartlead_campaign_id=222,
                smartlead_imported_rows=45,
                clay_total_rows=45,
                clay_ready_rows=45,
                source_url="https://example.com/target-show",
            )
            session.add_all([active_show, target_show])
            session.commit()

            with patch.dict(os.environ, {"OUTBOUND_SENDER_CAPACITY": "1"}):
                get_settings.cache_clear()
                try:
                    with self.assertRaisesRegex(ValueError, "at capacity"):
                        start_outbound_campaign(session, target_show)
                finally:
                    get_settings.cache_clear()

    def test_run_weekly_show_sync_filters_for_physical_goods_and_window(self) -> None:
        csv_payload = "\n".join(
            [
                "Show,Date,Place,Link",
                "High Point Market,2026-05-25,High Point NC,https://example.com/high-point",
                "Cloud Software Expo,2026-05-22,Las Vegas NV,https://example.com/cloud",
                "Packaging Summit,2026-07-20,Chicago IL,https://example.com/packaging",
            ]
        )

        with self.Session() as session, tempfile.TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "weekly.csv"
            source_path.write_text(csv_payload, encoding="utf-8")

            with patch.dict(
                os.environ,
                {
                    "WEEKLY_SHOW_SYNC_ENABLED": "true",
                    "WEEKLY_SHOW_SYNC_SOURCE_PATH": str(source_path),
                    "WEEKLY_SHOW_SYNC_WEEKDAY": "6",
                    "WEEKLY_SHOW_SYNC_HOUR": "10",
                    "WEEKLY_SHOW_SYNC_LOOKAHEAD_DAYS": "30",
                    "WEEKLY_SHOW_SYNC_TIMEZONE": "America/New_York",
                },
            ):
                get_settings.cache_clear()
                try:
                    result = run_weekly_show_sync(
                        session,
                        now=datetime(2026, 5, 24, 10, 5, tzinfo=ZoneInfo("America/New_York")),
                    )
                    self.assertIsNotNone(result)
                    assert result is not None
                    self.assertEqual(result.created, 1)
                    self.assertEqual(result.updated, 0)
                    self.assertEqual(result.filtered_out, 2)
                    self.assertIsNone(
                        run_weekly_show_sync(
                            session,
                            now=datetime(2026, 5, 24, 10, 10, tzinfo=ZoneInfo("America/New_York")),
                        )
                    )
                finally:
                    get_settings.cache_clear()

    def test_run_weekly_show_sync_uses_ai_scan_when_no_source_is_configured(self) -> None:
        with self.Session() as session:
            with patch.dict(
                os.environ,
                {
                    "WEEKLY_SHOW_SYNC_ENABLED": "true",
                    "WEEKLY_SHOW_SYNC_WEEKDAY": "6",
                    "WEEKLY_SHOW_SYNC_HOUR": "10",
                    "WEEKLY_SHOW_SYNC_LOOKAHEAD_DAYS": "30",
                    "WEEKLY_SHOW_SYNC_TIMEZONE": "America/New_York",
                    "WEEKLY_SHOW_SYNC_SOURCE_PATH": "",
                    "WEEKLY_SHOW_SYNC_SOURCE_URL": "",
                },
            ):
                get_settings.cache_clear()
                try:
                    with patch(
                        "app.services.scan_upcoming_trade_shows",
                        return_value=[
                            TradeShowScanCandidate(
                                show_name="High Point Market",
                                event_date_raw="2026-05-25",
                                place="High Point NC",
                                link="https://example.com/high-point",
                                summary="Home furnishings suppliers.",
                            )
                        ],
                    ):
                        result = run_weekly_show_sync(
                            session,
                            now=datetime(2026, 5, 24, 10, 5, tzinfo=ZoneInfo("America/New_York")),
                        )

                    self.assertIsNotNone(result)
                    assert result is not None
                    self.assertEqual(result.created, 1)
                finally:
                    get_settings.cache_clear()

    def test_register_bulk_shows_skips_duplicate_rows_in_same_upload(self) -> None:
        payload = "\n".join(
            [
                "Show,Date,Place,Link",
                "ICFF,2026-05-17,New York,https://example.com/icff",
                "ICFF,2026-05-17,New York,https://example.com/icff",
                "ICFF,2026-05-17,New York,https://example.com/icff?sort=desc",
            ]
        ).encode("utf-8")

        with self.Session() as session:
            summary, queued_shows = register_bulk_shows(session, payload, 14)

            self.assertEqual(summary.created, 1)
            self.assertEqual(summary.updated, 1)
            self.assertEqual(summary.skipped, 1)
            self.assertEqual(len(queued_shows), 2)
            shows = session.scalars(select(Show)).all()
            self.assertEqual(len(shows), 1)

    def test_register_bulk_shows_collapses_similar_show_names_on_same_date(self) -> None:
        payload = "\n".join(
            [
                "Show,Date,Place,Link",
                "National Restaurant Show,2026-05-16,Chicago,https://www.nationalrestaurantshow.com/",
                "National Restaurant Association Show,2026-05-16,Chicago,https://www.nationalrestaurantshow.com/home/search/",
            ]
        ).encode("utf-8")

        with self.Session() as session:
            summary, queued_shows = register_bulk_shows(session, payload, 14)

            self.assertEqual(summary.created, 1)
            self.assertEqual(summary.updated, 1)
            self.assertEqual(summary.skipped, 0)
            self.assertEqual(len(queued_shows), 2)
            shows = session.scalars(select(Show)).all()
            self.assertEqual(len(shows), 1)

    def test_bulk_scrape_route_starts_background_job(self) -> None:
        async def run_test() -> None:
            try:
                from fastapi import UploadFile
                from app.main import scrape_many_shows
            except ModuleNotFoundError:
                self.skipTest("fastapi is not installed in this test environment")

            upload = UploadFile(
                filename="shows.csv",
                file=io.BytesIO(
                    b"Show,Date,Place,Link\nLuxe Pack,2026-05-06,New York City,https://example.com/luxe\n"
                ),
            )
            request = type("Req", (), {"session": {}})()

            with (
                patch("app.web.routes.workflow.require_authenticated"),
                patch("app.web.routes.workflow.bulk_scrape_jobs.start_job", return_value="job-123") as start_job_mock,
            ):
                with self.Session() as session:
                    response = await scrape_many_shows(request=request, file=upload, db=session)

            self.assertEqual(response.status_code, 200)
            self.assertIn(b'"job_id":"job-123"', response.body)
            self.assertIn(b'"created":1', response.body)
            self.assertEqual(start_job_mock.call_count, 1)
            self.assertIn(b"Luxe Pack", start_job_mock.call_args.args[0])
            self.assertEqual(start_job_mock.call_args.kwargs["run_offset_days"], 14)
            self.assertEqual(len(start_job_mock.call_args.kwargs["queued_shows"]), 1)

        import asyncio

        asyncio.run(run_test())

    def test_add_single_show_redirects_to_dashboard_with_flash(self) -> None:
        from app.main import add_single_show

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            with patch("app.web.routes.workflow.require_authenticated"):
                response = add_single_show(
                    request=request,
                    show_name="ICFF",
                    event_date="2026-05-17",
                    place="New York, NY",
                    link="https://example.com/icff",
                    run_offset_days=14,
                    db=session,
                )

            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/shows/dashboard")
            self.assertEqual(request.session["flash_message"]["title"], "Trade show saved.")

    def test_import_shows_redirects_to_dashboard_with_flash(self) -> None:
        async def run_test() -> None:
            from fastapi import UploadFile
            from app.main import import_shows

            upload = UploadFile(
                filename="shows.csv",
                file=io.BytesIO(
                    b"Show,Date,Place,Link\nICFF,2026-05-17,New York,https://example.com/icff\n"
                ),
            )
            request = type("Req", (), {"session": {}})()

            with self.Session() as session:
                with patch("app.web.routes.workflow.require_authenticated"):
                    response = await import_shows(
                        request=request,
                        file=upload,
                        run_offset_days=14,
                        db=session,
                    )

            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/shows/dashboard")
            self.assertEqual(
                request.session["flash_message"]["title"],
                "Trade shows added to the dashboard.",
            )

        import asyncio

        asyncio.run(run_test())

    def test_configure_smartlead_route_smart_mode_links_or_creates_campaign(self) -> None:
        from app.main import configure_smartlead_route

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            show = make_show()
            session.add(show)
            session.commit()

            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch(
                    "app.web.routes.shows.ensure_smartlead_campaign",
                    return_value=SmartleadSyncResult(
                        name="smartlead",
                        status="success",
                        message="Attached to matching campaign.",
                        campaign_id=654,
                        campaign_name="ICFF Smart",
                    ),
                ),
            ):
                response = configure_smartlead_route(
                    show_id=show.id,
                    request=request,
                    campaign_mode="smart",
                    existing_campaign_id="",
                    db=session,
                )

            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], f"/shows/{show.id}")
            self.assertEqual(show.smartlead_campaign_id, 654)
            self.assertEqual(show.smartlead_campaign_name, "ICFF Smart")
            self.assertEqual(request.session["flash_message"]["title"], "Smartlead campaign configured.")

    def test_configure_smartlead_route_existing_mode_links_selected_campaign(self) -> None:
        from app.main import configure_smartlead_route

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            show = make_show()
            session.add(show)
            session.commit()

            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch(
                    "app.web.routes.shows.fetch_smartlead_campaign_option",
                    return_value={"id": 321, "name": "ICFF Buyers"},
                ),
            ):
                response = configure_smartlead_route(
                    show_id=show.id,
                    request=request,
                    campaign_mode="existing",
                    existing_campaign_id="321",
                    db=session,
                )

            self.assertEqual(response.status_code, 303)
            self.assertEqual(show.smartlead_campaign_id, 321)
            self.assertEqual(show.smartlead_campaign_name, "ICFF Buyers")
            self.assertEqual(request.session["flash_message"]["title"], "Smartlead campaign linked.")

    def test_create_smartlead_campaign_route_links_campaign(self) -> None:
        from app.main import create_smartlead_campaign_route

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            show = make_show()
            session.add(show)
            session.commit()

            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch(
                    "app.web.routes.shows.ensure_smartlead_campaign",
                    return_value=SmartleadSyncResult(
                        name="smartlead",
                        status="success",
                        message="Created Smartlead campaign 654.",
                        campaign_id=654,
                        campaign_name="Luxe Pack - May 6th 2026",
                    ),
                ),
            ):
                response = create_smartlead_campaign_route(show_id=show.id, request=request, db=session)

            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/shows/dashboard")
            self.assertEqual(show.smartlead_campaign_id, 654)
            self.assertEqual(show.smartlead_campaign_name, "Luxe Pack - May 6th 2026")
            self.assertEqual(request.session["flash_message"]["title"], "Smartlead campaign ready.")

    def test_rebuild_smartlead_campaign_route_forces_rebuild(self) -> None:
        from app.main import rebuild_smartlead_campaign_route

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            show = make_show(smartlead_campaign_id=321, smartlead_campaign_name="Old Campaign")
            session.add(show)
            session.commit()

            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch(
                    "app.web.routes.shows.ensure_smartlead_campaign",
                    return_value=SmartleadSyncResult(
                        name="smartlead",
                        status="success",
                        message="Rebuilt Smartlead campaign 654.",
                        campaign_id=654,
                        campaign_name="Luxe Pack - May 6th 2026",
                    ),
                ) as ensure_mock,
            ):
                response = rebuild_smartlead_campaign_route(show_id=show.id, request=request, db=session)

            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/shows/dashboard")
            self.assertEqual(show.smartlead_campaign_id, 654)
            self.assertEqual(show.smartlead_campaign_name, "Luxe Pack - May 6th 2026")
            self.assertEqual(ensure_mock.call_args.kwargs, {"force_rebuild": True})
            self.assertEqual(request.session["flash_message"]["title"], "Smartlead campaign rebuilt.")

    def test_ensure_smartlead_campaign_clones_template_settings_accounts_and_personalized_sequences(self) -> None:
        show = make_show(name="Car Wash Show", event_date=date(2026, 5, 11))
        request_calls: list[tuple[str, str, object | None]] = []

        def fake_smartlead_request(method: str, path: str, *, payload=None, **_: object):
            request_calls.append((method, path, payload))
            if path == "/campaigns/create":
                return 200, {"id": "654"}
            return 200, {}

        with patch.dict(
            os.environ,
            {
                "SMARTLEAD_API_KEY": "test-key",
                "SMARTLEAD_TEMPLATE_CAMPAIGN_ID": "999",
            },
        ):
            get_settings.cache_clear()
            try:
                with (
                    patch("app.providers._list_smartlead_campaigns", return_value=[]),
                    patch(
                        "app.providers._get_smartlead_campaign",
                        return_value={
                            "track_settings": ["DONT_EMAIL_OPEN", "DONT_LINK_CLICK"],
                            "stop_lead_settings": "REPLY_TO_AN_EMAIL",
                            "send_as_plain_text": True,
                        },
                    ),
                    patch(
                        "app.providers._get_smartlead_sequences",
                        return_value=[
                            {
                                "seq_number": 1,
                                "subject": "meet us at {{show_name_lower}}",
                                "email_body": "We are heading to {{show_name}}.",
                                "seq_delay_details": {"delay_in_days": 3},
                            }
                        ],
                    ),
                    patch("app.providers._get_smartlead_email_accounts", return_value=[{"id": 77}]),
                    patch("app.providers._smartlead_request", side_effect=fake_smartlead_request),
                ):
                    result = ensure_smartlead_campaign(show)
            finally:
                get_settings.cache_clear()

        self.assertEqual(result.status, "success")
        self.assertEqual(result.campaign_id, 654)
        self.assertEqual(result.campaign_name, "Car Wash Show - May 11th 2026")
        request_paths = [path for _method, path, _payload in request_calls]
        self.assertIn("/campaigns/create", request_paths)
        self.assertIn("/campaigns/654/settings", request_paths)
        self.assertIn("/campaigns/654/sequences", request_paths)
        self.assertIn("/campaigns/654/email-accounts", request_paths)
        self.assertNotIn("/campaigns/654/schedule", request_paths)

        settings_payload = next(payload for _method, path, payload in request_calls if path == "/campaigns/654/settings")
        self.assertNotIn("track_settings", settings_payload)
        self.assertEqual(settings_payload["stop_lead_settings"], "REPLY_TO_AN_EMAIL")
        self.assertEqual(settings_payload["send_as_plain_text"], True)

        sequence_payload = next(payload for _method, path, payload in request_calls if path == "/campaigns/654/sequences")
        self.assertEqual(sequence_payload["sequences"][0]["subject"], "meet us at car wash show")
        self.assertEqual(sequence_payload["sequences"][0]["email_body"], "We are heading to Car Wash Show.")

    def test_ensure_smartlead_campaign_rewrites_literal_template_show_name_in_sequences(self) -> None:
        show = make_show(name="Atlanta Market", event_date=date(2026, 6, 9))
        request_calls: list[tuple[str, str, object | None]] = []

        def fake_smartlead_request(method: str, path: str, *, payload=None, **_: object):
            request_calls.append((method, path, payload))
            if path == "/campaigns/create":
                return 200, {"id": "654"}
            return 200, {}

        with patch.dict(
            os.environ,
            {
                "SMARTLEAD_API_KEY": "test-key",
                "SMARTLEAD_TEMPLATE_CAMPAIGN_ID": "999",
            },
        ):
            get_settings.cache_clear()
            try:
                with (
                    patch("app.providers._list_smartlead_campaigns", return_value=[]),
                    patch(
                        "app.providers._get_smartlead_campaign",
                        return_value={
                            "name": "Car Wash Show - May 11th 2026",
                            "track_settings": {"reply_webhook": "slack"},
                        },
                    ),
                    patch(
                        "app.providers._get_smartlead_sequences",
                        return_value=[
                            {
                                "seq_number": 1,
                                "subject": "meet us at car wash show",
                                "email_body": "We are heading to Car Wash Show next month.",
                                "seq_delay_details": {"delay_in_days": 3},
                            }
                        ],
                    ),
                    patch("app.providers._get_smartlead_email_accounts", return_value=[{"id": 77}]),
                    patch("app.providers._smartlead_request", side_effect=fake_smartlead_request),
                ):
                    result = ensure_smartlead_campaign(show)
            finally:
                get_settings.cache_clear()

        self.assertEqual(result.status, "success")
        sequence_payload = next(payload for _method, path, payload in request_calls if path == "/campaigns/654/sequences")
        self.assertEqual(sequence_payload["sequences"][0]["subject"], "meet us at atlanta market")
        self.assertEqual(sequence_payload["sequences"][0]["email_body"], "We are heading to Atlanta Market next month.")

    def test_ensure_smartlead_campaign_force_rebuild_skips_existing_linked_campaign(self) -> None:
        show = make_show(smartlead_campaign_id=111, smartlead_campaign_name="Old", name="Car Wash Show", event_date=date(2026, 5, 11))
        request_calls: list[tuple[str, str, object | None]] = []

        def fake_smartlead_request(method: str, path: str, *, payload=None, **_: object):
            request_calls.append((method, path, payload))
            if path == "/campaigns/create":
                return 200, {"id": "654"}
            return 200, {}

        with patch.dict(
            os.environ,
            {
                "SMARTLEAD_API_KEY": "test-key",
                "SMARTLEAD_TEMPLATE_CAMPAIGN_ID": "999",
            },
        ):
            get_settings.cache_clear()
            try:
                with (
                    patch("app.providers._list_smartlead_campaigns", return_value=[]),
                    patch("app.providers._get_smartlead_campaign", return_value={"track_settings": {"reply_webhook": "slack"}}),
                    patch("app.providers._get_smartlead_sequences", return_value=[{"seq_number": 1, "subject": "{{show_name}}", "email_body": "{{show_name_lower}}", "seq_delay_details": {"delay_in_days": 1}}]),
                    patch("app.providers._get_smartlead_email_accounts", return_value=[{"id": 77}]),
                    patch("app.providers._smartlead_request", side_effect=fake_smartlead_request),
                ):
                    result = ensure_smartlead_campaign(show, force_rebuild=True)
            finally:
                get_settings.cache_clear()

        self.assertEqual(result.status, "success")
        self.assertEqual(result.campaign_id, 654)
        self.assertIn(("/campaigns/create"), [path for _method, path, _payload in request_calls])

    def test_ensure_smartlead_campaign_reuses_similar_existing_campaign_with_nearby_date(self) -> None:
        show = make_show(name="Sweets & Snacks", event_date=date(2026, 5, 18))

        with patch.dict(
            os.environ,
            {
                "SMARTLEAD_API_KEY": "test-key",
                "SMARTLEAD_TEMPLATE_CAMPAIGN_ID": "999",
            },
        ):
            get_settings.cache_clear()
            try:
                with (
                    patch(
                        "app.providers._list_smartlead_campaigns",
                        return_value=[
                            {
                                "id": 4321,
                                "name": "Sweets & Snacks Expo - May 19th 2026",
                            }
                        ],
                    ),
                    patch("app.providers._smartlead_request") as request_mock,
                ):
                    result = ensure_smartlead_campaign(show)
            finally:
                get_settings.cache_clear()

        self.assertEqual(result.status, "success")
        self.assertEqual(result.campaign_id, 4321)
        self.assertEqual(result.campaign_name, "Sweets & Snacks Expo - May 19th 2026")
        request_mock.assert_not_called()

    def test_scan_upcoming_trade_shows_route_returns_candidates(self) -> None:
        from app.main import scan_upcoming_trade_shows_route

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch(
                    "app.web.routes.shows.scan_upcoming_trade_shows_with_debug",
                    return_value=make_scan_run_result(
                        TradeShowScanCandidate(
                            show_name="High Point Market",
                            event_date_raw="2026-05-25",
                            place="High Point NC",
                            link="https://example.com/high-point",
                            summary="Home furnishings suppliers.",
                        )
                    ),
                ),
            ):
                response = scan_upcoming_trade_shows_route(
                    request=request,
                    query_hint="home furnishings",
                    db=session,
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'"status":"ready"', response.body)
        self.assertIn(b'"count":1', response.body)
        self.assertIn(b"High Point Market", response.body)
        self.assertIn(b'"debug"', response.body)

    def test_scan_upcoming_trade_shows_route_hides_existing_duplicates_from_candidates(self) -> None:
        from app.main import scan_upcoming_trade_shows_route

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            existing_show, _ = upsert_show(
                session,
                show_name="National Restaurant Association Show",
                event_date_raw="2026-05-16",
                place="Chicago, IL",
                link="https://www.nationalrestaurantshow.com/home/search/",
                run_offset_days=14,
            )
            session.commit()

            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch(
                    "app.web.routes.shows.scan_upcoming_trade_shows_with_debug",
                    return_value=make_scan_run_result(
                        TradeShowScanCandidate(
                            show_name="National Restaurant Show",
                            event_date_raw="2026-05-16",
                            place="Chicago, IL",
                            link="https://www.nationalrestaurantshow.com/",
                            summary="Restaurant supply trade show.",
                        ),
                        TradeShowScanCandidate(
                            show_name="Atlanta Market",
                            event_date_raw="2026-06-09",
                            place="Atlanta, GA",
                            link="https://www.atlantamarket.com/exhibitor/exhibitor-directory",
                            summary="Wholesale market.",
                        ),
                    ),
                ),
            ):
                response = scan_upcoming_trade_shows_route(
                    request=request,
                    query_hint="home furnishings",
                    db=session,
                )

        self.assertEqual(existing_show.name, "National Restaurant Association Show")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'"count":1', response.body)
        self.assertIn(b"Atlanta Market", response.body)
        self.assertNotIn(b"National Restaurant Show", response.body)

    def test_scan_upcoming_trade_shows_route_blocks_second_scan_same_day(self) -> None:
        from app.main import scan_upcoming_trade_shows_route

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch(
                    "app.web.routes.shows.scan_upcoming_trade_shows_with_debug",
                    return_value=make_scan_run_result(
                        TradeShowScanCandidate(
                            show_name="High Point Market",
                            event_date_raw="2026-05-25",
                            place="High Point NC",
                            link="https://example.com/high-point",
                            summary="Home furnishings suppliers.",
                        )
                    ),
                ),
            ):
                first_response = scan_upcoming_trade_shows_route(request=request, db=session)
                second_response = scan_upcoming_trade_shows_route(request=request, db=session)

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 429)
        self.assertIn(b'"status":"locked"', second_response.body)
        self.assertIn(b"Search has already been done today.", second_response.body)

    def test_scan_upcoming_trade_shows_route_resets_after_new_deploy_revision(self) -> None:
        from app.main import scan_upcoming_trade_shows_route

        request = type("Req", (), {"session": {}})()
        candidate = TradeShowScanCandidate(
            show_name="High Point Market",
            event_date_raw="2026-05-25",
            place="High Point NC",
            link="https://example.com/high-point",
            summary="Home furnishings suppliers.",
        )
        with self.Session() as session:
            with (
                patch.dict(os.environ, {"RENDER_GIT_COMMIT": "commit-a"}, clear=False),
                patch("app.web.routes.shows.require_authenticated"),
                patch("app.web.routes.shows.scan_upcoming_trade_shows_with_debug", return_value=make_scan_run_result(candidate)),
            ):
                get_settings.cache_clear()
                try:
                    first_response = scan_upcoming_trade_shows_route(request=request, db=session)
                finally:
                    get_settings.cache_clear()

            with (
                patch.dict(os.environ, {"RENDER_GIT_COMMIT": "commit-b"}, clear=False),
                patch("app.web.routes.shows.require_authenticated"),
                patch("app.web.routes.shows.scan_upcoming_trade_shows_with_debug", return_value=make_scan_run_result(candidate)),
            ):
                get_settings.cache_clear()
                try:
                    second_response = scan_upcoming_trade_shows_route(request=request, db=session)
                finally:
                    get_settings.cache_clear()

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)

    def test_scan_upcoming_trade_shows_route_resets_when_old_checkpoint_has_no_revision(self) -> None:
        from app.main import scan_upcoming_trade_shows_route

        request = type("Req", (), {"session": {}})()
        candidate = TradeShowScanCandidate(
            show_name="High Point Market",
            event_date_raw="2026-05-25",
            place="High Point NC",
            link="https://example.com/high-point",
            summary="Home furnishings suppliers.",
        )
        with self.Session() as session:
            with (
                patch.dict(os.environ, {"RENDER_GIT_COMMIT": "commit-a"}, clear=False),
                patch("app.web.routes.shows.require_authenticated"),
                patch("app.web.routes.shows.scan_upcoming_trade_shows_with_debug", return_value=make_scan_run_result(candidate)),
            ):
                get_settings.cache_clear()
                try:
                    first_response = scan_upcoming_trade_shows_route(request=request, db=session)
                finally:
                    get_settings.cache_clear()

            checkpoint = session.scalar(select(AutomationCheckpoint).where(AutomationCheckpoint.key == "manual_trade_show_scan"))
            assert checkpoint is not None
            checkpoint.meta_json = "{}"
            session.commit()

            with (
                patch.dict(os.environ, {"RENDER_GIT_COMMIT": "commit-b"}, clear=False),
                patch("app.web.routes.shows.require_authenticated"),
                patch("app.web.routes.shows.scan_upcoming_trade_shows_with_debug", return_value=make_scan_run_result(candidate)),
            ):
                get_settings.cache_clear()
                try:
                    second_response = scan_upcoming_trade_shows_route(request=request, db=session)
                finally:
                    get_settings.cache_clear()

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)

    def test_scan_upcoming_trade_shows_route_surfaces_provider_rate_limit_cleanly(self) -> None:
        from app.main import scan_upcoming_trade_shows_route

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch(
                    "app.web.routes.shows.scan_upcoming_trade_shows_with_debug",
                    side_effect=TradeShowScanError(
                        "Trade show scan is rate limited right now. Try again later.",
                        status_code=429,
                    ),
                ),
            ):
                response = scan_upcoming_trade_shows_route(request=request, db=session)

        self.assertEqual(response.status_code, 429)
        self.assertIn(b'"status":"error"', response.body)
        self.assertIn(b"Trade show scan is rate limited right now. Try again later.", response.body)

    def test_scan_upcoming_trade_shows_falls_back_when_gpt5_is_unavailable(self) -> None:
        primary_response = httpx.Response(
            404,
            json={
                "error": {
                    "message": "Your organization must be verified to use the model `gpt-5`.",
                    "code": "model_not_found",
                }
            },
            request=httpx.Request("POST", "https://api.openai.com/v1/responses"),
        )
        fallback_response = httpx.Response(
            200,
            json={"output": [{"content": [{"text": json.dumps({"shows": [{"show_name": "High Point Market", "event_date": "2026-05-25", "place": "High Point NC", "link": "https://example.com/high-point", "summary": "Home furnishings suppliers."}]})}]}]},
            request=httpx.Request("POST", "https://api.openai.com/v1/responses"),
        )

        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-openai-key", "TRADE_SHOW_SCAN_MODEL": "gpt-5"}):
            get_settings.cache_clear()
            try:
                with (
                    patch("app.trade_show_feeder.httpx.post", side_effect=[primary_response, fallback_response]) as post_mock,
                    patch(
                        "app.trade_show_feeder.extract_text_from_openai_response",
                        return_value=json.dumps(
                            {
                                "shows": [
                                    {
                                        "show_name": "High Point Market",
                                        "event_date": "2026-05-25",
                                        "place": "High Point NC",
                                        "link": "https://www.highpointmarket.org/ExhibitorDirectory?alpha=U",
                                        "summary": "Home furnishings suppliers.",
                                    }
                                ]
                            }
                        ),
                    ),
                ):
                    candidates = scan_upcoming_trade_shows()
            finally:
                get_settings.cache_clear()

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].show_name, "High Point Market")
        self.assertEqual(post_mock.call_args_list[0].kwargs["json"]["model"], "gpt-5")
        self.assertEqual(post_mock.call_args_list[1].kwargs["json"]["model"], "gpt-4.1-mini")
        self.assertEqual(post_mock.call_args_list[0].kwargs["json"]["tool_choice"], "required")
        self.assertEqual(post_mock.call_args_list[0].kwargs["json"]["include"], ["web_search_call.action.sources"])
        first_tool = post_mock.call_args_list[0].kwargs["json"]["tools"][0]
        second_tool = post_mock.call_args_list[1].kwargs["json"]["tools"][0]
        self.assertIn("tsnn.com", first_tool["filters"]["allowed_domains"])
        self.assertIn("highpointmarket.org", first_tool["filters"]["allowed_domains"])
        self.assertIn("thecarwashshow.com", first_tool["filters"]["allowed_domains"])
        self.assertTrue(first_tool["external_web_access"])
        self.assertEqual(first_tool["search_context_size"], "medium")
        self.assertEqual(first_tool["user_location"]["country"], "US")
        self.assertNotIn("filters", second_tool)
        self.assertTrue(second_tool["external_web_access"])
        self.assertEqual(second_tool["search_context_size"], "medium")
        self.assertEqual(second_tool["user_location"]["country"], "US")

    def test_scan_upcoming_trade_shows_uses_follow_up_passes_when_first_pass_is_empty(self) -> None:
        empty_response = httpx.Response(
            200,
            json={},
            request=httpx.Request("POST", "https://api.openai.com/v1/responses"),
        )
        populated_response = httpx.Response(
            200,
            json={},
            request=httpx.Request("POST", "https://api.openai.com/v1/responses"),
        )

        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-openai-key", "TRADE_SHOW_SCAN_MODEL": "gpt-4.1-mini"}):
            get_settings.cache_clear()
            try:
                with (
                    patch("app.trade_show_feeder.httpx.post", side_effect=[empty_response, populated_response]) as post_mock,
                    patch(
                        "app.trade_show_feeder.extract_text_from_openai_response",
                        side_effect=[
                            json.dumps({"shows": []}),
                            json.dumps(
                                {
                                    "shows": [
                                        {
                                            "show_name": "High Point Market",
                                            "event_date": "2026-05-25",
                                            "place": "High Point NC",
                                            "link": "https://www.highpointmarket.org/ExhibitorDirectory?alpha=U",
                                            "summary": "Home furnishings suppliers.",
                                        }
                                    ]
                                }
                            ),
                        ],
                    ),
                ):
                    candidates = scan_upcoming_trade_shows()
            finally:
                get_settings.cache_clear()

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].show_name, "High Point Market")
        self.assertEqual(post_mock.call_count, 2)

    def test_scan_upcoming_trade_shows_falls_back_to_curated_source_fetch(self) -> None:
        empty_response = httpx.Response(
            200,
            json={},
            request=httpx.Request("POST", "https://api.openai.com/v1/responses"),
        )

        def fake_get(url: str, **_: object) -> httpx.Response:
            payloads = {
                "https://www.highpointmarket.org/": "Market Oct 17-21, 2026",
                "https://www.atlantamarket.com/": "Atlanta Market: June 9 - 14, 2026",
                "https://www.lasvegasmarket.com/en/Visit/Market-Dates-and-Hours": "July 26 - Thursday, July 30, 2026",
                "https://www.dallasmarketcenter.com/lightovation": "Jun 24 - 27, 2026",
                "https://www.nationalrestaurantshow.com/": "May 16-19, 2026",
                "https://sweetsandsnacks.com/": "May 19-21, 2026",
                "https://thecarwashshow.com/": "May 11-13, 2026",
            }
            html = payloads.get(url, "")
            return httpx.Response(
                200,
                text=html,
                request=httpx.Request("GET", url),
            )

        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-openai-key", "TRADE_SHOW_SCAN_MODEL": "gpt-5"}):
            get_settings.cache_clear()
            try:
                with (
                    patch("app.trade_show_feeder.httpx.post", side_effect=[empty_response, empty_response, empty_response]),
                    patch("app.trade_show_feeder.extract_text_from_openai_response", side_effect=[json.dumps({"shows": []}), json.dumps({"shows": []}), json.dumps({"shows": []})]),
                    patch("app.trade_show_feeder.httpx.get", side_effect=fake_get),
                ):
                    result = scan_upcoming_trade_shows_with_debug(
                        today=date(2026, 5, 10),
                        lookahead_days=100,
                    )
            finally:
                get_settings.cache_clear()

        self.assertGreaterEqual(len(result.candidates), 4)
        candidate_names = {candidate.show_name for candidate in result.candidates}
        self.assertIn("Atlanta Market", candidate_names)
        self.assertIn("National Restaurant Association Show", candidate_names)
        self.assertEqual(result.debug.pass_reports[-1].pass_label, "curated_source_scan")

    def test_scan_upcoming_trade_shows_uses_curated_fallback_without_openai_key(self) -> None:
        def fake_get(url: str, **_: object) -> httpx.Response:
            payloads = {
                "https://www.atlantamarket.com/": "Atlanta Market: June 9 - 14, 2026",
                "https://www.nationalrestaurantshow.com/": "May 16-19, 2026",
            }
            html = payloads.get(url, "")
            return httpx.Response(200, text=html, request=httpx.Request("GET", url))

        with patch.dict(os.environ, {}, clear=True):
            get_settings.cache_clear()
            try:
                with patch("app.trade_show_feeder.httpx.get", side_effect=fake_get):
                    result = scan_upcoming_trade_shows_with_debug(
                        today=date(2026, 5, 10),
                        lookahead_days=100,
                    )
            finally:
                get_settings.cache_clear()

        self.assertGreaterEqual(len(result.candidates), 2)
        self.assertEqual(result.debug.pass_reports[0].pass_label, "api_scan_unavailable")

    def test_scan_upcoming_trade_shows_uses_curated_fallback_after_api_error(self) -> None:
        error_response = httpx.Response(
            429,
            json={"error": {"message": "quota exceeded"}},
            request=httpx.Request("POST", "https://api.openai.com/v1/responses"),
        )

        def fake_get(url: str, **_: object) -> httpx.Response:
            payloads = {
                "https://www.atlantamarket.com/": "Atlanta Market: June 9 - 14, 2026",
                "https://www.nationalrestaurantshow.com/": "May 16-19, 2026",
            }
            html = payloads.get(url, "")
            return httpx.Response(200, text=html, request=httpx.Request("GET", url))

        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-openai-key", "TRADE_SHOW_SCAN_MODEL": "gpt-5"}):
            get_settings.cache_clear()
            try:
                with (
                    patch("app.trade_show_feeder.httpx.post", return_value=error_response),
                    patch("app.trade_show_feeder.httpx.get", side_effect=fake_get),
                ):
                    result = scan_upcoming_trade_shows_with_debug(
                        today=date(2026, 5, 10),
                        lookahead_days=100,
                    )
            finally:
                get_settings.cache_clear()

        self.assertGreaterEqual(len(result.candidates), 2)
        self.assertIn("quota exceeded", result.debug.pass_reports[0].error_message)

    def test_settings_default_trade_show_scan_lookahead_is_100_days(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            get_settings.cache_clear()
            try:
                settings = get_settings()
            finally:
                get_settings.cache_clear()

        self.assertEqual(settings.weekly_show_sync_lookahead_days, 100)
        self.assertTrue(settings.deploy_revision)

    def test_icff_style_design_shows_are_excluded_from_trade_show_scan(self) -> None:
        self.assertFalse(
            is_b2b_physical_goods_show(
                "International Contemporary Furniture Fair",
                "https://icff.com",
            )
        )

    def test_trade_show_scan_final_source_url_requires_official_or_directory_domain(self) -> None:
        self.assertTrue(is_trade_show_scan_final_source_url("https://www.highpointmarket.org/ExhibitorDirectory?alpha=U"))
        self.assertTrue(is_trade_show_scan_final_source_url("https://sse26.mapyourshow.com/"))
        self.assertFalse(is_trade_show_scan_final_source_url("https://www.tsnn.com/tradeshows/high-point-market"))

    def test_trade_show_scan_resolves_curated_show_to_official_directory_url(self) -> None:
        self.assertEqual(
            resolve_trade_show_scan_source_url(
                "High Point Market",
                "https://www.tsnn.com/tradeshows/high-point-market",
            ),
            "https://www.highpointmarket.org/ExhibitorDirectory?alpha=U",
        )
        self.assertEqual(
            resolve_trade_show_scan_source_url(
                "National Restaurant Association Show",
                "https://www.eventsinamerica.com/events/national-restaurant-show",
            ),
            "https://www.nationalrestaurantshow.com/home/search/",
        )

    def test_confirm_scanned_trade_shows_route_adds_shows(self) -> None:
        from app.main import confirm_scanned_trade_shows_route

        request = type("Req", (), {"session": {}})()
        payload = json.dumps(
            [
                {
                    "show_name": "High Point Market",
                    "event_date_raw": "2026-05-25",
                    "place": "High Point NC",
                    "link": "https://example.com/high-point",
                    "summary": "Home furnishings suppliers.",
                }
            ]
        )
        with self.Session() as session:
            with patch("app.web.routes.shows.require_authenticated"):
                response = confirm_scanned_trade_shows_route(
                    request=request,
                    candidates_json=payload,
                    db=session,
                )

            self.assertEqual(response.status_code, 200)
            self.assertIn(b'"ok":true', response.body)
            shows = session.scalars(select(Show)).all()
            self.assertEqual(len(shows), 1)
            self.assertEqual(shows[0].name, "High Point Market")
            self.assertEqual(request.session["flash_message"]["title"], "Trade show scan applied.")

    def test_confirm_scanned_trade_shows_route_can_start_scrape_job(self) -> None:
        from app.main import confirm_scanned_trade_shows_route

        request = type("Req", (), {"session": {}})()
        payload = json.dumps(
            [
                {
                    "show_name": "Atlanta Market",
                    "event_date_raw": "2026-06-09",
                    "place": "Atlanta, GA",
                    "link": "https://www.atlantamarket.com/exhibitor/exhibitor-directory",
                    "summary": "Wholesale market.",
                }
            ]
        )
        with self.Session() as session:
            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch("app.web.routes.shows.bulk_scrape_jobs.start_job", return_value="job-123") as start_job_mock,
            ):
                response = confirm_scanned_trade_shows_route(
                    request=request,
                    candidates_json=payload,
                    scrape_after_add="true",
                    db=session,
                )

            self.assertEqual(response.status_code, 200)
            self.assertIn(b'"job_id":"job-123"', response.body)
            self.assertIn("Started scrape", request.session["flash_message"]["detail"])
            self.assertEqual(start_job_mock.call_count, 1)

    def test_scrape_pending_shows_route_only_queues_unpopulated_shows(self) -> None:
        from app.main import scrape_pending_shows_route

        request = type("Req", (), {"session": {}})()
        with self.Session() as session, tempfile.TemporaryDirectory() as tmp_dir:
            ready_export = Path(tmp_dir) / "ready.csv"
            ready_export.write_text("company_name\nAcme\n", encoding="utf-8")
            low_export = Path(tmp_dir) / "low.csv"
            low_export.write_text("company_name\nTiny\n", encoding="utf-8")

            show_needs_scrape = make_show(
                name="Atlanta Market",
                event_date=date(2026, 6, 9),
                source_url="https://www.atlantamarket.com/exhibitor/exhibitor-directory",
                latest_export_path="",
                company_count=0,
                status=ShowStatus.waiting.value,
            )
            show_ready = make_show(
                name="High Point Market",
                event_date=date(2026, 10, 24),
                source_url="https://www.highpointmarket.org/ExhibitorDirectory?alpha=A",
                latest_export_path=str(ready_export),
                company_count=88,
                status=ShowStatus.ready_for_review.value,
            )
            show_under_threshold = make_show(
                name="The Car Wash Show",
                event_date=date(2026, 5, 11),
                source_url="https://thecarwashshow.com/exhibitors",
                latest_export_path=str(low_export),
                company_count=18,
                status=ShowStatus.failed.value,
            )
            show_already_queued = make_show(
                name="PACK EXPO",
                event_date=date(2026, 6, 2),
                source_url="https://www.packexpo.com/show-directory",
                latest_export_path="",
                company_count=0,
                status=ShowStatus.queued.value,
            )
            session.add_all([show_needs_scrape, show_ready, show_under_threshold, show_already_queued])
            session.commit()

            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch("app.web.routes.shows.bulk_scrape_jobs.start_job", return_value="job-123") as start_job_mock,
            ):
                response = scrape_pending_shows_route(request=request, db=session)

            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/shows/dashboard")
            self.assertEqual(request.session["flash_message"]["title"], "Pending scrape started.")
            self.assertEqual(start_job_mock.call_count, 1)
            queued_shows = start_job_mock.call_args.kwargs["queued_shows"]
            self.assertEqual([item.show_name for item in queued_shows], ["Atlanta Market"])

    def test_scrape_selected_shows_route_queues_checked_shows(self) -> None:
        from app.main import scrape_selected_shows

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            first = make_show(
                name="Atlanta Market",
                event_date=date(2026, 6, 9),
                source_url="https://www.atlantamarket.com/exhibitor/exhibitor-directory",
                status=ShowStatus.waiting.value,
            )
            second = make_show(
                name="High Point Market",
                event_date=date(2026, 10, 24),
                source_url="https://www.highpointmarket.org/ExhibitorDirectory?alpha=A",
                status=ShowStatus.scraping.value,
            )
            session.add_all([first, second])
            session.commit()

            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch("app.web.routes.shows.bulk_scrape_jobs.start_job", return_value="job-456") as start_job_mock,
            ):
                response = scrape_selected_shows(
                    request=request,
                    show_ids=[first.id, second.id],
                    db=session,
                )

            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/shows/dashboard")
            self.assertEqual(start_job_mock.call_count, 1)
            queued_shows = start_job_mock.call_args.kwargs["queued_shows"]
            self.assertEqual([item.show_name for item in queued_shows], ["Atlanta Market"])

    def test_delete_show_route_purges_related_show_data(self) -> None:
        from app.main import delete_show

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            show = make_show()
            session.add(show)
            session.commit()

            session.add(ShowGuideRow(show_id=show.id, sheet_key="company_summary", position=0, values_json="{}"))
            session.add(ClaySyncRow(show_id=show.id, clay_row_id="row-1", row_status="ready"))
            session.commit()

            with patch("app.web.routes.shows.require_authenticated"):
                response = delete_show(show_id=show.id, request=request, db=session)

            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/shows/dashboard")
            self.assertIsNone(session.get(Show, show.id))
            self.assertEqual(session.scalars(select(ShowGuideRow)).all(), [])
            self.assertEqual(session.scalars(select(ClaySyncRow)).all(), [])

    def test_delete_show_leads_route_removes_selected_export_rows(self) -> None:
        from app.main import delete_show_leads
        from app.show_intelligence import _company_row_key

        request = type("Req", (), {"session": {}, "headers": {"x-requested-with": "fetch"}})()
        with self.Session() as session, tempfile.TemporaryDirectory() as tmp_dir:
            export_path = Path(tmp_dir) / "leads.csv"
            export_path.write_text(
                "company_name,website_url,booth_number\nAcme,https://acme.com,101\nBravo,https://bravo.com,102\n",
                encoding="utf-8",
            )
            show = make_show(latest_export_path=str(export_path), company_count=2)
            session.add(show)
            session.commit()

            row_key = _company_row_key(
                {
                    "company_name": "Acme",
                    "website_url": "https://acme.com",
                    "booth_number": "101",
                }
            )

            with patch("app.web.routes.shows.require_authenticated"):
                response = delete_show_leads(show_id=show.id, request=request, row_keys=[row_key], db=session)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(json.loads(response.body), {"ok": True, "deleted_count": 1, "remaining_count": 1})
            self.assertEqual(show.company_count, 1)
            self.assertIn("Bravo", export_path.read_text(encoding="utf-8"))
            self.assertNotIn("Acme", export_path.read_text(encoding="utf-8"))

    def test_run_direct_scrape_retries_until_minimum_company_count_is_met(self) -> None:
        from app.services import _run_direct_scrape

        with patch.dict(os.environ, {"MIN_SCRAPE_COMPANY_COUNT": "51"}):
            get_settings.cache_clear()
            try:
                with patch(
                    "app.services._run_direct_scrape_once",
                    side_effect=[
                        DirectScrapeResult(Path("/tmp/one.csv"), 12, 0, "Show", "Place"),
                        DirectScrapeResult(Path("/tmp/two.csv"), 44, 0, "Show", "Place"),
                        DirectScrapeResult(Path("/tmp/three.csv"), 63, 0, "Show", "Place"),
                    ],
                ) as scrape_once_mock:
                    result = _run_direct_scrape(
                        show_name="Show",
                        place="Place",
                        link="https://example.com",
                        output_path=Path("/tmp/output.csv"),
                    )
            finally:
                get_settings.cache_clear()

        self.assertEqual(result.company_count, 63)
        self.assertEqual(scrape_once_mock.call_count, 3)

    def test_run_direct_scrape_fails_when_all_attempts_stay_under_threshold(self) -> None:
        from app.services import _run_direct_scrape

        with patch.dict(os.environ, {"MIN_SCRAPE_COMPANY_COUNT": "51"}):
            get_settings.cache_clear()
            try:
                with patch(
                    "app.services._run_direct_scrape_once",
                    side_effect=[
                        DirectScrapeResult(Path("/tmp/one.csv"), 12, 0, "Show", "Place"),
                        DirectScrapeResult(Path("/tmp/two.csv"), 18, 0, "Show", "Place"),
                        DirectScrapeResult(Path("/tmp/three.csv"), 23, 0, "Show", "Place"),
                        DirectScrapeResult(Path("/tmp/four.csv"), 39, 0, "Show", "Place"),
                    ],
                ):
                    with self.assertRaisesRegex(RuntimeError, "need at least 51"):
                        _run_direct_scrape(
                            show_name="Show",
                            place="Place",
                            link="https://example.com",
                            output_path=Path("/tmp/output.csv"),
                        )
            finally:
                get_settings.cache_clear()

    def test_run_next_campaign_uses_quality_gate_and_marks_low_result_failed(self) -> None:
        with patch.dict(os.environ, {"MIN_SCRAPE_COMPANY_COUNT": "6"}):
            get_settings.cache_clear()
            try:
                with self.Session() as session:
                    show = make_show(
                        status=ShowStatus.queued.value,
                        company_count=0,
                    )
                    session.add(show)
                    session.flush()
                    session.add(CampaignRun(show=show, status=RunStatus.queued.value))
                    session.commit()

                    with patch(
                        "app.services._run_direct_scrape",
                        side_effect=RuntimeError("Best attempt found 5 exhibitors; need at least 6."),
                    ):
                        campaign_run = run_next_campaign(session)

                    self.assertIsNotNone(campaign_run)
                    assert campaign_run is not None
                    self.assertEqual(campaign_run.status, RunStatus.failed.value)
                    self.assertEqual(show.status, ShowStatus.failed.value)
                    self.assertIn("need at least 6", show.last_error)
            finally:
                get_settings.cache_clear()

    def test_build_trade_show_guide_route_populates_rows_from_export(self) -> None:
        from app.main import build_trade_show_guide_route

        request = type("Req", (), {"session": {}})()
        with self.Session() as session:
            show = make_show(latest_export_path="/tmp/export.csv")
            session.add(show)
            session.commit()

            with (
                patch("app.web.routes.shows.require_authenticated"),
                patch("app.web.routes.shows.rebuild_trade_show_guides", return_value=(5, 5)) as rebuild_mock,
            ):
                response = build_trade_show_guide_route(
                    show_id=show.id,
                    request=request,
                    db=session,
                )

            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], f"/shows/{show.id}#sheet-company_summary")
            self.assertEqual(rebuild_mock.call_count, 1)
            self.assertEqual(request.session["flash_message"]["title"], "Trade show guide built.")

    def test_show_guide_route_renders_separate_guide_page(self) -> None:
        from app.main import show_guide
        from starlette.requests import Request

        request = Request({"type": "http", "method": "GET", "path": "/shows/1/guide", "headers": [], "session": {}})
        with self.Session() as session:
            show = make_show()
            session.add(show)
            session.commit()

            response = show_guide(show_id=show.id, request=request, db=session)

            self.assertEqual(response.status_code, 200)

    def test_upload_guide_workbook_route_imports_excel_sheet(self) -> None:
        async def run_test() -> None:
            from fastapi import UploadFile
            from app.main import upload_guide_workbook_route

            request = type("Req", (), {"session": {}})()
            upload = UploadFile(filename="guide.xlsx", file=io.BytesIO(build_guide_workbook_file()))

            with self.Session() as session:
                show = make_show()
                session.add(show)
                session.commit()

                with patch("app.web.routes.shows.require_authenticated"):
                    response = await upload_guide_workbook_route(
                        show_id=show.id,
                        request=request,
                        workbook=upload,
                        db=session,
                    )

                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], f"/shows/{show.id}/guide")
                self.assertEqual(request.session["flash_message"]["title"], "Guide workbook imported.")
                rows = session.scalars(select(ShowGuideRow).where(ShowGuideRow.show_id == show.id)).all()
                self.assertEqual(len(rows), 1)

        import asyncio

        asyncio.run(run_test())

    def test_update_guide_row_route_supports_autosave_requests(self) -> None:
        async def run_test() -> None:
            from app.main import update_guide_row_route
            from app.guide_services import import_trade_show_guide_workbook
            from app.show_guides import parse_guide_row_values
            from starlette.requests import Request

            with self.Session() as session:
                show = make_show()
                session.add(show)
                session.commit()
                import_trade_show_guide_workbook(session, show=show, workbook_bytes=build_guide_workbook_file())
                row = session.scalars(select(ShowGuideRow).where(ShowGuideRow.show_id == show.id)).first()
                assert row is not None

                scope = {
                    "type": "http",
                    "method": "POST",
                    "path": f"/shows/{show.id}/guide/{row.id}/update",
                    "headers": [
                        (b"content-type", b"application/x-www-form-urlencoded"),
                        (b"x-guide-autosave", b"1"),
                    ],
                    "session": {},
                }
                body = b"company_name=Updated+Company&booth_number=3254"

                async def receive() -> dict[str, object]:
                    nonlocal body
                    chunk = body
                    body = b""
                    return {"type": "http.request", "body": chunk, "more_body": False}

                request = Request(scope, receive)

                with patch("app.web.routes.shows.require_authenticated"):
                    response = await update_guide_row_route(
                        show_id=show.id,
                        row_id=row.id,
                        request=request,
                        db=session,
                    )

                self.assertEqual(response.status_code, 204)
                session.refresh(row)
                self.assertEqual(parse_guide_row_values(row)["company_name"], "Updated Company")

        import asyncio

        asyncio.run(run_test())

    def test_download_guide_workbook_route_returns_xlsx(self) -> None:
        from app.main import download_guide_workbook
        from app.guide_services import import_trade_show_guide_workbook
        from starlette.requests import Request

        request = Request({"type": "http", "method": "GET", "path": "/shows/1/guide/download", "headers": []})
        with self.Session() as session:
            show = make_show()
            session.add(show)
            session.commit()
            import_trade_show_guide_workbook(session, show=show, workbook_bytes=build_guide_workbook_file())

            response = download_guide_workbook(show_id=show.id, request=request, db=session)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                response.media_type,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


if __name__ == "__main__":
    unittest.main()
