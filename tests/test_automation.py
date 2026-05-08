from __future__ import annotations

from datetime import date, datetime
import io
import os
from pathlib import Path
import tempfile
import unittest
import zipfile
from unittest.mock import patch

from openpyxl import Workbook
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.config import get_settings
from app.database import Base
from app.models import ClaySyncRow, RunStatus, Show, ShowGuideRow, ShowStatus
from app.providers import ClayPollResult, ClayRecord, ProviderResult, SmartleadSyncResult
from app.services import _build_prepared_lead, BulkDirectScrapeResult, DirectScrapeResult, launch_show, register_bulk_shows, run_bulk_direct_scrape, run_show_scrape, sync_show_from_clay, upsert_show


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


if __name__ == "__main__":
    unittest.main()
