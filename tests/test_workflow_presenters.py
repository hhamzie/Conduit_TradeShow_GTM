from __future__ import annotations

from datetime import date, datetime, timedelta
import unittest

from app.models import CampaignRun, RunStatus, Show, ShowStatus
from app.web.presenters import build_scrape_queue_positions, build_show_card, build_workflow_dashboard_view, summarize_show_error


def make_show(**overrides) -> Show:
    payload = {
        "name": "Sample Show",
        "event_date": date(2026, 5, 20),
        "place": "New York City, NY",
        "source_url": "https://example.com/exhibitors",
        "run_offset_days": 14,
        "run_at": datetime(2026, 5, 6, 9, 0),
        "status": ShowStatus.waiting.value,
        "latest_export_path": "",
        "company_count": 0,
        "failure_count": 0,
        "last_error": "",
    }
    payload.update(overrides)
    return Show(**payload)


class WorkflowPresenterTests(unittest.TestCase):
    def test_summarize_show_error_collapses_known_messages(self) -> None:
        summary = summarize_show_error("HTTP 429 too many requests from Clay")
        self.assertIn("Retry later", summary)

    def test_build_workflow_dashboard_view_groups_shows(self) -> None:
        now = datetime(2026, 5, 1, 9, 0)
        shows = [
            make_show(
                name="Scraping",
                status=ShowStatus.scraping.value,
                runs=[CampaignRun(status=RunStatus.running.value, started_at=datetime(2026, 5, 1, 8, 30))],
            ),
            make_show(
                name="Queued",
                status=ShowStatus.queued.value,
                runs=[CampaignRun(status=RunStatus.queued.value, created_at=datetime(2026, 5, 1, 8, 0))],
            ),
            make_show(name="Waiting", status=ShowStatus.waiting.value, run_at=datetime(2026, 5, 1, 9, 0)),
            make_show(name="Done", status=ShowStatus.ready_for_review.value, company_count=25),
        ]

        view = build_workflow_dashboard_view(shows, now)

        self.assertEqual(view.active_count, 3)
        self.assertEqual(view.scheduled_count, 0)
        self.assertEqual(view.completed_section_count, 1)
        self.assertEqual(view.completed_lead_count, 25)
        self.assertTrue(any(item.status_label == "#1 in line" for item in view.active))
        self.assertTrue(any(item.run_timing == "1 of 1 in scrape queue" for item in view.active))
        self.assertTrue(any(item.status_label == "Scraping" for item in view.active))
        self.assertEqual(view.completed[0].status_label, "Populated")
        self.assertTrue(any(item.status_label == "Queued" for item in view.active))

    def test_build_show_card_uses_queue_position_for_queued_shows(self) -> None:
        now = datetime(2026, 5, 8, 12, 0)
        show = make_show(status=ShowStatus.queued.value)

        card = build_show_card(show, now, queue_position=2, queue_total=4)

        self.assertEqual(card.status_label, "#2 in line")
        self.assertEqual(card.step_label, "Queue position 2 of 4")
        self.assertEqual(card.run_timing, "2 of 4 in scrape queue")

    def test_build_scrape_queue_positions_prefers_earlier_event_date(self) -> None:
        earlier_show = make_show(
            id=101,
            name="Earlier Show",
            event_date=date(2026, 5, 16),
            status=ShowStatus.queued.value,
            runs=[CampaignRun(status=RunStatus.queued.value, created_at=datetime(2026, 5, 12, 9, 30))],
        )
        later_show = make_show(
            id=202,
            name="Later Show",
            event_date=date(2026, 6, 2),
            status=ShowStatus.queued.value,
            runs=[CampaignRun(status=RunStatus.queued.value, created_at=datetime(2026, 5, 12, 8, 0))],
        )

        positions, total = build_scrape_queue_positions([later_show, earlier_show])

        self.assertEqual(total, 2)
        self.assertEqual(positions[earlier_show.id], 1)
        self.assertEqual(positions[later_show.id], 2)

    def test_build_show_card_shows_elapsed_time_for_scraping_shows(self) -> None:
        now = datetime(2026, 5, 8, 12, 0)
        show = make_show(
            status=ShowStatus.scraping.value,
            runs=[
                CampaignRun(
                    status=RunStatus.running.value,
                    started_at=now - timedelta(minutes=17),
                    created_at=now - timedelta(minutes=18),
                )
            ],
        )

        card = build_show_card(show, now, queue_position=1, queue_total=3)

        self.assertEqual(card.status_label, "Scraping")
        self.assertEqual(card.step_label, "Scraping now")
        self.assertEqual(card.run_timing, "Scraping for 17m")

    def test_build_show_card_hides_old_completion_notice(self) -> None:
        now = datetime(2026, 5, 8, 12, 0)
        show = make_show(
            status=ShowStatus.ready_for_review.value,
            company_count=25,
            runs=[
                CampaignRun(
                    status=RunStatus.success.value,
                    finished_at=now - timedelta(hours=2),
                )
            ],
        )

        card = build_show_card(show, now)

        self.assertIsNone(card.notice)

    def test_build_show_card_shows_recent_completion_notice(self) -> None:
        now = datetime(2026, 5, 8, 12, 0)
        show = make_show(
            status=ShowStatus.ready_for_review.value,
            company_count=25,
            runs=[
                CampaignRun(
                    status=RunStatus.success.value,
                    finished_at=now - timedelta(minutes=5),
                )
            ],
        )

        card = build_show_card(show, now)

        self.assertIsNotNone(card.notice)
        assert card.notice is not None
        self.assertEqual(card.notice.title, "Scrape completed")


if __name__ == "__main__":
    unittest.main()
