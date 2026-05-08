from __future__ import annotations

from datetime import date, datetime, timedelta
import unittest

from app.models import CampaignRun, RunStatus, Show, ShowStatus
from app.web.presenters import build_show_card, build_workflow_dashboard_view, summarize_show_error


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
            make_show(name="Queued", status=ShowStatus.queued.value),
            make_show(name="Scheduled", run_at=datetime(2026, 5, 10, 9, 0)),
            make_show(name="Done", status=ShowStatus.ready_for_review.value, company_count=25),
        ]

        view = build_workflow_dashboard_view(shows, now)

        self.assertEqual(view.active_count, 1)
        self.assertEqual(view.scheduled_count, 1)
        self.assertEqual(view.completed_section_count, 1)
        self.assertEqual(view.completed_lead_count, 25)

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
