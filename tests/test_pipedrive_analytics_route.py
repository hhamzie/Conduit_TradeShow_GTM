from __future__ import annotations

import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch


_TMP_ROOT = Path(tempfile.mkdtemp(prefix="pipedrive-analytics-route-tests-"))
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_ROOT / 'test.db'}"
os.environ["EXPORT_DIR"] = str(_TMP_ROOT / "exports")
os.environ["DASHBOARD_USERNAME"] = "analytics-test-operator"
os.environ["DASHBOARD_PASSWORD"] = "synthetic-analytics-password"
os.environ["SESSION_SECRET"] = "synthetic-analytics-session-secret"
os.environ["PIPEDRIVE_API_TOKEN"] = ""

from fastapi.testclient import TestClient  # noqa: E402

from app.application import create_app  # noqa: E402
from app.database import init_db  # noqa: E402


def _synthetic_payload() -> dict[str, object]:
    weekdays = [
        {
            "day_index": index,
            "label": label,
            "deals": 12 + index,
            "followed_up": 8 + index,
            "coverage": 66.0 + index,
        }
        for index, label in enumerate(("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"))
    ]
    hourly = [
        {
            "hour": hour,
            "label": "12am" if hour == 0 else f"{hour}am",
            "deals": 2,
            "followed_up": 1,
            "coverage": 50.0,
        }
        for hour in range(24)
    ]
    heatmap = [
        {
            "hour": hour,
            "hour_label": "12am" if hour == 0 else f"{hour}:00",
            "day_index": day,
            "day_label": weekdays[day]["label"],
            "deals": 2,
            "followed_up": 1,
            "coverage": 50.0,
        }
        for hour in range(24)
        for day in range(7)
    ]
    return {
        "report": {
            "date": "2026-07-30",
            "generated_at": "Jul 30, 2026 at 6:00 AM",
            "generated_at_display": "Jul 30, 2026 at 6:00 AM",
            "timezone": "America/New_York",
            "lookback_days": 30,
            "minimum_sample": 10,
            "window_start": "2026-06-30",
            "window_end": "2026-07-30",
            "definition": "At least one linked activity",
            "source_count": 241,
        },
        "kpis": {
            "total_deals": {"value": 241, "display": "241", "followed_up": 161},
            "coverage": {"value": 66.8, "display": "67%"},
            "best_hour": {"label": "6pm", "coverage": 100.0, "count": 21},
            "best_day": {"label": "Mon", "coverage": 94.0, "count": 16},
            "top_owner": {
                "label": "Synthetic Owner",
                "coverage": 92.0,
                "count": 50,
            },
        },
        "leaderboard": {
            "days": [{"date": "2026-07-27", "label": "Mon 7/27"}],
            "rows": [
                {
                    "owner_id": 1,
                    "owner": "Synthetic Owner",
                    "days": [
                        {
                            "date": "2026-07-27",
                            "deals": 4,
                            "followed_up": 3,
                            "coverage": 75.0,
                        }
                    ],
                    "period_deals": 50,
                    "period_coverage": 92.0,
                }
            ],
        },
        "weekday_blended": [
            {
                "day_index": index,
                "label": label,
                "deals": 20,
                "followed_up": 14,
                "coverage": 70.0,
                "avg_deals_per_day": 5.0,
            }
            for index, label in enumerate(("Mon", "Tue", "Wed", "Thu", "Fri"))
        ],
        "hourly": hourly,
        "weekdays": weekdays,
        "heatmap": heatmap,
    }


def setUpModule() -> None:
    init_db()


class PipedriveAnalyticsRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = create_app()

    def _client(self) -> TestClient:
        return TestClient(self.app, follow_redirects=False)

    def _login(self, client: TestClient) -> None:
        response = client.post(
            "/login",
            data={
                "username": os.environ["DASHBOARD_USERNAME"],
                "password": os.environ["DASHBOARD_PASSWORD"],
            },
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/analytics")

    def test_root_now_points_to_analytics(self) -> None:
        response = self._client().get("/")
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/analytics")

    def test_anonymous_analytics_request_redirects_to_login(self) -> None:
        response = self._client().get("/analytics")
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/login")

    def test_empty_snapshot_is_explicit_and_does_not_fabricate_data(self) -> None:
        client = self._client()
        self._login(client)
        with patch(
            "app.web.routes.analytics.get_latest_pipedrive_analytics",
            return_value=None,
        ):
            response = client.get("/analytics")
        self.assertEqual(response.status_code, 200)
        self.assertIn("first daily snapshot is not ready", response.text)
        self.assertNotIn("Synthetic Owner", response.text)

    def test_authenticated_dashboard_renders_snapshot_and_local_assets(self) -> None:
        client = self._client()
        self._login(client)
        with patch(
            "app.web.routes.analytics.get_latest_pipedrive_analytics",
            return_value=_synthetic_payload(),
        ):
            response = client.get("/analytics")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Pipedrive Sales Analytics", response.text)
        self.assertIn("Synthetic Owner", response.text)
        self.assertIn("241 source deals", response.text)
        self.assertIn("/static/pipedrive-analytics.css", response.text)
        self.assertIn("/static/pipedrive-analytics.js", response.text)
        self.assertNotIn("cdn.", response.text.lower())


if __name__ == "__main__":
    unittest.main()
