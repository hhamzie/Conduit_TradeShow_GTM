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
os.environ["OPENPHONE_API_KEY"] = ""

from app.config import get_settings  # noqa: E402

get_settings.cache_clear()

from fastapi.testclient import TestClient  # noqa: E402

from app.application import create_app  # noqa: E402
from app.database import init_db  # noqa: E402


def _synthetic_payload() -> dict[str, object]:
    weekdays = [
        {
            "day_index": index,
            "label": label,
            "calls": 12 + index,
            "connected": 2 + index,
            "connect_rate": 12.0 + index,
        }
        for index, label in enumerate(("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"))
    ]
    hourly = [
        {
            "hour": hour,
            "label": "12am" if hour == 0 else f"{hour}am",
            "calls": 20,
            "connected": 3,
            "connect_rate": 15.0,
        }
        for hour in range(24)
    ]
    heatmap = [
        {
            "hour": hour,
            "hour_label": "12am" if hour == 0 else f"{hour}:00",
            "day_index": day,
            "day_label": weekdays[day]["label"],
            "calls": 20,
            "connected": 3,
            "connect_rate": 15.0,
        }
        for hour in range(24)
        for day in range(7)
    ]
    return {
        "report": {
            "schema_version": "openphone-calls-v1",
            "date": "2026-07-30",
            "generated_at": "Jul 30, 2026 at 6:00 AM",
            "timezone": "America/New_York",
            "lookback_days": 30,
            "minimum_sample": 15,
            "window_start": "2026-06-30",
            "window_end": "2026-07-29",
            "window_end_exclusive": "2026-07-30",
            "definition": "Outbound completed calls lasting at least 90 seconds",
            "source_count": 6210,
        },
        "kpis": {
            "total_calls": {"value": 6210, "display": "6.2K", "connected": 779},
            "connect_rate": {"value": 12.5, "display": "13%"},
            "best_hour": {
                "label": "6pm",
                "connect_rate": 20.0,
                "count": 50,
                "calls": 50,
            },
            "best_day": {
                "label": "Wed",
                "connect_rate": 13.0,
                "count": 900,
                "calls": 900,
            },
            "top_rep": {
                "label": "Synthetic Rep",
                "name": "Synthetic Rep",
                "connect_rate": 25.0,
                "count": 120,
                "calls": 120,
            },
        },
        "leaderboard": {
            "days": [{"date": "2026-07-27", "label": "Mon 7/27"}],
            "rows": [
                {
                    "rep": "Synthetic Rep",
                    "days": [
                        {
                            "date": "2026-07-27",
                            "calls": 65,
                            "connected": 14,
                            "connect_rate": 21.5,
                        }
                    ],
                    "period_calls": 120,
                    "period_connected": 30,
                    "period_connect_rate": 25.0,
                }
            ],
        },
        "weekday_blended": [
            {
                "day_index": index,
                "label": label,
                "calls": 320,
                "connected": 40,
                "connect_rate": 12.5,
                "avg_calls_per_day": 80.0,
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
        self.assertIn("first scheduled snapshot is not ready", response.text)
        self.assertNotIn("Synthetic Rep", response.text)

    def test_authenticated_dashboard_renders_snapshot_and_local_assets(self) -> None:
        client = self._client()
        self._login(client)
        with patch(
            "app.web.routes.analytics.get_latest_pipedrive_analytics",
            return_value=_synthetic_payload(),
        ):
            response = client.get("/analytics")
        self.assertEqual(response.status_code, 200)
        self.assertIn("OpenPhone Call Analytics", response.text)
        self.assertIn("Connected = completed calls ≥ 90s", response.text)
        self.assertIn("Outbound only", response.text)
        self.assertIn("Synthetic Rep", response.text)
        self.assertIn("6.2K", response.text)
        self.assertIn("779 connected", response.text)
        self.assertIn("6210 source calls", response.text)
        self.assertIn("Updated daily at 9am, 1pm &amp; 5pm ET", response.text)
        self.assertIn("Rep leaderboard — this week by day", response.text)
        self.assertIn("Connected % by weekday — blended, last 4 weeks", response.text)
        self.assertIn("Connection rate by hour of day (30-day)", response.text)
        self.assertIn("Connection rate by day of week (30-day)", response.text)
        self.assertIn(
            "Connection rate heatmap — hour × day, blended last 4 weeks",
            response.text,
        )
        self.assertIn("/static/pipedrive-analytics.css", response.text)
        self.assertIn("/static/pipedrive-analytics.js", response.text)
        self.assertNotIn("cdn.", response.text.lower())


if __name__ == "__main__":
    unittest.main()
