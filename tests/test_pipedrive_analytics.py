from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import os
from pathlib import Path
import tempfile
from typing import Any
from urllib.parse import urlsplit
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo


_TMP_ROOT = Path(tempfile.mkdtemp(prefix="pipedrive-analytics-tests-"))
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_ROOT / 'test.db'}"
os.environ["EXPORT_DIR"] = str(_TMP_ROOT / "exports")

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.pipedrive_analytics import (
    DEAL_INCLUDE_FIELDS,
    PipedriveAnalyticsError,
    PipedriveAnalyticsSnapshot,
    build_payload,
    refresh_pipedrive_analytics,
    refresh_pipedrive_analytics_if_due,
)


EASTERN = ZoneInfo("America/New_York")


class FakePipedriveAnalyticsClient:
    def __init__(
        self,
        *,
        deal_pages: dict[str | None, dict[str, Any]] | None = None,
        users_response: dict[str, Any] | None = None,
    ) -> None:
        self.deal_pages = deepcopy(
            deal_pages
            or {
                None: {
                    "success": True,
                    "data": [],
                    "additional_data": {"next_cursor": None},
                }
            }
        )
        self.users_response = deepcopy(
            users_response
            or {
                "success": True,
                "data": [
                    {"id": 1, "name": "Lea Skoumbakis", "active_flag": True},
                    {"id": 2, "name": "John Yoon", "active_flag": True},
                ],
            }
        )
        self.calls: list[dict[str, Any]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        path = urlsplit(url).path
        call = {
            "method": method,
            "path": path,
            "params": deepcopy(params or {}),
        }
        self.calls.append(call)
        if method == "GET" and path == "/v1/users":
            return deepcopy(self.users_response)
        if method == "GET" and path == "/api/v2/deals":
            cursor = (params or {}).get("cursor")
            if cursor not in self.deal_pages:
                raise AssertionError(f"Unexpected deal cursor {cursor!r}")
            return deepcopy(self.deal_pages[cursor])
        raise AssertionError(f"Unexpected fake request: {method} {path}")


def deal(
    added_at: datetime,
    *,
    owner_id: int = 1,
    followed_up: bool = False,
) -> dict[str, Any]:
    return {
        "add_time": added_at.isoformat(),
        "owner_id": owner_id,
        "activities_count": 1 if followed_up else 0,
        # These values deliberately disagree in one direction: the analytics
        # definition is activities_count > 0, not either done/undone field.
        "done_activities_count": 0,
        "undone_activities_count": 0,
        "next_activity_id": None,
        "title": "Must never be persisted",
        "person_name": "Must never be persisted",
    }


class PipedriveAnalyticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            future=True,
        )
        self.db = self.Session()

    def tearDown(self) -> None:
        self.db.close()
        self.engine.dispose()

    def test_refresh_paginates_by_cursor_and_stops_at_calendar_cutoff(self) -> None:
        now = datetime(2026, 7, 30, 8, 0, tzinfo=EASTERN)
        client = FakePipedriveAnalyticsClient(
            deal_pages={
                None: {
                    "success": True,
                    "data": [
                        deal(datetime(2026, 7, 30, 11, 0, tzinfo=ZoneInfo("UTC"))),
                        deal(datetime(2026, 7, 15, 16, 0, tzinfo=ZoneInfo("UTC"))),
                    ],
                    "additional_data": {"next_cursor": "page-2"},
                },
                "page-2": {
                    "success": True,
                    "data": [
                        # The 30-calendar-day window starts at local midnight
                        # July 1, which is 04:00 UTC during daylight time.
                        deal(datetime(2026, 7, 1, 4, 0, tzinfo=ZoneInfo("UTC"))),
                        deal(datetime(2026, 7, 1, 3, 59, tzinfo=ZoneInfo("UTC"))),
                        deal(datetime(2026, 6, 30, 20, 0, tzinfo=ZoneInfo("UTC"))),
                    ],
                    # A sorted feed must not request this after hitting cutoff.
                    "additional_data": {"next_cursor": "page-3"},
                },
            }
        )

        payload = refresh_pipedrive_analytics(
            self.db,
            client=client,
            now=now,
            api_token="test-token",
            base_url="https://api.pipedrive.com/v1",
        )
        snapshot = self.db.scalar(select(PipedriveAnalyticsSnapshot))

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.source_count, 3)
        self.assertEqual(payload["report"]["window_start"], "2026-07-01")
        self.assertEqual(payload["report"]["window_end"], "2026-07-30")
        self.assertEqual(payload["kpis"]["total_deals"]["value"], 3)
        deal_calls = [
            call for call in client.calls if call["path"] == "/api/v2/deals"
        ]
        self.assertEqual(len(deal_calls), 2)
        self.assertNotIn("cursor", deal_calls[0]["params"])
        self.assertEqual(deal_calls[1]["params"]["cursor"], "page-2")
        self.assertEqual(deal_calls[0]["params"]["sort_by"], "add_time")
        self.assertEqual(deal_calls[0]["params"]["sort_direction"], "desc")
        self.assertEqual(
            deal_calls[0]["params"]["include_fields"],
            ",".join(DEAL_INCLUDE_FIELDS),
        )
        self.assertNotIn("title", snapshot.payload_json)
        self.assertNotIn("person_name", snapshot.payload_json)

    def test_build_payload_aggregates_kpis_leaderboard_and_chart_shapes(self) -> None:
        now = datetime(2026, 7, 30, 20, 0, tzinfo=EASTERN)
        monday = datetime(2026, 7, 27, 18, 0, tzinfo=EASTERN)
        tuesday = datetime(2026, 7, 28, 18, 0, tzinfo=EASTERN)
        wednesday = datetime(2026, 7, 29, 10, 0, tzinfo=EASTERN)
        deals = [
            deal(monday + timedelta(minutes=index), followed_up=index < 8)
            for index in range(10)
        ]
        deals.extend(
            deal(
                tuesday + timedelta(minutes=index),
                followed_up=True,
            )
            for index in range(5)
        )
        deals.extend(
            deal(
                wednesday + timedelta(minutes=index),
                owner_id=2,
                followed_up=index < 2,
            )
            for index in range(10)
        )
        owner_records = [
            {"id": 1, "name": "Lea Skoumbakis", "active_flag": True},
            {"id": 2, "name": "John Yoon", "active_flag": True},
            {"id": 3, "name": "Former Rep", "active_flag": False},
        ]

        payload = build_payload(deals, owner_records, now=now)

        self.assertEqual(
            payload["kpis"]["total_deals"],
            {"value": 25, "display": "25", "followed_up": 15},
        )
        self.assertEqual(
            payload["kpis"]["coverage"],
            {"value": 60.0, "display": "60%"},
        )
        self.assertEqual(
            payload["kpis"]["best_hour"],
            {"label": "6pm", "coverage": 86.7, "count": 15},
        )
        self.assertEqual(
            payload["kpis"]["best_day"],
            {"label": "Mon", "coverage": 80.0, "count": 10},
        )
        self.assertEqual(
            payload["kpis"]["top_owner"],
            {"label": "Lea Skoumbakis", "coverage": 86.7, "count": 15},
        )
        self.assertEqual(
            payload["leaderboard"]["days"],
            [
                {"date": "2026-07-27", "label": "Mon 7/27"},
                {"date": "2026-07-28", "label": "Tue 7/28"},
                {"date": "2026-07-29", "label": "Wed 7/29"},
                {"date": "2026-07-30", "label": "Thu 7/30"},
            ],
        )
        self.assertEqual(
            [row["owner"] for row in payload["leaderboard"]["rows"]],
            ["Lea Skoumbakis", "John Yoon"],
        )
        lea = payload["leaderboard"]["rows"][0]
        self.assertEqual(lea["period_deals"], 15)
        self.assertEqual(lea["days"][0]["deals"], 10)
        self.assertEqual(lea["days"][1]["coverage"], 100.0)
        self.assertEqual(
            [entry["label"] for entry in payload["weekday_blended"]],
            ["Mon", "Tue", "Wed", "Thu", "Fri"],
        )
        self.assertEqual(len(payload["hourly"]), 24)
        self.assertEqual(len(payload["weekdays"]), 7)
        self.assertEqual(len(payload["heatmap"]), 24 * 7)

    def test_if_due_refreshes_after_hour_only_once_and_missing_token_noops(self) -> None:
        client = FakePipedriveAnalyticsClient(
            deal_pages={
                None: {
                    "success": True,
                    "data": [
                        deal(
                            datetime(2026, 7, 30, 9, 0, tzinfo=EASTERN),
                            followed_up=True,
                        )
                    ],
                    "additional_data": {"next_cursor": None},
                }
            }
        )

        before_due = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 7, 30, 5, 59, tzinfo=EASTERN),
            refresh_hour=6,
            api_token="test-token",
        )
        self.assertIsNone(before_due)
        self.assertEqual(client.calls, [])

        first_payload = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN),
            refresh_hour=6,
            api_token="test-token",
        )
        call_count_after_first = len(client.calls)
        second = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 7, 30, 15, 0, tzinfo=EASTERN),
            refresh_hour=6,
            api_token="test-token",
        )

        self.assertIsNotNone(first_payload)
        self.assertIsNone(second)
        self.assertEqual(len(client.calls), call_count_after_first)
        self.assertEqual(
            self.db.scalar(select(func.count(PipedriveAnalyticsSnapshot.id))),
            1,
        )

        missing_token = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 7, 31, 10, 0, tzinfo=EASTERN),
            refresh_hour=6,
            api_token="",
        )
        self.assertIsNone(missing_token)
        self.assertEqual(len(client.calls), call_count_after_first)

    def test_sparse_buckets_have_null_kpi_labels_and_coverage(self) -> None:
        now = datetime(2026, 7, 30, 20, 0, tzinfo=EASTERN)
        sparse_deals = [
            deal(
                datetime(2026, 7, 27, 11, index, tzinfo=EASTERN),
                followed_up=True,
            )
            for index in range(9)
        ]

        payload = build_payload(
            sparse_deals,
            {1: "Lea Skoumbakis"},
            now=now,
            minimum_sample=10,
        )

        for key in ("best_hour", "best_day", "top_owner"):
            self.assertIsNone(payload["kpis"][key]["label"])
            self.assertIsNone(payload["kpis"][key]["coverage"])
            self.assertEqual(payload["kpis"][key]["count"], 0)
        self.assertEqual(payload["hourly"][11]["coverage"], 100.0)
        self.assertIsNone(payload["hourly"][0]["coverage"])
        self.assertIsNone(payload["weekdays"][0]["coverage"])
        empty_cell = next(
            cell
            for cell in payload["heatmap"]
            if cell["hour"] == 0 and cell["day_index"] == 0
        )
        self.assertIsNone(empty_cell["coverage"])

    def test_refresh_reads_positive_window_settings_from_environment(self) -> None:
        client = FakePipedriveAnalyticsClient()
        with patch.dict(
            "os.environ",
            {
                "PIPEDRIVE_ANALYTICS_LOOKBACK_DAYS": "14",
                "PIPEDRIVE_ANALYTICS_MIN_SAMPLE": "3",
            },
        ):
            payload = refresh_pipedrive_analytics(
                self.db,
                client=client,
                now=datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN),
                api_token="test-token",
            )

        self.assertEqual(payload["report"]["lookback_days"], 14)
        self.assertEqual(payload["report"]["minimum_sample"], 3)
        self.assertEqual(payload["report"]["window_start"], "2026-07-17")

    def test_invalid_window_environment_setting_fails_before_fetch(self) -> None:
        client = FakePipedriveAnalyticsClient()
        with patch.dict(
            "os.environ",
            {"PIPEDRIVE_ANALYTICS_LOOKBACK_DAYS": "0"},
        ):
            with self.assertRaisesRegex(
                PipedriveAnalyticsError,
                "PIPEDRIVE_ANALYTICS_LOOKBACK_DAYS must be a positive integer",
            ):
                refresh_pipedrive_analytics(
                    self.db,
                    client=client,
                    now=datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN),
                    api_token="test-token",
                )

        self.assertEqual(client.calls, [])

    def test_malformed_deals_response_fails_without_persisting_snapshot(self) -> None:
        client = FakePipedriveAnalyticsClient(
            deal_pages={
                None: {
                    "success": True,
                    "data": {"not": "a list"},
                }
            }
        )

        with self.assertRaisesRegex(
            PipedriveAnalyticsError,
            "deals response is missing a data list",
        ):
            refresh_pipedrive_analytics(
                self.db,
                client=client,
                now=datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN),
                api_token="test-token",
            )

        self.assertEqual(
            self.db.scalar(select(func.count(PipedriveAnalyticsSnapshot.id))),
            0,
        )


if __name__ == "__main__":
    unittest.main()
