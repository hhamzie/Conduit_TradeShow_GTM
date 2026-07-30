from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta
import os
from pathlib import Path
import tempfile
import traceback
from typing import Any
from urllib.parse import urlsplit
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo


_TMP_ROOT = Path(tempfile.mkdtemp(prefix="openphone-analytics-tests-"))
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_ROOT / 'test.db'}"
os.environ["EXPORT_DIR"] = str(_TMP_ROOT / "exports")

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.pipedrive_analytics import (
    MINIMUM_SAMPLE,
    PAYLOAD_SCHEMA_VERSION,
    PipedriveAnalyticsError,
    PipedriveAnalyticsSnapshot,
    _OpenPhoneReadApi,
    build_payload,
    get_latest_pipedrive_analytics,
    refresh_pipedrive_analytics,
    refresh_pipedrive_analytics_if_due,
)


EASTERN = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


class FakeOpenPhoneAnalyticsClient:
    def __init__(
        self,
        *,
        user_pages: dict[str | None, dict[str, Any]] | None = None,
        conversation_pages: dict[str | None, dict[str, Any]] | None = None,
        call_pages: dict[tuple[str, str, str | None], dict[str, Any]] | None = None,
    ) -> None:
        self.user_pages = deepcopy(
            user_pages
            or {
                None: {
                    "data": [
                        {
                            "id": "US-lea",
                            "firstName": "Lea",
                            "lastName": "Skoumbakis",
                        },
                        {
                            "id": "US-john",
                            "firstName": "John",
                            "lastName": "Yoon",
                        },
                    ],
                    "nextPageToken": None,
                }
            }
        )
        self.conversation_pages = deepcopy(
            conversation_pages
            or {None: {"data": [], "nextPageToken": None}}
        )
        self.call_pages = deepcopy(call_pages or {})
        self.calls: list[dict[str, Any]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        path = urlsplit(url).path
        request_params = deepcopy(params or {})
        self.calls.append(
            {
                "method": method,
                "path": path,
                "params": request_params,
            }
        )
        page_token = request_params.get("pageToken")
        if method == "GET" and path == "/v1/users":
            return deepcopy(self.user_pages[page_token])
        if method == "GET" and path == "/v1/conversations":
            return deepcopy(self.conversation_pages[page_token])
        if method == "GET" and path == "/v1/calls":
            participants = request_params.get("participants")
            if not isinstance(participants, list) or len(participants) != 1:
                raise AssertionError("Calls request must contain one participant")
            key = (
                str(request_params.get("phoneNumberId")),
                str(participants[0]),
                page_token,
            )
            if key not in self.call_pages:
                raise AssertionError(f"Unexpected calls page {key!r}")
            return deepcopy(self.call_pages[key])
        raise AssertionError(f"Unexpected fake request: {method} {path}")


def call(
    call_id: str,
    created_at: datetime,
    *,
    user_id: str = "US-lea",
    direction: str = "outgoing",
    status: str = "completed",
    duration: int | float | str = 120,
) -> dict[str, Any]:
    return {
        "id": call_id,
        "createdAt": created_at.isoformat(),
        "direction": direction,
        "status": status,
        "duration": duration,
        "userId": user_id,
        "phoneNumberId": "must-not-persist",
        "participants": ["+12025550199"],
    }


def conversation(
    phone_number_id: str,
    *participants: str,
) -> dict[str, Any]:
    return {
        "id": f"CN-{phone_number_id}",
        "phoneNumberId": phone_number_id,
        "participants": list(participants),
        "name": "Must not persist",
    }


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        body: Any,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def json(self) -> Any:
        return deepcopy(self._body)


class SequenceClient:
    def __init__(self, responses: list[Any], clock: FakeClock) -> None:
        self.responses = list(responses)
        self.clock = clock
        self.request_times: list[float] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        del method, url, params
        self.request_times.append(self.clock.monotonic())
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class OpenPhoneAnalyticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            future=True,
        )
        self.db = self.Session()
        self.sleep_patch = patch(
            "app.pipedrive_analytics.time_module.sleep",
            return_value=None,
        )
        self.sleep_patch.start()

    def tearDown(self) -> None:
        self.sleep_patch.stop()
        self.db.close()
        self.engine.dispose()

    def test_refresh_paginates_discovers_pairs_deduplicates_and_minimizes(self) -> None:
        now = datetime(2026, 7, 30, 8, 0, tzinfo=EASTERN)
        participant_one = "+12025550101"
        participant_two = "+12025550102"
        first_call = call(
            "AC-1",
            datetime(2026, 7, 28, 18, 0, tzinfo=EASTERN),
        )
        short_call = call(
            "AC-2",
            datetime(2026, 7, 28, 19, 0, tzinfo=EASTERN),
            duration=89,
        )
        incoming_call = call(
            "AC-3",
            datetime(2026, 7, 28, 20, 0, tzinfo=EASTERN),
            direction="incoming",
        )
        client = FakeOpenPhoneAnalyticsClient(
            user_pages={
                None: {
                    "data": [
                        {
                            "id": "US-lea",
                            "firstName": "Lea",
                            "lastName": "Skoumbakis",
                        }
                    ],
                    "nextPageToken": "users-2",
                },
                "users-2": {
                    "data": [
                        {
                            "id": "US-john",
                            "firstName": "John",
                            "lastName": "Yoon",
                        }
                    ],
                    "nextPageToken": None,
                },
            },
            conversation_pages={
                None: {
                    "data": [
                        conversation("PN-sales", participant_one, participant_two),
                        conversation("PN-sales", participant_one),
                    ],
                    "nextPageToken": "conversations-2",
                },
                "conversations-2": {
                    "data": [conversation("PN-sales", participant_two)],
                    "nextPageToken": None,
                },
            },
            call_pages={
                ("PN-sales", participant_one, None): {
                    "data": [first_call, short_call],
                    "nextPageToken": "calls-2",
                },
                ("PN-sales", participant_one, "calls-2"): {
                    "data": [first_call],
                    "nextPageToken": None,
                },
                ("PN-sales", participant_two, None): {
                    "data": [first_call, incoming_call],
                    "nextPageToken": None,
                },
            },
        )

        payload = refresh_pipedrive_analytics(
            self.db,
            client=client,
            now=now,
            api_token="mock-only-token",
            base_url="https://api.openphone.com/v1",
        )
        snapshot = self.db.scalar(select(PipedriveAnalyticsSnapshot))

        self.assertIsNotNone(snapshot)
        self.assertEqual(payload["report"]["source_count"], 2)
        self.assertEqual(
            payload["kpis"]["total_calls"],
            {"value": 2, "display": "2", "connected": 1},
        )
        self.assertEqual(
            payload["kpis"]["connect_rate"],
            {"value": 50.0, "display": "50%"},
        )
        self.assertEqual(snapshot.source_count, 2)

        users_requests = [
            request for request in client.calls if request["path"] == "/v1/users"
        ]
        self.assertEqual(len(users_requests), 2)
        self.assertEqual(users_requests[0]["params"]["maxResults"], 50)
        self.assertEqual(users_requests[1]["params"]["pageToken"], "users-2")

        conversation_requests = [
            request
            for request in client.calls
            if request["path"] == "/v1/conversations"
        ]
        self.assertEqual(len(conversation_requests), 2)
        self.assertEqual(
            conversation_requests[0]["params"]["updatedAfter"],
            "2026-06-30T03:59:59.999Z",
        )

        call_requests = [
            request for request in client.calls if request["path"] == "/v1/calls"
        ]
        self.assertEqual(len(call_requests), 3)
        self.assertEqual(
            {
                (
                    request["params"]["phoneNumberId"],
                    request["params"]["participants"][0],
                )
                for request in call_requests
            },
            {
                ("PN-sales", participant_one),
                ("PN-sales", participant_two),
            },
        )
        for request in call_requests:
            self.assertEqual(
                request["params"]["createdAfter"],
                "2026-06-30T03:59:59.999Z",
            )
            self.assertEqual(
                request["params"]["createdBefore"],
                "2026-07-30T04:00:00.000Z",
            )
            self.assertEqual(request["params"]["maxResults"], 100)

        stored = snapshot.payload_json
        self.assertNotIn(participant_one, stored)
        self.assertNotIn(participant_two, stored)
        self.assertNotIn("PN-sales", stored)
        self.assertNotIn("Must not persist", stored)
        self.assertNotIn("US-lea", stored)
        self.assertNotIn("US-john", stored)

    def test_exact_outbound_connected_definition_and_completed_day_bounds(self) -> None:
        now = datetime(2026, 7, 30, 20, 0, tzinfo=EASTERN)
        raw_calls = [
            call(
                "at-start",
                datetime(2026, 6, 30, 0, 0, tzinfo=EASTERN),
                duration=90,
            ),
            call(
                "short",
                datetime(2026, 7, 1, 10, 0, tzinfo=EASTERN),
                duration=89,
            ),
            call(
                "wrong-status",
                datetime(2026, 7, 2, 10, 0, tzinfo=EASTERN),
                status="missed",
                duration=300,
            ),
            call(
                "last-moment",
                datetime(2026, 7, 29, 23, 59, 59, tzinfo=EASTERN),
                duration="90",
            ),
            call(
                "today-excluded",
                datetime(2026, 7, 30, 0, 0, tzinfo=EASTERN),
            ),
            call(
                "before-start",
                datetime(2026, 6, 29, 23, 59, 59, tzinfo=EASTERN),
            ),
            call(
                "incoming",
                datetime(2026, 7, 10, 10, 0, tzinfo=EASTERN),
                direction="incoming",
            ),
        ]

        payload = build_payload(
            raw_calls,
            {"US-lea": "Lea Skoumbakis"},
            now=now,
        )

        self.assertEqual(payload["report"]["window_start"], "2026-06-30")
        self.assertEqual(payload["report"]["window_end"], "2026-07-29")
        self.assertEqual(
            payload["report"]["window_end_exclusive"],
            "2026-07-30",
        )
        self.assertEqual(payload["kpis"]["total_calls"]["value"], 4)
        self.assertEqual(payload["kpis"]["total_calls"]["connected"], 2)
        self.assertEqual(payload["kpis"]["connect_rate"]["value"], 50.0)

    def test_required_call_fields_fail_closed(self) -> None:
        now = datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN)
        for field in ("direction", "status", "duration"):
            malformed = call(
                f"missing-{field}",
                datetime(2026, 7, 29, 10, 0, tzinfo=EASTERN),
            )
            malformed.pop(field)
            with self.subTest(field=field):
                with self.assertRaises(PipedriveAnalyticsError):
                    build_payload(
                        [malformed],
                        {"US-lea": "Lea Skoumbakis"},
                        now=now,
                    )

        invalid_direction = call(
            "invalid-direction",
            datetime(2026, 7, 29, 10, 0, tzinfo=EASTERN),
            direction="sideways",
        )
        with self.assertRaisesRegex(
            PipedriveAnalyticsError,
            "invalid direction",
        ):
            build_payload(
                [invalid_direction],
                {"US-lea": "Lea Skoumbakis"},
                now=now,
            )

    def test_outbound_rep_attribution_prefers_initiated_by(self) -> None:
        raw_calls = []
        for index in range(MINIMUM_SAMPLE):
            raw_call = call(
                f"initiator-{index}",
                datetime(2026, 7, 29, 10, index, tzinfo=EASTERN),
                user_id="US-account-owner",
            )
            raw_call["initiatedBy"] = "US-calling-rep"
            raw_calls.append(raw_call)

        payload = build_payload(
            raw_calls,
            {
                "US-account-owner": "Account Owner",
                "US-calling-rep": "Calling Rep",
            },
            now=datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN),
        )

        self.assertEqual(payload["kpis"]["top_rep"]["name"], "Calling Rep")
        self.assertEqual(
            [row["rep"] for row in payload["leaderboard"]["rows"]],
            ["Calling Rep"],
        )

    def test_headline_connect_rate_display_rounds_to_whole_percent(self) -> None:
        raw_calls = [
            call(
                f"rounding-{index}",
                datetime(2026, 7, 29, 10, index, tzinfo=EASTERN),
                status="completed" if index == 0 else "missed",
            )
            for index in range(8)
        ]

        payload = build_payload(
            raw_calls,
            {"US-lea": "Lea Skoumbakis"},
            now=datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN),
        )

        self.assertEqual(payload["kpis"]["connect_rate"]["value"], 12.5)
        self.assertEqual(payload["kpis"]["connect_rate"]["display"], "13%")

    def test_aggregates_exact_30_28_and_7_day_windows(self) -> None:
        now = datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN)
        monday = datetime(2026, 7, 27, 18, 0, tzinfo=EASTERN)
        tuesday = datetime(2026, 7, 28, 18, 0, tzinfo=EASTERN)
        calls = [
            call(
                f"lea-{index}",
                monday + timedelta(minutes=index),
                user_id="US-lea",
                status="completed" if index < 15 else "missed",
                duration=120,
            )
            for index in range(20)
        ]
        calls.extend(
            call(
                f"john-{index}",
                tuesday + timedelta(minutes=index),
                user_id="US-john",
                status="completed" if index < 10 else "missed",
                duration=120,
            )
            for index in range(15)
        )
        calls.append(
            call(
                "thirty-only",
                datetime(2026, 7, 1, 10, 0, tzinfo=EASTERN),
                user_id="US-john",
                status="missed",
            )
        )

        payload = build_payload(
            calls,
            {
                "US-lea": "Lea Skoumbakis",
                "US-john": "John Yoon",
            },
            now=now,
        )

        self.assertEqual(payload["kpis"]["total_calls"]["value"], 36)
        self.assertEqual(payload["kpis"]["total_calls"]["connected"], 25)
        self.assertEqual(
            payload["kpis"]["best_hour"],
            {
                "label": "6pm",
                "connect_rate": 71.4,
                "count": 35,
                "calls": 35,
                "hour": 18,
            },
        )
        self.assertEqual(payload["kpis"]["best_day"]["label"], "Mon")
        self.assertEqual(payload["kpis"]["best_day"]["connect_rate"], 75.0)
        self.assertEqual(
            payload["kpis"]["top_rep"],
            {
                "label": "Lea Skoumbakis",
                "connect_rate": 75.0,
                "count": 20,
                "calls": 20,
                "name": "Lea Skoumbakis",
            },
        )
        self.assertEqual(
            payload["leaderboard"]["days"],
            [
                {"date": "2026-07-27", "label": "Mon 7/27"},
                {"date": "2026-07-28", "label": "Tue 7/28"},
                {"date": "2026-07-29", "label": "Wed 7/29"},
            ],
        )
        lea = payload["leaderboard"]["rows"][0]
        self.assertEqual(lea["rep"], "Lea Skoumbakis")
        self.assertEqual(lea["period_calls"], 20)
        self.assertEqual(lea["period_connected"], 15)
        self.assertEqual(lea["period_connect_rate"], 75.0)
        self.assertEqual(lea["days"][0]["calls"], 20)
        self.assertEqual(lea["days"][0]["connected"], 15)
        self.assertEqual(lea["days"][2]["calls"], 0)
        self.assertEqual(lea["days"][2]["connect_rate"], 0.0)

        monday_blended = payload["weekday_blended"][0]
        self.assertEqual(monday_blended["calls"], 20)
        self.assertEqual(monday_blended["connected"], 15)
        self.assertEqual(monday_blended["connect_rate"], 75.0)
        self.assertEqual(monday_blended["avg_calls_per_day"], 5.0)
        self.assertEqual(payload["hourly"][10]["calls"], 1)
        old_heatmap_cell = next(
            cell
            for cell in payload["heatmap"]
            if cell["hour"] == 10 and cell["day_label"] == "Wed"
        )
        self.assertEqual(old_heatmap_cell["calls"], 0)
        self.assertEqual(len(payload["hourly"]), 24)
        self.assertEqual(len(payload["weekdays"]), 7)
        self.assertEqual(len(payload["heatmap"]), 24 * 7)

    def test_top_rep_and_leaderboard_period_are_rolling_seven_days(self) -> None:
        now = datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN)
        raw_calls = [
            call(
                f"old-lea-{index}",
                datetime(2026, 7, 21, 12, index % 60, tzinfo=EASTERN),
                user_id="US-lea",
            )
            for index in range(30)
        ]
        raw_calls.extend(
            call(
                f"john-{index}",
                datetime(2026, 7, 28, 14, index, tzinfo=EASTERN),
                user_id="US-john",
                status="completed" if index < 9 else "missed",
            )
            for index in range(15)
        )

        payload = build_payload(
            raw_calls,
            {
                "US-lea": "Lea Skoumbakis",
                "US-john": "John Yoon",
            },
            now=now,
        )

        self.assertEqual(payload["kpis"]["top_rep"]["name"], "John Yoon")
        self.assertEqual(payload["kpis"]["top_rep"]["calls"], 15)
        self.assertEqual(payload["kpis"]["top_rep"]["connect_rate"], 60.0)
        self.assertEqual(
            [row["rep"] for row in payload["leaderboard"]["rows"]],
            ["John Yoon"],
        )
        self.assertEqual(
            payload["leaderboard"]["rows"][0]["period_connect_rate"],
            60.0,
        )

    def test_dst_window_uses_local_midnights_and_utc_api_bounds(self) -> None:
        now = datetime(2026, 3, 10, 8, 0, tzinfo=EASTERN)
        participant = "+12025550103"
        client = FakeOpenPhoneAnalyticsClient(
            conversation_pages={
                None: {
                    "data": [conversation("PN-sales", participant)],
                    "nextPageToken": None,
                }
            },
            call_pages={
                ("PN-sales", participant, None): {
                    "data": [],
                    "nextPageToken": None,
                }
            },
        )

        refresh_pipedrive_analytics(
            self.db,
            client=client,
            now=now,
            api_token="mock-only-token",
        )

        conversation_request = next(
            request
            for request in client.calls
            if request["path"] == "/v1/conversations"
        )
        call_request = next(
            request for request in client.calls if request["path"] == "/v1/calls"
        )
        self.assertEqual(
            conversation_request["params"]["updatedAfter"],
            "2026-02-08T04:59:59.999Z",
        )
        self.assertEqual(
            call_request["params"]["createdAfter"],
            "2026-02-08T04:59:59.999Z",
        )
        self.assertEqual(
            call_request["params"]["createdBefore"],
            "2026-03-10T04:00:00.000Z",
        )

    def test_monday_leaderboard_has_no_daily_columns_but_keeps_rolling_rate(self) -> None:
        now = datetime(2026, 8, 3, 10, 0, tzinfo=EASTERN)
        raw_calls = [
            call(
                f"prior-{index}",
                datetime(2026, 7, 31, 14, index, tzinfo=EASTERN),
            )
            for index in range(15)
        ]

        payload = build_payload(
            raw_calls,
            {"US-lea": "Lea Skoumbakis"},
            now=now,
        )

        self.assertEqual(payload["leaderboard"]["days"], [])
        self.assertEqual(len(payload["leaderboard"]["rows"]), 1)
        self.assertEqual(payload["leaderboard"]["rows"][0]["days"], [])
        self.assertEqual(
            payload["leaderboard"]["rows"][0]["period_connect_rate"],
            100.0,
        )

    def test_same_day_legacy_deal_snapshot_is_replaced_by_schema_version(self) -> None:
        legacy = PipedriveAnalyticsSnapshot(
            report_date=date(2026, 7, 30),
            generated_at=datetime(2026, 7, 30, 12, 0, tzinfo=UTC),
            timezone_name="America/New_York",
            source_count=99,
            payload_json='{"report":{"date":"2026-07-30"},"kpis":{"total_deals":99}}',
        )
        self.db.add(legacy)
        self.db.commit()
        original_id = legacy.id
        client = FakeOpenPhoneAnalyticsClient()

        payload = refresh_pipedrive_analytics(
            self.db,
            client=client,
            now=datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN),
            api_token="mock-only-token",
        )
        refreshed = self.db.scalar(select(PipedriveAnalyticsSnapshot))

        self.assertEqual(refreshed.id, original_id)
        self.assertEqual(
            payload["report"]["schema_version"],
            PAYLOAD_SCHEMA_VERSION,
        )
        self.assertEqual(refreshed.source_count, 0)
        self.assertNotIn("total_deals", refreshed.payload_json)
        self.assertEqual(
            self.db.scalar(select(func.count(PipedriveAnalyticsSnapshot.id))),
            1,
        )

    def test_current_snapshot_is_reused_and_latest_ignores_legacy_schema(self) -> None:
        client = FakeOpenPhoneAnalyticsClient()
        first = refresh_pipedrive_analytics(
            self.db,
            client=client,
            now=datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN),
            api_token="mock-only-token",
        )
        request_count = len(client.calls)
        second = refresh_pipedrive_analytics(
            self.db,
            client=client,
            now=datetime(2026, 7, 30, 15, 0, tzinfo=EASTERN),
            api_token="mock-only-token",
        )

        self.assertEqual(second, first)
        self.assertEqual(len(client.calls), request_count)
        self.assertEqual(get_latest_pipedrive_analytics(self.db), first)

        snapshot = self.db.scalar(select(PipedriveAnalyticsSnapshot))
        snapshot.payload_json = '{"report":{"date":"2026-07-30"}}'
        self.db.commit()
        self.assertIsNone(get_latest_pipedrive_analytics(self.db))

    def test_if_due_refreshes_at_each_daily_slot(self) -> None:
        client = FakeOpenPhoneAnalyticsClient()
        before_due = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 1, 8, 59, tzinfo=EASTERN),
            refresh_hours=(9, 13, 17),
            api_token="mock-only-token",
        )
        self.assertIsNone(before_due)
        self.assertEqual(client.calls, [])

        morning = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 1, 9, 0, tzinfo=EASTERN),
            refresh_hours=(9, 13, 17),
            api_token="mock-only-token",
        )
        morning_request_count = len(client.calls)
        after_morning = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 1, 12, 59, tzinfo=EASTERN),
            refresh_hours=(9, 13, 17),
            api_token="mock-only-token",
        )
        afternoon = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 1, 13, 0, tzinfo=EASTERN),
            refresh_hours=(9, 13, 17),
            api_token="mock-only-token",
        )
        afternoon_request_count = len(client.calls)
        before_evening = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 1, 16, 59, tzinfo=EASTERN),
            refresh_hours=(9, 13, 17),
            api_token="mock-only-token",
        )
        evening = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 1, 17, 0, tzinfo=EASTERN),
            refresh_hours=(9, 13, 17),
            api_token="mock-only-token",
        )
        evening_request_count = len(client.calls)
        after_evening = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 2, 8, 59, tzinfo=EASTERN),
            refresh_hours=(9, 13, 17),
            api_token="mock-only-token",
        )
        next_morning = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 2, 9, 0, tzinfo=EASTERN),
            refresh_hours=(9, 13, 17),
            api_token="mock-only-token",
        )
        missing_token = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 2, 13, 0, tzinfo=EASTERN),
            refresh_hours=(9, 13, 17),
            api_token="",
        )

        self.assertIsNotNone(morning)
        self.assertIsNone(after_morning)
        self.assertIsNotNone(afternoon)
        self.assertIsNone(before_evening)
        self.assertIsNotNone(evening)
        self.assertIsNone(after_evening)
        self.assertIsNotNone(next_morning)
        self.assertIsNone(missing_token)
        self.assertGreater(afternoon_request_count, morning_request_count)
        self.assertGreater(evening_request_count, afternoon_request_count)
        self.assertGreater(len(client.calls), evening_request_count)
        self.assertEqual(
            self.db.scalar(select(func.count(PipedriveAnalyticsSnapshot.id))),
            2,
        )
        self.assertEqual(
            evening["report"]["generated_at"],
            "2026-08-01T17:00:00-04:00",
        )

    def test_daily_refresh_catches_up_after_missed_slot(self) -> None:
        client = FakeOpenPhoneAnalyticsClient()

        catch_up = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 3, 14, 0, tzinfo=EASTERN),
            refresh_hours="9,13,17",
            api_token="mock-only-token",
        )
        before_next_slot = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 3, 16, 59, tzinfo=EASTERN),
            refresh_hours="9,13,17",
            api_token="mock-only-token",
        )
        evening = refresh_pipedrive_analytics_if_due(
            self.db,
            client=client,
            now=datetime(2026, 8, 3, 17, 1, tzinfo=EASTERN),
            refresh_hours="9,13,17",
            api_token="mock-only-token",
        )

        self.assertIsNotNone(catch_up)
        self.assertEqual(catch_up["report"]["date"], "2026-08-03")
        self.assertIsNone(before_next_slot)
        self.assertIsNotNone(evening)

    def test_sparse_buckets_require_fifteen_for_kpis_not_charts(self) -> None:
        raw_calls = [
            call(
                f"sparse-{index}",
                datetime(2026, 7, 28, 11, index, tzinfo=EASTERN),
            )
            for index in range(MINIMUM_SAMPLE - 1)
        ]

        payload = build_payload(
            raw_calls,
            {"US-lea": "Lea Skoumbakis"},
            now=datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN),
        )

        for key in ("best_hour", "best_day", "top_rep"):
            self.assertIsNone(payload["kpis"][key]["label"])
            self.assertIsNone(payload["kpis"][key]["connect_rate"])
            self.assertEqual(payload["kpis"][key]["calls"], 0)
        self.assertEqual(payload["hourly"][11]["connect_rate"], 100.0)
        self.assertIsNone(payload["hourly"][0]["connect_rate"])

    def test_rate_limit_and_retry_after_are_applied_to_every_attempt(self) -> None:
        clock = FakeClock()
        client = SequenceClient(
            [
                FakeResponse(
                    429,
                    {"message": "slow down"},
                    headers={"Retry-After": "2"},
                ),
                FakeResponse(200, {"data": [], "nextPageToken": None}),
                FakeResponse(200, {"data": [], "nextPageToken": None}),
            ],
            clock,
        )
        api = _OpenPhoneReadApi(
            api_token="mock-only-token",
            base_url="https://api.openphone.com",
            client=client,
            sleep_fn=clock.sleep,
            monotonic_fn=clock.monotonic,
        )

        api.get("/v1/users")
        api.get("/v1/conversations")

        self.assertEqual(len(client.request_times), 3)
        self.assertGreaterEqual(
            client.request_times[1] - client.request_times[0],
            2.0,
        )
        self.assertGreaterEqual(
            client.request_times[2] - client.request_times[1],
            0.125,
        )
        self.assertIn(2.0, clock.sleeps)

    def test_retry_failure_never_exposes_token(self) -> None:
        clock = FakeClock()
        token = "mock-secret-that-must-not-appear"
        participant = "+12025550199"
        client = SequenceClient(
            [
                RuntimeError(f"{token} participant={participant}")
                for _ in range(4)
            ],
            clock,
        )
        api = _OpenPhoneReadApi(
            api_token=token,
            base_url="https://api.openphone.com",
            client=client,
            sleep_fn=clock.sleep,
            monotonic_fn=clock.monotonic,
        )

        with self.assertRaises(PipedriveAnalyticsError) as raised:
            api.get("/v1/users")

        self.assertNotIn(token, str(raised.exception))
        rendered_traceback = "".join(
            traceback.format_exception(raised.exception)
        )
        self.assertNotIn(token, rendered_traceback)
        self.assertNotIn(participant, rendered_traceback)
        self.assertEqual(len(client.request_times), 4)

    def test_malformed_response_fails_without_persisting_snapshot(self) -> None:
        client = FakeOpenPhoneAnalyticsClient(
            conversation_pages={
                None: {
                    "data": {"not": "a list"},
                    "nextPageToken": None,
                }
            }
        )

        with self.assertRaisesRegex(
            PipedriveAnalyticsError,
            "conversations response is missing a data list",
        ):
            refresh_pipedrive_analytics(
                self.db,
                client=client,
                now=datetime(2026, 7, 30, 10, 0, tzinfo=EASTERN),
                api_token="mock-only-token",
            )

        self.assertEqual(
            self.db.scalar(select(func.count(PipedriveAnalyticsSnapshot.id))),
            0,
        )


if __name__ == "__main__":
    unittest.main()
