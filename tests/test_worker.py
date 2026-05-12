from __future__ import annotations

import unittest
from unittest.mock import patch

from app.worker import run_worker_loop


class _FakeSessionContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class WorkerTests(unittest.TestCase):
    def test_worker_loop_keeps_running_after_iteration_exception(self) -> None:
        sleep_calls = {"count": 0}

        def fake_sleep(_seconds: int) -> None:
            sleep_calls["count"] += 1
            if sleep_calls["count"] >= 2:
                raise StopIteration

        with (
            patch("app.worker.init_db"),
            patch("app.worker.get_settings") as settings_mock,
            patch("app.worker.SessionLocal", side_effect=lambda: _FakeSessionContext()),
            patch("app.worker.run_weekly_show_sync", side_effect=[RuntimeError("boom"), None]),
            patch("app.worker.queue_due_shows", return_value=0) as queue_mock,
            patch("app.worker.backfill_queued_runs", return_value=0),
            patch("app.worker.run_next_campaign", return_value=None),
            patch("app.worker.sync_approved_shows", return_value=0),
            patch("app.worker.time.sleep", side_effect=fake_sleep),
        ):
            settings_mock.return_value.worker_poll_seconds = 1
            with self.assertRaises(StopIteration):
                run_worker_loop()

        self.assertEqual(queue_mock.call_count, 1)


if __name__ == "__main__":
    unittest.main()
