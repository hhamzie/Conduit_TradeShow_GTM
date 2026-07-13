from __future__ import annotations

from datetime import date
import unittest

import httpx

from app.trade_show_verification import official_page_url_for_show, verify_trade_show_date


class TradeShowVerificationTests(unittest.TestCase):
    def test_curated_show_uses_exact_official_dates_page(self) -> None:
        self.assertEqual(
            official_page_url_for_show("Vegas Market", "https://example.com/directory"),
            "https://www.lasvegasmarket.com/en/Visit/Market-Dates-and-Hours",
        )

    def test_official_date_is_authoritative_on_one_day_mismatch(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(
                str(request.url),
                "https://www.lasvegasmarket.com/en/Visit/Market-Dates-and-Hours",
            )
            return httpx.Response(200, text="<main>Summer Market July 26–30, 2026</main>")

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            result = verify_trade_show_date(
                show_name="Vegas Market",
                tracker_start_date=date(2026, 7, 27),
                fallback_url="https://www.lasvegasmarket.com/en/exhibitor/exhibitor-directory",
                http_client=client,
            )

        self.assertEqual(result.status, "mismatch")
        self.assertEqual(result.official_start_date, date(2026, 7, 26))
        self.assertEqual(result.effective_start_date, date(2026, 7, 26))

    def test_failed_official_fetch_keeps_tracker_date(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, text="unavailable")

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            result = verify_trade_show_date(
                show_name="Unknown Hardware Expo",
                tracker_start_date=date(2026, 9, 10),
                fallback_url="https://expo.example.com/dates",
                http_client=client,
            )

        self.assertEqual(result.status, "unverified")
        self.assertIsNone(result.official_start_date)
        self.assertEqual(result.effective_start_date, date(2026, 9, 10))

    def test_unrelated_dates_do_not_override_tracker(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text="January 3–4, 2024")

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            result = verify_trade_show_date(
                show_name="Unknown Hardware Expo",
                tracker_start_date=date(2026, 9, 10),
                fallback_url="https://expo.example.com/dates",
                http_client=client,
            )

        self.assertEqual(result.status, "unverified")
        self.assertEqual(result.effective_start_date, date(2026, 9, 10))


if __name__ == "__main__":
    unittest.main()
