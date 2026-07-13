from __future__ import annotations

import unittest

import httpx

from app.notion_trade_shows import (
    NotionTradeShowClient,
    NotionTradeShowError,
    candidate_from_notion_page,
    fetch_notion_trade_shows,
    normalize_notion_id,
)


def notion_page(
    *,
    page_id: str,
    name: str,
    start: str,
    end: str | None = None,
    place: str = "",
    url: str = "",
    registered: bool = False,
) -> dict[str, object]:
    return {
        "id": page_id,
        "url": f"https://www.notion.so/{page_id.replace('-', '')}",
        "properties": {
            "Tradeshow Name": {
                "id": "title",
                "type": "title",
                "title": [{"type": "text", "plain_text": name, "text": {"content": name}}],
            },
            "Event Date": {
                "id": "date",
                "type": "date",
                "date": {"start": start, "end": end, "time_zone": None},
            },
            "Place": {
                "id": "place",
                "type": "rich_text",
                "rich_text": [{"type": "text", "plain_text": place}],
            },
            "URL": {"id": "url", "type": "url", "url": url or None},
            "Registered": {"id": "registered", "type": "checkbox", "checkbox": registered},
        },
    }


class NotionTradeShowTests(unittest.TestCase):
    def test_fetches_all_pages_and_maps_tracker_fields(self) -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.method == "GET":
                return httpx.Response(
                    200,
                    json={
                        "object": "database",
                        "id": "35612747-7edb-804d-89e7-c406ad08975b",
                        "data_sources": [
                            {"id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Trade shows"}
                        ],
                    },
                )
            body = __import__("json").loads(request.content)
            if "start_cursor" not in body:
                return httpx.Response(
                    200,
                    json={
                        "results": [
                            notion_page(
                                page_id="11111111-1111-1111-1111-111111111111",
                                name="Fancy Food Show",
                                start="2026-06-28",
                                end="2026-06-30",
                                place="New York, NY",
                                url="https://example.com/exhibitors",
                                registered=True,
                            )
                        ],
                        "has_more": True,
                        "next_cursor": "cursor-2",
                    },
                )
            self.assertEqual(body["start_cursor"], "cursor-2")
            return httpx.Response(
                200,
                json={
                    "results": [
                        notion_page(
                            page_id="22222222-2222-2222-2222-222222222222",
                            name="Regional Hardware Expo",
                            start="2026-07-02T09:00:00-04:00",
                            place="Orlando, FL",
                            url="https://regional.example.org/directory",
                        )
                    ],
                    "has_more": False,
                    "next_cursor": None,
                },
            )

        with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
            candidates = fetch_notion_trade_shows(
                token="secret_token",
                database_id="35612747-7edb-804d-89e7-c406ad08975b",
                http_client=http_client,
            )

        self.assertEqual(len(candidates), 2)
        self.assertEqual(candidates[0].show_name, "Fancy Food Show")
        self.assertEqual(candidates[0].event_date.isoformat(), "2026-06-28")
        self.assertEqual(candidates[0].event_end_date.isoformat() if candidates[0].event_end_date else "", "2026-06-30")
        self.assertEqual(candidates[0].event_date_raw, "2026-06-28")
        self.assertEqual(candidates[0].place, "New York, NY")
        self.assertEqual(candidates[0].link, "https://example.com/exhibitors")
        self.assertTrue(candidates[0].registered)
        self.assertEqual(candidates[1].event_date.isoformat(), "2026-07-02")
        self.assertEqual(len(requests), 3)
        self.assertEqual(requests[0].headers["authorization"], "Bearer secret_token")
        self.assertEqual(requests[0].headers["notion-version"], "2025-09-03")
        self.assertTrue(requests[0].url.path.endswith("/databases/356127477edb804d89e7c406ad08975b"))
        self.assertTrue(requests[1].url.path.endswith("/data_sources/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/query"))

    def test_resolves_known_curated_directory_without_searching(self) -> None:
        page = notion_page(
            page_id="33333333-3333-3333-3333-333333333333",
            name="Vegas Market",
            start="2026-07-27",
            place="Las Vegas, NV",
            url="https://www.lasvegasmarket.com/",
        )

        candidate = candidate_from_notion_page(page)

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.link, "https://www.lasvegasmarket.com/en/exhibitor/exhibitor-directory")

    def test_skips_incomplete_rows_and_reads_common_property_variants(self) -> None:
        incomplete = notion_page(
            page_id="44444444-4444-4444-4444-444444444444",
            name="",
            start="2026-08-01",
        )
        self.assertIsNone(candidate_from_notion_page(incomplete))

        page = notion_page(
            page_id="55555555-5555-5555-5555-555555555555",
            name="Design Expo",
            start="2026-08-03",
        )
        properties = page["properties"]
        assert isinstance(properties, dict)
        properties["place"] = {"type": "select", "select": {"name": "Chicago, IL"}}
        del properties["Place"]
        properties["Registered"] = {"type": "status", "status": {"name": "Registered"}}

        candidate = candidate_from_notion_page(page)

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.place, "Chicago, IL")
        self.assertTrue(candidate.registered)

    def test_surfaces_api_error_without_exposing_token(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(429, json={"code": "rate_limited", "message": "Slow down"})

        with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
            client = NotionTradeShowClient(
                token="do-not-leak",
                database_id="356127477edb804d89e7c406ad08975b",
                http_client=http_client,
            )
            with self.assertRaises(NotionTradeShowError) as raised:
                client.fetch_candidates()

        self.assertEqual(raised.exception.status_code, 429)
        self.assertEqual(raised.exception.error_code, "rate_limited")
        self.assertIn("Slow down", str(raised.exception))
        self.assertNotIn("do-not-leak", str(raised.exception))

    def test_rejects_broken_pagination_and_supports_data_source_endpoint(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertIn("/data_sources/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/query", request.url.path)
            return httpx.Response(200, json={"results": [], "has_more": True, "next_cursor": None})

        with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
            client = NotionTradeShowClient(
                token="secret",
                database_id="356127477edb804d89e7c406ad08975b",
                data_source_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                http_client=http_client,
            )
            with self.assertRaisesRegex(NotionTradeShowError, "next cursor"):
                client.fetch_candidates()

    def test_normalizes_ids_from_uuid_and_url(self) -> None:
        self.assertEqual(
            normalize_notion_id("35612747-7edb-804d-89e7-c406ad08975b"),
            "356127477edb804d89e7c406ad08975b",
        )
        self.assertEqual(
            normalize_notion_id("https://app.notion.com/p/conduitcommerce/356127477edb804d89e7c406ad08975b?v=view"),
            "356127477edb804d89e7c406ad08975b",
        )


if __name__ == "__main__":
    unittest.main()
