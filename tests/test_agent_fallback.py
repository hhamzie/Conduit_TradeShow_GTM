from __future__ import annotations

import unittest
from unittest.mock import patch

from scraper import (
    AgentDirectoryPlan,
    AgentProfileLink,
    AnchorRecord,
    ParsedPage,
    collect_entries_with_agent_fallback,
    extract_text_from_openai_response,
)


def make_anchor(text: str, href: str, absolute_url: str, order: int) -> AnchorRecord:
    return AnchorRecord(
        text=text,
        href=href,
        absolute_url=absolute_url,
        signature=("div.directory", "a.card"),
        order=order,
        in_header=False,
        in_footer=False,
        in_nav=False,
        in_main=True,
        title_attr="",
        aria_label="",
    )


def make_page(url: str, anchors: list[AnchorRecord]) -> ParsedPage:
    return ParsedPage(
        url=url,
        anchors=tuple(anchors),
        title="Test Directory",
        h1_texts=("Test Directory",),
        json_ld_blocks=(),
        actions=(),
        containers=(),
        images=(),
    )


class AgentFallbackTests(unittest.TestCase):
    def test_extract_text_from_openai_response_reads_output_blocks(self) -> None:
        payload = {
            "output": [
                {
                    "content": [
                        {"type": "output_text", "text": "{\"ok\":true}"},
                    ]
                }
            ]
        }

        text = extract_text_from_openai_response(payload)

        self.assertEqual(text, "{\"ok\":true}")

    def test_collect_entries_with_agent_fallback_follows_next_page(self) -> None:
        seed_url = "https://example.com/exhibitors?page=1"
        seed_page = make_page(
            seed_url,
            [
                make_anchor("Acme", "/company/acme", "https://example.com/company/acme", 1),
                make_anchor("Next", "?page=2", "https://example.com/exhibitors?page=2", 2),
            ],
        )
        page_two = make_page(
            "https://example.com/exhibitors?page=2",
            [
                make_anchor("Beta", "/company/beta", "https://example.com/company/beta", 1),
            ],
        )

        plans = [
            AgentDirectoryPlan(
                page_kind="directory",
                directory_url="",
                next_page_url="https://example.com/exhibitors?page=2",
                conference_name="Fancy Expo",
                profile_links=(AgentProfileLink(href="/company/acme", company_name="Acme"),),
            ),
            AgentDirectoryPlan(
                page_kind="directory",
                directory_url="",
                next_page_url="",
                conference_name="Fancy Expo",
                profile_links=(AgentProfileLink(href="/company/beta", company_name="Beta"),),
            ),
        ]

        def fake_loader(url: str):
            if url.endswith("page=2"):
                return url, "<html></html>", page_two
            return url, "<html></html>", seed_page

        with patch("scraper.request_agent_directory_plan", side_effect=plans):
            entries, adapter_title = collect_entries_with_agent_fallback(
                seed_url=seed_url,
                seed_page=seed_page,
                max_pages=5,
                page_loader=fake_loader,
                model="gpt-4.1-mini",
            )

        self.assertEqual(adapter_title, "Fancy Expo")
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].company_name, "Acme")
        self.assertEqual(entries[1].company_name, "Beta")
        self.assertEqual(entries[1].directory_page, 2)


if __name__ == "__main__":
    unittest.main()
