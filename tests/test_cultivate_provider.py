from __future__ import annotations

from datetime import date, datetime
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.config import get_settings
from app.models import Show, ShowStatus
from app.providers import SmartleadSyncResult, push_to_cultivate


class CultivateProviderTests(unittest.TestCase):
    def test_push_to_cultivate_sends_scraper_contacts_and_enables_smartlead(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            export_path = Path(temp_dir) / "show.csv"
            export_path.write_text(
                "company_name,website_url,general_contact_email,general_contact_phone,contact_name,contact_email\n"
                "Acme,https://acme.com,info@acme.com,800-555-0100,Ada Lovelace,ada@acme.com\n",
                encoding="utf-8",
            )
            export_path.with_name("show_contacts.csv").write_text(
                "company_name,person_name,job_title,email,phone,source_url\n"
                "Acme,Ada Lovelace,VP Sales,ada@acme.com,212-555-0101,https://example.com/acme\n"
                "Acme,Grace Hopper,Trade Show Manager,grace@acme.com,212-555-0102,https://example.com/acme\n",
                encoding="utf-8",
            )
            show = Show(
                id=7,
                name="Luxe Pack",
                event_date=date(2026, 5, 6),
                place="New York City, NY",
                source_url="https://example.com/exhibitors",
                run_offset_days=14,
                run_at=datetime(2026, 5, 1, 9, 0),
                status=ShowStatus.ready_for_review.value,
                latest_export_path=str(export_path),
            )

            with patch.dict(
                os.environ,
                {
                    "CULTIVATE_WEBHOOK_URL": "https://example.test/webhook/cultivate",
                    "CULTIVATE_ENABLE_SMARTLEAD": "true",
                    "SMARTLEAD_TEMPLATE_CAMPAIGN_ID": "999",
                },
                clear=False,
            ):
                get_settings.cache_clear()
                with (
                    patch(
                        "app.providers.ensure_smartlead_campaign",
                        return_value=SmartleadSyncResult(
                            "smartlead",
                            "success",
                            "Using show campaign.",
                            campaign_id=654,
                            campaign_name="Luxe Pack - May 6th 2026",
                        ),
                    ),
                    patch("app.providers._request_json", return_value=(200, {"ok": True})) as request_mock,
                ):
                    result = push_to_cultivate(show)
                get_settings.cache_clear()

            self.assertEqual(result.status, "success")
            payload = request_mock.call_args.kwargs["payload"]
            self.assertTrue(payload["enableSmartlead"])
            self.assertEqual(payload["smartleadCampaignId"], "654")
            self.assertEqual(payload["smartleadCampaignName"], "Luxe Pack - May 6th 2026")
            self.assertRegex(payload["cadenceEnrollmentDate"], r"^\d{4}-\d{2}-\d{2}$")
            self.assertEqual(payload["rows"][0]["general_contact_email"], "info@acme.com")
            self.assertEqual(payload["rows"][0]["contact_name"], "Ada Lovelace")
            self.assertEqual(len(payload["rows"][0]["scraped_contacts"]), 2)
            self.assertTrue(payload["rows"][0]["source_row_id"].startswith("show-"))


if __name__ == "__main__":
    unittest.main()
