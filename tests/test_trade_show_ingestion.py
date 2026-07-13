from __future__ import annotations

import csv
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.trade_show_ingestion import (
    EventMapping,
    LeadSheetRow,
    _fetch_clay_table_raw_rows,
    export_completed_clay_webhook_payload_to_csv,
    import_completed_clay_webhook_payload,
    import_rows_to_pipedrive,
    infer_rep,
    normalize_sheet,
    row_to_payload,
    sales_leader_contact_for_row,
)


class FakePipedriveClient:
    def __init__(self) -> None:
        self.org_created = False
        self.person_created = False
        self.deal_created = False
        self.updated_orgs = 0
        self.updated_people = 0
        self.updated_deals = 0
        self.created_notes = 0
        self.deal_events = []
        self.ensure_tradeshow_option_calls = []
        self.next_tradeshow_option_id = 777

    def upsert_organization(self, row, *, dry_run: bool):
        self.updated_orgs += 1
        return 43170, False

    def upsert_person(self, row, org_id, *, dry_run: bool):
        self.updated_people += 1
        return 22540, False

    def upsert_deal(self, row, event, org_id, person_id, *, dry_run: bool):
        self.updated_deals += 1
        self.deal_events.append(event)
        return 26665, False

    def upsert_sales_leader(self, row, org_id, *, dry_run: bool):
        if not row.sales_leader_name:
            return None, False
        self.updated_people += 1
        return 22541, False

    def ensure_tradeshow_option(self, event_name: str, *, dry_run: bool):
        self.ensure_tradeshow_option_calls.append((event_name, dry_run))
        return self.next_tradeshow_option_id

    def create_import_note(self, row, *, deal_id, org_id, primary_person_id, sales_leader_person_id, dry_run: bool):
        self.created_notes += 1
        return True


class TradeShowIngestionTests(unittest.TestCase):
    def test_infers_lea_from_filename(self) -> None:
        rep = infer_rep(Path("Lea_InfoComm_leads.csv"))
        self.assertEqual(rep.slug, "lea")
        self.assertEqual(rep.pipedrive_user_id, 25200571)

    def test_infers_active_reps_from_filename(self) -> None:
        expected = {
            "anand_lightning_show.csv": ("anand", 22329483),
            "austin_lightning_show.csv": ("austin", 25188570),
            "gavin_lightning_show.csv": ("gavin", 25897289),
            "hudson_lightning_show.csv": ("hudson", 23584737),
            "hunter_lightning_show.csv": ("hunter", 24521508),
            "john_lightning_show.csv": ("john", 25232735),
            "lea_lightning_show.csv": ("lea", 25200571),
            "noah_lightning_show.csv": ("noah", 24079506),
        }
        for filename, (slug, pipedrive_user_id) in expected.items():
            with self.subTest(filename=filename):
                rep = infer_rep(Path(filename))
                self.assertEqual(rep.slug, slug)
                self.assertEqual(rep.pipedrive_user_id, pipedrive_user_id)

    def test_normalizes_infocomm_csv_and_clay_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "lea_infocomm.csv"
            with path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=["company_name", "booth_number", "website_url", "Location", "Conference"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "company_name": "PoEWit",
                        "booth_number": "N123",
                        "website_url": "https://www.poewit.com",
                        "Location": "Orlando, FL",
                        "Conference": "InfoComm 2026",
                    }
                )

            parsed = normalize_sheet(path)
            self.assertEqual(parsed.rep.slug, "lea")
            self.assertEqual(parsed.event.slug if parsed.event else "", "infocomm")
            self.assertEqual(parsed.rows[0].domain, "poewit.com")

            payload = row_to_payload(parsed.rows[0])
            self.assertEqual(payload["company_name"], "PoEWit")
            self.assertEqual(payload["rep_name"], "Lea Skoumbakis")
            self.assertEqual(payload["booth_number"], "N123")

    def test_normalizes_dallas_market_filename(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "lea_dallasmarket.csv"
            with path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["company_name", "website_url"])
                writer.writeheader()
                writer.writerow({"company_name": "Modern Lighting Co", "website_url": "https://example.com"})

            parsed = normalize_sheet(path)

        self.assertEqual(parsed.rep.slug, "lea")
        self.assertEqual(parsed.event.slug if parsed.event else "", "dallasmarket")
        self.assertEqual(parsed.event.pipedrive_tradeshow_option_id if parsed.event else None, 319)
        self.assertEqual(parsed.rows[0].conference, "Dallas Market")

    def test_normalizes_dynamic_market_filename(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "lea_xyz_market.csv"
            with path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["company_name", "website_url"])
                writer.writeheader()
                writer.writerow({"company_name": "Modern Lighting Co", "website_url": "https://example.com"})

            parsed = normalize_sheet(path)

        self.assertEqual(parsed.rep.slug, "lea")
        self.assertEqual(parsed.event.slug if parsed.event else "", "xyz_market")
        self.assertEqual(parsed.event.name if parsed.event else "", "Xyz Market")
        self.assertEqual(parsed.event.pipedrive_tradeshow_option_id if parsed.event else None, 0)
        self.assertEqual(parsed.rows[0].conference, "Xyz Market")

    def test_normalizes_camel_case_event_filename(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "Gavin_FancyFood_Show.csv"
            with path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["Company", "Contact"])
                writer.writeheader()
                writer.writerow({"Company": "Gelato Festival", "Contact": "Gabriel Pulley"})

            parsed = normalize_sheet(path)

        self.assertEqual(parsed.rep.slug, "gavin")
        self.assertEqual(parsed.event.slug if parsed.event else "", "fancy_food_show")
        self.assertEqual(parsed.event.name if parsed.event else "", "Fancy Food Show")
        self.assertEqual(parsed.rows[0].conference, "Fancy Food Show")

    def test_normalizes_austin_build_show_csv_headers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "austin_build_show.csv"
            with path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=[
                        "POC",
                        "Company",
                        "Title",
                        "Email",
                        "Phone",
                        "Website",
                        "Source Channel ID",
                        "Tradeshow name (for emails)",
                        "Notes",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "POC": "Mitchell Vinsant",
                        "Company": "Aqua Clear Water Systems",
                        "Title": "Owner / operator",
                        "Email": "mitchellvinsant@aquaclearws.com",
                        "Phone": "(865) 986-4234",
                        "Website": "AquaClearWS.com",
                        "Source Channel ID": "Nashville Build Expo 2026",
                        "Tradeshow name (for emails)": "Nashville Build Expo 2026",
                        "Notes": "Strong demo target.",
                    }
                )

            parsed = normalize_sheet(path)

        self.assertEqual(parsed.rep.slug, "austin")
        self.assertEqual(parsed.event.slug if parsed.event else "", "nashville_build_expo")
        self.assertEqual(parsed.event.name if parsed.event else "", "Nashville Build Expo")
        self.assertEqual(parsed.rows[0].person_name, "Mitchell Vinsant")
        self.assertEqual(parsed.rows[0].conference, "Nashville Build Expo 2026")
        self.assertEqual(parsed.rows[0].domain, "aquaclearws.com")

    def test_dynamic_event_creates_pipedrive_tradeshow_option_on_real_import(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "lea_xyz_market_enriched.csv"
            with path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=["company_name", "person_name", "email", "Source File", "Conference"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "company_name": "Modern Lighting Co",
                        "person_name": "Alex Buyer",
                        "email": "alex@example.com",
                        "Source File": "lea_xyz_market.xlsx",
                        "Conference": "Xyz Market 2026",
                    }
                )
            parsed = normalize_sheet(path)

        client = FakePipedriveClient()
        summary = import_rows_to_pipedrive(parsed.rows, parsed.event, dry_run=False, client=client)

        self.assertEqual(client.ensure_tradeshow_option_calls, [("Xyz Market", False)])
        self.assertEqual(client.deal_events[0].pipedrive_tradeshow_option_id, 777)
        self.assertEqual(summary.imported_to_pipedrive, 1)

    def test_import_uses_row_conference_as_pipedrive_channel_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "lea_future_market_enriched.csv"
            with path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=["company_name", "person_name", "email", "Conference"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "company_name": "Future Goods Co",
                        "person_name": "Alex Buyer",
                        "email": "alex@example.com",
                        "Conference": "Future Market 2027",
                    }
                )
            parsed = normalize_sheet(path)

        client = FakePipedriveClient()
        summary = import_rows_to_pipedrive(parsed.rows, parsed.event, dry_run=False, client=client)

        self.assertEqual(client.deal_events[0].pipedrive_channel_id, "Future Market 2027")
        self.assertEqual(summary.imported_to_pipedrive, 1)

    def test_import_updates_existing_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "lea_infocomm_enriched.csv"
            with path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=["company_name", "person_name", "email", "phone", "website", "Conference"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "company_name": "PoEWit",
                        "person_name": "Lindsay Miller",
                        "email": "lindsay@poewit.com",
                        "phone": "+14157236540",
                        "website": "https://poewit.com",
                        "Conference": "InfoComm 2026",
                    }
                )
            parsed = normalize_sheet(path)

        client = FakePipedriveClient()
        event = EventMapping(
            slug="infocomm",
            name="InfoComm Las Vegas",
            filename_terms=("infocomm",),
            pipedrive_pipeline_id=39,
            pipedrive_stage_id=377,
            pipedrive_channel_id="InfoComm Las Vegas 2026",
            pipedrive_tradeshow_option_id=310,
            pipedrive_industry_option_id=311,
        )
        summary = import_rows_to_pipedrive(parsed.rows, event, dry_run=True, client=client)

        self.assertEqual(summary.imported_to_pipedrive, 1)
        self.assertEqual(summary.updated_orgs, 1)
        self.assertEqual(summary.updated_people, 1)
        self.assertEqual(summary.updated_deals, 1)
        self.assertEqual(summary.created_orgs, 0)
        self.assertEqual(summary.created_people, 0)
        self.assertEqual(summary.created_deals, 0)
        self.assertEqual(summary.created_notes, 1)

    def test_import_treats_sales_leader_as_second_person(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "clay_export.csv"
            with path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=[
                        "Company Name",
                        "Person Name",
                        "Email",
                        "LinkedIn Profile URL",
                        "Sales Leader Namae",
                        "Sales Leader LinkedIn",
                        "Email - Data",
                        "Mobile Number",
                        "Rep Pipedrive User Id",
                        "Source File",
                        "Conference",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "Company Name": "PoEWit",
                        "Person Name": "Lindsay Miller",
                        "Email": "lindsay@poewit.com",
                        "LinkedIn Profile URL": "https://linkedin.com/in/lindsay",
                        "Sales Leader Namae": "Alex Leader",
                        "Sales Leader LinkedIn": "https://linkedin.com/in/alexleader",
                        "Email - Data": "alex@poewit.com",
                        "Mobile Number": "+15551234567",
                        "Rep Pipedrive User Id": "25200571",
                        "Source File": "lea_infocomm.xlsx",
                        "Conference": "InfoComm 2026",
                    }
                )
            parsed = normalize_sheet(path)

        client = FakePipedriveClient()
        event = EventMapping(
            slug="infocomm",
            name="InfoComm Las Vegas",
            filename_terms=("infocomm",),
            pipedrive_pipeline_id=39,
            pipedrive_stage_id=377,
            pipedrive_channel_id="InfoComm Las Vegas 2026",
            pipedrive_tradeshow_option_id=310,
            pipedrive_industry_option_id=311,
        )
        summary = import_rows_to_pipedrive(parsed.rows, event, dry_run=True, client=client)

        self.assertEqual(parsed.rows[0].sales_leader_name, "Alex Leader")
        self.assertEqual(parsed.rows[0].sales_leader_linkedin_url, "https://linkedin.com/in/alexleader")
        self.assertEqual(summary.imported_to_pipedrive, 1)
        self.assertEqual(summary.updated_people, 2)
        self.assertEqual(summary.created_notes, 1)

    def test_completed_clay_webhook_payload_imports_once(self) -> None:
        payload = {
            "id": "clay-row-123",
            "row": {
                "Company Name": "PoEWit",
                "Person Name": "Lindsay Miller",
                "Email": "lindsay@poewit.com",
                "LinkedIn Profile URL": "https://linkedin.com/in/lindsay",
                "Sales Leader Namae": "Alex Leader",
                "Sales Leader LinkedIn": "https://linkedin.com/in/alexleader",
                "Email - Data": "alex@poewit.com",
                "Rep Pipedrive User Id": "25200571",
                "Source File": "lea_infocomm.xlsx",
                "Conference": "InfoComm 2026",
            },
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            client = FakePipedriveClient()
            first = import_completed_clay_webhook_payload(
                payload,
                root=Path(tmp_dir),
                dry_run=False,
                client=client,
            )
            second = import_completed_clay_webhook_payload(
                payload,
                root=Path(tmp_dir),
                dry_run=False,
                client=client,
            )

        self.assertEqual(first.imported_to_pipedrive, 1)
        self.assertEqual(first.updated_people, 2)
        self.assertEqual(first.created_notes, 0)
        self.assertEqual(second.imported_to_pipedrive, 0)
        self.assertEqual(second.skipped_rows, 1)

    def test_completed_clay_webhook_rejects_unresolved_placeholders(self) -> None:
        payload = {
            "Company Name": "{{Company Name}}",
            "Rep Pipedrive User Id": "25200571",
            "Source File": "lea_infocomm.xlsx",
            "Conference": "InfoComm 2026",
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            with self.assertRaisesRegex(ValueError, "unresolved column placeholders"):
                import_completed_clay_webhook_payload(
                    payload,
                    root=Path(tmp_dir),
                    dry_run=True,
                    client=FakePipedriveClient(),
                )

    def test_clay_view_records_translate_field_ids_to_headers(self) -> None:
        calls: list[str] = []

        def fake_clay_request(method, path, *, params=None, payload=None, timeout=45.0):
            calls.append(path)
            if path == "/tables/table_123/count":
                return 200, {"tableTotalRecordsCount": 1}
            if path == "/tables/table_123/views/view_456/records":
                return 200, {
                    "results": [
                        {
                            "id": "record_789",
                            "cells": {
                                "f_company": {"value": "PoEWit"},
                                "f_ready": {"value": "valid"},
                            },
                        }
                    ]
                }
            if path == "/tables/table_123":
                return 200, {
                    "table": {
                        "fields": [
                            {"id": "f_company", "name": "Company Name"},
                            {"id": "f_ready", "name": "Sales LinkedIn Validation"},
                        ]
                    }
                }
            raise AssertionError(f"unexpected Clay path: {path}")

        with patch("app.providers._clay_request", side_effect=fake_clay_request):
            rows = _fetch_clay_table_raw_rows("table_123", view_id="view_456")

        self.assertEqual(calls[:2], ["/tables/table_123/count", "/tables/table_123/views/view_456/records"])
        self.assertEqual(rows[0]["company name"], "PoEWit")
        self.assertEqual(rows[0]["sales linkedin validation"], "valid")
        self.assertEqual(rows[0]["clay row id"], "record_789")

    def test_negative_sales_leader_enrichment_is_ignored(self) -> None:
        contact = sales_leader_contact_for_row(
            LeadSheetRow(
                company_name="COVD, Inc.",
                sales_leader_name="No qualifying sales leader found at COVD",
                sales_leader_email="Missing input",
                sales_leader_phone="No phone found",
                sales_leader_linkedin_url="The provided Sales LinkedIn value does not contain a valid LinkedIn profile URL.",
            )
        )

        self.assertEqual(contact.name, "")
        self.assertEqual(contact.email, "")
        self.assertEqual(contact.phone, "")
        self.assertEqual(contact.linkedin_url, "")

        descriptive_contact = sales_leader_contact_for_row(
            LeadSheetRow(
                company_name="Northeast Lantern",
                sales_leader_name=(
                    "Identified top sales leader in the corpus: Tracey Mancini "
                    "(matches provided Person Name; per instructions, no alternate person returned)."
                ),
                sales_leader_linkedin_url=(
                    "The provided Sales LinkedIn value is a descriptive statement "
                    "and not a valid LinkedIn profile URL."
                ),
                sales_leader_title="None",
            )
        )

        self.assertEqual(descriptive_contact.name, "")
        self.assertEqual(descriptive_contact.linkedin_url, "")
        self.assertEqual(descriptive_contact.job_title, "")

    def test_completed_clay_webhook_exports_to_local_enriched_csv(self) -> None:
        payload = {
            "clay_row_id": "clay-row-export-123",
            "company_name": "PoEWit",
            "person_name": "Lindsay Miller",
            "email": "lindsay@poewit.com",
            "linkedin_url": "https://linkedin.com/in/lindsay",
            "sales_leader_name": "Alex Leader",
            "sales_leader_email": "alex@poewit.com",
            "sales_leader_phone": "+15551234567",
            "sales_leader_linkedin_url": "https://linkedin.com/in/alexleader",
            "rep_pipedrive_user_id": "25200571",
            "rep_name": "Lea Skoumbakis",
            "source_file": "lea_infocomm.xlsx",
            "conference": "InfoComm 2026",
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            summary = export_completed_clay_webhook_payload_to_csv(payload, root=Path(tmp_dir))
            export_path = Path(summary["csv_path"])
            with export_path.open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.DictReader(csv_file))

            second_summary = export_completed_clay_webhook_payload_to_csv(
                {**payload, "sales_leader_phone": "+15557654321"},
                root=Path(tmp_dir),
            )
            with export_path.open(newline="", encoding="utf-8") as csv_file:
                updated_rows = list(csv.DictReader(csv_file))

        self.assertEqual(summary["exported_rows"], 1)
        self.assertEqual(summary["total_rows"], 1)
        self.assertEqual(rows[0]["company_name"], "PoEWit")
        self.assertEqual(rows[0]["sales_leader_email"], "alex@poewit.com")
        self.assertEqual(second_summary["exported_rows"], 0)
        self.assertEqual(second_summary["updated_rows"], 1)
        self.assertEqual(updated_rows[0]["sales_leader_phone"], "+15557654321")


if __name__ == "__main__":
    unittest.main()
