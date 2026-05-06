from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scraper import (
    collect_table_directory_entries,
    CompanyRecord,
    extract_booth_number_from_profile,
    extract_table_row_entries,
    parse_page,
    write_csv,
)


class BoothNumberTests(unittest.TestCase):
    def test_extract_table_row_entries_keeps_booth_numbers(self) -> None:
        html = """
        <table>
          <tr>
            <td>C21</td>
            <td>Acme Packaging</td>
            <td><a href="/exhibitor/acme">View</a></td>
          </tr>
          <tr>
            <td>D14</td>
            <td>Beta Labs</td>
            <td><a href="/exhibitor/beta">View</a></td>
          </tr>
        </table>
        """

        entries = extract_table_row_entries(
            seed_url="https://www.luxepacknewyork.com/exhibitors-lp",
            html_text=html,
            directory_page=11,
        )

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].booth_number, "C21")
        self.assertEqual(entries[1].booth_number, "D14")

    def test_extract_table_row_entries_skips_header_rows(self) -> None:
        html = """
        <table>
          <tr>
            <th>Booth</th>
            <th>Name</th>
            <th>Profile</th>
          </tr>
          <tr>
            <td>C21</td>
            <td>Acme Packaging</td>
            <td><a href="/exhibitor/acme">View</a></td>
          </tr>
        </table>
        """

        entries = extract_table_row_entries(
            seed_url="https://www.luxepacknewyork.com/exhibitors-lp",
            html_text=html,
            directory_page=1,
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].company_name, "Acme Packaging")

    def test_write_csv_outputs_booth_number_column(self) -> None:
        records = [
            CompanyRecord(
                sort_index=0,
                directory_page=1,
                company_name="Acme Packaging",
                profile_url="https://example.com/acme",
                website_url="https://acme.com",
                booth_number="C21",
            )
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export.csv"
            write_csv(
                output_path=output_path,
                records=records,
                conference_name="Luxe Pack",
                conference_location="New York City, NY",
            )

            text = output_path.read_text(encoding="utf-8")

        self.assertIn("booth_number", text)
        self.assertIn("C21", text)

    def test_extract_booth_number_from_profile_looks_for_labeled_value(self) -> None:
        html = """
        <html>
          <head><title>Acme Packaging</title></head>
          <body>
            <main>
              <h1>Acme Packaging</h1>
              <p>Booth: C21</p>
            </main>
          </body>
        </html>
        """

        page = parse_page("https://example.com/exhibitor/acme", html)
        booth_number = extract_booth_number_from_profile(page, html)
        self.assertEqual(booth_number, "C21")

    def test_collect_table_directory_entries_keeps_booth_numbers(self) -> None:
        html = """
        <html>
          <body>
            <table>
              <tr>
                <td>C21</td>
                <td>Acme Packaging</td>
                <td><a href="/exhibitor/acme">View</a></td>
              </tr>
              <tr>
                <td>D14</td>
                <td>Beta Labs</td>
                <td><a href="/exhibitor/beta">View</a></td>
              </tr>
              <tr>
                <td>E07</td>
                <td>Gamma Group</td>
                <td><a href="/exhibitor/gamma">View</a></td>
              </tr>
            </table>
          </body>
        </html>
        """

        seed_url = "https://www.luxepacknewyork.com/exhibitors-lp"
        seed_page = parse_page(seed_url, html)
        entries = collect_table_directory_entries(
            seed_url=seed_url,
            seed_html=html,
            seed_page=seed_page,
            start_page=1,
            end_page=1,
            max_pages=1,
        )

        self.assertIsNotNone(entries)
        assert entries is not None
        self.assertEqual([entry.booth_number for entry in entries], ["C21", "D14", "E07"])

    def test_collect_table_directory_entries_uses_ajax_paginator(self) -> None:
        seed_html = """
        <html>
          <body>
            <table>
              <tr>
                <th>Booth</th>
                <th>Name</th>
                <th>Profile</th>
              </tr>
              <tr>
                <td>C21</td>
                <td>Acme Packaging</td>
                <td><a href="/exhibitor/acme">View</a></td>
              </tr>
              <tr>
                <td>D14</td>
                <td>Beta Labs</td>
                <td><a href="/exhibitor/beta">View</a></td>
              </tr>
              <tr>
                <td>E07</td>
                <td>Gamma Group</td>
                <td><a href="/exhibitor/gamma">View</a></td>
              </tr>
            </table>
            <script>
              var tk = 'seed-token';
              var tm = 'seed-time';
              jQuery('#directory').jsPaginator(
                {
                  'module':'organizations_organization_list',
                  'method':'paginationHandler',
                  'limit':'2',
                  'offset':'2'
                },
                {
                  'pageID':'openAjax'
                },
                4
              );
            </script>
          </body>
        </html>
        """
        page_two_html = """
        <table>
          <tr>
            <td>F09</td>
            <td>Delta Works</td>
            <td><a href="/exhibitor/delta">View</a></td>
          </tr>
          <tr>
            <td>G11</td>
            <td>Epsilon Systems</td>
            <td><a href="/exhibitor/epsilon">View</a></td>
          </tr>
        </table>
        """

        seed_url = "https://www.luxepacknewyork.com/exhibitors-lp"
        seed_page = parse_page(seed_url, seed_html)

        with patch(
            "scraper.fetch_ajax_paginator_payload",
            return_value={
                "data": page_two_html,
                "formToken": "next-token",
                "formTime": "next-time",
            },
        ) as mocked_fetch:
            entries = collect_table_directory_entries(
                seed_url=seed_url,
                seed_html=seed_html,
                seed_page=seed_page,
                start_page=1,
                end_page=None,
                max_pages=5,
            )

        self.assertIsNotNone(entries)
        assert entries is not None
        self.assertEqual(
            [entry.company_name for entry in entries],
            ["Acme Packaging", "Beta Labs", "Gamma Group", "Delta Works", "Epsilon Systems"],
        )
        self.assertEqual(
            [entry.booth_number for entry in entries],
            ["C21", "D14", "E07", "F09", "G11"],
        )
        self.assertEqual(mocked_fetch.call_count, 1)


if __name__ == "__main__":
    unittest.main()
