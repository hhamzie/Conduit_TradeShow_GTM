from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scraper import CompanyRecord, extract_table_row_entries, write_csv


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


if __name__ == "__main__":
    unittest.main()
