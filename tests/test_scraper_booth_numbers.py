from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scraper import (
    apply_website_requirement,
    build_listing_candidates,
    build_mapyourshow_search_url,
    collect_table_directory_entries,
    CompanyRecord,
    extract_directory_entry_candidates,
    find_embedded_directory_url,
    is_mapyourshow_directory,
    extract_booth_number_from_profile,
    extract_table_row_entries,
    collect_directory_entries_mapyourshow,
    normalize_booth_number_candidate,
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

    def test_extract_table_row_entries_ignores_script_templates(self) -> None:
        html = """
        <script type="text/x-template">
          <table>
            <tr>
              <td>{{category}}</td>
              <td>{{brand}}</td>
              <td><a href="exhURL()">View</a></td>
            </tr>
          </table>
        </script>
        <table>
          <tr>
            <td>C21</td>
            <td>Acme Packaging</td>
            <td><a href="/exhibitor/acme">View</a></td>
          </tr>
        </table>
        """

        entries = extract_table_row_entries(
            seed_url="https://example.com/exhibitors",
            html_text=html,
            directory_page=1,
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].company_name, "Acme Packaging")
        self.assertEqual(entries[0].booth_number, "C21")

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

    def test_generic_container_listing_keeps_booth_numbers(self) -> None:
        html = """
        <html>
          <body>
            <main>
              <section class="directory-grid">
                <article class="directory-card">
                  <h3><a href="/brands/acme-packaging">Acme Packaging</a></h3>
                  <p>Booth #1325</p>
                </article>
                <article class="directory-card">
                  <h3><a href="/brands/beta-labs">Beta Labs</a></h3>
                  <p>Booth 2101</p>
                </article>
                <article class="directory-card">
                  <h3><a href="/brands/gamma-group">Gamma Group</a></h3>
                  <p>Booth A17</p>
                </article>
              </section>
            </main>
          </body>
        </html>
        """

        seed_url = "https://directory.example.com/exhibitors"
        page = parse_page(seed_url, html)
        strategies = build_listing_candidates(page, seed_url)
        strategy = next(strategy for strategy in strategies if strategy.source_kind == "container")

        entries = extract_directory_entry_candidates(page, strategy, seed_url)

        self.assertEqual(
            [(entry.company_name, entry.booth_number) for entry in entries],
            [
                ("Acme Packaging", "1325"),
                ("Beta Labs", "2101"),
                ("Gamma Group", "A17"),
            ],
        )

    def test_anchor_listing_prefers_real_name_over_brand_thumbnail(self) -> None:
        html = """
        <html>
          <body>
            <main>
              <article>
                <a href="/brands/rbw">Brand Thumbnail</a>
                <a href="/brands/rbw">booth # 931</a>
                <a href="/brands/rbw">RBW</a>
                <p>Booth #931</p>
              </article>
              <article>
                <a href="/brands/wermo">Brand Thumbnail</a>
                <a href="/brands/wermo">WERMO</a>
                <p>Booth #529</p>
              </article>
              <article>
                <a href="/brands/teak-warehouse">Brand Thumbnail</a>
                <a href="/brands/teak-warehouse">Teak Warehouse</a>
                <p>Booth #123</p>
              </article>
            </main>
          </body>
        </html>
        """

        seed_url = "https://directory.example.com/exhibitors"
        page = parse_page(seed_url, html)
        strategies = build_listing_candidates(page, seed_url)
        strategy = next(strategy for strategy in strategies if strategy.source_kind == "anchor")

        entries = extract_directory_entry_candidates(page, strategy, seed_url)

        self.assertEqual(
            [(entry.company_name, entry.booth_number) for entry in entries],
            [("RBW", "931"), ("Teak Warehouse", "123"), ("WERMO", "529")],
        )

    def test_normalize_booth_number_candidate_strips_trailing_lowercase_junk(self) -> None:
        self.assertEqual(normalize_booth_number_candidate("2601randomstring"), "2601")
        self.assertEqual(normalize_booth_number_candidate("W746randomstring"), "W746")
        self.assertEqual(normalize_booth_number_candidate("2601ncp"), "2601")
        self.assertEqual(normalize_booth_number_candidate("W55ncp"), "W55")
        self.assertEqual(normalize_booth_number_candidate("W55A"), "W55A")

    def test_apply_website_requirement_keeps_records_when_none_have_websites(self) -> None:
        records = [
            CompanyRecord(
                sort_index=0,
                directory_page=1,
                company_name="da.studio",
                profile_url="https://icff.bulletin.co/icff/exhibitor-directory/da-studio",
                website_url="",
                booth_number="W47",
            ),
            CompanyRecord(
                sort_index=1,
                directory_page=1,
                company_name="C.Plot",
                profile_url="https://icff.bulletin.co/icff/exhibitor-directory/c-plot",
                website_url="",
                booth_number="W81",
            ),
        ]

        kept_records = apply_website_requirement(records)

        self.assertEqual(kept_records, records)

    def test_is_mapyourshow_directory_detects_whitelabel_host(self) -> None:
        html = """
        <script>
          function getExhibitorURL() {}
          const url = "/ajax/remote-proxy.cfm?action=search&searchtype=exhibitorgallery";
        </script>
        """

        self.assertTrue(
            is_mapyourshow_directory(
                "https://directory.nationalrestaurantshow.com/8_0/explore/exhibitor-gallery.cfm",
                html,
            )
        )

    def test_build_mapyourshow_search_url_keeps_list_filter(self) -> None:
        url = build_mapyourshow_search_url(
            "https://sse26.mapyourshow.com/8_0/explore/index.cfm?list=ABC123",
            start=0,
            search_size=50,
        )

        self.assertIn("list=ABC123", url)

    @patch("scraper.fetch_html")
    def test_collect_directory_entries_mapyourshow_keeps_booth_numbers(self, mocked_fetch) -> None:
        mocked_fetch.return_value = """
        {
          "DATA": {
            "results": {
              "exhibitor": {
                "found": 1,
                "hit": [
                  {
                    "fields": {
                      "exhid_l": "123",
                      "exhname_t": "Acme Packaging",
                      "boothsdisplay_la": ["C21", "C22"]
                    }
                  }
                ]
              }
            }
          }
        }
        """

        entries = collect_directory_entries_mapyourshow(
            seed_url="https://directory.example.com/8_0/explore/exhibitor-gallery.cfm?featured=false",
            start_page=1,
            end_page=1,
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].company_name, "Acme Packaging")
        self.assertEqual(entries[0].booth_number, "C21, C22")

    def test_find_embedded_directory_url_ignores_tracking_iframes(self) -> None:
        html = """
        <html>
          <body>
            <iframe src="https://insight.adsrvr.org/track/cei?foo=bar"></iframe>
            <iframe src="https://example.com/embed/exhibitor-directory"></iframe>
          </body>
        </html>
        """

        embedded = find_embedded_directory_url(
            "https://directory.example.com/exhibitors",
            html,
        )

        self.assertEqual(embedded, "https://example.com/embed/exhibitor-directory")


if __name__ == "__main__":
    unittest.main()
