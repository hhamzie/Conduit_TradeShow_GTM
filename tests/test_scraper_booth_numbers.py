from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scraper import (
    apply_website_requirement,
    build_listing_candidates,
    build_mapyourshow_search_url,
    collect_directory_entries_andmore_imc,
    collect_directory_entries_dallas_market_center,
    collect_direct_landing_entries,
    collect_company_records,
    collect_table_directory_entries,
    CompanyRecord,
    DirectoryEntry,
    extract_directory_entry_candidates,
    filter_plausible_company_records,
    find_gateway_show_directory_url,
    find_embedded_directory_url,
    is_mapyourshow_directory,
    is_bulletin_directory,
    extract_booth_number_from_profile,
    extract_table_row_entries,
    collect_directory_entries_mapyourshow,
    normalize_booth_number_candidate,
    parse_page,
    should_browser_resolve_company_record,
    stream_company_records_to_csv,
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

    def test_extract_table_row_entries_handles_company_booth_descriptor_layout(self) -> None:
        html = """
        <table>
          <tr>
            <td>(RE)SET</td>
            <td><img src="brand.png" alt=""></td>
            <td>GA25</td>
            <td>New Exhibitor</td>
            <td><a href="/en/exposants-lp/685bec4085932a38dbbda38e">View</a></td>
          </tr>
        </table>
        """

        entries = extract_table_row_entries(
            seed_url="https://lpmc25.eventmaker.io/en/exhibitors-list",
            html_text=html,
            directory_page=1,
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].company_name, "(RE)SET")
        self.assertEqual(entries[0].booth_number, "GA25")

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

    def test_filter_plausible_company_records_keeps_real_companies_and_drops_gibberish(self) -> None:
        records = [
            CompanyRecord(
                sort_index=0,
                directory_page=1,
                company_name="Acme Packaging",
                profile_url="https://example.com/acme",
                website_url="https://acme.com",
                booth_number="C21",
            ),
            CompanyRecord(
                sort_index=1,
                directory_page=1,
                company_name="Aoe <* Sng Cs leee",
                profile_url="",
                website_url="",
                booth_number="",
            ),
        ]

        filtered = filter_plausible_company_records(records)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].company_name, "Acme Packaging")
        self.assertEqual(filtered[0].booth_number, "C21")

    def test_collect_company_records_uses_serial_path_when_workers_is_one(self) -> None:
        entries = [
            DirectoryEntry(
                sort_index=0,
                directory_page=1,
                company_name="Acme Packaging",
                profile_url="https://example.com/acme",
                website_url_hint="",
                booth_number="C21",
            )
        ]

        with (
            patch("scraper.scrape_profile_details", return_value=("https://acme.com", "C21")) as scrape_mock,
            patch("scraper.ThreadPoolExecutor", side_effect=AssertionError("thread pool should not be used")),
        ):
            records, failures = collect_company_records(entries, workers=1)

        self.assertEqual(failures, 0)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].website_url, "https://acme.com")
        scrape_mock.assert_called_once_with("https://example.com/acme")

    def test_stream_company_records_to_csv_writes_serially_without_large_record_buffer(self) -> None:
        entries = [
            DirectoryEntry(
                sort_index=0,
                directory_page=1,
                company_name="Acme Packaging",
                profile_url="https://example.com/acme",
                website_url_hint="",
                booth_number="C21",
            ),
            DirectoryEntry(
                sort_index=1,
                directory_page=1,
                company_name="Beta Labs",
                profile_url="https://example.com/beta",
                website_url_hint="",
                booth_number="D14",
            ),
        ]

        def fake_scrape(url: str) -> tuple[str, str]:
            if "acme" in url:
                return "https://acme.com", "C21"
            return "", "D14"

        with tempfile.TemporaryDirectory() as tmp_dir, patch("scraper.scrape_profile_details", side_effect=fake_scrape):
            output_path = Path(tmp_dir) / "export.csv"
            count, failures = stream_company_records_to_csv(
                entries=entries,
                output_path=output_path,
                conference_name="Pack Expo",
                conference_location="Las Vegas, NV",
                require_website=True,
            )
            text = output_path.read_text(encoding="utf-8")

        self.assertEqual(failures, 0)
        self.assertEqual(count, 1)
        self.assertIn("Acme Packaging", text)
        self.assertNotIn("Beta Labs", text)

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

    def test_is_bulletin_directory_matches_exhibitor_directory_url(self) -> None:
        html = '<html><body><div id="app"></div></body></html>'

        self.assertTrue(
            is_bulletin_directory(
                "https://icff.bulletin.co/icff/exhibitor-directory?sortBy=brands_made_active_desc&page=1",
                html,
            )
        )

    def test_should_browser_resolve_company_record_skips_when_booth_exists_and_website_optional(self) -> None:
        record = CompanyRecord(
            sort_index=0,
            directory_page=1,
            company_name="da.studio",
            profile_url="https://icff.bulletin.co/brand/da-studio",
            website_url="",
            booth_number="W47",
        )

        self.assertFalse(
            should_browser_resolve_company_record(
                record,
                require_website=False,
            )
        )

    def test_should_browser_resolve_company_record_forces_resolution_when_website_required(self) -> None:
        record = CompanyRecord(
            sort_index=0,
            directory_page=1,
            company_name="da.studio",
            profile_url="https://icff.bulletin.co/brand/da-studio",
            website_url="",
            booth_number="W47",
        )

        self.assertTrue(
            should_browser_resolve_company_record(
                record,
                require_website=True,
            )
        )

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

    @patch("scraper.fetch_json_data")
    def test_collect_directory_entries_andmore_imc_uses_direct_api_and_dedupes_exhibitors(
        self,
        mocked_fetch,
    ) -> None:
        seed_html = """
        <script id="__JSS_STATE__">
        {"sitecore":{"context":{"site":{"name":"las-vegas-market"}}},"route":{"itemId":"cfa5a5c2-9bd7-4309-b356-1acdef98a65f"}}
        </script>
        <script>
        window.app={sitecoreApiKey:"391D75C6-01EE-463C-8B51-47B2748F8ACD"};
        </script>
        """

        def fake_fetch(url: str, extra_headers=None):
            self.assertEqual(extra_headers, {
                "Accept": "application/json, text/plain, */*",
                "Channel": "las-vegas-market",
            })
            if "/search/count" in url:
                return {"exhibitors": 3}
            if "/exhibitors/search" in url:
                return {
                    "count": 3,
                    "data": [
                        {
                            "exhibitorId": "30774",
                            "type": "Line",
                            "title": "Thompson's Candle Co.",
                            "companyDetails": {
                                "companyName": "Thompson's Candle Co.",
                                "activeLeases": [
                                    {
                                        "channel": {"siteCode": "las-vegas-market"},
                                        "showrooms": [{"showroomDisplayName": "C738"}],
                                    }
                                ],
                            },
                        },
                        {
                            "exhibitorId": "30774",
                            "type": "Exhibitor",
                            "title": "Thompson's Candle Co.",
                            "companyDetails": {
                                "companyName": "Thompson's Candle Co.",
                                "activeLeases": [
                                    {
                                        "channel": {"siteCode": "las-vegas-market"},
                                        "showrooms": [{"showroomDisplayName": "C738"}],
                                    }
                                ],
                            },
                        },
                        {
                            "exhibitorId": "65921",
                            "type": "Exhibitor",
                            "title": "HiEnd Accents",
                            "companyDetails": {
                                "companyName": "HiEnd Accents",
                                "activeLeases": [
                                    {
                                        "channel": {"siteCode": "las-vegas-market"},
                                        "showrooms": [{"showroomDisplayName": "B105"}],
                                    }
                                ],
                            },
                        },
                    ],
                }
            if "/OpenDetails" in url:
                return {
                    "count": 2,
                    "data": [
                        {
                            "exhibitorId": "30774",
                            "companyDetails": {
                                "companyName": "Thompson's Candle Co.",
                                "activeLeases": [
                                    {
                                        "channel": {"siteCode": "las-vegas-market"},
                                        "showrooms": [{"showroomDisplayName": "C738"}],
                                    }
                                ],
                            },
                            "companyInformation": {
                                "companyWebsiteUrl": "http://www.thompsonscandle.com"
                            },
                        },
                        {
                            "exhibitorId": "65921",
                            "companyDetails": {
                                "companyName": "HiEnd Accents",
                                "activeLeases": [
                                    {
                                        "channel": {"siteCode": "las-vegas-market"},
                                        "showrooms": [{"showroomDisplayName": "B105"}],
                                    }
                                ],
                            },
                            "companyInformation": {
                                "companyWebsiteUrl": "https://www.hiendaccents.com"
                            },
                        },
                    ],
                }
            raise AssertionError(f"Unexpected URL {url}")

        mocked_fetch.side_effect = fake_fetch

        entries, adapter_title = collect_directory_entries_andmore_imc(
            seed_url="https://www.lasvegasmarket.com/en/exhibitor/exhibitor-directory",
            seed_html=seed_html,
            start_page=1,
            end_page=1,
            max_pages=3,
            adapter_title="Las Vegas Market",
        )

        self.assertEqual(adapter_title, "Las Vegas Market")
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].company_name, "Thompson's Candle Co.")
        self.assertEqual(entries[0].booth_number, "C738")
        self.assertEqual(entries[0].website_url_hint, "http://www.thompsonscandle.com")
        self.assertEqual(entries[1].company_name, "HiEnd Accents")
        self.assertEqual(entries[1].booth_number, "B105")
        self.assertEqual(entries[1].website_url_hint, "https://www.hiendaccents.com")

    def test_collect_directory_entries_dallas_market_center_reads_rendered_exhibitor_cards(self) -> None:
        html = """
        <ul id="paginated-list" data-current-page="1">
          <li class="li__exhibitor" data-exhibitor-id="23735">
            <section class="section__heading">
              <header class="header__headingTop">
                <h2 class="h2__exhibitor">
                  <a class="a__exhibitor" href="/exhibitors/a-b-home/">A &amp; B Home</a>
                </h2>
              </header>
              <div class="block__headingBottom">
                <address class="address__exhibitor">tm 1 - 425</address>
              </div>
            </section>
            <div class="block__exhibitorProfile">
              <a class="a__exhibitor" href="/exhibitors/a-b-home/" aria-label="A &amp; B Home Profile">Show More</a>
            </div>
          </li>
          <li class="li__exhibitor" data-exhibitor-id="15391">
            <section class="section__heading">
              <header class="header__headingTop">
                <h2 class="h2__exhibitor">
                  <a class="a__exhibitor" href="/exhibitors/merci/">&amp; Merci</a>
                </h2>
              </header>
              <div class="block__headingBottom">
                <address class="address__exhibitor">wtc 12-1404 (temp)</address>
              </div>
            </section>
          </li>
        </ul>
        """

        entries, adapter_title = collect_directory_entries_dallas_market_center(
            seed_url="https://www.dallasmarketcenter.com/become-an-attendee/plan-your-visit/search-exhibitors-brands/",
            seed_html=html,
        )

        self.assertEqual(adapter_title, "Dallas Market Center")
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].company_name, "A & B Home")
        self.assertEqual(entries[0].profile_url, "https://www.dallasmarketcenter.com/exhibitors/a-b-home/")
        self.assertEqual(entries[0].booth_number, "tm 1 - 425")
        self.assertEqual(entries[1].company_name, "& Merci")

    def test_find_gateway_show_directory_url_prefers_luxepack_child_with_real_exhibitors(self) -> None:
        root_html = """
        <main>
          <a href="https://www.luxepacklosangeles.com/">Los Angeles</a>
          <a href="https://www.luxepacknewyork.com/">New York</a>
        </main>
        """
        los_angeles_html = """
        <html><body><main><a href="/why-exhibit">Why Exhibit</a></main></body></html>
        """
        new_york_html = """
        <html><body><main><a href="/exhibitors-lp">Exhibitors Listing</a></main></body></html>
        """
        root_page = parse_page("https://www.luxepack.com/", root_html)

        def fake_loader(url: str):
            pages = {
                "https://www.luxepacklosangeles.com/": (
                    "https://www.luxepacklosangeles.com/",
                    los_angeles_html,
                    parse_page("https://www.luxepacklosangeles.com/", los_angeles_html),
                ),
                "https://www.luxepacknewyork.com/": (
                    "https://www.luxepacknewyork.com/",
                    new_york_html,
                    parse_page("https://www.luxepacknewyork.com/", new_york_html),
                ),
            }
            return pages[url]

        resolved_url = find_gateway_show_directory_url(
            "https://www.luxepack.com/",
            root_page,
            fake_loader,
        )

        self.assertEqual(resolved_url, "https://www.luxepacknewyork.com/exhibitors-lp")

    def test_find_gateway_show_directory_url_uses_first_packexpo_child_with_directory_signal(self) -> None:
        root_html = """
        <main>
          <a href="https://www.packexpointernational.com/" class="card-link">Chicago</a>
          <a href="https://www.packexpolasvegas.com/" class="card-link">Las Vegas</a>
        </main>
        """
        chicago_html = """
        <html><body><main><a href="/attend">Attend</a></main></body></html>
        """
        vegas_html = """
        <html><body><main><a href="https://packexpo25.mapyourshow.com/8_0/">2025 Exhibitors</a></main></body></html>
        """
        root_page = parse_page("https://www.packexpo.com/", root_html)

        def fake_loader(url: str):
            pages = {
                "https://www.packexpointernational.com/": (
                    "https://www.packexpointernational.com/",
                    chicago_html,
                    parse_page("https://www.packexpointernational.com/", chicago_html),
                ),
                "https://www.packexpolasvegas.com/": (
                    "https://www.packexpolasvegas.com/",
                    vegas_html,
                    parse_page("https://www.packexpolasvegas.com/", vegas_html),
                ),
            }
            return pages[url]

        resolved_url = find_gateway_show_directory_url(
            "https://www.packexpo.com/",
            root_page,
            fake_loader,
        )

        self.assertEqual(resolved_url, "https://packexpo25.mapyourshow.com/8_0/")

    def test_collect_direct_landing_entries_skips_gateway_hubs(self) -> None:
        seed_html = """
        <html>
          <body>
            <p>REGISTER NOW</p>
            <p>Listen to Experts' Dialogs</p>
          </body>
        </html>
        """
        seed_page = parse_page("https://www.luxepack.com/", seed_html)

        entries = collect_direct_landing_entries(
            seed_url="https://www.luxepack.com/",
            seed_html=seed_html,
            seed_page=seed_page,
        )

        self.assertIsNone(entries)

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

    def test_find_embedded_directory_url_extracts_scripted_eventmaker_iframe_url(self) -> None:
        html = """
        <html>
          <body>
            <iframe id="mobicheckin-form"></iframe>
            <script>
              var urls = {
                fr: "https://lpmc25.eventmaker.io/fr/liste-exposants",
                en: "https://lpmc25.eventmaker.io/en/exhibitors-list"
              };
              var iframeUrl = urls.en || urls.fr;
              document.getElementById('mobicheckin-form').src = iframeUrl;
            </script>
          </body>
        </html>
        """

        embedded = find_embedded_directory_url(
            "https://www.luxepackmonaco.com/en/exhibitors-list",
            html,
        )

        self.assertEqual(embedded, "https://lpmc25.eventmaker.io/en/exhibitors-list")


if __name__ == "__main__":
    unittest.main()
