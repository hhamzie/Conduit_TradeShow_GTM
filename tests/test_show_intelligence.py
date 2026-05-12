from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import tempfile
import unittest

from app.models import Show, ShowGuideRow, ShowStatus
from app.show_guides import serialize_guide_values
from app.show_intelligence import build_show_analysis, infer_theme_playbook


def make_show(export_path: str = "", **overrides) -> Show:
    payload = {
        "name": "Luxe Pack",
        "event_date": date(2026, 5, 20),
        "place": "New York City, NY",
        "source_url": "https://example.com/exhibitors",
        "run_offset_days": 14,
        "run_at": datetime(2026, 5, 6, 9, 0),
        "status": ShowStatus.ready_for_review.value,
        "latest_export_path": export_path,
        "company_count": 0,
        "failure_count": 0,
        "last_error": "",
    }
    payload.update(overrides)
    return Show(**payload)


class ShowIntelligenceTests(unittest.TestCase):
    def test_infer_theme_playbook_matches_keywords(self) -> None:
        playbook = infer_theme_playbook("High Point Market")
        self.assertEqual(playbook.key, "furniture_design")

    def test_build_show_analysis_uses_export_data_and_ranks_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            export_path = Path(tmp_dir) / "luxe.csv"
            export_path.write_text(
                "\n".join(
                    [
                        "company_name,website_url,booth_number",
                        "Acme Packaging,https://acme.com,A12",
                        "Bravo Labs,,",
                        "Cinder Studio,https://cinder.example,",
                    ]
                ),
                encoding="utf-8",
            )
            show = make_show(str(export_path), company_count=3)

            analysis = build_show_analysis(show, today=date(2026, 5, 1), company_limit=2)

        self.assertEqual(analysis.export_company_count, 3)
        self.assertEqual(analysis.website_coverage_percent, 67)
        self.assertEqual(analysis.booth_coverage_percent, 33)
        self.assertEqual(len(analysis.company_profiles), 2)
        self.assertEqual(analysis.company_profiles[0].name, "Acme Packaging")
        self.assertTrue(analysis.export_ready)
        self.assertGreater(analysis.priority_score, 0)
        self.assertIn("packaging", analysis.theme_summary.lower())

    def test_build_show_analysis_handles_missing_export(self) -> None:
        show = make_show("", company_count=80, status=ShowStatus.waiting.value)

        analysis = build_show_analysis(show, today=date(2026, 5, 1))

        self.assertFalse(analysis.export_ready)
        self.assertEqual(analysis.website_coverage_percent, 0)
        self.assertTrue(any("No exhibitor export" in risk for risk in analysis.risks))
        self.assertEqual(analysis.guide_score_label, "Bad")

    def test_build_show_analysis_filters_implausible_export_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            export_path = Path(tmp_dir) / "bad.csv"
            export_path.write_text(
                "\n".join(
                    [
                        "company_name,website_url,booth_number",
                        "Aoe <* Sng Cs leee,,",
                        "Acme Packaging,https://acme.com,A12",
                    ]
                ),
                encoding="utf-8",
            )
            show = make_show(str(export_path), company_count=2)

            analysis = build_show_analysis(show, today=date(2026, 5, 1))

        self.assertEqual(analysis.export_company_count, 1)
        self.assertEqual(len(analysis.company_profiles), 1)
        self.assertEqual(analysis.company_profiles[0].name, "Acme Packaging")

    def test_build_show_analysis_scores_guide_quality_from_team_sizes(self) -> None:
        show = make_show("", company_count=4)
        show.guide_rows = [
            ShowGuideRow(
                sheet_key="company_summary",
                source="workbook",
                position=0,
                values_json=serialize_guide_values(
                    {
                        "company_name": "Acme",
                        "booth_number": "100",
                        "booth_category": "100s",
                        "sales_team_size": "20",
                        "customer_service_team_size": "10",
                        "total_team_size": "30",
                        "catalog_complexity": "",
                        "sales_leader_name": "",
                        "sales_leader_role": "",
                        "sales_leader_email": "",
                        "sales_leader_linkedin": "",
                        "source_url": "https://acme.com",
                    }
                ),
            ),
            ShowGuideRow(
                sheet_key="company_summary",
                source="workbook",
                position=1,
                values_json=serialize_guide_values(
                    {
                        "company_name": "Bravo",
                        "booth_number": "101",
                        "booth_category": "100s",
                        "sales_team_size": "12",
                        "customer_service_team_size": "8",
                        "total_team_size": "20",
                        "catalog_complexity": "",
                        "sales_leader_name": "",
                        "sales_leader_role": "",
                        "sales_leader_email": "",
                        "sales_leader_linkedin": "",
                        "source_url": "https://bravo.com",
                    }
                ),
            ),
        ]

        analysis = build_show_analysis(show, today=date(2026, 5, 1))

        self.assertEqual(analysis.guide_people_total, 50)
        self.assertEqual(analysis.guide_company_count, 2)
        self.assertEqual(analysis.relevant_company_count, 2)
        self.assertEqual(analysis.average_complexity_score, 0.0)
        self.assertEqual(analysis.guide_score_label, "Good")
        self.assertGreaterEqual(analysis.guide_score, 70)


if __name__ == "__main__":
    unittest.main()
