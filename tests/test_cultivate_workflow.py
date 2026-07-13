from __future__ import annotations

import json
import unittest

from scripts.deploy_cultivate_n8n_workflow import REQUIRED_CREDENTIALS, build_workflow


class CultivateWorkflowTests(unittest.TestCase):
    def test_airtable_and_smartlead_writes_are_deduplicated(self) -> None:
        credentials = {
            name: {"id": f"credential-{index}", "type": "httpHeaderAuth"}
            for index, name in enumerate(REQUIRED_CREDENTIALS.values(), start=1)
        }
        workflow = build_workflow(credentials)
        nodes = {node["name"]: node for node in workflow["nodes"]}

        self.assertEqual(nodes["Cultivate Intake Webhook"]["parameters"]["responseMode"], "onReceived")
        self.assertNotIn("Respond to Webhook", nodes)

        lead_write = nodes["Write Cultivate Lead"]["parameters"]
        self.assertEqual(lead_write["method"], "PATCH")
        self.assertIn("airtableLeadBody", lead_write["jsonBody"])

        contact_write = nodes["Upsert Cultivate Contact"]["parameters"]
        self.assertEqual(contact_write["method"], "PATCH")
        self.assertIn("airtableContactBody", contact_write["jsonBody"])

        smartlead_body = nodes["Add Lead to Smartlead"]["parameters"]["jsonBody"]
        self.assertIn('"ignore_duplicate_leads_in_other_campaign": false', smartlead_body)
        self.assertIn('"ignore_unsubscribe_list": false', smartlead_body)
        self.assertNotIn("allow_duplicate_leads_in_another_campaign", smartlead_body)

        normalize_code = nodes["Normalize Incoming Leads"]["parameters"]["jsCode"]
        self.assertIn("general_contact_email", normalize_code)
        self.assertIn("scrapedContact", normalize_code)
        self.assertIn("fields['Source Row ID']", normalize_code)

        contact_code = nodes["Materialize Contact Rows"]["parameters"]["jsCode"]
        self.assertIn("performUpsert", contact_code)
        self.assertIn("scraped_trade_show_contact", contact_code)
        self.assertIn("row.scrapedContacts", contact_code)
        self.assertIn("row.enableSmartlead === true && Boolean(first)", contact_code)
        self.assertIn("Pipedrive remains", contact_code)

        research_prompt = nodes["Build Lead Research Request"]["parameters"]["jsCode"]
        self.assertIn("Conduit Commerce's proven ICP", research_prompt)
        self.assertIn("STRONG FIT", research_prompt)
        self.assertIn("MODERATE FIT", research_prompt)
        self.assertIn("When ICP fit is STRONG FIT, MODERATE FIT, or WEAK FIT", research_prompt)
        parse_research = nodes["Parse Research Result"]["parameters"]["jsCode"]
        self.assertIn("'ICP SKU Estimate'", parse_research)
        self.assertIn("'ICP Fit Score'", parse_research)
        self.assertIn("icpQualified", parse_research)
        self.assertIn("'WEAK FIT'", parse_research)

        final_dedupe = nodes["Dedupe Final Contacts"]
        self.assertEqual(final_dedupe["parameters"]["mode"], "runOnceForAllItems")
        final_dedupe_code = final_dedupe["parameters"]["jsCode"]
        self.assertIn("finalWorkEmail || row.suppliedEmail", final_dedupe_code)
        self.assertIn("sourceContactMatched", final_dedupe_code)
        self.assertIn("duplicateSuppressedCount", final_dedupe_code)
        self.assertIn("Dedupe Final Contacts", workflow["connections"])

        pipedrive_code = nodes["Build Pipedrive Sync"]["parameters"]["jsCode"]
        self.assertIn("Lea Skoumbakis", pipedrive_code)
        self.assertIn("Austin Weitman", pipedrive_code)
        self.assertIn("assignmentIndex % reps.length", pipedrive_code)

    def test_requires_show_specific_campaign_and_never_activates_it(self) -> None:
        credentials = {
            name: {"id": f"credential-{index}", "type": "httpHeaderAuth"}
            for index, name in enumerate(REQUIRED_CREDENTIALS.values(), start=1)
        }
        workflow = build_workflow(credentials)
        nodes = {node["name"]: node for node in workflow["nodes"]}
        serialized = json.dumps(workflow)

        normalize_code = nodes["Normalize Incoming Leads"]["parameters"]["jsCode"]
        self.assertIn("cadenceEnrollmentDate", normalize_code)
        self.assertIn("smartleadCampaignId", normalize_code)
        self.assertNotIn("3578438", serialized)

        validation_code = nodes["Apply Final Email Validation"]["parameters"]["jsCode"]
        self.assertIn("Missing required show-specific Smartlead campaign ID", validation_code)
        self.assertIn("row.smartleadCampaignId", validation_code)
        self.assertIn("row.finalEmailValidationStatus === 'valid'", validation_code)
        parse_validation_code = nodes["Parse Final Email Validation"]["parameters"]["jsCode"]
        self.assertNotIn("trustedDirectorySourceFallback", parse_validation_code)
        self.assertEqual(
            nodes["Enrichley Validate Final Email"]["parameters"]["url"],
            "https://api.enrichley.io/api/v1/validate-single-email",
        )
        self.assertNotIn("continueOnFail", nodes["Enrichley Validate Final Email"])
        self.assertNotIn("continueOnFail", nodes["Enrichley Validate LeadMagic"])

        smartlead_node = nodes["Add Lead to Smartlead"]
        self.assertEqual(
            smartlead_node["parameters"]["url"],
            "=https://server.smartlead.ai/api/v1/campaigns/{{$json.smartleadCampaignId}}/leads",
        )
        self.assertNotIn("/start", serialized)
        self.assertNotIn("/activate", serialized)
        self.assertNotIn("campaign-status", serialized)

    def test_pipedrive_sync_accepts_any_valid_contact_channel(self) -> None:
        credentials = {
            name: {"id": f"credential-{index}", "type": "httpHeaderAuth"}
            for index, name in enumerate(REQUIRED_CREDENTIALS.values(), start=1)
        }
        workflow = build_workflow(credentials)
        nodes = {node["name"]: node for node in workflow["nodes"]}

        pipedrive_code = nodes["Build Pipedrive Sync"]["parameters"]["jsCode"]
        self.assertIn("finalWorkEmail || finalPhone || linkedinUrl", pipedrive_code)
        self.assertIn("Missing a valid final email, phone, or LinkedIn profile", pipedrive_code)
        self.assertIn("row.icpQualified === true", pipedrive_code)
        self.assertIn("ICP gate excluded", pipedrive_code)
        self.assertNotIn("linkedinUrl && clean(row.linkedinActive) === 'YES'", pipedrive_code)

        create_person_body = nodes["Pipedrive Create Person"]["parameters"]["jsonBody"]
        update_person_body = nodes["Pipedrive Update Person"]["parameters"]["jsonBody"]
        self.assertIn("body.phone", create_person_body)
        self.assertIn("$json.finalPhone", create_person_body)
        self.assertIn("body.phone", update_person_body)

    def test_pipedrive_materializes_idempotent_day_4_5_7_activities(self) -> None:
        credentials = {
            name: {"id": f"credential-{index}", "type": "httpHeaderAuth"}
            for index, name in enumerate(REQUIRED_CREDENTIALS.values(), start=1)
        }
        workflow = build_workflow(credentials)
        nodes = {node["name"]: node for node in workflow["nodes"]}

        materialize = nodes["Materialize Pipedrive Activities"]
        self.assertEqual(materialize["parameters"]["mode"], "runOnceForAllItems")
        activity_code = materialize["parameters"]["jsCode"]
        self.assertIn("linkedin_day_4", activity_code)
        self.assertIn("call_1_day_5", activity_code)
        self.assertIn("call_2_day_7", activity_code)
        self.assertIn("offsetDays: 4", activity_code)
        self.assertIn("offsetDays: 5", activity_code)
        self.assertIn("offsetDays: 7", activity_code)
        self.assertIn("cadenceEnrollmentDate", activity_code)

        search_parser = nodes["Parse Pipedrive Activity Search"]["parameters"]["jsCode"]
        self.assertIn("pipedriveActivitySubject", search_parser)
        self.assertIn("pipedriveActivityKey === 'linkedin_day_4'", search_parser)
        search_query = nodes["Pipedrive Search Activity"]["parameters"]["jsonQuery"]
        self.assertIn('"lead_id"', search_query)
        self.assertNotIn('"done"', search_query)

        for node_name in ("Pipedrive Create Activity", "Pipedrive Update Activity"):
            body = nodes[node_name]["parameters"]["jsonBody"]
            self.assertIn('"owner_id": $json.pipedriveOwnerId', body)
            self.assertNotIn('"user_id"', body)
            self.assertIn("$json.pipedriveActivityDueDate", body)
            self.assertIn("$json.pipedriveActivityType", body)

        summarize = nodes["Summarize Pipedrive Activities"]
        self.assertEqual(summarize["parameters"]["mode"], "runOnceForAllItems")
        self.assertIn("pipedriveActivityIds", summarize["parameters"]["jsCode"])
        self.assertIn("Summarize Pipedrive Activities", workflow["connections"])


if __name__ == "__main__":
    unittest.main()
