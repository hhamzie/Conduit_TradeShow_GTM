from __future__ import annotations

import json
import unittest

import httpx

from app.airtable_sync import (
    AirtableSyncClient,
    AirtableSyncError,
    deterministic_company_source_row_id,
)


class AirtableSyncTests(unittest.TestCase):
    def test_batches_show_upserts_and_combines_response_metadata(self) -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            body = json.loads(request.content)
            index = len(requests)
            records = [
                {"id": f"rec-{index}-{offset}", "fields": row["fields"]}
                for offset, row in enumerate(body["records"])
            ]
            return httpx.Response(
                200,
                json={
                    "records": records,
                    "createdRecords": [record["id"] for record in records[:1]],
                    "updatedRecords": [{"id": record["id"]} for record in records[1:]],
                },
            )

        shows = [
            {"Dashboard Show ID": str(index), "Show Name": f"Show {index}"}
            for index in range(23)
        ]
        with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
            client = AirtableSyncClient(
                token="pat-secret-value",
                base_id="appTestBase",
                http_client=http_client,
            )
            result = client.upsert_shows(table_id="Shows & Markets", records=shows)

        self.assertEqual(result.request_count, 3)
        self.assertEqual(len(result.records), 23)
        self.assertEqual(len(result.created_record_ids), 3)
        self.assertEqual(len(result.updated_record_ids), 20)
        self.assertEqual([len(json.loads(request.content)["records"]) for request in requests], [10, 10, 3])
        self.assertEqual(requests[0].url.raw_path, b"/v0/appTestBase/Shows%20%26%20Markets")
        self.assertEqual(requests[0].headers["authorization"], "Bearer pat-secret-value")
        first_body = json.loads(requests[0].content)
        self.assertEqual(first_body["performUpsert"], {"fieldsToMergeOn": ["Dashboard Show ID"]})
        self.assertTrue(first_body["typecast"])

    def test_company_helper_merges_on_show_and_source_row(self) -> None:
        captured: list[dict[str, object]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(json.loads(request.content))
            return httpx.Response(200, json={"records": [], "createdRecords": [], "updatedRecords": []})

        with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
            client = AirtableSyncClient(token="pat-test-token", base_id="appTest", http_client=http_client)
            client.upsert_companies(
                table_id="tblCompanies",
                records=[
                    {
                        "Company Name": "Acme",
                        "Show Name": "Vegas Market",
                        "Source Row ID": "company-123",
                    }
                ],
            )

        self.assertEqual(
            captured[0]["performUpsert"],
            {"fieldsToMergeOn": ["Show Name", "Source Row ID"]},
        )

    def test_contacts_and_campaign_pushes_use_schema_keys(self) -> None:
        bodies: list[dict[str, object]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            bodies.append(json.loads(request.content))
            return httpx.Response(200, json={"records": []})

        with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
            client = AirtableSyncClient(token="pat-test-token", base_id="appTest", http_client=http_client)
            client.upsert_contacts(table_id="Contacts", records=[{"Contact Key": "person-1"}])
            client.upsert_campaign_pushes(table_id="Campaign Pushes", records=[{"Push Key": "push-1"}])

        self.assertEqual(bodies[0]["performUpsert"], {"fieldsToMergeOn": ["Contact Key"]})
        self.assertEqual(bodies[1]["performUpsert"], {"fieldsToMergeOn": ["Push Key"]})

    def test_company_source_id_is_normalized_and_sensitive_to_source(self) -> None:
        first = deterministic_company_source_row_id(
            show_identifier=" Vegas Market ",
            source_identifier="HTTPS://EXAMPLE.COM/Directory ",
            company_name=" Acme   Home ",
            website="https://acme.example",
            booth_number=" A-12 ",
        )
        equivalent = deterministic_company_source_row_id(
            show_identifier="vegas market",
            source_identifier="https://example.com/directory",
            company_name="acme home",
            website="HTTPS://ACME.EXAMPLE",
            booth_number="a-12",
        )
        different = deterministic_company_source_row_id(
            show_identifier="vegas market",
            source_identifier="https://example.com/another-directory",
            company_name="acme home",
            website="https://acme.example",
            booth_number="a-12",
        )

        self.assertEqual(first, equivalent)
        self.assertNotEqual(first, different)
        self.assertRegex(first, r"^company-[0-9a-f]{24}$")

    def test_requires_non_empty_merge_values_before_request(self) -> None:
        calls = 0

        def handler(_request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(200, json={"records": []})

        with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
            client = AirtableSyncClient(token="pat-test-token", base_id="appTest", http_client=http_client)
            with self.assertRaisesRegex(ValueError, "Dashboard Show ID"):
                client.upsert_shows(table_id="Shows", records=[{"Show Name": "Missing ID"}])
            with self.assertRaisesRegex(ValueError, "Source Row ID"):
                client.upsert_companies(
                    table_id="Companies",
                    records=[{"Show Name": "Vegas Market", "Source Row ID": "  "}],
                )

        self.assertEqual(calls, 0)

    def test_empty_input_is_a_noop(self) -> None:
        with httpx.Client(
            transport=httpx.MockTransport(lambda _request: self.fail("No request expected"))
        ) as http_client:
            client = AirtableSyncClient(token="pat-test-token", base_id="appTest", http_client=http_client)
            result = client.upsert_shows(table_id="Shows", records=[])

        self.assertEqual(result.request_count, 0)
        self.assertEqual(result.records, ())

    def test_api_errors_redact_token_and_preserve_safe_metadata(self) -> None:
        token = "pat-secret-token-123456"

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                422,
                json={
                    "error": {
                        "type": "INVALID_VALUE_FOR_COLUMN",
                        "message": f"Bad Bearer {token} value",
                    }
                },
            )

        with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
            client = AirtableSyncClient(token=token, base_id="appTest", http_client=http_client)
            with self.assertRaises(AirtableSyncError) as raised:
                client.upsert_shows(
                    table_id="Shows",
                    records=[{"Dashboard Show ID": "1", "Show Name": "Vegas Market"}],
                )

        error = raised.exception
        self.assertEqual(error.status_code, 422)
        self.assertEqual(error.error_type, "INVALID_VALUE_FOR_COLUMN")
        self.assertNotIn(token, str(error))
        self.assertIn("[REDACTED]", str(error))

    def test_non_json_error_does_not_echo_response_body(self) -> None:
        secret_body = "gateway leaked pat-secret-token-123456"

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, text=secret_body)

        with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
            client = AirtableSyncClient(token="pat-secret-token-123456", base_id="appTest", http_client=http_client)
            with self.assertRaises(AirtableSyncError) as raised:
                client.upsert_records(
                    table_id="Shows",
                    records=[{"Dashboard Show ID": "1"}],
                    merge_fields=["Dashboard Show ID"],
                )

        self.assertEqual(raised.exception.status_code, 503)
        self.assertNotIn(secret_body, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
