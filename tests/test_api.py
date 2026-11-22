import json
from datetime import date, datetime
import unittest
from unittest.mock import patch
import uuid

import httpx2

from api import APIClient, InvalidRecordResponse, RecordCreateDTO, RecordReceiptDTO


class EnrollmentRequestTests(unittest.IsolatedAsyncioTestCase):
    def client_answering(self, status, body):
        requests = []

        def respond(request):
            requests.append(request)
            content = body if isinstance(body, bytes) else json.dumps(body).encode()
            return httpx2.Response(status, content=content)

        async_client = httpx2.AsyncClient
        with patch(
            "api.httpx2.AsyncClient",
            side_effect=lambda **kwargs: async_client(**kwargs, transport=httpx2.MockTransport(respond)),
        ):
            api = APIClient("http://server/")
        self.addAsyncCleanup(api.close)
        return api, requests

    async def request(self, api):
        return await api.request_enrollment("secret", hostname="scale-pc", ip_address="192.0.2.10")

    async def test_sends_the_credential_and_this_pc_to_the_enrollment_endpoint(self):
        api, requests = self.client_answering(202, {"status": "pending"})

        self.assertEqual(await self.request(api), "pending")
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].method, "POST")
        self.assertEqual(str(requests[0].url), "http://server/devices/api/gateways/enrollment/")
        self.assertEqual(requests[0].headers["Authorization"], "Gateway secret")
        self.assertEqual(json.loads(requests[0].content), {"hostname": "scale-pc", "ip_address": "192.0.2.10"})

    async def test_documented_answers(self):
        for status, expected in ((200, "approved"), (202, "pending"), (403, "revoked")):
            with self.subTest(status=status):
                api, _ = self.client_answering(status, {"status": expected})
                self.assertEqual(await self.request(api), expected)

    async def test_other_responses_are_errors_not_answers(self):
        for status, body in (
            (403, {"detail": "Forbidden"}), (403, b"<html>Forbidden</html>"),
            (401, {"status": "revoked"}), (429, {"detail": "Slow down"}), (500, b"Server error"),
        ):
            with self.subTest(status=status, body=body):
                api, _ = self.client_answering(status, body)
                with self.assertRaises(httpx2.HTTPStatusError):
                    await self.request(api)
        for status, body in ((200, {"status": "pending"}), (202, b"not json"), (200, ["approved"])):
            with self.subTest(status=status, body=body):
                api, _ = self.client_answering(status, body)
                with self.assertRaises(ValueError):
                    await self.request(api)


class RecordCreationTests(unittest.IsolatedAsyncioTestCase):
    def client_answering(self, status, body):
        requests = []

        def respond(request):
            requests.append(request)
            if isinstance(body, bytes):
                return httpx2.Response(status, content=body)
            return httpx2.Response(status, json=body)

        async_client = httpx2.AsyncClient
        with patch(
            "api.httpx2.AsyncClient",
            side_effect=lambda **kwargs: async_client(**kwargs, transport=httpx2.MockTransport(respond)),
        ):
            api = APIClient("http://server/")
        self.addAsyncCleanup(api.close)
        return api, requests

    def setUp(self):
        self.record = RecordCreateDTO(
            uuid=uuid.uuid4(),
            rfid_card_uid="12345678",
            station_id=7,
            weight=120,
            measured_at=datetime.fromisoformat("2026-10-25T09:34:56+09:00"),
        )

        self.body = {
            "uuid": str(self.record.uuid),
            "station_id": 7,
            "weight": 118,
            "measured_at": "2026-10-26T01:23:45Z",
            "cancelled_at": None,
            "daily_number": 3,
            "number_date": "2026-10-25",
        }

    async def test_sends_saved_station_with_record_for_created_and_duplicate_responses(self):
        for status in (201, 200):
            with self.subTest(status=status):
                api, requests = self.client_answering(status, self.body)
                self.assertEqual(await api.create_record("secret", self.record), RecordReceiptDTO(
                    uuid=self.record.uuid, station_id=7, weight=118,
                    measured_at=datetime.fromisoformat("2026-10-26T01:23:45Z"),
                    cancelled_at=None, daily_number=3, number_date=date(2026, 10, 25),
                ))
                self.assertEqual(len(requests), 1)
                request = requests[0]
                self.assertEqual(request.method, "POST")
                self.assertEqual(str(request.url), "http://server/weighing/api/records/")
                self.assertEqual(request.headers["Authorization"], "Gateway secret")
                self.assertEqual(json.loads(request.content), {
                    "uuid": str(self.record.uuid),
                    "rfid_card_uid": "12345678",
                    "station_id": 7,
                    "weight": 120,
                    "measured_at": "2026-10-25T09:34:56+09:00",
                })

    async def test_duplicate_can_acknowledge_a_cancelled_record(self):
        cancelled_at = "2026-10-28T18:54:32+09:00"
        api, _ = self.client_answering(200, self.body | {"cancelled_at": cancelled_at})
        receipt = await api.create_record("secret", self.record)
        self.assertEqual(receipt.cancelled_at, datetime.fromisoformat(cancelled_at))
        self.assertEqual(receipt.daily_number, 3)

    async def test_legacy_response_has_no_daily_number_or_number_date(self):
        legacy = self.body.copy()
        del legacy["daily_number"], legacy["number_date"]
        api, _ = self.client_answering(201, legacy)
        receipt = await api.create_record("secret", self.record)
        self.assertIsNone(receipt.daily_number)
        self.assertIsNone(receipt.number_date)
        self.assertEqual(receipt.weight, 118)

    async def test_server_integer_limits_are_valid_receipt_values(self):
        for weight, number in ((-2_147_483_648, 1), (2_147_483_647, 2_147_483_647)):
            with self.subTest(weight=weight, number=number):
                api, _ = self.client_answering(201, self.body | {"weight": weight, "daily_number": number})
                receipt = await api.create_record("secret", self.record)
                self.assertEqual((receipt.weight, receipt.daily_number), (weight, number))

    async def test_invalid_acknowledgments_raise_record_response_error(self):
        invalid_bodies = [
            [], b"not JSON",
            self.body | {"uuid": str(uuid.uuid4())}, self.body | {"uuid": 42},
            self.body | {"uuid": "bad-uuid"}, self.body | {"station_id": 8},
            self.body | {"station_id": 7.0}, self.body | {"weight": False},
            self.body | {"weight": -2_147_483_649}, self.body | {"weight": 2_147_483_648},
            self.body | {"measured_at": "2026-10-25T09:34:56"},
            self.body | {"measured_at": "0001-01-01T00:00:00+09:00"},
            self.body | {"cancelled_at": "9999-12-31T23:59:59-09:00"},
            self.body | {"cancelled_at": "unknown"}, self.body | {"cancelled_at": 0},
            self.body | {"daily_number": True}, self.body | {"daily_number": 0},
            self.body | {"daily_number": 2_147_483_648}, self.body | {"weight": 2**100},
            self.body | {"daily_number": None, "number_date": None},
            self.body | {"number_date": "2026-02-30"}, self.body | {"number_date": "20261025"},
            self.body | {"number_date": None},
        ]
        for field in self.body:
            invalid_bodies.append({k: v for k, v in self.body.items() if k != field})
        for body in invalid_bodies:
            with self.subTest(body=body):
                api, _ = self.client_answering(200, body)
                with self.assertRaises(InvalidRecordResponse):
                    await api.create_record("secret", self.record)

    async def test_other_success_status_does_not_acknowledge_a_record(self):
        api, _ = self.client_answering(202, self.body)
        with self.assertRaises(InvalidRecordResponse):
            await api.create_record("secret", self.record)

    async def test_station_and_uuid_rejections_propagate_to_the_uploader(self):
        for field in ("station_id", "uuid"):
            with self.subTest(field=field):
                body = {field: ["Invalid record."]}
                api, _ = self.client_answering(400, body)
                with self.assertRaises(httpx2.HTTPStatusError) as raised:
                    await api.create_record("secret", self.record)
                self.assertEqual(raised.exception.response.status_code, 400)
                self.assertEqual(raised.exception.response.json(), body)


if __name__ == "__main__":
    unittest.main()
