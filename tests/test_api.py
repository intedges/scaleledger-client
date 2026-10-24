import json
from datetime import datetime
import unittest
from unittest.mock import patch
import uuid

import httpx2

from api import APIClient, RecordCreateDTO


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

    async def test_sends_saved_station_with_record_for_created_and_duplicate_responses(self):
        body = {
            "uuid": str(self.record.uuid),
            "station_id": 7,
            "weight": 120,
            "measured_at": "2026-10-25T00:34:56Z",
        }
        for status in (201, 200):
            with self.subTest(status=status):
                api, requests = self.client_answering(status, body)
                self.assertEqual(await api.create_record("secret", self.record), body)
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
