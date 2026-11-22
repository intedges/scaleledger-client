import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import UTC, date, datetime
import json
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

import httpx2
from tortoise import Tortoise
from tortoise.exceptions import OperationalError
from tortoise.queryset import QuerySet

from api import APIClient, AuthDegradedError
from models import Record, RecordUploadResult
from workers import RecordUploadWorker


class RecordUploadWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await Tortoise.init(db_url="sqlite://:memory:", modules={"models": ["models"]})
        self.addAsyncCleanup(Tortoise.close_connections)
        await Tortoise.generate_schemas()
        self.record = await Record.create(
            uuid=uuid.uuid4(), rfid_card_uid="test-card", station_id=7,
            weight=85, measured_at=datetime(2026, 11, 23, 1, 30, tzinfo=UTC),
        )
        self.record_uuid = str(self.record.uuid)
        self.body = {
            "uuid": self.record_uuid, "station_id": 7, "weight": 58,
            "measured_at": "2026-11-24T00:10:00+09:00", "cancelled_at": None,
            "daily_number": 18, "number_date": "2026-11-23",
        }
        self.post = AsyncMock(return_value=httpx2.Response(201, json=self.body))
        client = httpx2.AsyncClient(base_url="http://test.invalid", transport=httpx2.MockTransport(self.post))
        with patch("api.httpx2.AsyncClient", return_value=client):
            api = APIClient("http://test.invalid")
        self.addAsyncCleanup(api.close)
        self.queue = asyncio.Queue()
        self.queue.put_nowait(self.record_uuid)
        self.worker = RecordUploadWorker(api, self.queue, "test-token")
        self.worker.retry_delay = 0
        self.worker.logger = Mock()

    @asynccontextmanager
    async def running_worker(self):
        task = asyncio.create_task(self.worker.run())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def process_queue(self):
        async with self.running_worker():
            await asyncio.wait_for(self.queue.join(), timeout=2)
        self.assertTrue(self.queue.empty())

    async def assert_requeued(self):
        self.assertEqual(self.queue.qsize(), 1)
        self.assertEqual(self.queue.get_nowait(), self.record_uuid)
        self.queue.task_done()
        await asyncio.wait_for(self.queue.join(), timeout=1)
        self.assertTrue(await Record.pending_uploads().filter(uuid=self.record.uuid).exists())

    async def cancel_while_blocked(self, operation):
        started = asyncio.Event()

        async def block(*args, **kwargs):
            started.set()
            await asyncio.Future()

        operation.side_effect = block
        async with self.running_worker():
            await asyncio.wait_for(started.wait(), timeout=1)
        await self.assert_requeued()

    async def test_cancellation_during_upload_returns_record_to_queue(self):
        await self.cancel_while_blocked(self.post)

    async def test_cancellation_during_local_lookup_returns_record_to_queue(self):
        with patch.object(QuerySet, "get_or_none", new_callable=AsyncMock) as lookup:
            await self.cancel_while_blocked(lookup)
        self.post.assert_not_awaited()

    async def test_cancellation_before_saving_response_returns_record_to_queue(self):
        with patch.object(RecordUploadResult, "create", new_callable=AsyncMock) as save:
            await self.cancel_while_blocked(save)
        self.post.assert_awaited_once()

    async def test_cancellation_during_retry_delay_returns_record_once(self):
        self.post.side_effect = httpx2.ConnectError("offline")
        with patch.object(self.worker, "_wait_before_retry", new_callable=AsyncMock) as delay:
            await self.cancel_while_blocked(delay)

    async def test_auth_rejection_preserves_record_for_next_uploader(self):
        for status in (401, 403):
            with self.subTest(status=status):
                self.post.return_value = httpx2.Response(status)
                with self.assertRaises(AuthDegradedError):
                    await self.worker.run()
                await self.assert_requeued()
                self.queue.put_nowait(self.record_uuid)

    async def test_server_and_proxy_errors_retry_then_confirm(self):
        self.post.side_effect = [
            httpx2.Response(503),
            httpx2.Response(400, text="<html>Bad request</html>"),
            httpx2.Response(201, json=self.body),
        ]
        await self.process_queue()
        self.assertEqual(self.post.await_count, 3)
        result = await RecordUploadResult.get(record_id=self.record_uuid)
        self.assertEqual(result.daily_number, 18)
        self.assertIsNone(result.error)

    async def test_lost_response_retries_original_and_preserves_cancelled_duplicate_receipt(self):
        cancelled_at = "2026-11-24T02:00:00+09:00"
        self.post.side_effect = [
            httpx2.ReadTimeout("Response lost after server save"),
            httpx2.Response(200, json=self.body | {"cancelled_at": cancelled_at}),
        ]
        await self.process_queue()
        payloads = [json.loads(call.args[0].content) for call in self.post.await_args_list]
        original = {
            "uuid": self.record_uuid, "station_id": 7, "rfid_card_uid": "test-card",
            "weight": 85, "measured_at": self.record.measured_at.isoformat(),
        }
        self.assertEqual(payloads, [original, original])
        result = await RecordUploadResult.get(record_id=self.record_uuid)
        self.assertEqual(result.weight, 58)
        self.assertEqual(result.measured_at, datetime.fromisoformat(self.body["measured_at"]))
        self.assertEqual(result.cancelled_at, datetime.fromisoformat(cancelled_at))
        self.assertEqual((result.daily_number, result.number_date), (18, date(2026, 11, 23)))
        self.assertIsNone(result.error)
        self.assertEqual((await Record.get(uuid=self.record.uuid)).weight, 85)

    async def test_legacy_success_completes_without_a_number(self):
        body = self.body.copy()
        del body["daily_number"], body["number_date"]
        self.post.return_value = httpx2.Response(200, json=body)
        self.queue.put_nowait(self.record_uuid)
        await self.process_queue()
        self.post.assert_awaited_once()
        result = await RecordUploadResult.get(record_id=self.record_uuid)
        self.assertEqual(result.weight, 58)
        self.assertIsNone(result.daily_number)
        self.assertIsNone(result.number_date)
        self.assertIsNone(result.error)

    async def assert_review(self, status, body, expected_error):
        self.post.return_value = httpx2.Response(status, json=body)
        self.queue.put_nowait(self.record_uuid)
        await self.process_queue()
        self.post.assert_awaited_once()
        result = await RecordUploadResult.get(record_id=self.record.uuid)
        self.assertIn(expected_error, result.error)
        self.assertIsNone(result.weight)
        self.assertIsNone(result.daily_number)
        self.assertEqual((await Record.get(uuid=self.record.uuid)).weight, 85)

    async def test_validation_error_marks_one_record_for_review_and_next_upload_succeeds(self):
        following = await Record.create(
            uuid=uuid.uuid4(), rfid_card_uid="next-card", station_id=8,
            weight=32, measured_at=self.record.measured_at,
        )
        self.queue.put_nowait(str(following.uuid))
        self.post.side_effect = [
            httpx2.Response(400, json={"uuid": ["conflict"]}),
            httpx2.Response(201, json=self.body | {
                "uuid": str(following.uuid), "station_id": 8, "weight": 32,
            }),
        ]
        await self.process_queue()
        rejected = await RecordUploadResult.get(record_id=self.record_uuid)
        self.assertIn("conflict", rejected.error)
        self.assertIsNone(rejected.daily_number)
        self.assertEqual((await Record.get(uuid=self.record_uuid)).weight, 85)
        accepted = await RecordUploadResult.get(record_id=following.uuid)
        self.assertIsNone(accepted.error)
        self.assertEqual(accepted.weight, 32)

    async def test_unprocessable_record_marks_record_for_review(self):
        await self.assert_review(422, {"station_id": ["unknown station"]}, "unknown station")

    async def test_mismatched_response_marks_original_for_review(self):
        await self.assert_review(200, self.body | {"station_id": 8}, "station_id")

    async def test_database_write_failure_retries_same_uuid(self):
        create_result = RecordUploadResult.create
        attempts = 0

        async def unavailable_once(**fields):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OperationalError("database is locked")
            return await create_result(**fields)

        with patch.object(RecordUploadResult, "create", side_effect=unavailable_once):
            await self.process_queue()
        self.assertEqual(attempts, 2)
        posted_uuids = [json.loads(call.args[0].content)["uuid"] for call in self.post.await_args_list]
        self.assertEqual(posted_uuids, [self.record_uuid, self.record_uuid])
        self.assertEqual((await RecordUploadResult.get(record_id=self.record_uuid)).daily_number, 18)

    async def test_error_reported_after_database_commit_does_not_repeat_upload(self):
        create_result = RecordUploadResult.create

        async def commit_then_fail(**fields):
            await create_result(**fields)
            raise OperationalError("commit acknowledgement lost")

        with patch.object(RecordUploadResult, "create", side_effect=commit_then_fail):
            await self.process_queue()
        self.post.assert_awaited_once()
        self.assertEqual(await RecordUploadResult.all().count(), 1)
        self.assertEqual((await RecordUploadResult.get(record_id=self.record_uuid)).daily_number, 18)

    async def test_missing_record_does_not_requeue_or_upload(self):
        await self.record.delete()
        await self.process_queue()
        self.post.assert_not_awaited()
