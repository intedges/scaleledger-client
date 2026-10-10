import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

import httpx

from api import AuthDegradedError
from workers import RecordUploadWorker


class RecordUploadWorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.record = SimpleNamespace(
            uuid=uuid.uuid4(),
            rfid_card_uid="test-card",
            weight=125,
            measured_at=datetime.now(timezone.utc),
            delete=AsyncMock(),
        )
        self.record_uuid = str(self.record.uuid)
        self.lookup = self.enterContext(
            patch("workers.Record.get_or_none", new=AsyncMock(return_value=self.record))
        )
        self.api = SimpleNamespace(create_record=AsyncMock())
        self.queue = asyncio.Queue()
        self.queue.put_nowait(self.record_uuid)
        self.worker = RecordUploadWorker(self.api, self.queue, "test-token")
        self.worker.logger = Mock()

    @staticmethod
    def http_error(status):
        request = httpx.Request("POST", "http://test.invalid/weighing/api/records/")
        response = httpx.Response(status, request=request)
        return httpx.HTTPStatusError("Simulated response", request=request, response=response)

    async def start_worker(self):
        task = asyncio.create_task(self.worker.run())

        async def stop():
            if not task.done():
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.addAsyncCleanup(stop)
        return task

    async def assert_one_pending_record(self):
        self.assertEqual(self.queue.qsize(), 1)
        self.assertEqual(self.queue.get_nowait(), self.record_uuid)
        self.queue.task_done()
        await asyncio.wait_for(self.queue.join(), timeout=1)

    async def cancel_while_blocked(self, operation):
        started = asyncio.Event()

        async def block(*args, **kwargs):
            started.set()
            await asyncio.Future()

        operation.side_effect = block
        task = await self.start_worker()
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await self.assert_one_pending_record()

    async def test_cancellation_during_upload_returns_record_to_queue(self):
        await self.cancel_while_blocked(self.api.create_record)
        self.record.delete.assert_not_awaited()

    async def test_cancellation_during_local_lookup_returns_record_to_queue(self):
        await self.cancel_while_blocked(self.lookup)
        self.api.create_record.assert_not_awaited()

    async def test_cancellation_during_local_delete_returns_record_to_queue(self):
        await self.cancel_while_blocked(self.record.delete)
        self.api.create_record.assert_awaited_once()

    async def test_cancellation_during_retry_delay_returns_record_once(self):
        for error in (httpx.ConnectError("Simulated outage"), self.http_error(500)):
            with self.subTest(error=type(error).__name__):
                self.api.create_record.side_effect = error
                retry_started = asyncio.Event()

                def log(message, **kwargs):
                    if message == "sys.worker.record_upload.requeue":
                        retry_started.set()

                self.worker.logger.info.side_effect = log
                self.worker.retry_delay = 3600
                task = await self.start_worker()
                await asyncio.wait_for(retry_started.wait(), timeout=1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                await self.assert_one_pending_record()
                self.record.delete.assert_not_awaited()
                self.queue.put_nowait(self.record_uuid)
        self.queue.get_nowait()
        self.queue.task_done()

    async def test_auth_rejection_preserves_record_for_next_uploader(self):
        for status in (401, 403):
            with self.subTest(status=status):
                self.api.create_record.side_effect = self.http_error(status)
                with self.assertRaises(AuthDegradedError):
                    await self.worker.run()
                self.record.delete.assert_not_awaited()
                await self.assert_one_pending_record()
                self.queue.put_nowait(self.record_uuid)
        self.queue.get_nowait()
        self.queue.task_done()

    async def test_network_and_server_errors_retry_then_purge_success(self):
        self.worker.retry_delay = 0
        self.api.create_record.side_effect = [
            httpx.ConnectError("Simulated outage"),
            self.http_error(503),
            {},
        ]
        task = await self.start_worker()
        await asyncio.wait_for(self.queue.join(), timeout=1)
        self.assertEqual(self.api.create_record.await_count, 3)
        self.record.delete.assert_awaited_once()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(self.queue.empty())

    async def test_validation_rejection_keeps_db_record_without_requeue(self):
        for status in (400, 422):
            with self.subTest(status=status):
                self.api.create_record.side_effect = self.http_error(status)
                task = await self.start_worker()
                await asyncio.wait_for(self.queue.join(), timeout=1)
                self.record.delete.assert_not_awaited()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertTrue(self.queue.empty())
                self.queue.put_nowait(self.record_uuid)
        self.queue.get_nowait()
        self.queue.task_done()

    async def test_missing_record_does_not_requeue_or_upload(self):
        self.lookup.return_value = None
        task = await self.start_worker()
        await asyncio.wait_for(self.queue.join(), timeout=1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.api.create_record.assert_not_awaited()
        self.assertTrue(self.queue.empty())
