import asyncio
from contextlib import closing, suppress
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

from tortoise import connections

from events import WeighingCompletedEvent
from main import HeadlessClient
from models import Record
from suwol1000 import (
    DisplayRequestPacket, PrinterRequestPacket, ResponsePacket, VoiceCode,
    WeighingStationWorker, WorkerState,
)
from tests.helpers import prepare_vehicle
from workers import RecordUploadWorker


class RecordingScale:
    port = "save-confirmation-test"

    def __init__(self):
        self.requests = []

    def connect(self):
        pass

    def disconnect(self):
        pass

    def interrupt(self):
        pass

    def send_and_receive(self, request):
        self.requests.append(request)
        return ResponsePacket(
            # Changed weight and another tag must not replace a pending record.
            weight_value=Decimal("99.8"), rfid_card_uid="87654321",
            voice_code=getattr(request, "voice_code", VoiceCode.NONE),
        )


class SaveConfirmationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Preserve real SQLite files for inspection; TMPDIR selects the artifact root.
        self.root = Path(tempfile.mkdtemp(prefix="scaleledger-save-confirmation-"))
        self.db_path = self.root / "records.sqlite3"
        self.enterContext(patch("main.get_logger", return_value=Mock()))
        self.enterContext(patch("suwol1000.get_logger", return_value=Mock()))
        self.client = HeadlessClient(None, self.db_path)
        await self.client.setup()
        self.client.main_loop = asyncio.get_running_loop()
        self.tasks = []
        self.locks = []
        self.threads = []
        self.submissions = []
        self.scale = RecordingScale()
        self.worker = WeighingStationWorker(
            self.scale, station_id=7, on_event=self.submit, polling_interval=0,
            receipt_builder=lambda event: b"receipt",
            vehicle_threshold=Decimal("1"),
        )
        self.worker.save_retry_interval = 0
        self.worker.last_plate = "12345678"
        self.worker.last_weight = Decimal("12.9")

    async def asyncTearDown(self):
        for connection in self.locks:
            connection.rollback()
            connection.close()
        self.worker.stop()
        for thread in self.threads:
            if thread.ident is not None:
                thread.join(timeout=2)
        for task in self.tasks:
            if not task.done():
                task.cancel()
        for task in self.tasks:
            with suppress(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=3)
        await self.client.close()

    def submit(self, event):
        acknowledgment = self.client.handle_hardware_event(event)
        self.submissions.append((event, acknowledgment))
        return acknowledgment

    def start(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    def start_consumer(self):
        return self.start(self.client.event_consumer_worker())

    async def drain(self):
        # handle_hardware_event schedules the queue insertion on the event loop.
        await asyncio.sleep(0)
        await asyncio.wait_for(self.client.event_queue.join(), timeout=3)

    async def start_measurement(self):
        prepare_vehicle(self.worker)
        self.assertEqual(self.worker.measure(), WorkerState.SAVE)
        await asyncio.sleep(0)
        return self.submissions[-1]

    async def lock_database(self, *, timeout_ms):
        await connections.get("default").execute_query(f"PRAGMA busy_timeout={timeout_ms}")
        connection = sqlite3.connect(self.db_path)
        connection.execute("BEGIN IMMEDIATE")
        self.locks.append(connection)
        return connection

    def assert_no_completion(self):
        self.assertFalse(any(isinstance(request, PrinterRequestPacket) for request in self.scale.requests))
        for request in self.scale.requests:
            if isinstance(request, DisplayRequestPacket):
                self.assertNotIn(request.voice_code, (VoiceCode.WEIGHT_COMPLETE, VoiceCode.THANK_YOU))
                self.assertFalse(request.green_blink and not request.red_blink)

    def assert_committed_record(self, event):
        # An independent connection cannot see the ORM's uncommitted insert.
        with closing(sqlite3.connect(self.db_path)) as connection:
            rows = connection.execute(
                "SELECT uuid, station_id, rfid_card_uid, weight, measured_at FROM record"
            ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(uuid.UUID(rows[0][0]), uuid.UUID(event.uuid))
        self.assertEqual(rows[0][1:4], (event.station_id, event.rfid_card_uid, event.weight))

    async def finish_save(self):
        for _ in range(10):
            state = self.worker.save()
            await self.drain()
            if state == WorkerState.COMPLETE:
                self.assertEqual(self.worker.complete(), WorkerState.PRINT)
                return
            self.assertEqual(state, WorkerState.SAVE)
        self.fail("저장 확인 후 출력 상태로 전환하지 않았습니다.")

    async def test_delayed_sqlite_commit_blocks_completion_and_ignores_new_tags(self):
        self.start_consumer()
        self.worker.save_warning_interval = 0
        lock = await self.lock_database(timeout_ms=2000)
        event, acknowledgment = await self.start_measurement()
        await asyncio.sleep(0.03)

        for _ in range(3):
            self.assertEqual(self.worker.save(), WorkerState.SAVE)
        self.assertFalse(acknowledgment.done())
        self.assertEqual(len(self.submissions), 1)
        self.assertIs(self.worker.last_event, event)
        self.assertEqual((event.weight, event.rfid_card_uid), (12, "12345678"))
        self.assertEqual(self.worker.get_live_weight().weight, Decimal("99.8"))
        pending = [
            call for call in self.worker.logger.warning.call_args_list
            if call.args[0] == "biz.weighing.local_save_pending"
        ]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].kwargs["event_id"], event.uuid)
        self.assert_no_completion()

        lock.rollback()
        await self.drain()
        self.assertIsNone(acknowledgment.result(timeout=0))
        self.assert_committed_record(event)
        await self.finish_save()
        self.worker.print()

        self.assertEqual(len(self.submissions), 1)
        self.assertEqual(sum(isinstance(request, PrinterRequestPacket) for request in self.scale.requests), 1)
        self.assertEqual(sum(getattr(request, "voice_code", None) == VoiceCode.WEIGHT_COMPLETE for request in self.scale.requests), 1)

    async def test_sqlite_lock_failure_retries_the_original_event(self):
        self.start_consumer()
        lock = await self.lock_database(timeout_ms=1)
        event, acknowledgment = await self.start_measurement()
        await self.drain()
        self.assertIn("locked", str(acknowledgment.exception(timeout=0)))
        self.assertEqual(self.worker.save(), WorkerState.SAVE)
        self.assert_no_completion()
        self.assertTrue(self.client.upload_queue.empty())

        lock.rollback()
        await self.finish_save()

        self.assertGreaterEqual(len(self.submissions), 2)
        self.assertTrue(all(submitted is event for submitted, _ in self.submissions))
        self.assertTrue(all(submitted.timestamp == event.timestamp for submitted, _ in self.submissions))
        self.assertEqual((event.station_id, event.weight, event.rfid_card_uid), (7, 12, "12345678"))
        self.assert_committed_record(event)

    async def test_sqlite_readonly_failure_does_not_ack_or_complete(self):
        self.start_consumer()
        database = connections.get("default")
        await database.execute_query("PRAGMA query_only=ON")
        event, acknowledgment = await self.start_measurement()
        await self.drain()

        self.assertIn("readonly", str(acknowledgment.exception(timeout=0)))
        self.assertEqual(await Record.all().count(), 0)
        self.assertEqual(self.worker.save(), WorkerState.SAVE)
        self.assert_no_completion()
        self.assertTrue(self.client.upload_queue.empty())

        await database.execute_query("PRAGMA query_only=OFF")
        await self.finish_save()
        self.assertTrue(all(submitted is event for submitted, _ in self.submissions))
        self.assert_committed_record(event)

    async def test_existing_identical_uuid_is_confirmed_without_a_second_record(self):
        self.start_consumer()
        event = WeighingCompletedEvent(station_id=7, rfid_card_uid="12345678", weight=12)
        await Record.create(
            uuid=event.uuid, station_id=event.station_id, rfid_card_uid=event.rfid_card_uid,
            weight=event.weight, measured_at=event.timestamp,
        )

        acknowledgment = self.submit(event)
        await self.drain()

        self.assertIsNone(acknowledgment.result(timeout=0))
        self.assert_committed_record(event)
        self.assertEqual(self.client.upload_queue.get_nowait(), event.uuid)

    async def test_committed_record_with_failed_response_is_confirmed_on_retry(self):
        self.start_consumer()
        create_record = Record.create
        calls = 0

        async def commit_then_fail(**kwargs):
            nonlocal calls
            calls += 1
            record = await create_record(**kwargs)
            if calls == 1:
                raise RuntimeError("response lost after SQLite commit")
            return record

        with patch("main.Record.create", side_effect=commit_then_fail):
            event, acknowledgment = await self.start_measurement()
            await self.drain()
            self.assertIn("after SQLite commit", str(acknowledgment.exception(timeout=0)))
            self.assert_committed_record(event)
            self.assertTrue(self.client.upload_queue.empty())
            self.assertEqual(self.worker.save(), WorkerState.SAVE)
            self.assert_no_completion()

            await self.finish_save()

        self.assertEqual(calls, 2)
        self.assertTrue(all(submitted is event for submitted, _ in self.submissions))
        self.assert_committed_record(event)
        self.assertEqual(self.client.upload_queue.get_nowait(), event.uuid)

    async def test_existing_uuid_with_different_source_is_not_confirmed(self):
        self.start_consumer()
        event = WeighingCompletedEvent(station_id=7, rfid_card_uid="12345678", weight=12)
        await Record.create(
            uuid=event.uuid, station_id=event.station_id, rfid_card_uid=event.rfid_card_uid,
            weight=event.weight, measured_at=event.timestamp,
        )
        for changed in (
            replace(event, station_id=8),
            replace(event, weight=13),
            replace(event, rfid_card_uid="87654321"),
            replace(event, timestamp=event.timestamp.replace(year=2025)),
        ):
            with self.subTest(event=changed):
                acknowledgment = self.submit(changed)
                await self.drain()
                self.assertIsNotNone(acknowledgment.exception(timeout=0))
                self.assertTrue(self.client.upload_queue.empty())
                self.assert_committed_record(event)

    async def test_measurements_from_two_stations_keep_their_source_ids(self):
        self.start_consumer()
        events = [
            WeighingCompletedEvent(station_id=station_id, rfid_card_uid="12345678", weight=12)
            for station_id in (7, 8)
        ]
        acknowledgments = [self.submit(event) for event in events]
        await self.drain()

        for event, acknowledgment in zip(events, acknowledgments):
            self.assertIsNone(acknowledgment.result(timeout=0))
            self.assertEqual((await Record.get(uuid=event.uuid)).station_id, event.station_id)
        self.assertEqual(await Record.all().count(), 2)
        self.assertEqual(
            {self.client.upload_queue.get_nowait() for _ in events},
            {event.uuid for event in events},
        )

    async def test_offline_local_commit_allows_completion_without_upload(self):
        self.start_consumer()
        event, acknowledgment = await self.start_measurement()
        await self.drain()
        self.assertIsNone(self.client.api_client)
        self.assertIsNone(acknowledgment.result(timeout=0))
        self.assert_committed_record(event)

        await self.finish_save()
        self.worker.print()

        self.assertEqual(self.client.upload_queue.get_nowait(), event.uuid)
        self.assertEqual(sum(isinstance(request, PrinterRequestPacket) for request in self.scale.requests), 1)
        self.assertTrue(await Record.filter(uuid=event.uuid).exists())

    async def test_upload_purge_before_station_observes_ack_does_not_resave(self):
        self.start_consumer()
        api = Mock(create_record=AsyncMock(return_value={}))
        uploader = RecordUploadWorker(api, self.client.upload_queue, "cached-token")
        uploader.logger = Mock()
        self.start(uploader.run())
        event, acknowledgment = await self.start_measurement()
        await self.drain()
        await asyncio.wait_for(self.client.upload_queue.join(), timeout=2)
        self.assertIsNone(acknowledgment.result(timeout=0))
        self.assertFalse(await Record.filter(uuid=event.uuid).exists())

        await self.finish_save()
        self.worker.print()

        self.assertEqual(len(self.submissions), 1)
        api.create_record.assert_awaited_once()
        self.assertFalse(await Record.filter(uuid=event.uuid).exists())
        self.assertEqual(sum(isinstance(request, PrinterRequestPacket) for request in self.scale.requests), 1)

    async def test_stop_during_locked_save_joins_station_and_drains_late_commit(self):
        lock = await self.lock_database(timeout_ms=2000)
        ready = asyncio.Event()
        stopped = asyncio.Event()
        loop = asyncio.get_running_loop()
        self.client.setup = AsyncMock()
        self.client.restore_local_state = AsyncMock(side_effect=ready.set)
        self.worker.polling_interval = 0.001
        prepare_vehicle(self.worker)
        self.worker.state = WorkerState.MEASURE
        thread = threading.Thread(target=self.worker.run, daemon=True)
        self.threads.append(thread)

        def stop_station():
            self.worker.stop()
            thread.join(timeout=1)
            loop.call_soon_threadsafe(stopped.set)

        self.client.station_manager.stop_all = stop_station
        running = self.start(self.client.run())
        await asyncio.wait_for(ready.wait(), timeout=1)
        thread.start()
        for _ in range(100):
            if self.submissions:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(len(self.submissions), 1)
        event, acknowledgment = self.submissions[0]
        await asyncio.sleep(0.03)
        self.assertFalse(acknowledgment.done())

        running.cancel()
        await asyncio.wait_for(stopped.wait(), timeout=1)
        self.assertFalse(thread.is_alive(), "저장 ACK 대기가 station 종료를 가로막았습니다.")
        self.assertFalse(running.done())
        self.assertFalse(acknowledgment.cancelled())
        self.assert_no_completion()

        lock.rollback()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(running, timeout=2)
        self.assertIsNone(acknowledgment.result(timeout=0))
        self.assert_committed_record(event)
        await self.drain()
