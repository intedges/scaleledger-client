import asyncio
from datetime import UTC, datetime
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import httpx2

from main import HeadlessClient
from models import CachedDeviceConfiguration, Gateway, Producer, Record, RecordUploadResult, RFIDCard, WeighingStation
from printer import ReceiptTemplate
from suwol1000 import PrinterRequestPacket, ResponsePacket, WorkerState
from tests.helpers import receipt_text_and_qrs
from weighing_cycle import CycleState


class FirstReceiptIntegrationTests(unittest.IsolatedAsyncioTestCase):
    """Run the real station threads, HTTP upload worker and file-backed SQLite together."""

    async def asyncSetUp(self):
        self.db = Path(self.enterContext(tempfile.TemporaryDirectory())) / "records.sqlite3"
        self.tasks = []
        self.post = AsyncMock(side_effect=self.confirm)
        self.scales = {i: Mock(port=f"COM{i}", timeout=2, write_timeout=2) for i in (7, 8)}
        for scale in self.scales.values():
            scale.send_and_receive.return_value = ResponsePacket()
        self.enterContext(patch("managers.SerialClient", side_effect=lambda port, **kw: self.scales[int(port[3:])]))
        for module in ("main", "managers", "suwol1000", "workers"):
            self.enterContext(patch(f"{module}.get_logger", return_value=Mock()))
        await self.open_client()
        self.addAsyncCleanup(self.shutdown)
        gateway = await Gateway.create(id=1, name="경매장 PC")
        now = datetime.now(UTC)
        for station_id in self.scales:
            await WeighingStation.create(
                id=station_id, gateway=gateway, name=f"{station_id}호", description="",
                serial_port=f"COM{station_id}", serial_description="", serial_location="",
                serial_number="", serial_manufacturer="",
            )
            producer = await Producer.create(id=station_id, uuid=uuid4(), name=f"생산자 {station_id}", created_at=now)
            await RFIDCard.create(
                id=station_id, uuid=uuid4(), uid=str(station_id) * 8, producer=producer,
                is_active=True, issued_at=now,
            )
        await CachedDeviceConfiguration.create(id=1, vehicle_empty_seconds=0.1, vehicle_ready_seconds=0.1)
        await self.restore_stations()

    async def open_client(self):
        url = "http://receipt.test/"
        http = httpx2.AsyncClient(base_url=url, transport=httpx2.MockTransport(self.post))
        with patch("api.httpx2.AsyncClient", return_value=http):
            self.client = HeadlessClient(url, db_path=self.db)
        self.client.access_token = "test-token"
        self.client.main_loop = asyncio.get_running_loop()
        await self.client.setup()
        self.tasks.append(asyncio.create_task(self.client.event_consumer_worker()))

    async def restore_stations(self):
        await self.client.restore_local_state()
        self.workers = {i: runtime.worker for i, runtime in self.client.station_manager.workers.items()}

    async def shutdown(self):
        self.client.station_manager.stop_all()
        for task in self.tasks:
            task.cancel()
        for completed in await asyncio.gather(*self.tasks, return_exceptions=True):
            if isinstance(completed, BaseException) and not isinstance(completed, asyncio.CancelledError):
                raise completed
        self.tasks.clear()
        await self.client.close()

    def start_uploads(self):
        for service in ("run_settings_sync_worker", "run_active_ws_loop", "run_telemetry_loop"):
            setattr(self.client, service, AsyncMock())
        self.tasks.append(asyncio.create_task(self.client.run_active_loop()))

    async def wait_until(self, condition):
        async with asyncio.timeout(4):
            while not condition():
                await asyncio.sleep(0.005)

    async def tag(self, station_id, weight):
        await self.wait_until(lambda: self.workers[station_id].cycle.state == CycleState.WAIT_ENTRY)
        self.scales[station_id].send_and_receive.return_value = ResponsePacket(
            weight_value=Decimal(weight), rfid_card_uid=str(station_id) * 8,
        )

    @staticmethod
    def confirm(request):
        body = json.loads(request.content)
        return httpx2.Response(201, json={
            "uuid": body["uuid"], "station_id": body["station_id"], "weight": 58,
            "measured_at": body["measured_at"], "cancelled_at": None,
            "daily_number": 100 + body["station_id"], "number_date": "2026-11-22",
        })

    def printed(self, station_id):
        return [
            call.args[0].document_bytes
            for call in self.scales[station_id].send_and_receive.call_args_list
            if isinstance(call.args[0], PrinterRequestPacket)
        ]

    async def assert_receipt(self, station_id, weight, label):
        await self.wait_until(lambda: self.printed(station_id))
        original = await Record.get(station_id=station_id)
        self.assertEqual(original.weight, weight)
        document, = self.printed(station_id)
        self.assertIn(label.encode("cp949"), document)
        self.assertIn(f"{weight} kg".encode(), document)
        text, qrs = receipt_text_and_qrs(document)
        self.assertIn(f"\n{original.uuid}\n", text)
        self.assertEqual(qrs, [{
            "uuid": str(original.uuid), "station_id": station_id,
            "rfid_card_uid": original.rfid_card_uid, "weight": weight,
            "measured_at": original.measured_at.astimezone().isoformat(),
        }])
        return original

    async def test_simultaneous_tags_receive_their_own_numbers_and_original_weights(self):
        self.start_uploads()
        await asyncio.gather(self.tag(7, 85), self.tag(8, 120))
        originals = await asyncio.gather(
            self.assert_receipt(7, 85, "계량번호 107번"),
            self.assert_receipt(8, 120, "계량번호 108번"),
        )
        await asyncio.wait_for(self.client.upload_queue.join(), 4)
        for original in originals:
            station_id = original.station_id
            result = await RecordUploadResult.get(record_id=original.uuid)
            self.assertEqual((result.weight, result.daily_number), (58, 100 + station_id))
            self.assertIn("기준일 2026-11-22".encode("cp949"), self.printed(station_id)[0])

    async def test_stalled_upload_allows_both_stations_to_save_and_print_then_only_updates_db(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def delayed(request):
            started.set()
            await release.wait()
            return self.confirm(request)

        self.post.side_effect = delayed
        self.start_uploads()
        for worker in self.workers.values():
            worker.RECEIPT_WAIT_SECONDS = 0.1
        await self.tag(7, 85)
        await asyncio.wait_for(started.wait(), 4)
        await self.tag(8, 120)
        originals = [
            await self.assert_receipt(7, 85, "계량번호 미확인"),
            await self.assert_receipt(8, 120, "계량번호 미확인"),
        ]
        self.assertEqual(await Record.pending_uploads().count(), 2)
        self.assertEqual(self.post.await_count, 1)
        release.set()
        await asyncio.wait_for(self.client.upload_queue.join(), 4)
        for original in originals:
            station_id = original.station_id
            result = await RecordUploadResult.get(record_id=original.uuid)
            self.assertEqual(result.daily_number, 100 + station_id)
            sequence = self.workers[station_id].get_telemetry().sample_seq
            await self.wait_until(lambda: self.workers[station_id].get_telemetry().sample_seq > sequence)
            self.assertEqual(len(self.printed(station_id)), 1)

    async def test_offline_print_is_recovered_by_uuid_without_reprinting_on_restart(self):
        await self.tag(7, 85)
        original = await self.assert_receipt(7, 85, "계량번호 미확인")
        self.post.assert_not_awaited()
        await self.shutdown()
        await self.open_client()
        await self.restore_stations()
        self.start_uploads()
        await asyncio.wait_for(self.client.upload_queue.join(), 4)
        self.assertEqual(json.loads(self.post.await_args.args[0].content)["uuid"], str(original.uuid))
        self.assertEqual((await RecordUploadResult.get(record_id=original.uuid)).daily_number, 107)
        await self.wait_until(lambda: self.workers[7].get_telemetry().sample_seq >= 2)
        self.assertEqual(await Record.all().count(), 1)
        self.assertEqual(len(self.printed(7)), 1)
        self.assertEqual(self.printed(8), [])

    async def test_shutdown_prevents_print_and_new_weighing_during_slow_settings_cleanup(self):
        # The fixture has already restored the DB and started the stations.
        # Let run() own the consumer and network tasks for this shutdown.
        consumer, = self.tasks
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        self.tasks.clear()
        self.client.setup = AsyncMock()
        self.client.restore_local_state = AsyncMock()
        self.client.prepare_credential = AsyncMock(return_value=True)
        self.client.wait_for_approval = AsyncMock()
        self.client.run_active_ws_loop = AsyncMock()
        self.client.run_telemetry_loop = AsyncMock()
        posting, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def stalled_upload(request):
            posting.set()
            await asyncio.Future()

        async def settings_worker():
            try:
                await asyncio.Future()
            finally:
                # A cancelled settings refresh waits for its sync thread.
                cleaning.set()
                await release.wait()

        self.post.side_effect = stalled_upload
        self.client.run_settings_sync_worker = settings_worker
        self.workers[7].RECEIPT_WAIT_SECONDS = 60
        running = asyncio.create_task(self.client.run())
        try:
            await self.wait_until(lambda: self.workers[8].cycle.state == CycleState.WAIT_ENTRY)
            await self.tag(7, 85)
            await asyncio.wait_for(posting.wait(), 4)
            running.cancel()
            await asyncio.wait_for(cleaning.wait(), 4)
            self.assertTrue(all(worker.stop_event.is_set() for worker in self.workers.values()))
            self.workers[7].RECEIPT_WAIT_SECONDS = 0
            await self.tag(8, 120)
            runtimes = self.client.station_manager.workers.values()
            await self.wait_until(lambda: all(not runtime.thread.is_alive() for runtime in runtimes))
            self.assertEqual(self.printed(7), [])
            self.assertEqual(await Record.all().values_list("station_id", flat=True), [7])
        finally:
            release.set()
            if not running.cancelling():
                running.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(running, 4)

    async def test_upload_recovery_during_offline_receipt_render_can_still_cancel_print(self):
        loop = asyncio.get_running_loop()
        rendering = asyncio.Event()
        release = threading.Event()
        render_original = ReceiptTemplate.render

        def delay_render(receipt):
            loop.call_soon_threadsafe(rendering.set)
            if not release.wait(4):
                raise TimeoutError("offline receipt render was not released")
            return render_original(receipt)

        def cancelled(request):
            record = self.confirm(request).json()
            return httpx2.Response(200, json=record | {"cancelled_at": record["measured_at"]})

        self.post.side_effect = cancelled
        with patch.object(ReceiptTemplate, "render", side_effect=delay_render):
            try:
                await self.tag(7, 85)
                await asyncio.wait_for(rendering.wait(), 4)
                self.start_uploads()
                await asyncio.wait_for(self.client.upload_queue.join(), 4)
                result = await RecordUploadResult.get()
                self.assertIsNotNone(result.cancelled_at)
            finally:
                release.set()
            await self.wait_until(lambda: self.workers[7].state == WorkerState.IDLE)
        self.assertEqual(self.printed(7), [])
        self.assertIsNone(self.workers[7]._completion_at)
        self.assertEqual(self.workers[7].cycle.state, CycleState.LOCKED)
