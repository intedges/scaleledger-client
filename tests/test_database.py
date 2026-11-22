import asyncio
from contextlib import chdir, suppress
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

from api import RecordReceiptDTO
from events import WeighingCompletedEvent
from main import HeadlessClient
from models import Record, RecordUploadResult, WeighingStation
from settings_sync import SettingsSnapshot
from workers import RecordUploadWorker


class DatabaseLocationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        # These characters must remain a file path, not become URL syntax.
        self.program_dir = self.root / "프로그램 #1 100%+client"
        self.program_dir.mkdir()
        self.working_dirs = [self.root / "first-cwd", self.root / "second-cwd"]
        for directory in self.working_dirs:
            directory.mkdir()
        self.enterContext(patch("main.program_dir", return_value=self.program_dir))
        self.enterContext(patch("main.get_ip_address", return_value="127.0.0.1"))

    def make_client(self, db_path: Path | None = None) -> HeadlessClient:
        client = HeadlessClient(base_url="http://127.0.0.1:8000", db_path=db_path)
        client.logger = Mock()
        return client

    async def test_default_database_is_created_and_reused_after_restart_from_another_directory(self):
        db_path = self.program_dir / "data" / "db.sqlite3"
        record_uuid = uuid.uuid4()
        measured_at = datetime(2026, 10, 11, 8, 30, 12, 345678, tzinfo=UTC)
        self.assertFalse(db_path.parent.exists())

        with chdir(self.working_dirs[0]):
            client = self.make_client()
            try:
                await client.setup()
                self.assertTrue(db_path.is_file())
                self.assertEqual(await Record.all().count(), 0)
                await Record.create(
                    uuid=record_uuid, station_id=7, rfid_card_uid="12345678", weight=31,
                    measured_at=measured_at,
                )
            finally:
                await client.close()

        with chdir(self.working_dirs[1]):
            restarted = self.make_client()
            try:
                await restarted.setup()
                record = await Record.get(uuid=record_uuid)
                self.assertEqual(record.station_id, 7)
                self.assertEqual(record.weight, 31)
                self.assertEqual(record.measured_at, measured_at)
                self.assertEqual(await Record.all().count(), 1)
            finally:
                await restarted.close()

        for directory in self.working_dirs:
            self.assertEqual(list(directory.iterdir()), [])
        self.assertFalse((self.program_dir / "db.sqlite3").exists())

    async def test_explicit_database_path_does_not_create_the_default_database(self):
        db_path = self.root / "별도 DB #2 100%+" / "custom.sqlite3"
        with chdir(self.working_dirs[0]):
            client = self.make_client(db_path=db_path)
            try:
                await client.setup()
                self.assertTrue(db_path.is_file())
                self.assertEqual(await Record.all().count(), 0)
            finally:
                await client.close()

        self.assertFalse((self.program_dir / "data").exists())
        self.assertEqual(list(self.working_dirs[0].iterdir()), [])

    async def test_restart_and_settings_replacement_upload_the_original_station(self):
        db_path = self.root / "offline.sqlite3"
        event = WeighingCompletedEvent(
            station_id=7, rfid_card_uid="12345678", weight=31,
            timestamp=datetime(2026, 11, 23, 1, tzinfo=UTC),
        )

        def snapshot(station_id):
            return SettingsSnapshot.from_responses(
                gateway={"id": 1, "name": "Gateway"},
                stations=[{
                    "id": station_id, "gateway": 1, "name": f"Station {station_id}",
                    "description": "", "serial_port": f"COM{station_id}",
                    "serial_description": "", "serial_location": "",
                    "serial_number": "", "serial_manufacturer": "",
                }],
                species=[], producers=[], rfid_cards=[],
            )

        client = HeadlessClient(base_url=None, db_path=db_path)
        client.logger = Mock()
        try:
            await client.setup()
            await snapshot(7).persist()
            await client.save_record(event)
        finally:
            await client.close()

        restarted = HeadlessClient(base_url=None, db_path=db_path)
        restarted.logger = Mock()
        restarted.station_manager = Mock()
        try:
            await restarted.setup()
            await snapshot(8).persist()
            self.assertEqual(await WeighingStation.all().values_list("id", flat=True), [8])
            self.assertEqual((await Record.get(uuid=event.uuid)).station_id, 7)
            await restarted.restore_local_state()

            receipt = RecordReceiptDTO(
                uuid=uuid.UUID(event.uuid), station_id=7, weight=28,
                measured_at=event.timestamp + timedelta(minutes=5), cancelled_at=None,
                daily_number=18, number_date=date(2026, 11, 23),
            )
            api = Mock(create_record=AsyncMock(return_value=receipt))
            uploader = RecordUploadWorker(api, restarted.upload_queue, "cached-token")
            uploader.logger = Mock()
            uploading = asyncio.create_task(uploader.run())
            try:
                await asyncio.wait_for(restarted.upload_queue.join(), timeout=2)
            finally:
                uploading.cancel()
                with suppress(asyncio.CancelledError):
                    await uploading

            api.create_record.assert_awaited_once()
            uploaded = api.create_record.call_args.kwargs["record"]
            self.assertEqual(
                (str(uploaded.uuid), uploaded.station_id, uploaded.rfid_card_uid,
                 uploaded.weight, uploaded.measured_at),
                (event.uuid, 7, event.rfid_card_uid, event.weight, event.timestamp),
            )
            await snapshot(9).persist()
            original = await Record.get(uuid=event.uuid)
            self.assertEqual(
                (original.station_id, original.weight, original.measured_at),
                (7, event.weight, event.timestamp),
            )
            confirmed = await RecordUploadResult.get(record_id=event.uuid)
            self.assertEqual(
                (confirmed.weight, confirmed.measured_at, confirmed.daily_number, confirmed.number_date),
                (receipt.weight, receipt.measured_at, 18, receipt.number_date),
            )
            self.assertIsNone(confirmed.error)
        finally:
            await restarted.close()

    async def test_restart_queues_only_records_without_a_result(self):
        db_path = self.root / "upload-results.sqlite3"
        client = self.make_client(db_path)
        pending, confirmed, rejected = [
            WeighingCompletedEvent(station_id=7, rfid_card_uid="12345678", weight=31)
            for _ in range(3)
        ]
        try:
            await client.setup()
            for event in (pending, confirmed, rejected):
                await client.save_record(event)
            await RecordUploadResult.create(
                record_id=confirmed.uuid, weight=29, measured_at=confirmed.timestamp,
                cancelled_at=confirmed.timestamp + timedelta(hours=1),
                daily_number=1, number_date=confirmed.timestamp.date(),
            )
            await RecordUploadResult.create(record_id=rejected.uuid, error="HTTP 422")
        finally:
            await client.close()

        restarted = self.make_client(db_path)
        try:
            await restarted.setup()
            await restarted.restore_local_state()
            self.assertEqual(restarted.upload_queue.get_nowait(), pending.uuid)
            self.assertTrue(restarted.upload_queue.empty())
            self.assertEqual(await Record.all().count(), 3)
            result = await RecordUploadResult.get(record_id=confirmed.uuid)
            self.assertEqual(result.cancelled_at, confirmed.timestamp + timedelta(hours=1))
            review = await RecordUploadResult.get(record_id=rejected.uuid)
            self.assertEqual(review.error, "HTTP 422")
        finally:
            await restarted.close()
