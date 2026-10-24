import asyncio
from contextlib import chdir, suppress
from datetime import UTC, datetime
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

from events import WeighingCompletedEvent
from main import HeadlessClient
from models import Record, WeighingStation
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
        event = WeighingCompletedEvent(station_id=7, rfid_card_uid="12345678", weight=31)

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

            api = Mock(create_record=AsyncMock(return_value={}))
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
            self.assertEqual(await Record.all().count(), 0)
        finally:
            await restarted.close()
