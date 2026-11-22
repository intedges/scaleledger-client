import asyncio
from contextlib import asynccontextmanager, chdir, closing, suppress
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

from tortoise.backends.sqlite.client import SqliteTransactionWrapper
from tortoise.connection import get_connection

from api import RecordReceiptDTO
from events import WeighingCompletedEvent
from main import HeadlessClient
from models import CachedDeviceConfiguration, Gateway, Record, RecordUploadResult, ServerBinding, WeighingStation
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


class DatabaseServerBindingTests(unittest.IsolatedAsyncioTestCase):
    old_url = "http://first.test"
    new_url = "http://second.test"

    @asynccontextmanager
    async def open_client(self, server_url):
        client = HeadlessClient(server_url, self.root / "records.sqlite3", self.root / "credential.json")
        client.logger = Mock()
        client.station_manager = Mock()
        try:
            await client.setup()
            yield client
        finally:
            await client.close()

    async def asyncSetUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.pending = WeighingCompletedEvent(station_id=7, rfid_card_uid="12345678", weight=85)
        async with self.open_client(self.old_url) as client:
            self.assertTrue(await client.prepare_credential())
            await Gateway.create(id=1, name="Previous gateway")
            await client.save_record(self.pending)
            confirmed = WeighingCompletedEvent(station_id=7, rfid_card_uid="12345678", weight=37)
            await client.save_record(confirmed)
            await RecordUploadResult.create(record_id=confirmed.uuid, weight=37, measured_at=confirmed.timestamp)

    async def assert_old_data(self):
        self.assertEqual((await ServerBinding.get(id=1)).server_url, self.old_url)
        self.assertEqual(await Record.all().count(), 2)
        self.assertEqual(await RecordUploadResult.all().count(), 1)
        self.assertTrue(await Gateway.filter(id=1).exists())

    async def assert_new_database(self):
        self.assertEqual((await ServerBinding.get(id=1)).server_url, self.new_url)
        for model in (Gateway, Record, RecordUploadResult):
            self.assertEqual(await model.all().count(), 0, model.__name__)

    async def switch_with_previous_client(self):
        event = WeighingCompletedEvent(station_id=2, rfid_card_uid="new-card", weight=125)
        async with self.open_client(self.new_url) as client:
            # Version 407c892 deletes these on address changes without knowing about server_binding.
            await CachedDeviceConfiguration.all().delete()
            await Record.all().delete()
            await Gateway.all().delete()
            client.credentials.create(self.new_url)
            await SettingsSnapshot.from_responses({"id": 2, "name": "New gateway"}, [], [], [], []).persist()
            await client.save_record(event)
        return event

    async def test_same_server_after_downgrade_keeps_the_previous_clients_new_records(self):
        event = await self.switch_with_previous_client()
        async with self.open_client(self.new_url) as client:
            self.assertTrue(await client.prepare_credential())
            await client.restore_local_state()
            self.assertEqual(client.upload_queue.get_nowait(), event.uuid)
            self.assertEqual((await Record.get(uuid=event.uuid)).weight, event.weight)
            self.assertEqual(client.gateway_id, 2)
            self.assertEqual((await ServerBinding.get(id=1)).server_url, self.new_url)

    async def test_switch_back_after_downgrade_does_not_upload_previous_clients_records(self):
        await self.switch_with_previous_client()
        async with self.open_client(self.old_url) as client:
            self.assertTrue(await client.prepare_credential())
            await client.restore_local_state()
            self.assertTrue(client.upload_queue.empty())
            self.assertEqual(await Record.all().count(), 0)
            self.assertIsNone(client.gateway_id)
            self.assertEqual((await ServerBinding.get(id=1)).server_url, self.old_url)

    async def test_unidentified_records_after_downgrade_remain_offline_across_restarts(self):
        event = await self.switch_with_previous_client()
        (self.root / "credential.json").write_text("{broken", encoding="utf-8")
        for url in (self.old_url, self.new_url):
            async with self.open_client(url) as client:
                self.assertFalse(await client.prepare_credential())
                self.assertIsNone(client.access_token)
                self.assertFalse(await ServerBinding.all().exists())
                self.assertEqual((await Record.get(uuid=event.uuid)).weight, event.weight)

    async def test_standalone_development_binding_with_conflicting_credential_stays_offline(self):
        async with self.open_client(self.new_url) as client:
            await get_connection("default").execute_query("DROP TRIGGER forget_settings_server")
            client.credentials.create(self.new_url)

        for url in (self.new_url, self.old_url):
            async with self.open_client(url) as restarted:
                self.assertFalse(await restarted.prepare_credential())
                self.assertIsNone(restarted.access_token)
                self.assertEqual(restarted.credentials.load().server_url, self.new_url)
                await self.assert_old_data()

    async def test_verified_development_binding_survives_settings_updates_and_expires_on_switch(self):
        async with self.open_client(self.old_url) as client:
            await get_connection("default").execute_query("DROP TRIGGER forget_settings_server")
            await CachedDeviceConfiguration.all().delete()
            self.assertTrue(await client.prepare_credential())
            await SettingsSnapshot.from_responses({"id": 1, "name": "Synced"}, [], [], [], []).persist()
            await self.assert_old_data()
            await CachedDeviceConfiguration.all().delete()
            self.assertFalse(await ServerBinding.all().exists())

    async def test_interrupted_first_binding_keeps_legacy_recovery_available(self):
        async with self.open_client(self.old_url) as client:
            database = get_connection("default")
            await database.execute_query("DROP TRIGGER forget_settings_server")
            await ServerBinding.all().delete()
            await CachedDeviceConfiguration.all().delete()
            (self.root / "credential.json").unlink()
            with patch.object(SqliteTransactionWrapper, "commit", side_effect=sqlite3.OperationalError("I/O error")):
                self.assertFalse(await client.prepare_credential())
            self.assertFalse(await CachedDeviceConfiguration.all().exists())
            self.assertFalse(await ServerBinding.all().exists())
            self.assertEqual(await database.execute_query_dict(
                "SELECT name FROM sqlite_master WHERE name='forget_settings_server'"
            ), [])

        async with self.open_client(self.old_url) as restarted:
            self.assertTrue(await restarted.prepare_credential())
            await self.assert_old_data()

    async def test_damaged_credential_cannot_move_previous_records_to_another_server(self):
        (self.root / "credential.json").write_text("{broken", encoding="utf-8")
        async with self.open_client(self.new_url) as client:
            self.assertTrue(await client.prepare_credential())
            await self.assert_new_database()
            await client.restore_local_state()
            self.assertTrue(client.upload_queue.empty())

    async def test_return_to_bound_server_after_credential_replacement_keeps_its_data(self):
        async with self.open_client(self.old_url) as client:
            other_token = client.credentials.create(self.new_url)
            self.assertTrue(await client.prepare_credential())
            self.assertEqual(client.credentials.load().server_url, self.old_url)
            self.assertNotEqual(client.access_token, other_token)
            await self.assert_old_data()

    async def test_binding_a_legacy_database_retries_when_another_writer_finishes(self):
        async with self.open_client(self.old_url) as client:
            await ServerBinding.all().delete()
            await get_connection("default").execute_query("PRAGMA busy_timeout=0")
            client.retry_interval = 0.01
            previous = client.credentials.load()
            with closing(sqlite3.connect(client.db_path)) as writer:
                writer.execute("BEGIN IMMEDIATE")
                waiting = asyncio.Event()
                client.logger.warning.side_effect = lambda *args, **kwargs: waiting.set()
                async with asyncio.TaskGroup() as tasks:
                    prepared = tasks.create_task(client.prepare_credential())
                    await asyncio.wait_for(waiting.wait(), timeout=1)
                    self.assertIsNone(client.access_token)
                    self.assertFalse(await ServerBinding.all().exists())
                    writer.rollback()
                    self.assertTrue(await asyncio.wait_for(prepared, timeout=1))
            self.assertEqual(client.access_token, previous.credential)
            await self.assert_old_data()

    async def test_locked_switch_commit_retries_with_the_same_replacement_credential(self):
        commit = SqliteTransactionWrapper.commit
        attempts = []
        async with self.open_client(self.new_url) as client:
            client.retry_interval = 0.01

            async def commit_after_lock(connection):
                stored = client.credentials.load()
                if stored.pending is not None:
                    attempts.append(stored.pending.credential)
                    if len(attempts) == 1:
                        error = sqlite3.OperationalError("table is locked")
                        error.sqlite_errorcode = sqlite3.SQLITE_LOCKED_SHAREDCACHE
                        raise error
                await commit(connection)

            with patch.object(SqliteTransactionWrapper, "commit", new=commit_after_lock):
                self.assertTrue(await asyncio.wait_for(client.prepare_credential(), timeout=1))
            self.assertEqual(attempts, [client.access_token, client.access_token])
            await self.assert_new_database()

    async def test_failed_switch_commit_restores_old_data_and_restart_retries_with_new_credential(self):
        async with self.open_client(self.new_url) as client:
            with patch.object(SqliteTransactionWrapper, "commit", side_effect=sqlite3.OperationalError("I/O error")):
                self.assertFalse(await client.prepare_credential())

            stored = client.credentials.load()
            self.assertEqual(stored.server_url, self.old_url)
            self.assertEqual(stored.pending.server_url, self.new_url)
            self.assertIsNone(client.access_token)
            await self.assert_old_data()
            await client.restore_local_state()
            self.assertEqual(client.upload_queue.get_nowait(), self.pending.uuid)

        async with self.open_client(self.new_url) as restarted:
            self.assertTrue(await restarted.prepare_credential())
            self.assertEqual(restarted.access_token, stored.pending.credential)
            await self.assert_new_database()

    async def test_lost_ack_after_switch_commit_leaves_no_records_or_binding(self):
        commit = SqliteTransactionWrapper.commit

        async def commit_then_fail(connection):
            await commit(connection)
            raise sqlite3.OperationalError("acknowledgement lost")

        async with self.open_client(self.new_url) as client:
            with patch.object(SqliteTransactionWrapper, "commit", new=commit_then_fail):
                self.assertFalse(await client.prepare_credential())
            self.assertIsNone(client.access_token)
            for model in (Record, RecordUploadResult, Gateway, ServerBinding):
                self.assertEqual(await model.all().count(), 0, model.__name__)
            stored = client.credentials.load()
            self.assertEqual(stored.server_url, self.old_url)
            self.assertEqual(stored.pending.server_url, self.new_url)

        async with self.open_client(self.new_url) as restarted:
            self.assertTrue(await restarted.prepare_credential())
            self.assertEqual(restarted.access_token, stored.pending.credential)
            await self.assert_new_database()
            await restarted.restore_local_state()
            self.assertTrue(restarted.upload_queue.empty())

    async def test_failed_activation_recovers_staged_credential_after_restart(self):
        async with self.open_client(self.new_url) as client:
            old = client.credentials.load()
            save = client.credentials.save

            def fail_activation(active, pending=None):
                if pending is None:
                    raise OSError("credential locked")
                save(active, pending)

            with patch.object(client.credentials, "save", side_effect=fail_activation):
                self.assertFalse(await client.prepare_credential())
            stored = client.credentials.load()
            self.assertEqual((stored.server_url, stored.credential), (self.old_url, old.credential))
            self.assertIsNone(client.access_token)
            self.assertFalse(await ServerBinding.all().exists())
            self.assertFalse(await Record.all().exists())
            self.assertFalse(await Gateway.all().exists())

        async with self.open_client(self.new_url) as restarted:
            self.assertTrue(await restarted.prepare_credential())
            self.assertEqual(restarted.access_token, stored.pending.credential)
            self.assertEqual(restarted.credentials.load(), stored.pending)
            await self.assert_new_database()
