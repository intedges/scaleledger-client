from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch
import uuid

from tortoise import Tortoise
from tortoise.backends.sqlite.client import SqliteTransactionWrapper
from tortoise.connection import get_connection
from tortoise.exceptions import IntegrityError

from main import HeadlessClient
from models import Gateway, Producer, Record, RFIDCard, Species, WeighingStation
from schema import migrate_optional_card_species
from settings_sync import SettingsSnapshot


OLD_CARD_SCHEMA = '''CREATE TABLE "rfid_card" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "uuid" CHAR(36) NOT NULL UNIQUE,
    "uid" VARCHAR(50) NOT NULL UNIQUE,
    "is_active" INT NOT NULL,
    "issued_at" TIMESTAMP NOT NULL,
    "producer_id" INT NOT NULL REFERENCES "producer" ("id") ON DELETE CASCADE,
    "species_id" INT NOT NULL REFERENCES "species" ("id") ON DELETE CASCADE
)'''


class OptionalCardSpeciesMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.db_path = self.root / "existing.sqlite3"
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(OLD_CARD_SCHEMA)
            connection.commit()
        self.enterContext(patch("main.get_ip_address", return_value="127.0.0.1"))
        await Tortoise.init(config={
            "connections": {"default": {
                "engine": "tortoise.backends.sqlite",
                "credentials": {"file_path": str(self.db_path)},
            }},
            "apps": {"models": {"models": ["models"], "default_connection": "default"}},
        })
        await Tortoise.generate_schemas()
        self.now = datetime(2026, 10, 27, 5, 6, 7, 123456, tzinfo=UTC)
        self.responses = {
            "gateway": {"id": 1, "name": "Saved gateway"},
            "stations": [{
                "id": 7, "gateway": 1, "name": "Saved station", "description": "",
                "serial_port": "COM7", "serial_description": "", "serial_location": "",
                "serial_number": "", "serial_manufacturer": "",
            }],
            "species": [{"id": 2, "name": "Saved species"}],
            "producers": [{
                "id": 3, "uuid": str(uuid.uuid4()), "name": "Saved producer",
                "phone": None, "created_at": self.now.isoformat(),
            }],
            "rfid_cards": [{
                "id": 4, "uuid": str(uuid.uuid4()), "uid": "12345678", "producer": 3,
                "species": 2, "is_active": False, "issued_at": self.now.isoformat(),
            }],
        }
        await SettingsSnapshot.from_responses(**self.responses).persist()
        self.record = await Record.create(
            uuid=uuid.uuid4(), station_id=7, rfid_card_uid="12345678", weight=123,
            measured_at=self.now,
        )

    async def asyncTearDown(self):
        await Tortoise.close_connections()

    async def stored_data(self):
        return {
            model.__name__: await model.all().order_by(model._meta.pk_attr).values()
            for model in (Gateway, WeighingStation, Species, Producer, RFIDCard, Record)
        }

    async def schema_objects(self):
        return await get_connection("default").execute_query_dict(
            "SELECT type, name, sql FROM sqlite_master WHERE tbl_name = 'rfid_card' ORDER BY name"
        )

    async def species_is_required(self):
        columns = await get_connection("default").execute_query_dict('PRAGMA table_info("rfid_card")')
        return next(column["notnull"] for column in columns if column["name"] == "species_id")

    async def foreign_keys_enabled(self):
        rows = await get_connection("default").execute_query_dict("PRAGMA foreign_keys")
        return rows[0]["foreign_keys"]

    async def test_migration_preserves_rows_indexes_triggers_and_sequence(self):
        connection = get_connection("default")
        await connection.execute_query("UPDATE sqlite_sequence SET seq = 500 WHERE name = 'rfid_card'")
        await connection.execute_query('CREATE INDEX "card_active_uid" ON "rfid_card" ("uid") WHERE "is_active" = 1')
        await connection.execute_query('CREATE TABLE "card_changes" ("uid" TEXT NOT NULL)')
        trigger_sql = '''CREATE TRIGGER "card_uid_changed" AFTER UPDATE OF "uid" ON "rfid_card"
            BEGIN INSERT INTO "card_changes" ("uid") VALUES (NEW."uid"); END'''
        await connection.execute_query(trigger_sql)
        before = await self.stored_data()
        objects = {row["name"]: row for row in await self.schema_objects() if row["type"] != "table"}

        self.assertTrue(await migrate_optional_card_species())

        self.assertEqual(await self.stored_data(), before)
        self.assertEqual(await self.species_is_required(), 0)
        self.assertEqual(await self.foreign_keys_enabled(), 1)
        self.assertEqual(await connection.execute_query_dict("PRAGMA foreign_key_check"), [])
        self.assertEqual(
            {row["name"]: row for row in await self.schema_objects() if row["type"] != "table"},
            objects,
        )
        self.assertEqual(await connection.execute_query_dict(
            "SELECT seq FROM sqlite_sequence WHERE name = 'rfid_card'"
        ), [{"seq": 500}])
        await RFIDCard.filter(id=4).update(uid="87654321")
        self.assertEqual(await connection.execute_query_dict('SELECT uid FROM "card_changes"'), [{"uid": "87654321"}])
        card = await RFIDCard.create(
            uuid=uuid.uuid4(), uid="new-card", producer_id=3, species_id=None,
            is_active=True, issued_at=self.now,
        )
        self.assertEqual(card.id, 501)
        for values in (
            {"uuid": uuid.uuid4(), "uid": "new-card", "producer_id": 3},
            {"uuid": card.uuid, "uid": "other-card", "producer_id": 3},
            {"uuid": uuid.uuid4(), "uid": "missing-producer", "producer_id": 999},
            {"uuid": uuid.uuid4(), "uid": "missing-species", "producer_id": 3, "species_id": 999},
        ):
            with self.subTest(values=values), self.assertRaises(IntegrityError):
                await RFIDCard.create(is_active=True, issued_at=self.now, **values)

    async def test_startup_migrates_existing_database_and_null_sync_survives_reopen(self):
        before = await self.stored_data()
        await Tortoise.close_connections()
        client = HeadlessClient(base_url=None, db_path=self.db_path)
        client.logger = Mock()
        client.station_manager = Mock()
        try:
            await client.setup()
            self.assertEqual(await self.stored_data(), before)
            self.assertEqual(await self.species_is_required(), 0)
            self.responses["rfid_cards"][0].update(species=None, is_active=True)
            self.responses["species"] = []
            await SettingsSnapshot.from_responses(**self.responses).persist()
        finally:
            await client.close()

        restarted = HeadlessClient(base_url=None, db_path=self.db_path)
        restarted.logger = Mock()
        restarted.station_manager = Mock()
        try:
            await restarted.setup()
            await restarted.restore_local_state()

            card = await RFIDCard.get(id=4)
            self.assertIsNone(card.species_id)
            info = restarted.market_cache.get_rfid_info("12345678")
            self.assertTrue(info.is_active)
            self.assertEqual((info.producer_name, info.species_name), ("Saved producer", ""))
            self.assertEqual(restarted.market_cache.gateway_name, "Saved gateway")
            self.assertEqual(restarted.upload_queue.get_nowait(), str(self.record.uuid))
            self.assertEqual((await Record.get(uuid=self.record.uuid)).measured_at, self.now)
            restarted.station_manager.sync.assert_called_once()
        finally:
            await restarted.close()

    async def test_repeated_migration_does_not_change_schema_or_data(self):
        self.assertTrue(await migrate_optional_card_species())
        before = await self.stored_data()
        objects = await self.schema_objects()

        self.assertFalse(await migrate_optional_card_species())

        self.assertEqual(await self.stored_data(), before)
        self.assertEqual(await self.schema_objects(), objects)

    async def test_failure_after_drop_rolls_back_original_table_and_restores_foreign_keys(self):
        before = await self.stored_data()
        objects = await self.schema_objects()
        original_query = SqliteTransactionWrapper.execute_query

        async def fail_rename(connection, query, values=None):
            if "RENAME TO" in query.upper():
                raise RuntimeError("injected rename failure")
            return await original_query(connection, query, values)

        for foreign_keys in (0, 1):
            with self.subTest(foreign_keys=foreign_keys):
                await get_connection("default").execute_query(f"PRAGMA foreign_keys = {foreign_keys}")
                with patch.object(SqliteTransactionWrapper, "execute_query", new=fail_rename):
                    with self.assertRaisesRegex(RuntimeError, "injected rename failure"):
                        await migrate_optional_card_species()

                self.assertEqual(await self.stored_data(), before)
                self.assertEqual(await self.schema_objects(), objects)
                self.assertEqual(await self.species_is_required(), 1)
                self.assertEqual(await self.foreign_keys_enabled(), foreign_keys)
                self.assertEqual(await get_connection("default").execute_query_dict(
                    "SELECT name FROM sqlite_master WHERE name = 'rfid_card_optional_species'"
                ), [])

    async def test_foreign_key_check_failure_rolls_back_and_preserves_disabled_setting(self):
        connection = get_connection("default")
        await connection.execute_query("PRAGMA foreign_keys = OFF")
        await connection.execute_query('UPDATE "rfid_card" SET "producer_id" = 999')
        before = await self.stored_data()
        objects = await self.schema_objects()

        with self.assertRaises(ValueError):
            await migrate_optional_card_species()

        self.assertEqual(await self.stored_data(), before)
        self.assertEqual(await self.schema_objects(), objects)
        self.assertEqual(await self.species_is_required(), 1)
        self.assertEqual(await self.foreign_keys_enabled(), 0)

    async def test_unknown_column_is_rejected_without_changing_existing_database(self):
        connection = get_connection("default")
        await connection.execute_query('ALTER TABLE "rfid_card" ADD COLUMN "custom_note" TEXT')
        await connection.execute_query('UPDATE "rfid_card" SET "custom_note" = ?', ["keep this note"])
        before = await connection.execute_query_dict('SELECT * FROM "rfid_card"')
        objects = await self.schema_objects()

        with self.assertRaises(ValueError):
            await migrate_optional_card_species()

        self.assertEqual(await connection.execute_query_dict('SELECT * FROM "rfid_card"'), before)
        self.assertEqual(await self.schema_objects(), objects)
        self.assertEqual(await self.species_is_required(), 1)
        self.assertEqual(await self.foreign_keys_enabled(), 1)

    async def test_startup_logs_schema_failure_and_preserves_existing_data(self):
        await get_connection("default").execute_query(
            'ALTER TABLE "rfid_card" ADD COLUMN "custom_note" TEXT'
        )
        before = await self.stored_data()
        await Tortoise.close_connections()
        client = HeadlessClient(base_url=None, db_path=self.db_path)
        client.logger = Mock()
        try:
            with self.assertRaisesRegex(ValueError, "Unsupported RFID card schema"):
                await client.setup()

            client.logger.exception.assert_called_once_with(
                "sys.db.schema.upgrade_failed", db_path=str(self.db_path),
            )
            self.assertEqual(await self.stored_data(), before)
            self.assertEqual(await self.species_is_required(), 1)
        finally:
            await client.close()


if __name__ == "__main__":
    unittest.main()
