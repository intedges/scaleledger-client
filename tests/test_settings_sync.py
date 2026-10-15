import asyncio
from copy import deepcopy
from datetime import UTC, datetime
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from tortoise import Tortoise
from tortoise.connection import get_connection
from tortoise.exceptions import IntegrityError, ValidationError

from cache import RFIDInfo
from models import Gateway, Producer, Record, RFIDCard, Species, WeighingStation
from settings_sync import SettingsSnapshot


class SettingsSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await Tortoise.init(db_url="sqlite://:memory:", modules={"models": ["models"]})
        await Tortoise.generate_schemas()
        self.now = datetime(2026, 10, 15, 12, tzinfo=UTC)
        self.mac_address = "00:11:22:33:44:55"
        self.responses = self.make_responses(1, "Cached")
        await self.snapshot().persist()
        self.record = await Record.create(
            uuid=uuid.uuid4(), rfid_card_uid="old-card", weight=23, measured_at=self.now,
        )
        self.responses = self.make_responses(2, "Updated")

    async def asyncTearDown(self):
        await Tortoise.close_connections()

    def make_responses(self, identifier, label):
        return {
            "gateway": {
                "id": identifier, "mac_address": self.mac_address,
                "hostname": "test-gateway", "ip_address": "127.0.0.1",
                "name": f"{label} gateway", "description": "",
                "access_token": "untrusted-response-token", "last_heartbeat": None,
                "created_at": self.now.isoformat(), "updated_at": self.now.isoformat(),
            },
            "stations": [{
                "id": identifier, "gateway": identifier, "name": f"{label} station",
                "description": "", "serial_port": f"COM{identifier}",
                "serial_description": "", "serial_location": "",
                "serial_number": "", "serial_manufacturer": "",
            }],
            "species": [{"id": identifier, "name": f"{label} species"}],
            "producers": [{
                "id": identifier, "uuid": str(uuid.uuid4()), "name": f"{label} producer",
                "phone": None, "created_at": self.now.isoformat(),
            }],
            "rfid_cards": [{
                "id": identifier, "uuid": str(uuid.uuid4()), "uid": f"card-{identifier}",
                "producer": identifier, "species": identifier, "is_active": True,
                "issued_at": self.now.isoformat(),
            }],
        }

    def snapshot(self, responses=None):
        return SettingsSnapshot.from_responses(
            **(responses if responses is not None else self.responses),
            mac_address=self.mac_address, access_token="verified-token",
        )

    async def stored_state(self):
        return {
            model.__name__: await model.all().order_by(model._meta.pk_attr).values()
            for model in (Gateway, WeighingStation, Species, Producer, RFIDCard, Record)
        }

    async def test_snapshot_builds_complete_cache_before_changing_database(self):
        before = await self.stored_state()
        snapshot = self.snapshot()

        self.assertEqual(snapshot.gateway_id, 2)
        self.assertEqual(snapshot.gateway_name, "Updated gateway")
        self.assertEqual(snapshot.stations[0].serial_port, "COM2")
        self.assertEqual(snapshot.rfid_map, {
            "card-2": RFIDInfo(True, "Updated producer", "Updated species"),
        })
        self.assertEqual(await self.stored_state(), before)

        await snapshot.persist()

        self.assertEqual(await Gateway.all().values_list("id", flat=True), [2])
        self.assertEqual((await Gateway.get(id=2)).access_token, "verified-token")
        self.assertEqual(await WeighingStation.all().values_list("id", flat=True), [2])
        self.assertEqual((await WeighingStation.get(id=2)).gateway_id, 2)
        self.assertEqual(await Species.all().values_list("id", flat=True), [2])
        self.assertEqual(await Producer.all().values_list("id", flat=True), [2])
        self.assertEqual(await RFIDCard.all().values_list("uid", flat=True), ["card-2"])
        self.assertEqual((await Record.get(uuid=self.record.uuid)).weight, 23)

    async def test_empty_lists_remove_cached_settings_but_keep_gateway_and_records(self):
        for key in ("stations", "species", "producers", "rfid_cards"):
            self.responses[key] = []
        snapshot = self.snapshot()
        await snapshot.persist()

        self.assertEqual(snapshot.stations, [])
        self.assertEqual(snapshot.rfid_map, {})
        self.assertEqual(await Gateway.all().count(), 1)
        for model in (WeighingStation, Species, Producer, RFIDCard):
            self.assertEqual(await model.all().count(), 0)
        self.assertTrue(await Record.filter(uuid=self.record.uuid).exists())

    async def test_card_response_and_new_database_have_no_last_used_at_field(self):
        self.assertNotIn("last_used_at", self.responses["rfid_cards"][0])
        await self.snapshot().persist()

        columns = await get_connection("default").execute_query_dict('PRAGMA table_info("rfid_card")')
        column_names = {column["name"] for column in columns}
        self.assertIn("issued_at", column_names)
        self.assertNotIn("last_used_at", column_names)
        stored = await RFIDCard.get(id=2).values()
        self.assertEqual(stored["uid"], "card-2")
        self.assertNotIn("last_used_at", stored)

    async def test_unrelated_response_metadata_is_not_persisted(self):
        self.responses["rfid_cards"][0]["metadata"] = {"source": "server"}
        await self.snapshot().persist()
        self.assertEqual((await RFIDCard.get(id=2)).uid, "card-2")

    async def test_inactive_cards_stay_in_cache_with_inactive_flag(self):
        self.responses["rfid_cards"][0]["is_active"] = False
        snapshot = self.snapshot()
        await snapshot.persist()
        self.assertFalse(snapshot.rfid_map["card-2"].is_active)
        self.assertFalse((await RFIDCard.get(id=2)).is_active)

    async def test_incomplete_or_malformed_responses_leave_all_stored_data_unchanged(self):
        before = await self.stored_state()
        cases = []

        def malformed(section, key, value):
            data = deepcopy(self.responses)
            row = data[section] if section == "gateway" else data[section][0]
            row[key] = value
            cases.append((f"{section}.{key}={value!r}", data))

        for section, key in (
            ("gateway", "name"), ("stations", "serial_port"), ("species", "name"),
            ("producers", "uuid"), ("rfid_cards", "issued_at"),
        ):
            data = deepcopy(self.responses)
            row = data[section] if section == "gateway" else data[section][0]
            del row[key]
            cases.append((f"missing {section}.{key}", data))
        malformed("gateway", "mac_address", "AA:BB:CC:DD:EE:FF")
        malformed("stations", "gateway", 99)
        malformed("rfid_cards", "producer", 99)
        malformed("rfid_cards", "species", 99)
        malformed("rfid_cards", "is_active", "false")
        malformed("rfid_cards", "is_active", 1)
        malformed("species", "id", True)
        malformed("producers", "id", 2.5)
        malformed("stations", "id", 0)
        malformed("species", "name", None)
        malformed("stations", "serial_port", "X" * 101)
        malformed("rfid_cards", "uuid", "not-a-uuid")
        malformed("gateway", "created_at", "not-a-date")
        malformed("stations", "serial_port", {"port": "COM2"})
        malformed("species", "name", [])
        malformed("gateway", "name", 123)
        malformed("rfid_cards", "uid", 12345678)
        malformed("producers", "phone", 12345678)
        malformed("rfid_cards", "uuid", 123)
        malformed("producers", "uuid", {"uuid": str(uuid.uuid4())})
        malformed("producers", "created_at", 12345678)
        malformed("gateway", "last_heartbeat", False)
        malformed("rfid_cards", "issued_at", [self.now.isoformat()])
        for section in ("stations", "species", "producers", "rfid_cards"):
            for value in (None, {}, {"results": []}, [None]):
                data = deepcopy(self.responses)
                data[section] = value
                cases.append((f"invalid {section} list: {value!r}", data))

        for label, data in cases:
            with self.subTest(label=label):
                with self.assertRaises((KeyError, ValueError, TypeError, ValidationError)):
                    self.snapshot(data)
                self.assertEqual(await self.stored_state(), before)

    async def test_duplicate_database_keys_are_rejected_before_persistence(self):
        before = await self.stored_state()
        for section, keys in (
            ("stations", ("id",)), ("species", ("id",)),
            ("producers", ("id", "uuid")), ("rfid_cards", ("id", "uuid", "uid")),
        ):
            for key in keys:
                with self.subTest(section=section, key=key):
                    data = deepcopy(self.responses)
                    duplicate = dict(data[section][0], id=3)
                    if "uuid" in duplicate:
                        duplicate["uuid"] = str(uuid.uuid4())
                    if "uid" in duplicate:
                        duplicate["uid"] = "another-card"
                    duplicate[key] = data[section][0][key]
                    data[section].append(duplicate)
                    with self.assertRaises(ValueError):
                        self.snapshot(data)
                    self.assertEqual(await self.stored_state(), before)

    async def test_database_failure_rolls_back_gateway_stations_and_market_together(self):
        before = await self.stored_state()
        snapshot = self.snapshot()
        with patch("settings_sync.RFIDCard.bulk_create", new=AsyncMock(side_effect=IntegrityError("failed"))):
            with self.assertRaises(IntegrityError):
                await snapshot.persist()
        self.assertEqual(await self.stored_state(), before)

    async def test_cancellation_during_persistence_rolls_back_all_settings(self):
        before = await self.stored_state()
        snapshot = self.snapshot()
        reached_cards = asyncio.Event()

        async def blocked_write(*args, **kwargs):
            reached_cards.set()
            await asyncio.Future()

        with patch("settings_sync.RFIDCard.bulk_create", new=blocked_write):
            task = asyncio.create_task(snapshot.persist())
            try:
                await asyncio.wait_for(reached_cards.wait(), timeout=2)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        self.assertEqual(await self.stored_state(), before)


if __name__ == "__main__":
    unittest.main()
