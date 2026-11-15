from dataclasses import asdict
import unittest
from unittest.mock import AsyncMock, patch
import uuid

import httpx2

from device_configuration import DeviceConfiguration
from main import HeadlessClient
from models import CachedDeviceConfiguration, Gateway, Producer, Record, RFIDCard, Species, WeighingStation
from settings_sync import load_configuration
from tests import test_client as lifecycle
from tests.test_device_configuration import configuration_response


class ConfigurationClientTests(unittest.IsolatedAsyncioTestCase):
    # Share the lifecycle fixture without collecting its tests a second time.
    asyncTearDown = lifecycle.ClientLifecycleTests.asyncTearDown
    cached_setup = lifecycle.ClientLifecycleTests.cached_setup
    online_responses = lifecycle.ClientLifecycleTests.online_responses

    async def asyncSetUp(self):
        await lifecycle.ClientLifecycleTests.asyncSetUp(self)
        self.manager.sync.side_effect = None
        await self.cached_setup()
        self.initial = DeviceConfiguration.from_response(configuration_response(
            auction_name="기존 경매장", print_copies=3,
        ))
        await CachedDeviceConfiguration.create(id=1, **asdict(self.initial))
        self.record = await Record.create(
            uuid=uuid.uuid4(), station_id=1, rfid_card_uid="12345678", weight=130,
            measured_at=self.now,
        )
        await self.client.restore_local_state()
        self.runtime_before = self.client.configuration
        self.market_before = self.client.market_cache.snapshot
        self.manager.sync.reset_mock()
        self.online_responses()
        self.next_payload = configuration_response(
            auction_name="변경 경매장", print_copies=9, vehicle_threshold=120,
            updated_at="2026-11-15T11:00:00+09:00",
        )
        self.api.retrieve_gateway_self.return_value["configuration"] = self.next_payload

    async def stored_state(self):
        return {
            model.__name__: await model.all().order_by(model._meta.pk_attr).values()
            for model in (
                CachedDeviceConfiguration, Gateway, WeighingStation, Species, Producer, RFIDCard, Record,
            )
        }

    async def assert_previous_configuration_preserved(self, before):
        self.assertEqual(await self.stored_state(), before)
        self.assertIs(self.client.configuration, self.runtime_before)
        self.assertIs(self.client.market_cache.snapshot, self.market_before)
        self.assertEqual(await load_configuration(), self.initial)
        self.manager.sync.assert_not_called()

    async def test_complete_sync_persists_configuration_then_passes_it_to_station_manager(self):
        self.assertTrue(await self.client.sync_settings())

        expected = DeviceConfiguration.from_response(self.next_payload)
        self.assertEqual(self.client.configuration, expected)
        self.assertEqual(await load_configuration(), expected)
        self.manager.sync.assert_called_once()
        args, kwargs = self.manager.sync.call_args
        self.assertEqual([station.id for station in args[0]], [1])
        self.assertEqual(kwargs, {"configuration": expected})
        self.assertEqual(self.client.market_cache.gateway_name, "Server gateway")
        self.assertEqual((await Record.get(uuid=self.record.uuid)).weight, 130)
        self.assertTrue(self.client._station_config_ready.is_set())

    async def test_invalid_configuration_preserves_database_and_running_snapshot(self):
        before = await self.stored_state()
        for payload in (None, {}, configuration_response(print_copies=True), configuration_response(serial_read_timeout=2.0)):
            with self.subTest(payload=payload):
                self.api.retrieve_gateway_self.return_value["configuration"] = payload

                self.assertFalse(await self.client.sync_settings())

                await self.assert_previous_configuration_preserved(before)

    async def test_invalid_other_settings_do_not_apply_valid_new_configuration(self):
        before = await self.stored_state()
        self.api.list_gateway_stations.return_value[0]["gateway"] = 999

        self.assertFalse(await self.client.sync_settings())

        await self.assert_previous_configuration_preserved(before)

    async def test_network_error_preserves_database_and_running_snapshot(self):
        before = await self.stored_state()
        self.api.fetch_rfid_cards.side_effect = httpx2.ConnectError("offline")

        self.assertFalse(await self.client.sync_settings())

        await self.assert_previous_configuration_preserved(before)

    async def test_database_error_rolls_back_configuration_and_keeps_running_snapshot(self):
        before = await self.stored_state()
        with patch("settings_sync.RFIDCard.bulk_create", new=AsyncMock(side_effect=RuntimeError("disk failure"))):
            self.assertFalse(await self.client.sync_settings())

        await self.assert_previous_configuration_preserved(before)

    async def test_old_server_after_successful_configuration_sync_keeps_last_value(self):
        self.assertTrue(await self.client.sync_settings())
        expected = self.client.configuration
        del self.api.retrieve_gateway_self.return_value["configuration"]
        self.api.retrieve_gateway_self.return_value["name"] = "Updated PC name"
        self.manager.sync.reset_mock()

        self.assertTrue(await self.client.sync_settings())

        self.assertEqual(self.client.configuration, expected)
        self.assertEqual(await load_configuration(), expected)
        self.assertEqual(self.client.market_cache.gateway_name, "Updated PC name")
        self.assertEqual(self.manager.sync.call_args.kwargs["configuration"], expected)

    async def test_unchanged_configuration_timestamp_does_not_skip_other_settings(self):
        self.assertTrue(await self.client.sync_settings())
        expected = self.client.configuration
        self.api.fetch_producers.return_value[0]["name"] = "Changed producer"
        self.manager.sync.reset_mock()

        self.assertTrue(await self.client.sync_settings())

        self.assertEqual(self.client.configuration, expected)
        self.assertEqual(self.client.market_cache.get_rfid_info("12345678").producer_name, "Changed producer")
        self.assertEqual((await Producer.get(id=1)).name, "Changed producer")
        self.manager.sync.assert_called_once()

    async def test_offline_restart_restores_saved_configuration_before_starting_stations(self):
        restarted = HeadlessClient(base_url=None, credential_path=self.credential_path)
        self.assertEqual(restarted.configuration, DeviceConfiguration())
        self.manager.sync.reset_mock()

        await restarted.restore_local_state()

        self.assertEqual(restarted.configuration, self.initial)
        self.manager.sync.assert_called_once()
        self.assertEqual(self.manager.sync.call_args.kwargs["configuration"], self.initial)
        self.assertEqual(restarted.upload_queue.get_nowait(), str(self.record.uuid))
        self.assertEqual(restarted.market_cache.gateway_name, "Cached gateway")
        self.api.retrieve_gateway_self.assert_not_awaited()

    async def test_discarding_old_server_data_clears_common_cache_and_runtime_value(self):
        self.assertEqual(await self.client.discard_server_data(), 1)

        self.assertEqual(await CachedDeviceConfiguration.all().count(), 0)
        self.assertEqual(self.client.configuration, DeviceConfiguration())
        self.assertEqual(await load_configuration(), DeviceConfiguration())
        for model in (Gateway, WeighingStation, Producer, Species, RFIDCard, Record):
            self.assertEqual(await model.all().count(), 0)

    async def test_failed_server_data_discard_does_not_reset_running_configuration(self):
        before = await self.stored_state()
        with patch("main.Record.all", side_effect=RuntimeError("disk failure")):
            with self.assertRaisesRegex(RuntimeError, "disk failure"):
                await self.client.discard_server_data()

        await self.assert_previous_configuration_preserved(before)


if __name__ == "__main__":
    unittest.main()
