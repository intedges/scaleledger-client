import asyncio
from copy import deepcopy
import json
import threading
import unittest
from unittest.mock import AsyncMock, patch
import uuid

import httpx2

from api import AuthDegradedError
from models import Gateway, Producer, Record, RFIDCard, Species, WeighingStation
from tests import test_client as lifecycle


class QueueWebSocket:
    def __init__(self):
        self.messages = asyncio.Queue()
        self.sent = asyncio.Queue()
        self.listening = asyncio.Event()

    async def __aiter__(self):
        self.listening.set()
        while True:
            yield await self.messages.get()

    async def send(self, message):
        self.sent.put_nowait(json.loads(message))

    def notify(self, message_type):
        self.messages.put_nowait(json.dumps({"type": message_type}))


class SettingsRefreshTests(unittest.IsolatedAsyncioTestCase):
    # Reuse setup helpers without inheriting and collecting the lifecycle tests.
    asyncTearDown = lifecycle.ClientLifecycleTests.asyncTearDown
    start = lifecycle.ClientLifecycleTests.start
    wait_for = lifecycle.ClientLifecycleTests.wait_for
    cached_setup = lifecycle.ClientLifecycleTests.cached_setup
    online_responses = lifecycle.ClientLifecycleTests.online_responses
    emit_and_save = lifecycle.ClientLifecycleTests.emit_and_save

    async def asyncSetUp(self):
        await lifecycle.ClientLifecycleTests.asyncSetUp(self)
        self.applied = asyncio.Queue()
        loop = asyncio.get_running_loop()

        def applied(stations):
            loop.call_soon_threadsafe(self.applied.put_nowait, stations)

        self.manager.sync.side_effect = applied

    async def prepare_cached_client(self):
        await self.cached_setup()
        await self.client.restore_local_state()
        self.manager.sync.reset_mock()
        self.online_responses()

    async def assert_cached_state(self):
        self.assertEqual((await Gateway.get(id=1)).name, "Cached gateway")
        self.assertEqual((await WeighingStation.get(id=1)).name, "Cached station")
        self.assertEqual((await Producer.get(id=1)).name, "Cached producer")
        self.assertEqual((await Species.get(id=1)).name, "Cached species")
        self.assertTrue((await RFIDCard.get(id=1)).is_active)
        self.assertEqual(self.client.market_cache.gateway_name, "Cached gateway")
        self.assertEqual(
            self.client.market_cache.get_rfid_info("12345678").producer_name,
            "Cached producer",
        )
        self.manager.sync.assert_not_called()

    async def start_connected_client(self):
        await self.cached_setup()
        self.online_responses()
        socket = QueueWebSocket()
        self.connections.sockets = [socket]
        running = self.start(self.client.run())
        await self.wait_for(socket.listening.wait())
        while self.client.market_cache.gateway_name != "Server gateway":
            await self.wait_for(self.applied.get())
        return socket, running

    async def test_complete_snapshot_updates_names_card_status_and_new_card(self):
        await self.prepare_cached_client()
        self.api.fetch_rfid_cards.return_value[0]["is_active"] = False
        new_card = dict(
            self.api.fetch_rfid_cards.return_value[0],
            id=2, uuid=str(uuid.uuid4()), uid="87654321", is_active=True,
        )
        self.api.fetch_rfid_cards.return_value.append(new_card)

        self.assertTrue(await self.client.sync_settings())

        self.assertEqual((await Gateway.get(id=1)).name, "Server gateway")
        self.assertEqual((await WeighingStation.get(id=1)).name, "Server station")
        self.assertFalse(self.client.market_cache.get_rfid_info("12345678").is_active)
        added = self.client.market_cache.get_rfid_info("87654321")
        self.assertTrue(added.is_active)
        self.assertEqual(added.producer_name, "Server producer")
        self.assertEqual(added.species_name, "Server species")
        self.assertEqual(self.client.market_cache.gateway_name, "Server gateway")
        self.assertEqual(await RFIDCard.all().count(), 2)
        self.manager.sync.assert_called_once()

    async def test_failed_last_response_preserves_entire_cached_snapshot(self):
        await self.prepare_cached_client()
        self.api.fetch_rfid_cards.side_effect = httpx2.ConnectError("offline")

        self.assertFalse(await self.client.sync_settings())

        await self.assert_cached_state()

    async def test_refresh_timeout_cancels_fetch_and_preserves_cached_snapshot(self):
        await self.prepare_cached_client()
        cancelled = asyncio.Event()

        async def blocked_cards(token):
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        self.client.settings_sync_timeout = 0.01
        self.api.fetch_rfid_cards.side_effect = blocked_cards

        self.assertFalse(await self.client.sync_settings())

        self.assertTrue(cancelled.is_set())
        await self.assert_cached_state()

    async def test_invalid_foreign_key_preserves_entire_cached_snapshot(self):
        await self.prepare_cached_client()
        self.api.fetch_rfid_cards.return_value[0]["producer"] = 999

        self.assertFalse(await self.client.sync_settings())

        await self.assert_cached_state()

    async def test_station_from_another_gateway_is_not_applied(self):
        await self.prepare_cached_client()
        self.api.list_gateway_stations.return_value[0]["gateway"] = 999

        self.assertFalse(await self.client.sync_settings())

        await self.assert_cached_state()

    async def test_malformed_station_response_is_not_treated_as_empty_list(self):
        await self.prepare_cached_client()
        self.api.list_gateway_stations.return_value = {"detail": "unavailable"}

        self.assertFalse(await self.client.sync_settings())

        await self.assert_cached_state()

    async def test_database_write_failure_rolls_back_every_snapshot_table(self):
        await self.prepare_cached_client()
        with patch("main.RFIDCard.bulk_create", side_effect=RuntimeError("disk failure")):
            self.assertFalse(await self.client.sync_settings())

        await self.assert_cached_state()

    async def test_valid_empty_station_list_removes_cached_stations(self):
        await self.prepare_cached_client()
        self.api.list_gateway_stations.return_value = []

        self.assertTrue(await self.client.sync_settings())

        self.assertEqual(await WeighingStation.all().count(), 0)
        self.manager.sync.assert_called_once_with([])
        self.assertEqual(self.client.market_cache.gateway_name, "Server gateway")
        self.assertIsNotNone(self.client.market_cache.get_rfid_info("12345678"))

    async def test_card_response_without_recent_use_field_syncs(self):
        await self.prepare_cached_client()
        self.assertNotIn("last_used_at", self.api.fetch_rfid_cards.return_value[0])

        self.assertTrue(await self.client.sync_settings())

        self.assertEqual(
            self.client.market_cache.get_rfid_info("12345678").producer_name,
            "Server producer",
        )

    async def test_auth_rejection_propagates_without_replacing_settings(self):
        await self.prepare_cached_client()
        response = httpx2.Response(401, request=httpx2.Request("GET", "http://server/"))
        self.api.fetch_species.side_effect = httpx2.HTTPStatusError(
            "unauthorized", request=response.request, response=response,
        )

        with self.assertRaises(AuthDegradedError):
            await self.client.sync_settings()

        await self.assert_cached_state()

    async def test_websocket_notification_refreshes_while_connection_stays_open(self):
        socket, running = await self.start_connected_client()
        self.api.fetch_producers.return_value[0]["name"] = "Updated producer"
        self.api.retrieve_gateway_self.return_value["name"] = "Updated gateway"
        self.api.fetch_rfid_cards.return_value[0]["is_active"] = False

        socket.notify("sync.market_data")
        while self.client.market_cache.gateway_name != "Updated gateway":
            await self.wait_for(self.applied.get())

        self.assertFalse(running.done())
        self.assertEqual(self.connections.opened.qsize(), 1)
        updated = self.client.market_cache.get_rfid_info("12345678")
        self.assertEqual(updated.producer_name, "Updated producer")
        self.assertFalse(updated.is_active)
        self.assertEqual((await Gateway.get(id=1)).name, "Updated gateway")

    async def test_gateway_notification_updates_cached_receipt_gateway_name(self):
        socket, running = await self.start_connected_client()
        self.api.retrieve_gateway_self.return_value["name"] = "Renamed gateway"

        socket.notify("sync.gateway")
        while self.client.market_cache.gateway_name != "Renamed gateway":
            await self.wait_for(self.applied.get())

        self.assertFalse(running.done())
        self.assertEqual((await Gateway.get(id=1)).name, "Renamed gateway")
        self.assertEqual(self.connections.opened.qsize(), 1)

    async def test_startup_refreshes_rest_settings_when_control_websocket_is_unavailable(self):
        await self.cached_setup()
        self.online_responses()
        running = self.start(self.client.run())

        while self.client.market_cache.gateway_name != "Server gateway":
            await self.wait_for(self.applied.get())

        self.assertFalse(running.done())
        self.assertTrue(self.connections.opened.empty())
        self.assertEqual((await Gateway.get(id=1)).name, "Server gateway")
        self.assertEqual((await WeighingStation.get(id=1)).name, "Server station")
        self.assertEqual(
            self.client.market_cache.get_rfid_info("12345678").producer_name,
            "Server producer",
        )

    async def test_slow_refresh_does_not_block_websocket_scan_or_record_upload(self):
        socket, _ = await self.start_connected_client()
        entered = asyncio.Event()
        release = asyncio.Event()
        cards = deepcopy(self.api.fetch_rfid_cards.return_value)

        async def slow_cards(token):
            entered.set()
            await release.wait()
            return cards

        self.api.fetch_rfid_cards.side_effect = slow_cards
        socket.notify("sync.weighing_stations")
        await self.wait_for(entered.wait())
        with patch("main.scan_peripherals", return_value=[{"device": "COM3"}]):
            socket.notify("scan.peripherals")
            scanned = await self.wait_for(socket.sent.get())
        self.assertEqual(scanned, {"type": "peripherals.scanned", "payload": [{"device": "COM3"}]})

        event = await self.emit_and_save()
        await self.wait_for(self.client.upload_queue.join())
        self.assertEqual(str(self.api.create_record.call_args.kwargs["record"].uuid), event.uuid)
        self.assertFalse(await Record.filter(uuid=event.uuid).exists())
        self.assertFalse(release.is_set())
        release.set()

    async def test_initial_refresh_does_not_delay_control_websocket_receiver(self):
        await self.cached_setup()
        self.online_responses()
        entered = asyncio.Event()
        release = asyncio.Event()
        cards = deepcopy(self.api.fetch_rfid_cards.return_value)

        async def slow_cards(token):
            entered.set()
            await release.wait()
            return cards

        self.api.fetch_rfid_cards.side_effect = slow_cards
        socket = QueueWebSocket()
        self.connections.sockets = [socket]
        self.start(self.client.run())
        await self.wait_for(entered.wait())
        await self.wait_for(socket.listening.wait())

        with patch("main.scan_peripherals", return_value=[]):
            socket.notify("scan.peripherals")
            self.assertEqual(
                await self.wait_for(socket.sent.get()),
                {"type": "peripherals.scanned", "payload": []},
            )
        self.assertEqual(self.client.market_cache.gateway_name, "Cached gateway")
        release.set()

    async def test_burst_requests_share_one_pending_followup(self):
        first_entered = asyncio.Event()
        first_release = asyncio.Event()
        second_entered = asyncio.Event()
        second_release = asyncio.Event()

        async def synchronize():
            if self.client.sync_settings.await_count == 1:
                first_entered.set()
                await first_release.wait()
            else:
                second_entered.set()
                await second_release.wait()
            return True

        self.client.sync_settings = AsyncMock(side_effect=synchronize)
        self.start(self.client.run_settings_sync_worker())
        first = self.client.request_settings_sync()
        self.assertIs(self.client.request_settings_sync(), first)
        await self.wait_for(first_entered.wait())
        followup = self.client.request_settings_sync()
        self.assertIsNot(first, followup)
        for _ in range(20):
            self.assertIs(self.client.request_settings_sync(), followup)
        self.assertEqual(self.client.sync_settings.await_count, 1)
        first_release.set()
        self.assertTrue(await self.wait_for(asyncio.shield(first)))
        await self.wait_for(second_entered.wait())
        second_release.set()
        self.assertTrue(await self.wait_for(asyncio.shield(followup)))
        await asyncio.sleep(0.02)
        self.assertEqual(self.client.sync_settings.await_count, 2)

    async def test_failed_refresh_retries_then_stops_polling_after_success(self):
        recovered = asyncio.Event()

        async def synchronize():
            if self.client.sync_settings.await_count == 1:
                return False
            recovered.set()
            return True

        self.client.sync_settings = AsyncMock(side_effect=synchronize)
        self.start(self.client.run_settings_sync_worker())
        result = self.client.request_settings_sync()
        await self.wait_for(recovered.wait())
        await self.wait_for(asyncio.shield(result))
        await asyncio.sleep(0.02)
        self.assertEqual(self.client.sync_settings.await_count, 2)

    async def test_cancelled_request_waiter_does_not_cancel_shared_refresh(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        completed = asyncio.Event()

        async def synchronize():
            entered.set()
            await release.wait()
            completed.set()
            return True

        self.client.sync_settings = AsyncMock(side_effect=synchronize)
        self.start(self.client.run_settings_sync_worker())
        waiting = self.start(self.client.sync_weighing_stations())
        await self.wait_for(entered.wait())
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting
        release.set()
        await self.wait_for(completed.wait())
        self.assertEqual(self.client.sync_settings.await_count, 1)

    async def test_shutdown_cancels_inflight_refresh_and_stops_workers(self):
        socket, running = await self.start_connected_client()
        entered = asyncio.Event()
        cancelled = asyncio.Event()

        async def blocked_cards(token):
            entered.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        self.api.fetch_rfid_cards.side_effect = blocked_cards
        socket.notify("sync.market_data")
        await self.wait_for(entered.wait())
        running.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.wait_for(running)

        self.assertTrue(cancelled.is_set())
        self.manager.stop_all.assert_called_once()
        self.assertEqual((await Gateway.get(id=1)).name, "Server gateway")

    async def test_cancellation_waits_for_station_application_thread(self):
        await self.prepare_cached_client()
        entered = asyncio.Event()
        release = threading.Event()
        finished = threading.Event()
        loop = asyncio.get_running_loop()

        def apply_stations(stations):
            loop.call_soon_threadsafe(entered.set)
            release.wait(timeout=2)
            finished.set()

        self.manager.sync.side_effect = apply_stations
        synchronization = self.start(self.client.sync_settings())
        try:
            await self.wait_for(entered.wait())
            synchronization.cancel()
            await asyncio.sleep(0)
            self.assertFalse(synchronization.done())
            self.assertFalse(finished.is_set())
            # A second shutdown/error cancellation must not abandon to_thread.
            synchronization.cancel()
            await asyncio.sleep(0)
            self.assertFalse(synchronization.done())
            self.assertFalse(finished.is_set())
        finally:
            release.set()
        with self.assertRaises(asyncio.CancelledError):
            await self.wait_for(synchronization)
        self.assertTrue(finished.is_set())
