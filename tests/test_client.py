import asyncio
from contextlib import suppress
from datetime import UTC, datetime
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

import httpx
from tortoise import Tortoise
from websockets.exceptions import ConnectionClosedError

from api import APIClient
from events import WeighingCompletedEvent
from main import HeadlessClient
from models import Gateway, Producer, Record, RFIDCard, Species, WeighingStation


class StubWebSocket:
    def __init__(self, *, failure=None, hold=None, messages=(), after_messages=None):
        self.failure = failure
        self.hold = hold
        self.messages = messages
        self.after_messages = after_messages
        self.listening = asyncio.Event()

    async def __aiter__(self):
        self.listening.set()
        if self.hold is not None:
            await self.hold.wait()
        if self.failure is not None:
            raise self.failure
        for message in self.messages:
            yield message
        if self.after_messages is not None:
            await self.after_messages.wait()


class StubConnections:
    def __init__(self, sockets=()):
        self.sockets = sockets
        self.opened = asyncio.Queue()
        self.resume = None

    async def __aiter__(self):
        for index, socket in enumerate(self.sockets):
            if index and self.resume is not None:
                await self.resume.wait()
            self.opened.put_nowait(socket)
            yield socket
        await asyncio.Future()


class ClientLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await Tortoise.init(db_url="sqlite://:memory:", modules={"models": ["models"]})
        await Tortoise.generate_schemas()
        self.tasks = []
        self.now = datetime(2026, 10, 10, tzinfo=UTC)
        self.started = asyncio.Event()
        self.manager = Mock()
        self.manager.sync.side_effect = lambda stations: self.started.set() if stations else None
        self.api = Mock(spec=APIClient)
        self.api.client = SimpleNamespace(base_url="http://127.0.0.1:8000")
        self.api.close = AsyncMock()
        for name in (
            "retrieve_gateway_self", "list_gateway_stations", "send_heartbeat",
            "create_record", "fetch_species", "fetch_producers", "fetch_rfid_cards",
        ):
            setattr(self.api, name, AsyncMock(side_effect=httpx.ConnectError("offline")))
        self.connections = StubConnections()
        patches = (
            patch("main.APIClient", return_value=self.api),
            patch("main.WeighingStationManager", return_value=self.manager),
            patch("main.get_mac_address", return_value="00:11:22:33:44:55"),
            patch("main.get_ip_address", return_value="127.0.0.1"),
            patch("main.get_hostname", return_value="test-gateway"),
            patch("main.get_logger", return_value=Mock()),
            patch("main.setup_logging"),
            patch("main.websockets.connect", return_value=self.connections),
        )
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = HeadlessClient(base_url="http://127.0.0.1:8000")
        self.client.setup = AsyncMock()
        self.client.retry_interval = 0.001

    async def asyncTearDown(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        for task in self.tasks:
            with suppress(asyncio.CancelledError):
                await task
        await Tortoise.close_connections()

    def start(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    async def wait_for(self, awaitable):
        return await asyncio.wait_for(awaitable, timeout=2)

    async def cached_setup(self, *, token="cached-token"):
        self.gateway = await Gateway.create(
            id=1, mac_address="00:11:22:33:44:55", hostname="test-gateway",
            ip_address="127.0.0.1", name="Cached gateway", description="",
            access_token=token, created_at=self.now, updated_at=self.now,
        )
        self.station = await WeighingStation.create(
            id=1, gateway=self.gateway, name="Cached station", description="",
            serial_port="mock-port", serial_description="", serial_location="",
            serial_number="", serial_manufacturer="",
        )
        self.species = await Species.create(id=1, name="Cached species")
        self.producer = await Producer.create(
            id=1, uuid=uuid.uuid4(), name="Cached producer", created_at=self.now,
        )
        self.card = await RFIDCard.create(
            id=1, uuid=uuid.uuid4(), uid="12345678", producer=self.producer,
            species=self.species, is_active=True, issued_at=self.now,
        )

    def online_responses(self):
        responses = {
            "retrieve_gateway_self": {
                "id": 1, "mac_address": "00:11:22:33:44:55",
                "hostname": "test-gateway", "ip_address": "127.0.0.1",
                "name": "Server gateway", "description": "",
                "access_token": "cached-token", "last_heartbeat": None,
                "created_at": self.now.isoformat(), "updated_at": self.now.isoformat(),
            },
            "list_gateway_stations": [{
                "id": 1, "gateway": 1, "name": "Server station", "description": "",
                "serial_port": "mock-port", "serial_description": "", "serial_location": "",
                "serial_number": "", "serial_manufacturer": "",
            }],
            "fetch_species": [{"id": 1, "name": "Server species"}],
            "fetch_producers": [{
                "id": 1, "uuid": str(self.producer.uuid), "name": "Server producer",
                "phone": None, "created_at": self.now.isoformat(),
            }],
            "fetch_rfid_cards": [{
                "id": 1, "uuid": str(self.card.uuid), "uid": "12345678",
                "producer": 1, "species": 1, "is_active": True,
                "issued_at": self.now.isoformat(), "last_used_at": None,
            }],
            "send_heartbeat": {},
            "create_record": {},
        }
        for name, value in responses.items():
            method = getattr(self.api, name)
            method.side_effect = None
            method.return_value = value

    async def emit_and_save(self):
        event = WeighingCompletedEvent(rfid_card_uid="12345678", weight=12)
        self.client.handle_hardware_event(event)
        await asyncio.sleep(0)
        await self.wait_for(self.client.event_queue.join())
        return event

    async def test_offline_start_restores_station_and_persists_measurements(self):
        await self.cached_setup()
        self.start(self.client.run())
        await self.wait_for(self.started.wait())

        event = await self.emit_and_save()

        record = await Record.get(uuid=event.uuid)
        self.assertEqual(record.weight, 12)
        self.assertEqual(self.client.gateway_id, self.gateway.id)
        self.assertEqual(self.client.market_cache.gateway_name, "Cached gateway")
        self.assertEqual(self.client.market_cache.get_rfid_info("12345678").producer_name, "Cached producer")
        self.manager.stop_all.assert_not_called()

    async def test_normal_and_abnormal_websocket_closes_reconnect_without_stopping_http(self):
        await self.cached_setup()
        self.online_responses()
        connected = StubWebSocket(hold=asyncio.Event())
        self.connections.sockets = [
            StubWebSocket(),
            StubWebSocket(failure=ConnectionClosedError(None, None)),
            connected,
        ]
        uploaded = asyncio.Event()

        async def accept_record(**kwargs):
            uploaded.set()
            return {}

        self.api.create_record.side_effect = accept_record
        self.start(self.client.run())
        await self.wait_for(connected.listening.wait())
        event = await self.emit_and_save()
        await self.wait_for(uploaded.wait())
        await self.wait_for(self.client.upload_queue.join())

        self.assertEqual(self.connections.opened.qsize(), 3)
        self.api.send_heartbeat.assert_awaited()
        self.assertEqual(str(self.api.create_record.call_args.kwargs["record"].uuid), event.uuid)
        self.assertFalse(await Record.filter(uuid=event.uuid).exists())
        self.manager.stop_all.assert_not_called()

    async def test_cached_pending_record_uploads_without_a_websocket_connection(self):
        await self.cached_setup()
        self.online_responses()
        record = await Record.create(
            uuid=uuid.uuid4(), rfid_card_uid="12345678", weight=19,
            measured_at=self.now,
        )
        uploaded = asyncio.Event()

        async def accept_record(**kwargs):
            uploaded.set()
            return {}

        self.api.create_record.side_effect = accept_record
        self.start(self.client.run())
        await self.wait_for(uploaded.wait())
        await self.wait_for(self.client.upload_queue.join())

        self.assertTrue(self.connections.opened.empty())
        self.api.create_record.assert_awaited_once()
        self.assertEqual(self.api.create_record.call_args.kwargs["record"].uuid, record.uuid)
        self.assertFalse(await Record.filter(uuid=record.uuid).exists())
        self.manager.stop_all.assert_not_called()

    async def test_reconnected_socket_refreshes_cached_market_and_station_data(self):
        await self.cached_setup()
        self.online_responses()
        cached_gateway = dict(self.api.retrieve_gateway_self.return_value, name="Cached gateway")
        failed_sync = StubWebSocket()
        connected = StubWebSocket(hold=asyncio.Event())
        self.connections.sockets = [failed_sync, connected]
        self.connections.resume = asyncio.Event()
        sync_attempted = asyncio.Event()

        async def unavailable_gateway(token):
            if self.api.retrieve_gateway_self.await_count == 1:
                return cached_gateway
            if not self.connections.opened.empty():
                sync_attempted.set()
            raise httpx.ConnectError("offline")

        self.api.retrieve_gateway_self.side_effect = unavailable_gateway
        self.start(self.client.run())
        await self.wait_for(sync_attempted.wait())
        self.assertFalse(failed_sync.listening.is_set())
        self.assertEqual(self.client.market_cache.gateway_name, "Cached gateway")

        self.online_responses()
        self.connections.resume.set()
        await self.wait_for(connected.listening.wait())

        self.assertEqual(self.client.market_cache.gateway_name, "Server gateway")
        info = self.client.market_cache.get_rfid_info("12345678")
        self.assertEqual((info.producer_name, info.species_name), ("Server producer", "Server species"))
        self.assertEqual((await WeighingStation.get(id=1)).name, "Server station")
        self.assertEqual(self.manager.sync.call_args.args[0][0].name, "Server station")
        self.manager.stop_all.assert_not_called()

    async def test_websocket_failure_does_not_cancel_a_local_write_in_progress(self):
        await self.cached_setup()
        self.online_responses()
        disconnected = StubWebSocket(
            failure=ConnectionClosedError(None, None), hold=asyncio.Event(),
        )
        connected = StubWebSocket(hold=asyncio.Event())
        self.connections.sockets = [disconnected, connected]
        write_started = asyncio.Event()
        finish_write = asyncio.Event()
        write_cancelled = asyncio.Event()
        create_record = Record.create

        async def delayed_create(**kwargs):
            write_started.set()
            try:
                await finish_write.wait()
            except asyncio.CancelledError:
                write_cancelled.set()
                raise
            return await create_record(**kwargs)

        with patch("main.Record.create", side_effect=delayed_create):
            self.start(self.client.run())
            await self.wait_for(disconnected.listening.wait())
            event = WeighingCompletedEvent(rfid_card_uid="12345678", weight=23)
            self.client.handle_hardware_event(event)
            await self.wait_for(write_started.wait())
            disconnected.hold.set()
            try:
                await self.wait_for(connected.listening.wait())
                self.assertFalse(write_cancelled.is_set())
            finally:
                finish_write.set()
                await self.wait_for(self.client.event_queue.join())
            await self.wait_for(self.client.upload_queue.join())

        self.assertEqual(str(self.api.create_record.call_args.kwargs["record"].uuid), event.uuid)
        self.manager.stop_all.assert_not_called()

    async def test_graceful_shutdown_saves_last_event_emitted_while_station_stops(self):
        await self.cached_setup()
        final_event = WeighingCompletedEvent(rfid_card_uid="12345678", weight=37)
        self.manager.stop_all.side_effect = lambda: self.client.handle_hardware_event(final_event)
        task = self.start(self.client.run())
        await self.wait_for(self.started.wait())

        task.cancel()
        with suppress(asyncio.CancelledError):
            await self.wait_for(task)

        self.assertEqual((await Record.get(uuid=final_event.uuid)).weight, 37)
        self.assertTrue(self.client.event_queue.empty())
        self.manager.stop_all.assert_called_once()

    async def test_auth_rejection_preserves_station_and_market_cache_for_restart(self):
        await self.cached_setup()
        await self.client.restore_local_state()
        await self.client.wipe_local_auth()

        self.assertEqual((await Gateway.get(id=1)).access_token, "")
        self.assertIsNone(self.client.access_token)
        self.assertEqual(self.client.gateway_id, 1)
        self.assertEqual(await WeighingStation.all().count(), 1)
        self.assertEqual(await RFIDCard.all().count(), 1)
        self.assertTrue(self.client.market_cache.get_rfid_info("12345678").is_active)

        self.client.gateway_id = None
        await self.client.restore_local_state()
        self.assertEqual(self.client.gateway_id, 1)
        self.assertFalse(self.client.access_token)
        self.assertEqual(self.manager.sync.call_args.args[0][0].serial_port, "mock-port")

    async def test_failed_station_snapshot_keeps_last_successful_configuration(self):
        await self.cached_setup()
        self.online_responses()
        self.api.list_gateway_stations.return_value.append({"id": 2, "gateway": 1})
        self.client.access_token = "cached-token"
        self.client.gateway_id = 1

        await self.client.sync_weighing_stations()

        station = await WeighingStation.get(id=1)
        self.assertEqual(station.name, "Cached station")
        self.assertEqual(await WeighingStation.all().count(), 1)
        self.manager.sync.assert_not_called()

    async def test_validated_reregistration_survives_failed_sync_and_restart(self):
        await self.cached_setup()
        self.online_responses()
        await self.client.restore_local_state()
        await self.client.wipe_local_auth()
        self.api.retrieve_gateway_self.return_value["access_token"] = "new-token"
        await self.client.dispatch_provisioning(None, json.dumps({
            "type": "gateway.registered", "payload": {"access_token": "new-token"},
        }))

        self.assertTrue(await self.client.bootstrap())
        self.api.list_gateway_stations.side_effect = httpx.ConnectError("offline")
        self.assertFalse(await self.client.sync_weighing_stations())

        restarted = HeadlessClient(base_url="http://127.0.0.1:8000")
        await restarted.restore_local_state()
        self.assertEqual(restarted.access_token, "new-token")
        self.assertEqual(restarted.gateway_id, 1)
        self.assertEqual(restarted.market_cache.gateway_name, "Cached gateway")
        self.assertEqual((await WeighingStation.get(id=1)).name, "Cached station")

        connected = StubWebSocket(hold=asyncio.Event())
        self.connections.sockets = [connected]
        self.api.list_gateway_stations.side_effect = None
        restarted.run_provisioning_loop = AsyncMock()
        self.start(restarted.run_network_loop())
        await self.wait_for(connected.listening.wait())
        restarted.run_provisioning_loop.assert_not_awaited()
        self.api.retrieve_gateway_self.assert_awaited_with("new-token")

    async def test_first_registration_survives_failed_station_sync_and_restart(self):
        self.api.retrieve_gateway_self.side_effect = None
        self.api.retrieve_gateway_self.return_value = {
            "id": 1, "mac_address": "00:11:22:33:44:55",
            "hostname": "test-gateway", "ip_address": "127.0.0.1",
            "name": "New gateway", "description": "",
            "access_token": "new-token", "last_heartbeat": None,
            "created_at": self.now.isoformat(), "updated_at": self.now.isoformat(),
        }
        await self.client.dispatch_provisioning(None, json.dumps({
            "type": "gateway.registered", "payload": {"access_token": "new-token"},
        }))

        self.assertTrue(await self.client.bootstrap())
        self.assertFalse(await self.client.sync_weighing_stations())

        restarted = HeadlessClient(base_url="http://127.0.0.1:8000")
        await restarted.restore_local_state()
        self.assertEqual(restarted.access_token, "new-token")
        self.assertEqual(restarted.gateway_id, 1)
        self.assertEqual(restarted.market_cache.gateway_name, "New gateway")
        self.assertEqual(await WeighingStation.all().count(), 0)
        self.assertFalse(any(call.args[0] for call in self.manager.sync.call_args_list))

    async def test_changed_registration_resolves_current_socket_after_offline_restart(self):
        await self.cached_setup()
        self.online_responses()
        await self.client.restore_local_state()
        await self.client.wipe_local_auth()
        self.api.retrieve_gateway_self.return_value.update(id=2, access_token="new-token")
        self.api.list_gateway_stations.return_value[0].update(id=2, gateway=2)
        await self.client.dispatch_provisioning(None, json.dumps({
            "type": "gateway.registered", "payload": {"access_token": "new-token"},
        }))
        self.assertTrue(await self.client.bootstrap())
        self.api.list_gateway_stations.side_effect = httpx.ConnectError("offline")
        self.assertFalse(await self.client.sync_weighing_stations())

        restarted = HeadlessClient(base_url="http://127.0.0.1:8000")
        restarted.retry_interval = 0.001
        await restarted.restore_local_state()
        self.assertEqual(restarted.access_token, "new-token")
        self.assertEqual(restarted.gateway_id, 1)
        self.assertEqual((await WeighingStation.get(id=1)).gateway_id, 1)

        heartbeat_sent = asyncio.Event()
        self.api.send_heartbeat.side_effect = lambda token: heartbeat_sent.set()
        self.api.retrieve_gateway_self.side_effect = httpx.ConnectError("offline")
        restarted.run_provisioning_loop = AsyncMock()
        connected = StubWebSocket(hold=asyncio.Event())
        self.connections.sockets = [connected]
        with patch("main.websockets.connect", return_value=self.connections) as connect:
            self.start(restarted.run_network_loop())
            await self.wait_for(heartbeat_sent.wait())
            connect.assert_not_called()
            self.api.send_heartbeat.assert_awaited_with("new-token")

            self.api.retrieve_gateway_self.side_effect = None
            self.api.list_gateway_stations.side_effect = None
            await self.wait_for(connected.listening.wait())
            connect.assert_called_once_with("ws://127.0.0.1:8000/ws/devices/gateways/2/")

        restarted.run_provisioning_loop.assert_not_awaited()
        self.assertEqual(restarted.gateway_id, 2)
        self.assertEqual(await Gateway.all().values_list("id", flat=True), [2])
        self.assertEqual((await WeighingStation.get(id=2)).gateway_id, 2)

    async def test_reregistration_replaces_gateway_only_with_complete_station_snapshot(self):
        await self.cached_setup()
        self.online_responses()
        await self.client.restore_local_state()
        self.manager.sync.reset_mock()
        self.api.retrieve_gateway_self.return_value.update(id=2, access_token="new-token")
        replacement = dict(
            self.api.list_gateway_stations.return_value[0],
            id=2, gateway=2, serial_port="replacement-port",
        )
        await self.client.dispatch_provisioning(None, json.dumps({
            "type": "gateway.registered", "payload": {"access_token": "new-token"},
        }))
        self.assertTrue(await self.client.bootstrap())
        self.assertEqual(self.client.gateway_id, 2)
        self.assertEqual(await Gateway.all().values_list("id", flat=True), [1])

        self.api.list_gateway_stations.side_effect = httpx.ConnectError("offline")
        self.assertFalse(await self.client.sync_weighing_stations())
        self.assertEqual(await Gateway.all().values_list("id", flat=True), [1])
        self.assertEqual((await WeighingStation.get(id=1)).serial_port, "mock-port")
        self.manager.sync.assert_not_called()

        self.api.list_gateway_stations.side_effect = None
        self.api.list_gateway_stations.return_value = [replacement, {"id": 3, "gateway": 2}]
        self.assertFalse(await self.client.sync_weighing_stations())
        self.assertEqual(await Gateway.all().values_list("id", flat=True), [1])
        self.assertEqual((await Gateway.get(id=1)).access_token, "new-token")
        self.assertEqual(await WeighingStation.all().values_list("id", flat=True), [1])
        self.assertEqual((await WeighingStation.get(id=1)).serial_port, "mock-port")
        self.manager.sync.assert_not_called()

        self.api.list_gateway_stations.return_value = [replacement]
        self.assertTrue(await self.client.sync_weighing_stations())
        self.assertEqual(await Gateway.all().values_list("id", flat=True), [2])
        self.assertEqual((await Gateway.get(id=2)).access_token, "new-token")
        self.assertEqual(await WeighingStation.all().values_list("id", flat=True), [2])
        station = await WeighingStation.get(id=2)
        self.assertEqual((station.gateway_id, station.serial_port), (2, "replacement-port"))
        self.assertEqual(self.manager.sync.call_args.args[0][0].id, 2)

    async def test_failed_station_notification_reconnects_and_applies_updated_configuration(self):
        await self.cached_setup()
        self.online_responses()
        current = self.api.list_gateway_stations.return_value
        updated = [dict(current[0], name="Updated station", serial_port="updated-port")]
        self.api.list_gateway_stations.side_effect = [
            current, httpx.ConnectError("temporary outage"), updated,
        ]
        notifying = StubWebSocket(
            messages=[json.dumps({"type": "sync.weighing_stations"})],
            after_messages=asyncio.Event(),
        )
        connected = StubWebSocket(hold=asyncio.Event())
        self.connections.sockets = [notifying, connected]
        self.start(self.client.run())
        await self.wait_for(connected.listening.wait())

        station = await WeighingStation.get(id=1)
        self.assertEqual((station.name, station.serial_port), ("Updated station", "updated-port"))
        self.assertEqual(self.connections.opened.qsize(), 2)
        self.assertEqual(self.api.list_gateway_stations.await_count, 3)
        self.assertFalse(notifying.after_messages.is_set())
        self.assertEqual(self.manager.sync.call_args.args[0][0].serial_port, "updated-port")
        self.manager.stop_all.assert_not_called()

    async def test_first_install_waits_for_registration_without_starting_stations(self):
        provisioning = asyncio.Event()

        async def wait_for_registration():
            provisioning.set()
            await asyncio.Future()

        self.client.run_provisioning_loop = AsyncMock(side_effect=wait_for_registration)
        self.start(self.client.run())
        await self.wait_for(provisioning.wait())

        self.assertIsNone(self.client.access_token)
        self.assertIsNone(self.client.gateway_id)
        self.assertFalse(any(call.args[0] for call in self.manager.sync.call_args_list))
        self.api.send_heartbeat.assert_not_awaited()
        self.api.list_gateway_stations.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
