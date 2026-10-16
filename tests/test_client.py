import asyncio
from contextlib import chdir, suppress
from datetime import UTC, datetime
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, call, patch
import uuid

import httpx2
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
        self.settings_synced = asyncio.Event()
        self.manager = Mock()
        loop = asyncio.get_running_loop()
        self.manager.sync.side_effect = (
            lambda stations: loop.call_soon_threadsafe(self.started.set) if stations else None
        )
        self.api = Mock(spec=APIClient)
        self.api.close = AsyncMock()
        for name in (
            "retrieve_gateway_self", "list_gateway_stations", "send_heartbeat",
            "create_record", "fetch_species", "fetch_producers", "fetch_rfid_cards",
        ):
            setattr(self.api, name, AsyncMock(side_effect=httpx2.ConnectError("offline")))
        self.connections = StubConnections()
        self.publisher = Mock()

        async def hold_telemetry():
            await asyncio.Future()

        self.publisher.run = AsyncMock(side_effect=hold_telemetry)
        patches = (
            patch("main.APIClient", return_value=self.api),
            patch("main.WeighingStationManager", return_value=self.manager),
            patch("main.get_mac_address", return_value="00:11:22:33:44:55"),
            patch("main.get_ip_address", return_value="127.0.0.1"),
            patch("main.get_hostname", return_value="test-gateway"),
            patch("main.get_logger", return_value=Mock()),
            patch("main.websockets.connect", return_value=self.connections),
            patch("main.LiveWeightPublisher", return_value=self.publisher),
        )
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = HeadlessClient(base_url="http://127.0.0.1:8000")
        self.client.logger.info.side_effect = (
            lambda event, **kwargs: self.settings_synced.set()
            if event == "sys.sync.settings.completed" else None
        )
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
                "issued_at": self.now.isoformat(),
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
        self.api.send_heartbeat.assert_not_awaited()
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

    async def test_measurement_time_keeps_its_local_offset_through_storage_and_upload(self):
        await self.cached_setup()
        self.online_responses()
        uploaded = asyncio.Event()

        async def accept_record(**kwargs):
            uploaded.set()
            return {}

        self.api.create_record.side_effect = accept_record
        self.start(self.client.run())
        await self.wait_for(self.started.wait())
        event = await self.emit_and_save()
        await self.wait_for(uploaded.wait())

        measured_at = self.api.create_record.call_args.kwargs["record"].measured_at
        self.assertEqual(event.timestamp.utcoffset(), datetime.now().astimezone().utcoffset())
        self.assertEqual(measured_at, event.timestamp)

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
            raise httpx2.ConnectError("offline")

        self.api.retrieve_gateway_self.side_effect = unavailable_gateway
        self.start(self.client.run())
        await self.wait_for(sync_attempted.wait())
        self.assertTrue(failed_sync.listening.is_set())
        self.assertEqual(self.client.market_cache.gateway_name, "Cached gateway")

        self.online_responses()
        self.connections.resume.set()
        await self.wait_for(connected.listening.wait())
        await self.wait_for(self.settings_synced.wait())

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

        self.assertFalse(await self.client.sync_settings())

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
        self.api.list_gateway_stations.side_effect = httpx2.ConnectError("offline")
        self.assertFalse(await self.client.sync_settings())

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
        self.assertFalse(await self.client.sync_settings())

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
        self.api.list_gateway_stations.side_effect = httpx2.ConnectError("offline")
        self.assertFalse(await self.client.sync_settings())

        restarted = HeadlessClient(base_url="http://127.0.0.1:8000")
        restarted.retry_interval = 0.001
        await restarted.restore_local_state()
        self.assertEqual(restarted.access_token, "new-token")
        self.assertEqual(restarted.gateway_id, 1)
        self.assertEqual((await WeighingStation.get(id=1)).gateway_id, 1)

        heartbeat_sent = asyncio.Event()
        self.api.send_heartbeat.side_effect = lambda token: heartbeat_sent.set()
        resolution_attempted = asyncio.Event()

        async def unavailable_gateway(token):
            resolution_attempted.set()
            raise httpx2.ConnectError("offline")

        self.api.retrieve_gateway_self.side_effect = unavailable_gateway
        restarted.run_provisioning_loop = AsyncMock()
        connected = StubWebSocket(
            messages=[json.dumps({"type": "heartbeat.request"})],
            after_messages=asyncio.Event(),
        )
        self.connections.sockets = [connected]
        with patch("main.websockets.connect", return_value=self.connections) as connect:
            self.start(restarted.run_network_loop())
            await self.wait_for(resolution_attempted.wait())
            connect.assert_not_called()
            self.api.send_heartbeat.assert_not_awaited()

            self.api.retrieve_gateway_self.side_effect = None
            self.api.list_gateway_stations.side_effect = None
            await self.wait_for(connected.listening.wait())
            await self.wait_for(heartbeat_sent.wait())
            await self.wait_for(self.settings_synced.wait())
            connect.assert_called_once_with("ws://127.0.0.1:8000/ws/devices/gateways/2/")
            self.api.send_heartbeat.assert_awaited_once_with("new-token")

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

        self.api.list_gateway_stations.side_effect = httpx2.ConnectError("offline")
        self.assertFalse(await self.client.sync_settings())
        self.assertEqual(await Gateway.all().values_list("id", flat=True), [1])
        self.assertEqual((await WeighingStation.get(id=1)).serial_port, "mock-port")
        self.manager.sync.assert_not_called()

        self.api.list_gateway_stations.side_effect = None
        self.api.list_gateway_stations.return_value = [replacement, {"id": 3, "gateway": 2}]
        self.assertFalse(await self.client.sync_settings())
        self.assertEqual(await Gateway.all().values_list("id", flat=True), [1])
        self.assertEqual((await Gateway.get(id=1)).access_token, "new-token")
        self.assertEqual(await WeighingStation.all().values_list("id", flat=True), [1])
        self.assertEqual((await WeighingStation.get(id=1)).serial_port, "mock-port")
        self.manager.sync.assert_not_called()

        self.api.list_gateway_stations.return_value = [replacement]
        self.assertTrue(await self.client.sync_settings())
        self.assertEqual(await Gateway.all().values_list("id", flat=True), [2])
        self.assertEqual((await Gateway.get(id=2)).access_token, "new-token")
        self.assertEqual(await WeighingStation.all().values_list("id", flat=True), [2])
        station = await WeighingStation.get(id=2)
        self.assertEqual((station.gateway_id, station.serial_port), (2, "replacement-port"))
        self.assertEqual(self.manager.sync.call_args.args[0][0].id, 2)

    async def test_failed_station_notification_retries_on_same_socket_and_applies_configuration(self):
        await self.cached_setup()
        self.online_responses()
        current = self.api.list_gateway_stations.return_value
        updated = [dict(current[0], name="Updated station", serial_port="updated-port")]
        self.api.list_gateway_stations.side_effect = [
            current, httpx2.ConnectError("temporary outage"), updated,
        ]
        notifying = StubWebSocket(
            hold=asyncio.Event(),
            messages=[json.dumps({"type": "sync.weighing_stations"})],
            after_messages=asyncio.Event(),
        )
        connected = StubWebSocket(hold=asyncio.Event())
        self.connections.sockets = [notifying, connected]
        self.start(self.client.run())
        await self.wait_for(self.settings_synced.wait())
        self.settings_synced.clear()
        notifying.hold.set()
        await self.wait_for(self.settings_synced.wait())

        station = await WeighingStation.get(id=1)
        self.assertEqual((station.name, station.serial_port), ("Updated station", "updated-port"))
        self.assertEqual(self.connections.opened.qsize(), 1)
        self.assertEqual(self.api.list_gateway_stations.await_count, 3)
        self.assertFalse(connected.listening.is_set())
        self.assertFalse(notifying.after_messages.is_set())
        self.assertEqual(self.manager.sync.call_args.args[0][0].serial_port, "updated-port")
        self.manager.stop_all.assert_not_called()

    async def test_without_server_url_stations_and_local_storage_keep_running(self):
        await self.cached_setup()
        with patch("main.APIClient") as api_client, patch("main.websockets.connect") as connect:
            client = HeadlessClient(base_url=None)
            client.setup = AsyncMock()
            self.start(client.run())
            await self.wait_for(self.started.wait())
            event = WeighingCompletedEvent(rfid_card_uid="12345678", weight=41)
            client.handle_hardware_event(event)
            await asyncio.sleep(0)
            await self.wait_for(client.event_queue.join())

        self.assertEqual((await Record.get(uuid=event.uuid)).weight, 41)
        self.assertEqual(client.market_cache.get_rfid_info("12345678").producer_name, "Cached producer")
        api_client.assert_not_called()
        connect.assert_not_called()

    async def test_repeated_server_failures_are_logged_once_until_resynchronized(self):
        await self.cached_setup()
        logger = self.client.logger
        retried = asyncio.Event()

        async def offline(token):
            if self.api.retrieve_gateway_self.await_count == 20:
                retried.set()
            raise httpx2.ConnectError("offline")

        self.api.retrieve_gateway_self.side_effect = offline
        connected = StubWebSocket(hold=asyncio.Event())
        self.connections.sockets = [connected]
        self.start(self.client.run())
        await self.wait_for(retried.wait())

        self.assertEqual([c.args[1] for c in logger.log.call_args_list], ["sys.boot.network.offline"])
        self.assertEqual(
            [c for c in logger.info.call_args_list if c.args[0] == "sys.boot.remote_api.syncing"],
            [call("sys.boot.remote_api.syncing")],
        )

        self.online_responses()
        await self.wait_for(connected.listening.wait())
        await self.wait_for(self.settings_synced.wait())
        recovered = [c for c in logger.info.call_args_list if c.args[0] == "net.server.recovered"]
        self.assertEqual(len(recovered), 1)
        self.assertGreaterEqual(recovered[0].kwargs["total_failures"], 20)
        self.assertFalse(self.client.network_failures.failing)

    async def test_repeated_network_task_failures_are_summarized(self):
        attempts = asyncio.Event()

        async def rejected_registration():
            if self.client.run_provisioning_loop.await_count == 20:
                attempts.set()
            raise RuntimeError("provisioning socket rejected")

        self.client.run_provisioning_loop = AsyncMock(side_effect=rejected_registration)
        self.start(self.client.run())
        await self.wait_for(attempts.wait())

        failures = [c for c in self.client.logger.log.call_args_list if c.args[1] == "net.worker.failed"]
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0].kwargs["exc_info"].exceptions[0], RuntimeError)

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


class DatabaseLocationTests(unittest.IsolatedAsyncioTestCase):
    async def test_database_file_is_created_at_the_configured_path_and_reused(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        # The configured path decides the location, not the working directory.
        self.enterContext(chdir(self.enterContext(tempfile.TemporaryDirectory())))
        db_path = root / "data" / "db.sqlite3"
        record_uuid = uuid.uuid4()

        client = HeadlessClient(base_url="http://127.0.0.1:8000", db_path=db_path)
        client.logger = Mock()
        try:
            await client.setup()
            await Record.create(
                uuid=record_uuid, rfid_card_uid="12345678", weight=31,
                measured_at=datetime(2026, 10, 11, tzinfo=UTC),
            )
        finally:
            await client.close()
        self.assertTrue(db_path.is_file())
        self.assertFalse(list(Path.cwd().iterdir()))

        restarted = HeadlessClient(base_url="http://127.0.0.1:8000", db_path=db_path)
        restarted.logger = Mock()
        try:
            await restarted.setup()
            self.assertEqual((await Record.get(uuid=record_uuid)).weight, 31)
        finally:
            await restarted.close()


if __name__ == "__main__":
    unittest.main()
