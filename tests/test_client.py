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
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, InvalidStatus
from websockets.http11 import Response

from api import APIClient
from events import WeighingCompletedEvent
from main import HeadlessClient
from models import Gateway, Producer, Record, RFIDCard, Species, WeighingStation


SERVER_URL = "http://127.0.0.1:8000"


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


class RejectedConnections:
    """A handshake the server refuses; websockets raises it instead of retrying."""

    def __init__(self, status):
        self.error = InvalidStatus(Response(status, "Rejected", Headers()))

    async def __aiter__(self):
        raise self.error
        yield


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
            "request_enrollment", "retrieve_gateway_self", "list_gateway_stations", "send_heartbeat",
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
            patch("main.get_ip_address", return_value="127.0.0.1"),
            patch("main.get_hostname", return_value="test-gateway"),
            patch("main.get_logger", return_value=Mock()),
            patch("main.websockets.connect", return_value=self.connections),
            patch("main.LiveWeightPublisher", return_value=self.publisher),
        )
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        # Restarted clients share this file, like the program's data folder.
        self.credential_path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "credential.json"
        self.client = HeadlessClient(base_url=SERVER_URL, credential_path=self.credential_path)
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

    async def cached_setup(self):
        # The credential approved before this start and the settings synchronized with it.
        self.credential_path.write_text(
            json.dumps({"server_url": SERVER_URL, "credential": "cached-token"}), encoding="utf-8",
        )
        self.gateway = await Gateway.create(id=1, name="Cached gateway")
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
            "request_enrollment": "approved",
            "retrieve_gateway_self": {
                "id": 1, "name": "Server gateway", "description": "",
                "hostname": "test-gateway", "ip_address": "127.0.0.1", "last_heartbeat": None,
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

    async def test_auth_rejection_keeps_the_credential_and_cached_settings_for_restart(self):
        await self.cached_setup()
        await self.client.restore_local_state()
        self.assertTrue(await self.client.prepare_credential())
        self.client.forget_server_session()

        self.assertEqual(self.client.access_token, "cached-token")
        self.assertEqual(self.client.gateway_id, 1)
        self.assertEqual(await WeighingStation.all().count(), 1)
        self.assertEqual(await RFIDCard.all().count(), 1)
        self.assertTrue(self.client.market_cache.get_rfid_info("12345678").is_active)

        restarted = HeadlessClient(base_url=SERVER_URL, credential_path=self.credential_path)
        self.assertTrue(await restarted.prepare_credential())
        await restarted.restore_local_state()
        self.assertEqual(restarted.access_token, "cached-token")
        self.assertEqual(restarted.gateway_id, 1)
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

    async def test_revoked_credential_is_replaced_and_kept_across_failed_sync_and_restart(self):
        await self.cached_setup()
        self.online_responses()
        sync_attempted = asyncio.Event()

        async def enrollment(token, **identity):
            # The administrator replaced this PC; its previous credential is no longer accepted.
            return "revoked" if token == "cached-token" else "approved"

        async def offline_stations(token):
            sync_attempted.set()
            raise httpx2.ConnectError("offline")

        self.api.request_enrollment.side_effect = enrollment
        self.api.list_gateway_stations.side_effect = offline_stations
        self.start(self.client.run())
        await self.wait_for(sync_attempted.wait())

        renewed = json.loads(self.credential_path.read_text(encoding="utf-8"))
        self.assertEqual(renewed["server_url"], SERVER_URL)
        self.assertNotEqual(renewed["credential"], "cached-token")
        self.assertEqual(self.client.access_token, renewed["credential"])
        self.assertEqual(
            [request.args[0] for request in self.api.request_enrollment.await_args_list],
            ["cached-token", renewed["credential"]],
        )
        self.api.retrieve_gateway_self.assert_awaited_with(renewed["credential"])
        self.assertEqual(self.client.market_cache.gateway_name, "Cached gateway")
        self.assertEqual((await WeighingStation.get(id=1)).name, "Cached station")

        restarted = HeadlessClient(base_url=SERVER_URL, credential_path=self.credential_path)
        self.assertTrue(await restarted.prepare_credential())
        self.assertEqual(restarted.access_token, renewed["credential"])

    async def test_first_run_creates_a_credential_waits_for_approval_and_reuses_it(self):
        answers = iter(["pending", "pending", "approved"])
        resolved = asyncio.Event()

        async def gateway(token):
            resolved.set()
            return {"id": 1, "name": "New gateway"}

        self.api.request_enrollment.side_effect = lambda token, **identity: next(answers)
        self.api.retrieve_gateway_self.side_effect = gateway
        self.start(self.client.run())
        await self.wait_for(resolved.wait())

        stored = json.loads(self.credential_path.read_text(encoding="utf-8"))
        self.assertEqual(stored["server_url"], SERVER_URL)
        self.assertEqual(self.client.access_token, stored["credential"])
        self.assertEqual(self.api.request_enrollment.await_count, 3)
        for request in self.api.request_enrollment.await_args_list:
            self.assertEqual(request.args, (stored["credential"],))
            self.assertEqual(request.kwargs, {"hostname": "test-gateway", "ip_address": "127.0.0.1"})
        pending = [c for c in self.client.logger.info.call_args_list if c.args[0] == "biz.enrollment.pending"]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].kwargs["hostname"], "test-gateway")
        self.assertEqual(self.client.gateway_id, 1)
        self.assertEqual(await Gateway.all().count(), 0)
        self.assertFalse(any(call.args[0] for call in self.manager.sync.call_args_list))

        restarted = HeadlessClient(base_url=SERVER_URL, credential_path=self.credential_path)
        self.assertTrue(await restarted.prepare_credential())
        self.assertEqual(restarted.access_token, stored["credential"])

    async def test_new_registration_resolves_its_socket_after_an_offline_restart(self):
        # Settings cached for Gateway 1; the approved credential now belongs to Gateway 2.
        await self.cached_setup()
        self.online_responses()
        self.api.retrieve_gateway_self.return_value.update(id=2)
        self.api.list_gateway_stations.return_value[0].update(id=2, gateway=2)
        server_gateway = self.api.retrieve_gateway_self.return_value

        heartbeat_sent = asyncio.Event()
        self.api.send_heartbeat.side_effect = lambda token: heartbeat_sent.set()
        resolution_attempted = asyncio.Event()

        async def unavailable_gateway(token):
            resolution_attempted.set()
            raise httpx2.ConnectError("offline")

        self.api.retrieve_gateway_self.side_effect = unavailable_gateway
        connected = StubWebSocket(
            messages=[json.dumps({"type": "heartbeat.request"})],
            after_messages=asyncio.Event(),
        )
        self.connections.sockets = [connected]
        with patch("main.websockets.connect", return_value=self.connections) as connect:
            self.start(self.client.run())
            await self.wait_for(resolution_attempted.wait())
            self.assertEqual(self.client.gateway_id, 1)
            self.assertEqual((await WeighingStation.get(id=1)).gateway_id, 1)
            connect.assert_not_called()
            self.api.send_heartbeat.assert_not_awaited()

            self.api.retrieve_gateway_self.side_effect = None
            self.api.retrieve_gateway_self.return_value = server_gateway
            await self.wait_for(connected.listening.wait())
            await self.wait_for(heartbeat_sent.wait())
            await self.wait_for(self.settings_synced.wait())
            connect.assert_called_once_with(
                "ws://127.0.0.1:8000/ws/devices/gateways/2/",
                additional_headers={"Authorization": "Gateway cached-token"},
            )
            self.api.send_heartbeat.assert_awaited_once_with("cached-token")

        self.assertEqual(self.client.gateway_id, 2)
        self.assertEqual(await Gateway.all().values_list("id", flat=True), [2])
        self.assertEqual((await WeighingStation.get(id=2)).gateway_id, 2)

    async def test_new_registration_replaces_gateway_only_with_complete_station_snapshot(self):
        await self.cached_setup()
        self.online_responses()
        self.assertTrue(await self.client.prepare_credential())
        await self.client.restore_local_state()
        self.manager.sync.reset_mock()
        self.api.retrieve_gateway_self.return_value.update(id=2)
        replacement = dict(
            self.api.list_gateway_stations.return_value[0],
            id=2, gateway=2, serial_port="replacement-port",
        )
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
        self.assertEqual(await WeighingStation.all().values_list("id", flat=True), [1])
        self.assertEqual((await WeighingStation.get(id=1)).serial_port, "mock-port")
        self.manager.sync.assert_not_called()

        self.api.list_gateway_stations.return_value = [replacement]
        self.assertTrue(await self.client.sync_settings())
        self.assertEqual(await Gateway.all().values_list("id", flat=True), [2])
        self.api.list_gateway_stations.assert_awaited_with("cached-token")
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
        self.api.request_enrollment.side_effect = None
        self.api.request_enrollment.return_value = "approved"
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

        async def rejected_enrollment():
            if self.client.wait_for_approval.await_count == 20:
                attempts.set()
            raise RuntimeError("enrollment check failed")

        self.client.wait_for_approval = AsyncMock(side_effect=rejected_enrollment)
        self.start(self.client.run())
        await self.wait_for(attempts.wait())

        failures = [c for c in self.client.logger.log.call_args_list if c.args[1] == "net.worker.failed"]
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0].kwargs["exc_info"].exceptions[0], RuntimeError)

    async def test_first_install_waits_for_approval_without_starting_stations(self):
        checked = asyncio.Event()

        async def pending(token, **identity):
            if self.api.request_enrollment.await_count == 3:
                checked.set()
            return "pending"

        self.api.request_enrollment.side_effect = pending
        self.start(self.client.run())
        await self.wait_for(checked.wait())

        self.assertTrue(self.credential_path.is_file())
        self.assertIsNone(self.client.gateway_id)
        self.assertFalse(any(call.args[0] for call in self.manager.sync.call_args_list))
        self.api.retrieve_gateway_self.assert_not_awaited()
        self.api.send_heartbeat.assert_not_awaited()
        self.api.list_gateway_stations.assert_not_awaited()
        logged = [c.args[0] for c in self.client.logger.info.call_args_list]
        self.assertEqual(logged.count("sys.auth.credential.created"), 1)
        self.assertEqual(logged.count("biz.enrollment.pending"), 1)

    async def test_another_server_discards_previous_data_and_requests_a_new_approval(self):
        await self.cached_setup()
        self.credential_path.write_text(
            json.dumps({"server_url": "http://previous-server:8000", "credential": "previous-token"}),
            encoding="utf-8",
        )
        await Record.create(uuid=uuid.uuid4(), rfid_card_uid="12345678", weight=19, measured_at=self.now)
        requested = asyncio.Event()

        async def pending(token, **identity):
            requested.set()
            return "pending"

        self.api.request_enrollment.side_effect = pending
        self.start(self.client.run())
        await self.wait_for(requested.wait())

        stored = json.loads(self.credential_path.read_text(encoding="utf-8"))
        self.assertEqual(stored["server_url"], SERVER_URL)
        self.assertNotEqual(stored["credential"], "previous-token")
        self.api.request_enrollment.assert_awaited_with(
            stored["credential"], hostname="test-gateway", ip_address="127.0.0.1",
        )
        for model in (Record, Gateway, WeighingStation, Species, Producer, RFIDCard):
            self.assertEqual(await model.all().count(), 0, model.__name__)
        self.assertIsNone(self.client.gateway_id)
        self.assertTrue(self.client.upload_queue.empty())
        self.assertFalse(any(call.args[0] for call in self.manager.sync.call_args_list))
        self.assertIn(
            call("sys.auth.server_changed", previous="http://previous-server:8000",
                 current=SERVER_URL, discarded_records=1),
            self.client.logger.warning.call_args_list,
        )

    async def test_damaged_credential_file_is_replaced_and_cached_settings_stay(self):
        await self.cached_setup()
        self.credential_path.write_text("{not json", encoding="utf-8")
        requested = asyncio.Event()

        async def pending(token, **identity):
            requested.set()
            return "pending"

        self.api.request_enrollment.side_effect = pending
        self.start(self.client.run())
        await self.wait_for(self.started.wait())
        await self.wait_for(requested.wait())

        stored = json.loads(self.credential_path.read_text(encoding="utf-8"))
        self.assertEqual(stored["server_url"], SERVER_URL)
        self.assertEqual(self.client.access_token, stored["credential"])
        self.assertEqual(self.client.gateway_id, 1)
        self.assertEqual(self.manager.sync.call_args.args[0][0].serial_port, "mock-port")
        warnings = [c.args[0] for c in self.client.logger.warning.call_args_list]
        self.assertIn("sys.auth.credential.invalid", warnings)
        self.assertNotIn("sys.auth.server_changed", warnings)

    async def test_unreadable_credential_keeps_weighing_without_the_server(self):
        await self.cached_setup()
        self.credential_path.unlink()
        # Reading a directory fails with an OSError on every platform.
        self.credential_path.mkdir()
        self.online_responses()
        self.start(self.client.run())
        await self.wait_for(self.started.wait())
        event = await self.emit_and_save()

        self.assertEqual((await Record.get(uuid=event.uuid)).weight, 12)
        self.api.request_enrollment.assert_not_awaited()
        self.api.create_record.assert_not_awaited()
        errors = [c.args[0] for c in self.client.logger.error.call_args_list]
        self.assertEqual(errors.count("sys.auth.credential.unusable"), 1)
        self.assertTrue(self.credential_path.is_dir())

    async def test_rejected_control_socket_rechecks_approval_and_keeps_the_credential(self):
        await self.cached_setup()
        self.online_responses()
        rechecked = asyncio.Event()

        async def approved(token, **identity):
            if self.api.request_enrollment.await_count == 2:
                rechecked.set()
            return "approved"

        self.api.request_enrollment.side_effect = approved
        with patch("main.websockets.connect", return_value=RejectedConnections(403)):
            self.start(self.client.run())
            await self.wait_for(rechecked.wait())

        self.assertEqual(
            [request.args[0] for request in self.api.request_enrollment.await_args_list[:2]],
            ["cached-token", "cached-token"],
        )
        self.assertEqual(json.loads(self.credential_path.read_text(encoding="utf-8"))["credential"], "cached-token")
        self.assertIn(call("sys.auth.credentials.rejected"), self.client.logger.warning.call_args_list)
        self.assertEqual(self.client.gateway_id, 1)
        self.manager.stop_all.assert_not_called()


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
