import asyncio
import json
import unittest
from unittest.mock import patch
import uuid

import httpx2
from websockets.exceptions import ConnectionClosedError

from api import APIClient
from models import Record, RFIDCard, WeighingStation
from tests import test_client as lifecycle


class ControlSocket:
    def __init__(self):
        self.messages = asyncio.Queue()
        self.sent = asyncio.Queue()
        self.listening = asyncio.Event()

    async def __aiter__(self):
        self.listening.set()
        while True:
            message = await self.messages.get()
            try:
                if message is None:
                    return
                if isinstance(message, Exception):
                    raise message
                yield message
            finally:
                self.messages.task_done()

    async def send(self, message):
        self.sent.put_nowait(json.loads(message))

    def notify(self, message_type):
        self.messages.put_nowait(json.dumps({"type": message_type}))

    def disconnect(self, error=None):
        self.messages.put_nowait(error)


class HeartbeatTests(unittest.IsolatedAsyncioTestCase):
    # Reuse lifecycle helpers without collecting its tests a second time.
    asyncSetUp = lifecycle.ClientLifecycleTests.asyncSetUp
    asyncTearDown = lifecycle.ClientLifecycleTests.asyncTearDown
    start = lifecycle.ClientLifecycleTests.start
    wait_for = lifecycle.ClientLifecycleTests.wait_for
    cached_setup = lifecycle.ClientLifecycleTests.cached_setup
    online_responses = lifecycle.ClientLifecycleTests.online_responses

    @staticmethod
    def http_error(status):
        request = httpx2.Request("POST", "http://server/devices/api/gateways/heartbeat/")
        response = httpx2.Response(status, request=request)
        return httpx2.HTTPStatusError("Simulated response", request=request, response=response)

    async def start_connected(self, *, sockets=None):
        await self.cached_setup()
        self.online_responses()
        socket = ControlSocket() if sockets is None else sockets[0]
        self.connections.sockets = [socket] if sockets is None else sockets
        running = self.start(self.client.run())
        await self.wait_for(socket.listening.wait())
        await self.wait_for(self.settings_synced.wait())
        return socket, running

    async def test_startup_reconnect_and_other_messages_do_not_send_heartbeat(self):
        first, second = ControlSocket(), ControlSocket()
        socket, _ = await self.start_connected(sockets=[first, second])
        socket.notify("sync.gateway")
        socket.notify("unknown.message")
        await self.wait_for(socket.messages.join())
        socket.disconnect()
        await self.wait_for(second.listening.wait())
        await asyncio.sleep(0.02)

        self.api.send_heartbeat.assert_not_awaited()
        self.assertTrue(first.sent.empty())
        self.assertTrue(second.sent.empty())

    async def test_each_completed_server_request_calls_existing_rest_api_once(self):
        socket, _ = await self.start_connected()
        requests = []

        def respond(request):
            requests.append(request)
            return httpx2.Response(200, json={})

        async_client = httpx2.AsyncClient
        with patch(
            "api.httpx2.AsyncClient",
            side_effect=lambda **kwargs: async_client(
                **kwargs, transport=httpx2.MockTransport(respond),
            ),
        ):
            api = APIClient("http://server/")
        self.addAsyncCleanup(api.close)
        self.api.send_heartbeat.side_effect = api.send_heartbeat
        for count in (1, 2):
            socket.notify("heartbeat.request")
            await self.wait_for(socket.messages.join())
            await asyncio.sleep(0)
            self.assertEqual(len(requests), count)
            self.assertEqual(self.api.send_heartbeat.await_count, count)
        for request in requests:
            self.assertEqual(request.method, "POST")
            self.assertEqual(str(request.url), "http://server/devices/api/gateways/heartbeat/")
            self.assertEqual(request.headers["Authorization"], "Gateway cached-token")
            self.assertEqual(request.content, b"")
            self.assertEqual(request.extensions["timeout"]["read"], 10.0)
        self.assertTrue(socket.sent.empty())

    async def test_requests_during_inflight_heartbeat_are_dropped_without_followup(self):
        socket, _ = await self.start_connected()
        entered, release = asyncio.Event(), asyncio.Event()

        async def blocked(token):
            entered.set()
            await release.wait()
            return {}

        self.api.send_heartbeat.side_effect = blocked
        socket.notify("heartbeat.request")
        await self.wait_for(entered.wait())
        for _ in range(20):
            socket.notify("heartbeat.request")
        await self.wait_for(socket.messages.join())
        self.api.send_heartbeat.assert_awaited_once_with("cached-token")
        release.set()
        await asyncio.sleep(0.02)
        self.api.send_heartbeat.assert_awaited_once()

        socket.notify("heartbeat.request")
        await self.wait_for(socket.messages.join())
        self.assertEqual(self.api.send_heartbeat.await_count, 2)
        self.assertTrue(socket.sent.empty())

    async def test_failures_wait_for_a_fresh_server_request_without_retrying(self):
        socket, running = await self.start_connected()
        errors = (
            httpx2.ReadTimeout("heartbeat timed out"),
            httpx2.ConnectError("network unavailable"),
            self.http_error(503),
            self.http_error(400),
        )
        for error in errors:
            with self.subTest(error=repr(error)):
                before = self.api.send_heartbeat.await_count
                self.api.send_heartbeat.side_effect = error
                socket.notify("heartbeat.request")
                await self.wait_for(socket.messages.join())
                await asyncio.sleep(0.02)
                self.assertEqual(self.api.send_heartbeat.await_count, before + 1)
                self.assertFalse(running.done())
                self.api.send_heartbeat.side_effect = None
                socket.notify("heartbeat.request")
                await self.wait_for(socket.messages.join())
                self.assertEqual(self.api.send_heartbeat.await_count, before + 2)
        self.assertEqual(self.connections.opened.qsize(), 1)
        self.assertTrue(socket.sent.empty())

    async def test_slow_settings_sync_does_not_delay_requested_heartbeat(self):
        socket, running = await self.start_connected()
        entered, release, heartbeat_sent = asyncio.Event(), asyncio.Event(), asyncio.Event()
        cards = self.api.fetch_rfid_cards.return_value

        async def slow_cards(token):
            entered.set()
            await release.wait()
            return cards

        self.api.fetch_rfid_cards.side_effect = slow_cards
        self.api.send_heartbeat.side_effect = lambda token: heartbeat_sent.set()
        socket.notify("sync.market_data")
        await self.wait_for(entered.wait())
        socket.notify("heartbeat.request")
        await self.wait_for(heartbeat_sent.wait())

        self.api.send_heartbeat.assert_awaited_once_with("cached-token")
        self.assertFalse(release.is_set())
        self.assertFalse(running.done())
        release.set()

    async def test_inflight_heartbeat_does_not_block_other_control_messages(self):
        socket, _ = await self.start_connected()
        entered = asyncio.Event()

        async def blocked(token):
            entered.set()
            await asyncio.Future()

        self.api.send_heartbeat.side_effect = blocked
        socket.notify("heartbeat.request")
        await self.wait_for(entered.wait())
        with patch("main.scan_peripherals", return_value=[{"device": "COM3"}]):
            socket.notify("scan.peripherals")
            self.assertEqual(await self.wait_for(socket.sent.get()), {
                "type": "peripherals.scanned", "payload": [{"device": "COM3"}],
            })
        self.api.send_heartbeat.assert_awaited_once()

    async def test_normal_and_failed_disconnect_cancel_before_next_connection(self):
        sockets = [ControlSocket(), ControlSocket(), ControlSocket()]
        _, running = await self.start_connected(sockets=sockets)
        entered, cancelled = asyncio.Queue(), asyncio.Queue()

        async def blocked(token):
            attempt = self.api.send_heartbeat.await_count
            entered.put_nowait(attempt)
            if attempt <= 2:
                try:
                    await asyncio.Future()
                finally:
                    cancelled.put_nowait(attempt)
            return {}

        self.api.send_heartbeat.side_effect = blocked
        for index, error in enumerate((None, ConnectionClosedError(None, None))):
            sockets[index].notify("heartbeat.request")
            self.assertEqual(await self.wait_for(entered.get()), index + 1)
            sockets[index].disconnect(error)
            await self.wait_for(sockets[index + 1].listening.wait())
            self.assertEqual(cancelled.get_nowait(), index + 1)
            self.assertEqual(self.api.send_heartbeat.await_count, index + 1)

        sockets[2].notify("heartbeat.request")
        self.assertEqual(await self.wait_for(entered.get()), 3)
        self.assertFalse(running.done())
        self.assertEqual(self.connections.opened.qsize(), 3)
        self.manager.stop_all.assert_not_called()

    async def test_shutdown_waits_for_inflight_heartbeat_cancellation(self):
        socket, running = await self.start_connected()
        entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def blocked(token):
            entered.set()
            try:
                await asyncio.Future()
            finally:
                cleaning.set()
                await release.wait()

        self.api.send_heartbeat.side_effect = blocked
        socket.notify("heartbeat.request")
        await self.wait_for(entered.wait())
        running.cancel()
        try:
            await self.wait_for(cleaning.wait())
            self.assertFalse(running.done())
            self.manager.stop_all.assert_not_called()
        finally:
            release.set()
        with self.assertRaises(asyncio.CancelledError):
            await self.wait_for(running)
        self.manager.stop_all.assert_called_once()

    async def assert_auth_recovery_preserves_pending_record(self, status):
        socket, running = await self.start_connected()
        record = await Record.create(
            uuid=uuid.uuid4(), rfid_card_uid="12345678", station_id=7,
            weight=23, measured_at=self.now,
        )
        uploading, renewed, approved = asyncio.Event(), asyncio.Event(), asyncio.Event()
        recovered = ControlSocket()

        async def blocked_upload(**kwargs):
            uploading.set()
            await asyncio.Future()

        async def enrollment(token, **identity):
            # The administrator replaced this PC; a new credential waits for approval.
            if token == "cached-token":
                return "revoked"
            renewed.set()
            await approved.wait()
            return "approved"

        self.api.create_record.side_effect = blocked_upload
        self.client.upload_queue.put_nowait(str(record.uuid))
        await self.wait_for(uploading.wait())
        self.api.request_enrollment.side_effect = enrollment
        self.api.send_heartbeat.side_effect = self.http_error(status)
        socket.notify("heartbeat.request")
        await self.wait_for(renewed.wait())

        renewed_token = json.loads(self.credential_path.read_text(encoding="utf-8"))["credential"]
        self.assertNotEqual(renewed_token, "cached-token")
        self.assertEqual(self.client.access_token, renewed_token)
        self.assertTrue(await Record.filter(uuid=record.uuid).exists())
        self.assertEqual(self.client.upload_queue.qsize(), 1)
        self.assertEqual((await WeighingStation.get(id=1)).name, "Server station")
        self.assertEqual(await RFIDCard.all().count(), 1)
        self.assertEqual(self.client.market_cache.gateway_name, "Server gateway")
        self.manager.stop_all.assert_not_called()

        self.connections.sockets = [recovered]
        self.api.send_heartbeat.side_effect = None
        self.api.create_record.side_effect = None
        approved.set()
        await self.wait_for(recovered.listening.wait())
        await self.wait_for(self.client.upload_queue.join())
        recovered.notify("heartbeat.request")
        await self.wait_for(recovered.messages.join())

        self.api.send_heartbeat.assert_awaited_with(renewed_token)
        self.assertEqual(self.api.send_heartbeat.await_count, 2)
        self.assertFalse(await Record.filter(uuid=record.uuid).exists())
        self.assertEqual(self.api.create_record.call_args.kwargs["access_token"], renewed_token)
        self.assertFalse(running.done())

    async def test_401_rechecks_approval_and_preserves_pending_record(self):
        await self.assert_auth_recovery_preserves_pending_record(401)

    async def test_403_rechecks_approval_and_preserves_pending_record(self):
        await self.assert_auth_recovery_preserves_pending_record(403)
