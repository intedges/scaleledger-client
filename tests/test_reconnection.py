import asyncio
from contextlib import suppress
from http import HTTPStatus
import json
import unittest
from unittest.mock import AsyncMock, Mock

import websockets

from api import AuthDegradedError
from logconfig import RepeatedFailureLog
from main import HeadlessClient


class WebSocketReconnectionTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def make_client(base_url):
        client = object.__new__(HeadlessClient)
        client.ws_url = base_url
        client.ws_kwargs = {}
        client.retry_interval = 0.01
        client.gateway_id = 1
        client._gateway_data = {"id": 1}
        client.access_token = "test-token"
        client.hostname = "test-gateway"
        client.server_host = "127.0.0.1"
        client.logger = Mock()
        client.network_failures = RepeatedFailureLog(client.logger, "net.server", settle_time=0)
        client.bootstrap = AsyncMock(return_value=True)
        client.request_settings_sync = Mock()
        return client

    async def test_active_socket_reconnects_after_normal_and_abnormal_closes(self):
        for close_code in (1000, 1011):
            with self.subTest(close_code=close_code):
                connections = []
                command_received_again = asyncio.Event()

                async def handler(ws):
                    connections.append(ws.request.path)
                    await ws.send(json.dumps({"type": "sync.weighing_stations"}))
                    if len(connections) == 1:
                        await ws.close(code=close_code)
                    else:
                        await ws.wait_closed()

                async with websockets.serve(handler, "127.0.0.1", 0) as server:
                    port = server.sockets[0].getsockname()[1]
                    client = self.make_client(f"ws://127.0.0.1:{port}")

                    def request_sync():
                        # Startup and each connection request a snapshot, then receive a command.
                        if client.request_settings_sync.call_count == 5:
                            command_received_again.set()

                    client.request_settings_sync.side_effect = request_sync
                    task = asyncio.create_task(client.run_active_ws_loop())
                    try:
                        await asyncio.wait_for(command_received_again.wait(), timeout=2)
                        self.assertEqual(connections, ["/ws/devices/gateways/1/"] * 2)
                        client.bootstrap.assert_not_awaited()
                        self.assertEqual(client.request_settings_sync.call_count, 5)
                    finally:
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task

    async def test_control_socket_presents_the_credential(self):
        authorizations = []
        received = asyncio.Event()

        async def handler(ws):
            authorizations.append(ws.request.headers["Authorization"])
            received.set()
            await ws.wait_closed()

        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            client = self.make_client(f"ws://127.0.0.1:{port}")
            task = asyncio.create_task(client.run_active_ws_loop())
            try:
                await asyncio.wait_for(received.wait(), timeout=2)
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

        self.assertEqual(authorizations, ["Gateway test-token"])

    async def test_rejected_control_handshake_returns_to_the_enrollment_check(self):
        for status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            with self.subTest(status=status):
                async def reject(connection, request):
                    return connection.respond(status, "Rejected")

                async with websockets.serve(AsyncMock(), "127.0.0.1", 0, process_request=reject) as server:
                    port = server.sockets[0].getsockname()[1]
                    client = self.make_client(f"ws://127.0.0.1:{port}")
                    with self.assertRaises(AuthDegradedError):
                        await asyncio.wait_for(client.run_active_ws_loop(), timeout=2)
