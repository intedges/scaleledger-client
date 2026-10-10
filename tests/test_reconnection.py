import asyncio
from contextlib import suppress
import json
import unittest
from unittest.mock import AsyncMock, Mock

import websockets
from websockets.protocol import State

from main import HeadlessClient


class WebSocketReconnectionTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def make_client(base_url):
        client = object.__new__(HeadlessClient)
        client.ws_url = base_url
        client.provisioning_url = f"{base_url}/ws/devices/gateways/provisioning/"
        client.ws_kwargs = {}
        client.retry_interval = 0.01
        client.gateway_id = 1
        client._gateway_data = {"id": 1}
        client.access_token = "test-token"
        client.mac_address = "00:11:22:33:44:55"
        client.hostname = "test-gateway"
        client.ip_address = "127.0.0.1"
        client.logger = Mock()
        client.bootstrap = AsyncMock(return_value=True)
        client.sync_market_data = AsyncMock(return_value=True)
        client.refresh_market_cache = AsyncMock()
        client.sync_weighing_stations = AsyncMock(return_value=True)
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

                    async def sync_stations():
                        # Each connection performs initial sync, then receives a command.
                        if client.sync_weighing_stations.await_count == 4:
                            command_received_again.set()
                        return True

                    client.sync_weighing_stations.side_effect = sync_stations
                    task = asyncio.create_task(client.run_active_ws_loop())
                    try:
                        await asyncio.wait_for(command_received_again.wait(), timeout=2)
                        self.assertEqual(connections, ["/ws/devices/gateways/1/"] * 2)
                        self.assertEqual(client.bootstrap.await_count, 2)
                        self.assertEqual(client.sync_market_data.await_count, 2)
                        self.assertEqual(client.refresh_market_cache.await_count, 2)
                    finally:
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task

    async def test_provisioning_reconnects_and_closes_before_registration_handover(self):
        connections = []
        identities = []

        async def handler(ws):
            connections.append(ws)
            await ws.send(json.dumps({"type": "identify"}))
            identities.append(json.loads(await ws.recv()))
            if len(connections) == 1:
                await ws.close(code=1000)
            else:
                await ws.send(json.dumps({
                    "type": "gateway.registered",
                    "payload": {"access_token": "new-test-token"},
                }))
                await ws.wait_closed()

        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            client = self.make_client(f"ws://127.0.0.1:{port}")
            client.access_token = None
            await asyncio.wait_for(client.run_provisioning_loop(), timeout=2)

            self.assertEqual(len(connections), 2)
            self.assertEqual(identities, [{
                "type": "identity",
                "payload": {
                    "mac_address": "00:11:22:33:44:55",
                    "hostname": "test-gateway",
                    "ip_address": "127.0.0.1",
                },
            }] * 2)
            self.assertEqual(client.access_token, "new-test-token")
            self.assertIsNone(client.gateway_id)
            self.assertIs(connections[-1].state, State.CLOSED)
