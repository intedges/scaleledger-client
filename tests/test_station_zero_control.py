import asyncio
from concurrent.futures import Future
import json
import unittest
from unittest.mock import Mock

from cache import MarketDataCache
from managers import StationRuntime, WeighingStationManager
from suwol1000 import ZeroRequestError, ZeroResult
from tests import test_client as lifecycle
from tests import test_heartbeat as heartbeat
from tests.test_heartbeat import ControlSocket


class StationZeroControlTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = lifecycle.ClientLifecycleTests.asyncSetUp
    asyncTearDown = lifecycle.ClientLifecycleTests.asyncTearDown
    start = lifecycle.ClientLifecycleTests.start
    wait_for = lifecycle.ClientLifecycleTests.wait_for
    cached_setup = lifecycle.ClientLifecycleTests.cached_setup
    online_responses = lifecycle.ClientLifecycleTests.online_responses
    start_connected = heartbeat.HeartbeatTests.start_connected

    async def start_listener(self):
        self.client.gateway_id = 1
        socket = ControlSocket()
        listener = self.start(self.client.listen_active_ws(socket))
        await self.wait_for(socket.listening.wait())
        return socket, listener

    @staticmethod
    def request(socket, payload):
        socket.messages.put_nowait(json.dumps({"type": "station.zero", "payload": payload}))

    async def test_control_command_uses_only_selected_worker_and_never_sends_ack(self):
        manager = WeighingStationManager(Mock(), MarketDataCache())
        workers = {7: Mock(), 11: Mock()}
        pending = Future()
        workers[7].request_zero.return_value = pending
        for station_id, worker in workers.items():
            manager.workers[station_id] = StationRuntime(
                worker, Mock(is_alive=Mock(return_value=True)), f"fake-{station_id}", "Station",
                gateway_id=1,
            )
        self.client.station_manager = manager
        socket, listener = await self.start_listener()

        self.request(socket, {"station_id": 99})
        self.request(socket, {"station_id": 7})
        await self.wait_for(socket.messages.join())

        workers[7].request_zero.assert_called_once_with()
        workers[11].request_zero.assert_not_called()
        self.assertFalse(pending.done())
        self.assertFalse(listener.done())
        self.assertTrue(socket.sent.empty())

        await asyncio.to_thread(pending.set_result, ZeroResult(True, True, -990, 0))

        self.client.logger.info.assert_any_call(
            "biz.active.zero.finished", station_id=7,
            output_released=True, zero_observed=True, error=None,
        )
        self.assertEqual(
            sum(call.args[0] == "biz.active.zero.finished" for call in self.client.logger.info.call_args_list),
            1,
        )
        self.assertFalse(listener.done())
        self.assertTrue(socket.sent.empty())
        workers[7].request_zero.assert_called_once_with()

    async def test_invalid_payloads_and_unresolved_gateway_do_not_reach_any_worker(self):
        socket, listener = await self.start_listener()
        for envelope in (None, [], 1, {}, {"type": None}, {"type": 1}):
            socket.messages.put_nowait(json.dumps(envelope))
        socket.messages.put_nowait("{invalid json")
        payloads = [None, [], "7", {}, {"station_id": True}, {"station_id": False}]
        payloads.extend({"station_id": value} for value in (0, -1, 7.0, "7", None, [], {}))
        for payload in payloads:
            self.request(socket, payload)
        socket.messages.put_nowait(json.dumps({"type": "station.zero"}))
        await self.wait_for(socket.messages.join())

        for gateway_id in (None, 0, -1, True, "1"):
            with self.subTest(gateway_id=gateway_id):
                self.client.gateway_id = gateway_id
                self.request(socket, {"station_id": 7})
                await self.wait_for(socket.messages.join())

        self.manager.request_zero.assert_not_called()
        self.assertFalse(listener.done())
        self.assertTrue(socket.sent.empty())

        self.client.gateway_id = 1
        self.manager.request_zero.return_value = Future()
        self.request(socket, {"station_id": 7})
        await self.wait_for(socket.messages.join())
        self.manager.request_zero.assert_called_once_with(7, gateway_id=1)

    async def test_pending_zero_does_not_block_heartbeat_sync_or_reconnect(self):
        first, second = ControlSocket(), ControlSocket()
        socket, running = await self.start_connected(sockets=[first, second])
        pending = Future()
        self.manager.request_zero.return_value = pending
        heartbeat_sent = asyncio.Event()

        async def heartbeat(token):
            heartbeat_sent.set()
            return {}

        self.api.send_heartbeat.side_effect = heartbeat
        self.settings_synced.clear()
        self.request(socket, {"station_id": 1})
        socket.notify("heartbeat.request")
        socket.notify("sync.weighing_stations")
        await self.wait_for(socket.messages.join())
        await self.wait_for(heartbeat_sent.wait())
        await self.wait_for(self.settings_synced.wait())

        self.api.send_heartbeat.assert_awaited_once_with("cached-token")
        self.assertGreaterEqual(self.api.list_gateway_stations.await_count, 2)
        self.assertFalse(pending.done())
        socket.disconnect()
        await self.wait_for(second.listening.wait())

        self.manager.request_zero.assert_called_once_with(1, gateway_id=1)
        self.assertFalse(pending.done())
        self.assertFalse(pending.cancelled())
        self.assertFalse(running.done())
        self.assertEqual(self.connections.opened.qsize(), 2)
        self.assertTrue(first.sent.empty())
        self.assertTrue(second.sent.empty())

        await asyncio.to_thread(pending.set_result, ZeroResult(True, False, -990, -990))
        second.notify("heartbeat.request")
        await self.wait_for(second.messages.join())
        await asyncio.sleep(0)

        self.assertEqual(self.api.send_heartbeat.await_count, 2)
        self.manager.request_zero.assert_called_once_with(1, gateway_id=1)
        self.assertTrue(second.sent.empty())
        self.assertFalse(running.done())

    async def test_rejected_failed_and_cancelled_requests_leave_listener_usable(self):
        socket, listener = await self.start_listener()
        rejected, failed, cancelled, succeeded = Future(), Future(), Future(), Future()
        rejected.set_exception(ZeroRequestError("busy"))
        cancelled.cancel()
        succeeded.set_result(ZeroResult(True, True, -990, 0))
        outcomes = [RuntimeError("simulated dispatch failure"), rejected, failed, cancelled, succeeded]
        self.manager.request_zero.side_effect = outcomes

        for outcome in outcomes:
            self.request(socket, {"station_id": 7})
            await self.wait_for(socket.messages.join())
            if outcome is failed:
                await asyncio.to_thread(failed.set_exception, RuntimeError("simulated worker failure"))
            self.assertFalse(listener.done())

        self.assertEqual(self.manager.request_zero.call_count, len(outcomes))
        self.client.logger.warning.assert_called_with(
            "biz.active.zero.rejected", station_id=7, reason="busy",
        )
        self.assertEqual(
            [call.args[0] for call in self.client.logger.exception.call_args_list],
            ["biz.active.zero.dispatch_failed", "biz.active.zero.failed"],
        )
        self.assertTrue(socket.sent.empty())


if __name__ == "__main__":
    unittest.main()
