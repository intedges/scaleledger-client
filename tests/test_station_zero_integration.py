"""Connect control messages to the real station worker using simulated hardware."""

import asyncio
from concurrent.futures import Future
from decimal import Decimal
import json
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from events import WeighingCompletedEvent
from main import HeadlessClient
from managers import StationRuntime
from suwol1000 import (
    DisplayRequestPacket, PrinterRequestPacket, ResponsePacket, VoiceCode,
    WeighingStationWorker, WorkerState,
)
from tests.test_heartbeat import ControlSocket
from tests.test_station_zero import Clock, SimulatedStop, ZeroScale
from weighing_cycle import CycleState


class StationZeroIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.clock = Clock()
        self.logger = Mock()
        self.enterContext(patch("main.get_logger", return_value=self.logger))
        self.enterContext(patch("main.get_hostname", return_value="zero-test-gateway"))
        self.enterContext(patch("managers.get_logger", return_value=Mock()))
        self.enterContext(patch("suwol1000.get_logger", return_value=Mock()))
        self.client = HeadlessClient(base_url=None)
        self.client.gateway_id = 3
        self.scales = {}
        for station_id in (7, 11):
            scale = ZeroScale(self.clock)
            worker = WeighingStationWorker(
                scale, station_id=station_id, on_event=Mock(),
                on_telemetry=lambda sample, sid=station_id: self.client.telemetry_updates.update(sid, sample),
            )
            worker.stop_event = SimulatedStop(self.clock)
            worker.state = WorkerState.IDLE
            self.scales[station_id] = scale
            thread = Mock()
            thread.is_alive.return_value = True
            self.client.station_manager.workers[station_id] = StationRuntime(
                worker, thread, f"simulated-{station_id}", f"Station {station_id}", gateway_id=3,
            )
        self.worker = self.client.station_manager.workers[7].worker

    def prime(self):
        for runtime in self.client.station_manager.workers.values():
            runtime.worker.exchange(DisplayRequestPacket())

    @staticmethod
    def zero_message(socket):
        socket.messages.put_nowait(json.dumps({
            "type": "station.zero", "payload": {"station_id": 7},
        }))

    async def test_selected_station_pulses_updates_telemetry_and_accepts_a_new_visit(self):
        with patch("suwol1000.time", SimpleNamespace(monotonic=self.clock)), patch(
            "telemetry.time", SimpleNamespace(monotonic=self.clock),
        ):
            self.prime()
            socket = ControlSocket()
            self.zero_message(socket)
            socket.disconnect()
            await self.client.listen_active_ws(socket)
            self.assertIsNotNone(self.worker._zero_request)
            other = self.client.station_manager.workers[11].worker
            self.assertIsNone(other._zero_request)

            self.assertTrue(self.worker._run_zero_request())

            pulse = [call for call in self.scales[7].calls if call[3] is not None]
            self.assertEqual(len(pulse), 2)
            self.assertEqual([call[2].zero_output for call in pulse], [True, False])
            self.assertEqual([call[2].voice_code for call in pulse], [VoiceCode.ALERT_SOUND, VoiceCode.NONE])
            self.assertAlmostEqual(pulse[1][0] - pulse[0][0], 1.5)
            self.assertEqual(len(self.scales[11].calls), 1)  # Its initial observation only.
            self.assertTrue(socket.sent.empty())
            updates = [await self.client.telemetry_updates.get() for _ in range(4)]
            samples = {
                item["payload"]["station_id"]: item["payload"]
                for item in updates if item["type"] == "station.telemetry"
            }
            self.assertEqual(samples[7]["weight"], "0")
            self.assertEqual(samples[11]["weight"], "-990")
            self.assertEqual(self.worker.cycle.state, CycleState.WAIT_EMPTY)
            self.worker.on_event.assert_not_called()

            # Only new ordinary empty/entry observations re-arm card acceptance.
            self.worker.idle()
            self.clock.advance(1.0)
            self.worker.idle()
            self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
            self.scales[7].after_weight = Decimal("120")
            self.worker.idle()
            self.clock.advance(1.0)
            self.worker.state = self.worker.idle()
            self.assertEqual(self.worker.state, WorkerState.VERIFY)
            self.worker.state = self.worker.verify()
            saved = Future()
            self.worker.on_event.return_value = saved
            self.worker.state = self.worker.measure()
            record = self.worker.on_event.call_args.args[0]
            self.assertIsInstance(record, WeighingCompletedEvent)
            self.assertEqual((record.station_id, record.weight, record.rfid_card_uid), (7, 120, "87654321"))
            self.assertFalse(saved.done())

            self.worker.receipt_builder = Mock(return_value=b"new visit receipt")
            printed = []

            def print_response(request):
                printed.append(request)
                return ResponsePacket(weight_value=Decimal("120"))

            self.scales[7].send_and_receive = print_response
            saved.set_result(None)
            self.worker.state = self.worker.save()
            self.assertEqual(self.worker.state, WorkerState.PRINT)
            self.worker.state = self.worker.print()
            self.assertEqual(self.worker.state, WorkerState.IDLE)
            self.assertEqual(len(printed), 1)
            self.assertIsInstance(printed[0], PrinterRequestPacket)
            self.assertEqual(printed[0].document_bytes, b"new visit receipt")
            self.worker.receipt_builder.assert_called_once_with(record)

    async def test_control_disconnect_during_on_keeps_off_cleanup_without_reconnect_replay(self):
        with patch("suwol1000.time", SimpleNamespace(monotonic=self.clock)):
            self.prime()
            socket = ControlSocket()
            listener = asyncio.create_task(self.client.listen_active_ws(socket))
            go = threading.Event()
            finished = asyncio.Event()
            failures = []
            loop = asyncio.get_running_loop()

            async def close_control():
                socket.disconnect()
                await listener

            def disconnect_during_on():
                asyncio.run_coroutine_threadsafe(close_control(), loop).result(timeout=2)

            def run_pulse():
                try:
                    if not go.wait(timeout=2):
                        raise TimeoutError("Test did not dispatch zero")
                    self.worker._run_zero_request()
                except BaseException as exc:
                    failures.append(exc)
                finally:
                    loop.call_soon_threadsafe(finished.set)

            thread = threading.Thread(target=run_pulse, name="zero-integration")
            self.client.station_manager.workers[7].thread = thread
            self.scales[7].after_on = disconnect_during_on
            thread.start()
            try:
                self.zero_message(socket)
                await socket.messages.join()
                future = self.worker._zero_request.future
                go.set()
                await finished.wait()
                self.assertTrue(listener.done())
                self.assertEqual(failures, [])
                self.assertFalse(future.cancelled())
                self.assertTrue(future.result().output_released)
                self.assertEqual(self.scales[7].off_attempts, 1)

                reconnected = ControlSocket()
                reconnected.notify("unknown.message")
                reconnected.disconnect()
                await self.client.listen_active_ws(reconnected)
                self.assertFalse(self.worker._run_zero_request())
                self.assertEqual(sum(call[2].zero_output for call in self.scales[7].calls), 1)
                self.assertTrue(socket.sent.empty())
                self.assertTrue(reconnected.sent.empty())
            finally:
                go.set()
                await asyncio.to_thread(thread.join, 3)
                if not listener.done():
                    socket.disconnect()
                    await listener
                self.assertFalse(thread.is_alive())
