import asyncio
from concurrent.futures import Future
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
import json
import sys
import threading
import unittest
from unittest.mock import AsyncMock, Mock, patch
from uuid import UUID

from cache import MarketDataCache
from managers import StationRuntime, WeighingStationManager
from models import Record
from printer import ReceiptTemplate
from reprint import ReprintRequest, ReprintRequestError
from suwol1000 import (
    DisplayRequestPacket, PrinterRequestPacket, PrinterStatus, ResponsePacket, SerialClient,
    WeighingStationWorker, WeightStatus, WeightType, WorkerState,
)
from weighing_cycle import CycleState
from tests import test_client as lifecycle
from tests import test_heartbeat as heartbeat
from tests.test_heartbeat import ControlSocket


def reprint_payload(*, station_id=7, copies=3):
    return {
        "station_id": station_id,
        "copies": copies,
        "receipt": {
            "record_uuid": "55a7c359-8524-469b-8c13-c447f8eaadf9",
            "gateway_name": "변경된 경매장 PC",
            "station_name": "지정한 계량대",
            "rfid_card_uid": "미지정",
            "producer_name": "서버의 생산자",
            "species_name": "미지정",
            "weight": 240,
            "measured_at": "2026-11-15T09:34:56+09:00",
            "auction_name": "",
        },
    }


class ReprintPayloadTests(unittest.TestCase):
    def test_server_values_are_preserved_and_parsed_without_local_records(self):
        payload = reprint_payload()
        payload["receipt"]["auction_name"] = "긴한국어경매장이름" * 12
        request = ReprintRequest.from_payload(payload)
        receipt = request.receipt

        self.assertEqual(request.station_id, 7)
        self.assertEqual(request.copies, 3)
        self.assertEqual(receipt.record_uuid, UUID(payload["receipt"]["record_uuid"]))
        self.assertEqual(receipt.measured_at.isoformat(), payload["receipt"]["measured_at"])
        self.assertEqual(receipt.measured_at.utcoffset(), timedelta(hours=9))
        self.assertEqual(receipt.weight, Decimal(240))
        text = ReceiptTemplate.render(receipt).decode("cp949")
        for value in ("55A7C359", "서버의 생산자", "미지정", "변경된 경매장 PC", "지정한 계량대", "240 kg"):
            self.assertIn(value, text)
        self.assertIn(payload["receipt"]["auction_name"], text.replace("\n", ""))

    def test_required_fields_and_types_are_not_defaulted_or_coerced(self):
        valid = reprint_payload()
        invalid = [None, [], "payload", {}, {"station_id": 7, "copies": 3}]
        for field, values in {
            "station_id": [None, True, False, 0, -1, 7.0, "7", [], {}],
            "copies": [None, True, False, 0, -1, 10, 1.0, 1.5, "3", [], {}],
            "receipt": [None, True, 3, "receipt", [], {}],
        }.items():
            missing = deepcopy(valid)
            missing.pop(field)
            invalid.append(missing)
            for value in values:
                payload = deepcopy(valid)
                payload[field] = value
                invalid.append(payload)
        for field in valid["receipt"]:
            missing = deepcopy(valid)
            missing["receipt"].pop(field)
            invalid.append(missing)
            for value in (None, True, [], {}):
                payload = deepcopy(valid)
                payload["receipt"][field] = value
                invalid.append(payload)
        for field, values in {
            "record_uuid": [1, "", "not-a-uuid"],
            "weight": ["240", 240.0, 240.5, float("nan"), float("inf")],
            "measured_at": [1, "", "2026-11-15", "2026-11-15T09:34:56", "2026-13-01T00:00:00Z"],
            "gateway_name": [1], "station_name": [1], "rfid_card_uid": [1],
            "producer_name": [1], "species_name": [1], "auction_name": [1],
        }.items():
            for value in values:
                payload = deepcopy(valid)
                payload["receipt"][field] = value
                invalid.append(payload)
        for payload in invalid:
            with self.subTest(payload=payload), self.assertRaises(ReprintRequestError):
                ReprintRequest.from_payload(payload)

    def test_selected_copies_and_valid_utc_offset_are_accepted(self):
        for copies in (1, 3, 9):
            payload = reprint_payload(copies=copies)
            payload["receipt"]["measured_at"] = "2026-11-15T00:34:56Z"
            with self.subTest(copies=copies):
                request = ReprintRequest.from_payload(payload)
                self.assertEqual(request.copies, copies)
                self.assertEqual(request.receipt.measured_at.utcoffset(), timedelta(0))

    def test_control_and_unencodable_text_is_rejected_before_rendering(self):
        for field in ("gateway_name", "station_name", "rfid_card_uid", "producer_name",
                      "species_name", "auction_name"):
            for value in ("이름\n다음줄", "이름\r", "이름\t", "이름\x00", "\x1b@", "이름🐟", "이름\ud800"):
                with self.subTest(field=field, value=repr(value)):
                    payload = reprint_payload()
                    payload["receipt"][field] = value
                    with self.assertRaises(ReprintRequestError):
                        ReprintRequest.from_payload(payload)


class ReprintRoutingTests(unittest.TestCase):
    def test_missing_foreign_and_stopped_station_never_fall_back_to_another_worker(self):
        request = ReprintRequest.from_payload(reprint_payload())
        for gateway_id, alive, reason in ((2, True, "wrong_gateway"), (1, False, "not_running")):
            with self.subTest(reason=reason):
                manager = WeighingStationManager(Mock(), MarketDataCache())
                worker = Mock()
                manager.workers[7] = StationRuntime(
                    worker, Mock(is_alive=Mock(return_value=alive)), "COM7", "계량대",
                    gateway_id=gateway_id,
                )
                for station_id, expected in ((7, reason), (99, "unknown_station")):
                    result = manager.request_reprint(
                        station_id, request.receipt, copies=request.copies, gateway_id=1,
                    )
                    with self.assertRaises(ReprintRequestError) as caught:
                        result.result()
                    self.assertEqual(caught.exception.reason, expected)
                worker.request_reprint.assert_not_called()


class StationReprintControlTests(unittest.IsolatedAsyncioTestCase):
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
        socket.messages.put_nowait(json.dumps({"type": "station.reprint", "payload": payload}))

    async def test_server_receipt_reaches_only_selected_worker_without_record_or_ack(self):
        manager = WeighingStationManager(Mock(), MarketDataCache())
        workers = {7: Mock(), 11: Mock()}
        pending = Future()
        workers[7].request_reprint.return_value = pending
        for station_id, worker in workers.items():
            manager.workers[station_id] = StationRuntime(
                worker, Mock(is_alive=Mock(return_value=True)), f"fake-{station_id}", "로컬 이름",
                gateway_id=1,
            )
        self.client.station_manager = manager
        socket, listener = await self.start_listener()
        payload = reprint_payload()
        self.request(socket, payload)
        await self.wait_for(socket.messages.join())

        request = ReprintRequest.from_payload(payload)
        workers[7].request_reprint.assert_called_once_with(request.receipt, copies=3)
        workers[11].request_reprint.assert_not_called()
        self.assertFalse(pending.done())
        self.assertFalse(listener.done())
        self.assertTrue(socket.sent.empty())
        self.assertEqual(await Record.all().count(), 0)
        self.assertTrue(self.client.event_queue.empty())
        self.assertTrue(self.client.upload_queue.empty())

        await asyncio.to_thread(pending.set_result, PrinterStatus.NORMAL)
        self.client.logger.info.assert_any_call(
            "biz.active.reprint.command_sent", station_id=7,
            record_uuid=payload["receipt"]["record_uuid"], copies=3,
        )
        self.assertTrue(socket.sent.empty())
        self.assertFalse(listener.done())

    async def test_invalid_payloads_and_unresolved_gateway_leave_listener_usable(self):
        socket, listener = await self.start_listener()
        for payload in (None, [], {}, {"station_id": 7}, reprint_payload(copies=True)):
            self.request(socket, payload)
        for field in ("record_uuid", "measured_at", "producer_name", "weight"):
            payload = reprint_payload()
            payload["receipt"][field] = None
            self.request(socket, payload)
        await self.wait_for(socket.messages.join())
        for gateway_id in (None, 0, -1, True, "1"):
            self.client.gateway_id = gateway_id
            self.request(socket, reprint_payload())
            await self.wait_for(socket.messages.join())
        self.manager.request_reprint.assert_not_called()
        self.assertFalse(listener.done())
        self.client.gateway_id = 1
        self.manager.request_reprint.return_value = Future()
        self.request(socket, reprint_payload())
        await self.wait_for(socket.messages.join())
        self.manager.request_reprint.assert_called_once()

    async def test_invalid_json_frames_preserve_reprint_and_heartbeat_handling(self):
        socket, listener = await self.start_listener()
        self.manager.request_reprint.return_value = Future()
        self.client.send_heartbeat = AsyncMock()
        malformed = [b"\xff"]
        max_digits = sys.get_int_max_str_digits()
        if max_digits:
            malformed.append(
                '{"type":"station.reprint","payload":{"station_id":7,"copies":'
                + "9" * (max_digits + 1) + "}}"
            )
        for count, message in enumerate(malformed, 1):
            with self.subTest(message_type=type(message).__name__):
                socket.messages.put_nowait(message)
                self.request(socket, reprint_payload())
                socket.notify("heartbeat.request")
                await self.wait_for(socket.messages.join())
                await asyncio.sleep(0)
                self.assertFalse(listener.done())
                self.assertEqual(self.manager.request_reprint.call_count, count)
                self.assertEqual(self.client.send_heartbeat.await_count, count)
        self.assertEqual(self.client.logger.error.call_count, len(malformed))
        self.client.logger.error.assert_called_with("net.ws.message.invalid_json")

    async def test_decoder_depth_error_preserves_followup_control_messages(self):
        socket, listener = await self.start_listener()
        self.manager.request_reprint.return_value = Future()
        self.client.send_heartbeat = AsyncMock()
        original_loads = json.loads
        nested = '{"type":"station.reprint","payload":' + "[" * 1100 + "0" + "]" * 1100 + "}"

        def decode(message):
            if message == nested:
                # The C decoder's nesting limit varies between Python builds.
                raise RecursionError("maximum JSON nesting depth exceeded")
            return original_loads(message)

        with patch("main.json.loads", side_effect=decode):
            socket.messages.put_nowait(nested)
            self.request(socket, reprint_payload())
            socket.notify("heartbeat.request")
            await self.wait_for(socket.messages.join())
            await asyncio.sleep(0)
        self.assertFalse(listener.done())
        self.manager.request_reprint.assert_called_once()
        self.client.send_heartbeat.assert_awaited_once()
        self.client.logger.error.assert_called_once_with("net.ws.message.invalid_json")

    async def test_socket_command_reaches_real_worker_and_packet_without_creating_record(self):
        manager = WeighingStationManager(Mock(), MarketDataCache())
        serial = SerialClient("reprint-integration-test-port")
        serial.send_and_receive = Mock(return_value=ResponsePacket(
            weight_value=Decimal(240), weight_unit="kg", weight_status=WeightStatus.STABLE,
            weight_type=WeightType.GROSS, printer_status=PrinterStatus.NORMAL,
        ))
        worker = WeighingStationWorker(serial, station_id=7, on_event=manager.on_event)
        worker.state = WorkerState.IDLE
        worker.cycle.state = CycleState.LOCKED
        ready, dispatch = threading.Event(), threading.Event()
        outcome = Future()

        def port_owner():
            try:
                # Establish the real worker's fresh telemetry from a serial reply.
                worker.exchange(DisplayRequestPacket())
                ready.set()
                if not dispatch.wait(timeout=2):
                    raise TimeoutError("test did not hand off a command")
                outcome.set_result(worker._run_reprint_request())
            except Exception as exc:
                outcome.set_exception(exc)

        owner = threading.Thread(target=port_owner, daemon=True)
        manager.workers[7] = StationRuntime(
            worker, owner, serial.port, "로컬 계량대", gateway_id=1,
        )
        self.client.station_manager = manager
        owner.start()
        try:
            self.assertTrue(await asyncio.to_thread(ready.wait, 1))
            socket, listener = await self.start_listener()
            count_before = await Record.all().count()
            payload = reprint_payload(copies=9)
            self.request(socket, payload)
            await self.wait_for(socket.messages.join())
            self.assertIsNotNone(worker._reprint_request)
            dispatch.set()
            self.assertTrue(await asyncio.to_thread(outcome.result, 2))

            packets = [call.args[0] for call in serial.send_and_receive.call_args_list]
            self.assertEqual(len(packets), 2)
            self.assertIsInstance(packets[0], DisplayRequestPacket)
            self.assertIsInstance(packets[1], PrinterRequestPacket)
            self.assertEqual(packets[1].copies, 9)
            self.assertEqual(
                packets[1].document_bytes,
                ReceiptTemplate.render(ReprintRequest.from_payload(payload).receipt),
            )
            manager.on_event.assert_not_called()
            self.assertEqual(count_before, 0)
            self.assertEqual(await Record.all().count(), count_before)
            self.assertTrue(self.client.event_queue.empty())
            self.assertTrue(self.client.upload_queue.empty())
            self.assertTrue(socket.sent.empty())
            self.assertFalse(listener.done())
            self.assertIsNone(worker._reprint_request)
            self.assertEqual(worker.cycle.state, CycleState.LOCKED)
        finally:
            dispatch.set()
            await asyncio.to_thread(owner.join, 2)
        self.assertFalse(owner.is_alive())

    async def test_pending_reprint_keeps_heartbeat_sync_and_reconnect_without_replay(self):
        first, second = ControlSocket(), ControlSocket()
        socket, running = await self.start_connected(sockets=[first, second])
        pending = Future()
        self.manager.request_reprint.return_value = pending
        heartbeat_sent = asyncio.Event()

        async def heartbeat(token):
            heartbeat_sent.set()
            return {}

        self.api.send_heartbeat.side_effect = heartbeat
        self.settings_synced.clear()
        self.request(socket, reprint_payload(station_id=1))
        socket.notify("heartbeat.request")
        socket.notify("sync.weighing_stations")
        await self.wait_for(socket.messages.join())
        await self.wait_for(heartbeat_sent.wait())
        await self.wait_for(self.settings_synced.wait())
        self.api.send_heartbeat.assert_awaited_once_with("cached-token")
        self.assertGreaterEqual(self.api.list_gateway_stations.await_count, 2)

        socket.disconnect()
        await self.wait_for(second.listening.wait())
        self.assertEqual(self.manager.request_reprint.call_count, 1)
        self.assertFalse(pending.done())
        self.assertFalse(pending.cancelled())
        self.assertFalse(running.done())
        await asyncio.to_thread(pending.set_exception, TimeoutError("response lost"))
        await asyncio.sleep(0)
        self.assertEqual(self.manager.request_reprint.call_count, 1)
        self.assertTrue(first.sent.empty())
        self.assertTrue(second.sent.empty())

        # An explicit follow-up for the same record is a new operator request.
        self.manager.request_reprint.return_value = Future()
        self.request(second, reprint_payload(station_id=1, copies=9))
        await self.wait_for(second.messages.join())
        self.assertEqual(self.manager.request_reprint.call_count, 2)
        self.assertEqual(self.manager.request_reprint.call_args.kwargs["copies"], 9)

    async def test_rejections_failures_and_printer_statuses_do_not_trigger_retry(self):
        socket, listener = await self.start_listener()
        outcomes = [RuntimeError("dispatch failure")]
        for result in (ReprintRequestError("busy"), RuntimeError("serial failure"),
                       PrinterStatus.NORMAL, PrinterStatus.TRANSMITTING, PrinterStatus.NO_PAPER, None):
            future = Future()
            if isinstance(result, Exception):
                future.set_exception(result)
            else:
                future.set_result(result)
            outcomes.append(future)
        cancelled = Future()
        cancelled.cancel()
        outcomes.append(cancelled)
        self.manager.request_reprint.side_effect = outcomes
        for _ in outcomes:
            self.request(socket, reprint_payload())
            await self.wait_for(socket.messages.join())
            self.assertFalse(listener.done())
        self.assertEqual(self.manager.request_reprint.call_count, len(outcomes))
        self.assertEqual(
            [call.args[0] for call in self.client.logger.exception.call_args_list],
            ["biz.active.reprint.dispatch_failed", "biz.active.reprint.failed"],
        )
        self.assertEqual(
            [call.args[0] for call in self.client.logger.warning.call_args_list],
            ["biz.active.reprint.rejected", "biz.active.reprint.no_paper", "biz.active.reprint.status_unknown"],
        )
        self.client.logger.info.assert_any_call(
            "biz.active.reprint.transmitting", station_id=7,
            record_uuid=reprint_payload()["receipt"]["record_uuid"], copies=3,
        )
        self.assertTrue(socket.sent.empty())


if __name__ == "__main__":
    unittest.main()
