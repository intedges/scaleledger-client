"""Exercise web reprints on the existing station owner without real hardware."""

from concurrent.futures import Future
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
import threading
import unittest
from unittest.mock import Mock, patch
from uuid import UUID

from serial import SerialException

from device_configuration import DeviceConfiguration
from events import RFIDTaggedEvent, WeighingCompletedEvent
from printer import Receipt, ReceiptTemplate
from reprint import ReprintRequestError
from suwol1000 import (
    DeviceResponseTimeout, DisplayRequestPacket, PrinterRequestPacket, PrinterStatus,
    ResponsePacket, WeighingStationWorker, WorkerState,
)
from tests.helpers import prepare_vehicle
from weighing_cycle import CycleState


class ReprintScale:
    port = "reprint-test-port"
    timeout = 2.0
    write_timeout = 2.0

    def __init__(self):
        self.requests = []
        self.owners = []
        self.connections = 0
        self.disconnections = 0
        self.interruptions = 0
        self.response = ResponsePacket(weight_value=Decimal("120"))
        self.on_print = None
        self.on_display = None
        self.on_disconnect = None
        self.interrupted = threading.Event()
        self.io_lock = threading.Lock()

    def connect(self):
        self.connections += 1

    def disconnect(self):
        self.disconnections += 1
        if self.on_disconnect is not None:
            self.on_disconnect()

    def interrupt(self):
        self.interruptions += 1
        self.interrupted.set()

    def send_and_receive(self, request):
        if not self.io_lock.acquire(blocking=False):
            raise AssertionError("동일 포트에서 병렬 I/O가 발생했습니다.")
        try:
            self.requests.append(request)
            self.owners.append(threading.get_ident())
            callback = self.on_print if isinstance(request, PrinterRequestPacket) else self.on_display
            if callback is not None:
                callback(request)
            return self.response
        finally:
            self.io_lock.release()


class StationReprintTests(unittest.TestCase):
    def setUp(self):
        self.now = 10.0
        self.enterContext(patch("suwol1000.time.monotonic", side_effect=lambda: self.now))
        self.enterContext(patch("suwol1000.get_logger", return_value=Mock()))
        self.receipt = Receipt(
            record_uuid=UUID("55a7c359-8524-469b-8c13-c447f8eaadf9"),
            gateway_name="서버 PC", station_name="서버 계량대", rfid_card_uid="미지정",
            producer_name="서버 생산자", species_name="미지정", weight=Decimal("240"),
            measured_at=datetime.fromisoformat("2026-11-15T09:34:56+09:00"),
            auction_name="동해 수산물 경매장",
        )
        self.reset_station()

    def reset_station(self):
        self.scale = ReprintScale()
        self.events = Mock()
        self.builder = Mock(side_effect=AssertionError("로컬 기준정보로 재발행하면 안 됩니다."))
        self.worker = WeighingStationWorker(
            self.scale, station_id=7, on_event=self.events, receipt_builder=self.builder,
            polling_interval=0, retry_interval=0,
            configuration=DeviceConfiguration(print_copies=9),
        )
        self.worker.state = WorkerState.IDLE
        self.worker.last_weight = Decimal("120")
        prepare_vehicle(self.worker)
        self.assertTrue(self.worker.cycle.consume(now=self.now))
        self.worker.exchange(DisplayRequestPacket())
        self.scale.requests.clear()
        self.scale.owners.clear()

    def print_requests(self):
        return [request for request in self.scale.requests if isinstance(request, PrinterRequestPacket)]

    def assert_rejected(self, future, reason):
        self.assertTrue(future.done())
        with self.assertRaises(ReprintRequestError) as caught:
            future.result()
        self.assertEqual(caught.exception.reason, reason)
        self.assertEqual(self.print_requests(), [])

    def execute(self, copies=1):
        future = self.worker.request_reprint(self.receipt, copies=copies)
        self.assertFalse(future.done())
        self.assertTrue(self.worker._run_reprint_request())
        return future

    def test_selected_copies_and_same_uuid_are_independent_of_auto_print_configuration(self):
        for copies in (1, 3, 9):
            self.assertEqual(self.execute(copies).result(), PrinterStatus.NORMAL)
        requests = self.print_requests()
        self.assertEqual([request.copies for request in requests], [1, 3, 9])
        self.assertTrue(all(request.document_bytes == ReceiptTemplate.render(self.receipt) for request in requests))
        self.assertIn(b"55A7C359", requests[0].document_bytes)
        self.assertIn(b"240 kg", requests[0].document_bytes)
        self.assertEqual(self.scale.connections, 0)
        self.assertFalse(self.worker._run_reprint_request())
        self.events.assert_not_called()
        self.builder.assert_not_called()

    def test_reprint_preserves_current_completion_and_automatic_receipt_state(self):
        self.worker._receipt_attempted = True
        self.worker._receipt_bytes = b"previous automatic receipt"
        self.worker._receipt_event_uuid = "previous automatic event"
        self.worker._saved_event_uuid = "previous saved event"
        self.worker._completion_at = self.now - 1
        self.worker._completion_announced = True
        self.worker._weighed_vehicle_on = True
        fields = (
            "state", "last_event", "last_plate", "_pending_save", "_save_result", "_processing",
            "_receipt_attempted", "_receipt_bytes", "_receipt_event_uuid", "_saved_event_uuid",
            "_completion_at", "_completion_announced", "_weighed_vehicle_on", "_no_paper_pending",
        )
        before = {name: getattr(self.worker, name) for name in fields}
        self.execute().result()
        self.assertEqual({name: getattr(self.worker, name) for name in fields}, before)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.events.assert_not_called()

    def test_confirmed_empty_platform_accepts_reprint_without_accepting_a_card(self):
        self.scale.response = replace(self.scale.response, weight_value=Decimal("0"))
        self.worker.exchange(DisplayRequestPacket())
        self.now += 1.1
        self.worker.exchange(DisplayRequestPacket())
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
        self.execute().result()
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
        self.events.assert_not_called()

    def test_busy_states_and_active_weighing_reject_without_output(self):
        changes = [
            {"state": state} for state in WorkerState if state != WorkerState.IDLE
        ] + [
            {"_processing": True}, {"_pending_save": object()}, {"_save_result": Future()},
            {"_save_fault": True}, {"last_event": RFIDTaggedEvent(rfid_card_uid="12345678")},
            {"_zero_active": True}, {"_zero_release_pending": True},
        ]
        for values in changes:
            with self.subTest(values=values):
                self.reset_station()
                for name, value in values.items():
                    setattr(self.worker, name, value)
                self.assert_rejected(self.worker.request_reprint(self.receipt, copies=1), "busy")
        for state in (CycleState.WAIT_EMPTY, CycleState.READY, CycleState.RESERVED):
            with self.subTest(cycle=state):
                self.reset_station()
                self.worker.cycle.state = state
                self.assert_rejected(self.worker.request_reprint(self.receipt, copies=1), "busy")
        self.reset_station()
        self.worker.cycle.state = CycleState.WAIT_ENTRY
        self.assert_rejected(self.worker.request_reprint(self.receipt, copies=1), "busy")

    def test_zero_and_reprint_pending_requests_exclude_each_other(self):
        zero = self.worker.request_zero()
        self.assertFalse(zero.done())
        self.assert_rejected(self.worker.request_reprint(self.receipt, copies=1), "busy")
        self.worker.stop()
        self.reset_station()
        reprint = self.worker.request_reprint(self.receipt, copies=1)
        self.assertFalse(reprint.done())
        self.assertEqual(self.worker.request_zero().exception().reason, "busy")
        self.assert_rejected(self.worker.request_reprint(self.receipt, copies=3), "busy")
        self.assertTrue(self.worker._run_reprint_request())
        self.assertEqual(reprint.result(), PrinterStatus.NORMAL)
        self.assertEqual(len(self.print_requests()), 1)

    def test_no_paper_unknown_transmitting_and_stale_status_reject(self):
        cases = (
            ({"printer_status": PrinterStatus.NO_PAPER}, "printer_no_paper"),
            ({"printer_status": PrinterStatus.TRANSMITTING}, "printer_transmitting"),
            ({"printer_status": None}, "printer_unknown"),
            ({"sample_valid": False}, "stale_status"),
            ({"sampled_monotonic": None}, "stale_status"),
            ({"sampled_monotonic": self.now - 3}, "stale_status"),
            ({"sampled_monotonic": self.now + 1}, "stale_status"),
            ({"connection_status": "disconnected"}, "disconnected"),
        )
        for values, reason in cases:
            with self.subTest(values=values):
                self.reset_station()
                self.worker._telemetry = replace(self.worker.get_telemetry(), **values)
                self.assert_rejected(self.worker.request_reprint(self.receipt, copies=1), reason)

    def test_invalid_copies_and_receipt_do_not_reach_the_serial_port(self):
        for copies in (None, 0, 10, -1, 1.0, "1", True, False):
            with self.subTest(copies=copies):
                self.assert_rejected(self.worker.request_reprint(self.receipt, copies=copies), "invalid_copies")
        self.assert_rejected(self.worker.request_reprint({}, copies=1), "invalid_receipt")
        oversized = replace(self.receipt, producer_name="가" * 6000)
        future = self.worker.request_reprint(oversized, copies=1)
        self.worker._run_reprint_request()
        self.assert_rejected(future, "invalid_receipt")

    def test_changed_state_is_revalidated_before_output(self):
        future = self.worker.request_reprint(self.receipt, copies=1)
        self.worker.state = WorkerState.MEASURE
        self.worker._run_reprint_request()
        self.assert_rejected(future, "busy")
        self.assertIsNone(self.worker._reprint_request)

    def test_rendering_cannot_bypass_shutdown_or_status_freshness(self):
        for change, reason in ((lambda: self.worker.stop(), "stopped"),
                               (lambda: setattr(self, "now", self.now + 3), "stale_status")):
            with self.subTest(reason=reason):
                self.reset_station()
                future = self.worker.request_reprint(self.receipt, copies=1)

                def render(receipt):
                    change()
                    return b"receipt"

                with patch("suwol1000.ReceiptTemplate.render", side_effect=render):
                    self.worker._run_reprint_request()
                self.assert_rejected(future, reason)

    def test_cancelled_or_stopped_pending_requests_never_print(self):
        future = self.worker.request_reprint(self.receipt, copies=1)
        self.assertTrue(future.cancel())
        self.assertTrue(self.worker._run_reprint_request())
        self.assertTrue(future.cancelled())
        self.assertIsNone(self.worker._reprint_request)
        self.assertEqual(self.print_requests(), [])
        future = self.worker.request_reprint(self.receipt, copies=1)
        # Completion callbacks may safely re-enter the request path.
        future.add_done_callback(lambda _: self.worker.request_reprint(self.receipt, copies=1))
        self.worker.stop()
        self.assert_rejected(future, "stopped")
        self.assertFalse(self.worker._run_reprint_request())

    def test_printer_response_is_reported_once_without_changing_completion(self):
        for status in (PrinterStatus.NO_PAPER, PrinterStatus.TRANSMITTING, None):
            with self.subTest(status=status):
                self.reset_station()
                self.scale.response = replace(self.scale.response, printer_status=status)
                self.assertEqual(self.execute().result(), status)
                self.assertFalse(self.worker._run_reprint_request())
                self.assertEqual(len(self.print_requests()), 1)
                self.assertFalse(self.worker._no_paper_pending)
                self.assertIsNone(self.worker._completion_at)
                self.events.assert_not_called()

    def test_worker_loop_recovers_after_timeout_without_replaying_the_print(self):
        def fail(_):
            raise DeviceResponseTimeout("simulated lost printer response")

        self.scale.on_print = fail
        recovery_states = []
        self.scale.on_disconnect = lambda: recovery_states.append(self.worker.state)
        self.scale.on_display = lambda _: self.worker.stop()
        future = self.worker.request_reprint(self.receipt, copies=3)
        self.worker.run()
        self.assertIsInstance(future.exception(), DeviceResponseTimeout)
        self.assertIn(WorkerState.RECOVER, recovery_states)
        self.assertEqual(self.scale.connections, 1)
        self.assertEqual(len(self.print_requests()), 1)
        self.assertIsNone(self.worker._reprint_request)
        self.events.assert_not_called()

    def prepare_automatic_weighing(self):
        saved = Future()
        saved.set_result(None)
        self.events.return_value = saved
        self.builder.side_effect = None
        self.builder.return_value = b"automatic receipt"
        prepare_vehicle(self.worker, reserve=False)
        self.scale.response = replace(self.scale.response, rfid_card_uid="12345678")

    def run_until_stopped(self):
        thread = threading.Thread(target=self.worker.run)
        thread.start()
        try:
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
        finally:
            self.worker.stop()
            thread.join(timeout=2)

    def test_disconnect_after_save_before_automatic_print_allows_manual_reprint(self):
        self.prepare_automatic_weighing()
        self.assertEqual(self.worker.idle(), WorkerState.VERIFY)
        self.assertEqual(self.worker.verify(), WorkerState.MEASURE)
        self.assertEqual(self.worker.measure(), WorkerState.SAVE)
        self.worker.state = self.worker.save()
        self.assertEqual(self.worker.state, WorkerState.PRINT)
        original = self.events.call_args.args[0]
        receipt = replace(
            self.receipt, record_uuid=UUID(original.uuid), rfid_card_uid=original.rfid_card_uid,
            weight=Decimal(original.weight),
        )

        def configure_timeouts(read, write):
            if self.worker.state == WorkerState.PRINT:
                raise SerialException("port disconnected before the automatic print command")
            self.scale.timeout, self.scale.write_timeout = read, write

        self.scale.configure_timeouts = configure_timeouts
        self.worker.request_configuration(replace(self.worker.configuration, serial_read_timeout=3))
        manual = []

        def request_after_reconnect(_):
            if self.worker.get_telemetry().connection_status == "connected":
                future = self.worker.request_reprint(receipt, copies=1)
                manual.append(future)
                future.add_done_callback(lambda _: self.worker.stop())

        self.scale.on_display = request_after_reconnect
        self.run_until_stopped()

        self.assertEqual(self.scale.connections, 1)
        self.assertEqual(len(manual), 1)
        self.assertEqual(manual[0].result(timeout=0), PrinterStatus.NORMAL)
        self.assertEqual([(p.copies, p.document_bytes) for p in self.print_requests()], [
            (1, ReceiptTemplate.render(receipt)),
        ])
        events = [call.args[0] for call in self.events.call_args_list]
        self.assertEqual([type(event) for event in events], [RFIDTaggedEvent, WeighingCompletedEvent])
        self.assertEqual(events[1], original)

    def test_lost_automatic_print_allows_only_an_explicit_reprint_after_recovery(self):
        self.prepare_automatic_weighing()
        rejected = []
        manual = []
        recovery_polls = 0

        def print_once(request):
            if len(self.print_requests()) == 1:
                event = self.events.call_args.args[0]
                self.receipt = replace(
                    self.receipt, record_uuid=UUID(event.uuid),
                    rfid_card_uid=event.rfid_card_uid, weight=Decimal(event.weight),
                )
                raise DeviceResponseTimeout("automatic print response lost")

        def recover_printer(_):
            nonlocal recovery_polls
            if not self.print_requests():
                return
            recovery_polls += 1
            if recovery_polls <= 2:
                # The first poll still has no fresh response after reconnect.
                rejected.append(self.worker.request_reprint(self.receipt, copies=3))
                status = PrinterStatus.NO_PAPER if recovery_polls == 1 else PrinterStatus.NORMAL
                self.scale.response = replace(self.scale.response, printer_status=status)
            elif recovery_polls == 3:
                manual.append(self.worker.request_reprint(self.receipt, copies=3))
            elif recovery_polls == 5:
                self.worker.stop()

        self.scale.on_print = print_once
        self.scale.on_display = recover_printer
        self.run_until_stopped()

        self.assertEqual(len(manual), 1)
        self.assertEqual(manual[0].result(timeout=0), PrinterStatus.NORMAL)
        self.assertEqual([future.exception(timeout=0).reason for future in rejected], [
            "disconnected", "printer_no_paper",
        ])
        prints = self.print_requests()
        self.assertEqual([(request.copies, request.document_bytes) for request in prints], [
            (9, b"automatic receipt"), (3, ReceiptTemplate.render(self.receipt)),
        ])
        events = [call.args[0] for call in self.events.call_args_list]
        self.assertEqual([type(event) for event in events], [RFIDTaggedEvent, WeighingCompletedEvent])
        self.assertEqual(str(self.receipt.record_uuid), events[1].uuid)
        self.assertEqual(self.scale.connections, 1)

    def test_worker_loop_prints_on_only_the_existing_owner_thread(self):
        future = self.worker.request_reprint(self.receipt, copies=3)
        future.add_done_callback(lambda _: self.worker.stop())
        thread = threading.Thread(target=self.worker.run)
        thread.start()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(future.result(timeout=0), PrinterStatus.NORMAL)
        self.assertEqual(self.scale.owners, [thread.ident])
        self.assertNotEqual(thread.ident, threading.get_ident())
        self.assertEqual(self.scale.connections, 0)
        self.assertEqual(len(self.print_requests()), 1)
        self.events.assert_not_called()

    def test_active_request_cannot_cancel_or_overlap_and_stop_interrupts_owner(self):
        entered = threading.Event()

        def block(_):
            entered.set()
            if not self.scale.interrupted.wait(timeout=2):
                raise AssertionError("종료 요청이 활성 출력 I/O를 중단하지 못했습니다.")
            raise DeviceResponseTimeout("simulated cancelled serial read")

        self.scale.on_print = block
        future = self.worker.request_reprint(self.receipt, copies=1)
        thread = threading.Thread(target=self.worker.run)
        thread.start()
        try:
            self.assertTrue(entered.wait(timeout=2))
            self.assertFalse(future.cancel())
            self.assertEqual(self.worker.request_zero().exception().reason, "busy")
            self.assertEqual(self.worker.request_reprint(self.receipt, copies=1).exception().reason, "busy")
        finally:
            self.worker.stop()
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(future.exception(timeout=0), DeviceResponseTimeout)
        self.assertEqual(len(self.print_requests()), 1)
        self.assertEqual(self.scale.owners, [thread.ident])
        self.assertIsNone(self.worker._reprint_request)
        self.assertFalse(self.worker._reprint_active)
        self.events.assert_not_called()


if __name__ == "__main__":
    unittest.main()
