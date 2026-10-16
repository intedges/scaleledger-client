from concurrent.futures import Future
from decimal import Decimal
import threading
import unittest
from unittest.mock import Mock, patch

import serial

from events import WeighingCompletedEvent
from suwol1000 import (
    DisplayRequestPacket, PrinterRequestPacket, ResponsePacket, VoiceCode,
    WeighingStationWorker, WorkerState,
)
from tests.helpers import prepare_vehicle


class StationSaveTests(unittest.TestCase):
    def setUp(self):
        self.result = Future()
        self.on_event = Mock(return_value=self.result)
        self.receipt_builder = Mock(return_value=b"saved receipt")
        self.client = Mock(port="save-test-port")
        self.requests = []
        self.client.send_and_receive.side_effect = self.answer
        logger = Mock()
        with patch("suwol1000.get_logger") as get_logger:
            get_logger.return_value.bind.return_value = logger
            self.worker = WeighingStationWorker(
                self.client, on_event=self.on_event, receipt_builder=self.receipt_builder,
                polling_interval=0, retry_interval=0,
            )
        self.worker.last_plate = "12345678"
        self.worker.last_weight = Decimal("120.9")

    def answer(self, request):
        self.requests.append(request)
        return ResponsePacket(
            rfid_card_uid="87654321", weight_value=Decimal("456"),
            voice_code=getattr(request, "voice_code", VoiceCode.NONE),
        )

    def begin_measurement(self):
        prepare_vehicle(self.worker)
        self.assertEqual(self.worker.measure(), WorkerState.SAVE)
        event = self.on_event.call_args.args[0]
        self.assertIsInstance(event, WeighingCompletedEvent)
        self.assertEqual((event.rfid_card_uid, event.weight), ("12345678", 120))
        return event

    def assert_no_completion(self):
        self.assertFalse(any(isinstance(request, PrinterRequestPacket) for request in self.requests))
        self.assertFalse(any(
            request.voice_code in (VoiceCode.WEIGHT_COMPLETE, VoiceCode.THANK_YOU)
            or (request.green_blink and not request.red_blink)
            for request in self.requests if isinstance(request, DisplayRequestPacket)
        ))
        self.receipt_builder.assert_not_called()

    def test_pending_save_keeps_one_future_and_ignores_new_cards_after_delay_warning(self):
        with patch("suwol1000.time.monotonic", return_value=10):
            event = self.begin_measurement()
        for now in (14.9, 15, 20, 120):
            with patch("suwol1000.time.monotonic", return_value=now):
                self.assertEqual(self.worker.save(), WorkerState.SAVE)
            self.assert_no_completion()

        self.on_event.assert_called_once_with(event)
        self.assertFalse(self.result.cancelled())
        self.assertFalse(self.result.done())
        self.assertIs(self.worker.last_event, event)
        self.assertEqual(self.worker.last_plate, "12345678")
        self.assertEqual(self.worker.last_weight, Decimal("456"))
        self.assertEqual((event.rfid_card_uid, event.weight), ("12345678", 120))
        warnings = [
            call for call in self.worker.logger.warning.call_args_list
            if call.args[0] == "biz.weighing.local_save_pending"
        ]
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0].kwargs["event_id"], event.uuid)

    def test_confirmed_failure_retries_the_original_only_after_the_retry_delay(self):
        retry_result = Future()
        self.on_event.side_effect = [self.result, retry_result]
        with patch("suwol1000.time.monotonic", return_value=10):
            event = self.begin_measurement()
            self.result.set_exception(OSError("database or disk is full"))
            self.assertEqual(self.worker.save(), WorkerState.SAVE)
        with patch("suwol1000.time.monotonic", return_value=14.9):
            self.assertEqual(self.worker.save(), WorkerState.SAVE)
        self.on_event.assert_called_once_with(event)
        self.assert_no_completion()

        with patch("suwol1000.time.monotonic", return_value=15):
            self.assertEqual(self.worker.save(), WorkerState.SAVE)
        self.assertEqual(self.on_event.call_count, 2)
        self.assertTrue(all(call.args[0] is event for call in self.on_event.call_args_list))
        self.assertEqual(event.weight, 120)
        self.assert_no_completion()

        retry_result.set_result(None)
        self.assertEqual(self.worker.save(), WorkerState.COMPLETE)
        self.assertIs(self.worker.last_event, event)

    def test_missing_acknowledgement_blocks_completion_and_retries_same_event(self):
        self.on_event.side_effect = [None, self.result]
        self.assert_submission_failure_retries_original()

    def test_synchronous_callback_failure_blocks_completion_and_retries_same_event(self):
        self.on_event.side_effect = [RuntimeError("event loop unavailable"), self.result]
        self.assert_submission_failure_retries_original()

    def assert_submission_failure_retries_original(self):
        with patch("suwol1000.time.monotonic", return_value=10):
            event = self.begin_measurement()
            self.assertEqual(self.worker.save(), WorkerState.SAVE)
        self.on_event.assert_called_once_with(event)
        self.worker.logger.critical.assert_called_once()
        self.assert_no_completion()
        self.assertEqual(self.worker.complete(), WorkerState.IDLE)
        self.assertEqual(self.worker.print(), WorkerState.IDLE)
        self.assert_no_completion()

        with patch("suwol1000.time.monotonic", return_value=15):
            self.assertEqual(self.worker.save(), WorkerState.SAVE)
        self.assertEqual(self.on_event.call_count, 2)
        self.assertIs(self.on_event.call_args.args[0], event)
        self.result.set_result(None)
        self.assertEqual(self.worker.save(), WorkerState.COMPLETE)

    def test_success_announces_completion_then_prints_the_saved_original_once(self):
        event = self.begin_measurement()
        self.assert_no_completion()
        self.result.set_result(None)

        self.assertEqual(self.worker.save(), WorkerState.COMPLETE)
        self.assert_no_completion()
        self.assertEqual(self.worker.complete(), WorkerState.PRINT)
        self.assertFalse(any(isinstance(request, PrinterRequestPacket) for request in self.requests))
        self.assertEqual(self.worker.print(), WorkerState.IDLE)

        actions = [
            "print" if isinstance(request, PrinterRequestPacket) else request.voice_code.name
            for request in self.requests
            if isinstance(request, PrinterRequestPacket) or request.voice_code != VoiceCode.NONE
        ]
        self.assertEqual(actions, ["WEIGHT_COMPLETE", "print", "THANK_YOU"])
        self.receipt_builder.assert_called_once_with(event)
        self.on_event.assert_called_once_with(event)
        self.assertIsNone(self.worker.last_event)
        self.assertEqual(self.worker.last_plate, "")
        request_count = len(self.requests)
        self.assertEqual(self.worker.complete(), WorkerState.IDLE)
        self.assertEqual(self.worker.print(), WorkerState.IDLE)
        self.assertEqual(len(self.requests), request_count)

    def test_direct_completion_and_print_cannot_bypass_a_pending_or_failed_save(self):
        self.begin_measurement()
        for failed in (False, True):
            with self.subTest(failed=failed):
                if failed:
                    self.result.set_exception(OSError("disk full"))
                    self.assertEqual(self.worker.save(), WorkerState.SAVE)
                self.assertEqual(self.worker.complete(), WorkerState.IDLE)
                self.assertEqual(self.worker.print(), WorkerState.IDLE)
                self.assert_no_completion()

    def test_a_save_acknowledgement_does_not_authorize_another_event(self):
        self.begin_measurement()
        self.result.set_result(None)
        self.assertEqual(self.worker.save(), WorkerState.COMPLETE)
        self.worker.last_event = WeighingCompletedEvent(rfid_card_uid="12345678", weight=120)

        self.assertEqual(self.worker.complete(), WorkerState.IDLE)
        self.assertEqual(self.worker.print(), WorkerState.IDLE)
        self.assert_no_completion()

    def test_stop_leaves_pending_future_uncancelled_and_blocks_output_after_late_success(self):
        event = self.begin_measurement()
        self.worker.stop()

        self.assertEqual(self.worker.save(), WorkerState.SAVE)
        self.assertFalse(self.result.cancelled())
        self.assertIs(self.worker.last_event, event)
        self.result.set_result(None)
        self.assertEqual(self.worker.save(), WorkerState.SAVE)
        self.assertEqual(self.worker.complete(), WorkerState.IDLE)
        self.assertEqual(self.worker.print(), WorkerState.IDLE)
        self.assert_no_completion()
        self.on_event.assert_called_once_with(event)

    def test_stop_after_confirmation_prevents_completion_and_print(self):
        self.begin_measurement()
        self.result.set_result(None)
        self.assertEqual(self.worker.save(), WorkerState.COMPLETE)
        self.worker.stop()

        self.assertEqual(self.worker.complete(), WorkerState.IDLE)
        self.assertEqual(self.worker.print(), WorkerState.IDLE)
        self.assert_no_completion()

    def test_serial_parse_failure_preserves_pending_save_in_the_running_worker(self):
        self.assert_run_preserves_pending_save(ValueError("invalid response length"))

    def test_serial_disconnection_reconnects_and_preserves_pending_save(self):
        self.assert_run_preserves_pending_save(serial.SerialException("device disconnected"))

    def assert_run_preserves_pending_save(self, failure):
        event = self.begin_measurement()
        self.worker.state = WorkerState.SAVE
        recovered = threading.Event()
        requests_after_failure = []

        def respond_after_failure(request):
            requests_after_failure.append(request)
            recovered.set()
            self.worker.stop()
            return ResponsePacket(rfid_card_uid="87654321", weight_value=Decimal("999"))

        responses = iter([failure, respond_after_failure])

        def respond(request):
            self.requests.append(request)
            response = next(responses)
            if isinstance(response, Exception):
                raise response
            return response(request)

        self.client.send_and_receive.side_effect = respond
        thread = threading.Thread(target=self.worker.run, daemon=True)
        thread.start()
        try:
            self.assertTrue(recovered.wait(timeout=2))
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
        finally:
            self.worker.stop()
            thread.join(timeout=2)

        self.on_event.assert_called_once_with(event)
        self.assertIs(self.worker.last_event, event)
        self.assertFalse(self.result.cancelled())
        self.assertFalse(self.result.done())
        self.assertEqual(len(requests_after_failure), 1)
        self.assertEqual(requests_after_failure[0].display_plate, "12345678")
        self.assertEqual(requests_after_failure[0].voice_code, VoiceCode.NONE)
        if isinstance(failure, serial.SerialException):
            self.client.connect.assert_called_once_with()
        else:
            self.client.connect.assert_not_called()
        self.assert_no_completion()
