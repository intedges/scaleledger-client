from concurrent.futures import Future
from dataclasses import replace
from datetime import date
from decimal import Decimal
import unittest
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

from api import RecordReceiptDTO
from events import ReceiptConfirmation, ReceiptFuture
from printer import Receipt
from suwol1000 import (
    DeviceResponseTimeout, PrinterRequestPacket, VoiceCode, WeighingStationWorker, WorkerState,
)
from tests.helpers import prepare_vehicle
from tests.test_station_cycle import Clock, CycleScale, FIRST_CARD, SECOND_CARD
from weighing_cycle import CycleState


def capture_receipt(event):
    return Receipt(
        record_uuid=UUID(event.uuid), source_station_id=event.station_id,
        gateway_name="현장", station_name="저울 7", rfid_card_uid=event.rfid_card_uid,
        producer_name="생산자", species_name="", weight=event.weight, measured_at=event.timestamp,
    )


class StationReceiptWaitTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.enterContext(patch("suwol1000.time.monotonic", self.clock))
        self.enterContext(patch("suwol1000.get_logger", return_value=Mock()))
        self.scale = CycleScale(self.clock)
        self.saved = Future()
        self.worker = WeighingStationWorker(
            self.scale, station_id=7, on_event=Mock(return_value=self.saved),
            receipt_builder=capture_receipt, polling_interval=0, retry_interval=0,
            vehicle_ready_seconds=0.2, vehicle_empty_seconds=0.2,
        )
        self.worker.last_weight = self.scale.weight
        prepare_vehicle(self.worker, reserve=False)
        self.scale.card = FIRST_CARD
        self.assertEqual(self.step("idle"), WorkerState.VERIFY)
        self.assertEqual(self.step("verify"), WorkerState.MEASURE)
        self.assertEqual(self.step("measure"), WorkerState.SAVE)
        self.event = self.worker.last_event
        self.confirmation = ReceiptFuture()
        self.saved.set_result(self.confirmation)
        self.assertEqual(self.step("save"), WorkerState.WAIT_RECEIPT)
        self.response = RecordReceiptDTO(
            uuid=UUID(self.event.uuid), station_id=self.event.station_id,
            weight=103, measured_at=self.event.timestamp, cancelled_at=None,
            daily_number=18, number_date=date(2026, 11, 22),
        )

    def step(self, method):
        self.worker.state = getattr(self.worker, method)()
        return self.worker.state

    def confirm(self, at, **changes):
        self.confirmation.set_result(ReceiptConfirmation(replace(self.response, **changes), at))

    def finish(self):
        self.assertEqual(self.step("wait_receipt"), WorkerState.PRINT)
        self.assertEqual(self.step("print"), WorkerState.IDLE)

    def documents(self):
        return [request.document_bytes.decode("cp949") for request in self.scale.requests
                if isinstance(request, PrinterRequestPacket)]

    def test_number_available_by_cutoff_survives_a_slow_poll_with_original_weight(self):
        self.assertEqual(self.step("print"), WorkerState.WAIT_RECEIPT)
        self.confirm(at=2.0)
        self.clock.now = 3.0
        self.finish()
        document, = self.documents()
        for expected in ("계량번호 18번", "기준일 2026-11-22", self.event.uuid, "120 kg"):
            self.assertIn(expected, document)
        self.assertNotIn("103 kg", document)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)

    def test_reply_published_after_cutoff_cannot_number_the_first_print(self):
        self.clock.now = 2.1
        self.confirm(at=self.clock.now)
        self.finish()
        document, = self.documents()
        self.assertIn("계량번호 미확인", document)

    def test_timeout_preserves_empty_and_next_arrival_and_drops_old_reply(self):
        for at, weight in ((0.1, 0), (0.4, 0), (0.7, 230), (1.0, 230)):
            self.clock.now = at
            self.scale.weight = Decimal(weight)
            self.scale.card = SECOND_CARD
            self.assertEqual(self.step("wait_receipt"), WorkerState.WAIT_RECEIPT)
            poll = self.scale.requests[-1]
            self.assertTrue(poll.green_blink and poll.red_blink)
            self.assertEqual(poll.voice_code, VoiceCode.NONE)
        self.assertEqual(self.worker.get_telemetry().weight, 230)
        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.assertEqual(self.worker.request_zero().exception().reason, "busy")
        self.assertEqual(self.worker.request_reprint(capture_receipt(self.event), copies=1).exception().reason, "busy")
        self.clock.now = 2.0
        self.finish()
        self.assertIsNone(self.worker._completion_at)
        first, = self.documents()
        self.assertIn("계량번호 미확인", first)

        self.scale.card = SECOND_CARD
        self.assertEqual(self.step("idle"), WorkerState.VERIFY)
        self.assertEqual(self.step("verify"), WorkerState.MEASURE)
        offline_save = Future()
        offline_save.set_result(None)
        self.worker.on_event.return_value = offline_save
        self.assertEqual(self.step("measure"), WorkerState.SAVE)
        second_event = self.worker.last_event
        self.confirm(at=self.clock.now)
        self.assertEqual(self.step("save"), WorkerState.PRINT)
        self.assertEqual(self.step("print"), WorkerState.IDLE)
        first, second = self.documents()
        self.assertIn(self.event.uuid, first)
        self.assertIn(second_event.uuid, second)
        self.assertNotIn(self.event.uuid, second)
        self.assertIn("계량번호 미확인", second)
        self.assertIn("230 kg", second)

    def test_known_cancellation_even_between_timeout_and_print_issues_no_command(self):
        self.clock.now = 2.0
        self.assertEqual(self.step("wait_receipt"), WorkerState.PRINT)
        self.confirm(at=2.1, cancelled_at=self.event.timestamp)
        self.clock.now = 2.2
        self.assertEqual(self.step("print"), WorkerState.IDLE)
        self.assertEqual(self.documents(), [])
        self.assertIsNone(self.worker._completion_at)
        self.assertIsNone(self.worker.last_event)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)

    def test_unrelated_cancellation_cannot_suppress_or_number_this_receipt(self):
        self.confirm(at=0.1, uuid=uuid4(), cancelled_at=self.event.timestamp)
        self.finish()
        document, = self.documents()
        self.assertIn("계량번호 미확인", document)

    def test_disconnect_during_wait_discards_automatic_print_and_preserves_visit_lock(self):
        self.scale.responses.append((0.1, DeviceResponseTimeout("poll lost")))
        with self.assertRaises(DeviceResponseTimeout):
            self.step("wait_receipt")
        self.clock.now = 3
        self.confirm(at=1)
        self.assertEqual(self.step("recover"), WorkerState.CONNECT)
        self.assertEqual(self.step("connect"), WorkerState.IDLE)
        self.assertEqual(self.step("idle"), WorkerState.IDLE)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.assertEqual(self.documents(), [])
        manual = self.worker.request_reprint(capture_receipt(self.event), copies=1)
        self.assertTrue(self.worker._run_reprint_request())
        self.assertIsNone(manual.exception())
        self.assertEqual(len(self.documents()), 1)

    def test_uncertain_print_is_final_after_reconnection_and_late_confirmation(self):
        self.clock.now = 3
        self.assertEqual(self.step("wait_receipt"), WorkerState.PRINT)
        self.scale.responses.append((0.1, DeviceResponseTimeout("print reply lost")))
        with self.assertRaises(DeviceResponseTimeout):
            self.step("print")
        self.confirm(at=3.2)
        self.assertEqual(self.step("recover"), WorkerState.CONNECT)
        self.assertEqual(self.step("connect"), WorkerState.IDLE)
        self.assertEqual(self.step("idle"), WorkerState.IDLE)
        self.assertEqual(len(self.documents()), 1)

    def test_failed_confirmation_still_prints_and_releases_the_station(self):
        self.confirmation.set_exception(RuntimeError("uploader stopped"))
        self.finish()
        document, = self.documents()
        self.assertIn("계량번호 미확인", document)
        self.assertIsNone(self.worker.last_event)

    def test_stop_during_wait_does_not_print_on_late_reply(self):
        self.worker.stop()
        self.confirm(at=0.1)
        self.worker.run()
        self.worker.print()
        self.assertEqual(self.documents(), [])
