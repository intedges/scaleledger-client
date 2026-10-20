from concurrent.futures import Future
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
import threading
import unittest
from unittest.mock import Mock, patch

import serial

from events import WeighingCompletedEvent
from telemetry import weight_payload
from suwol1000 import (
    DisplayRequestPacket, LiveWeightSnapshot, PrinterRequestPacket, ResponsePacket,
    VoiceCode, WeighingStationWorker, WeightStatus, WeightType,
)
from tests.helpers import prepare_vehicle


def response_bytes(weight: bytes) -> bytes:
    return (
        b"\x02" b"0" b"D" b"00000000" b"0" b"000000" b"00" b"00" b"00" b"025" b"30" b"05" b"0"
        b"0000" b"ST" b"," b"NT" b"," b"+" + weight + b"kg" b"\x03"
    )


class LiveWeightSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.serial = Mock(port="test-port")
        self.samples = []
        self.worker = WeighingStationWorker(
            self.serial, polling_interval=0, receipt_builder=lambda event: b"receipt",
            on_live_weight=self.samples.append,
            vehicle_threshold=Decimal("1"),
        )
        self.worker.logger = Mock()
        self.worker.failures.logger = self.worker.logger

    def test_initial_snapshot_has_no_measurement_or_assumed_connection(self):
        sample = self.worker.get_live_weight()
        self.assertIsNone(sample.weight)
        self.assertIsNone(sample.sampled_at)
        self.assertIsNone(sample.sampled_monotonic)
        self.assertEqual(sample.sample_seq, 0)
        self.assertEqual(sample.connection_status, "waiting")
        self.assertFalse(sample.sample_valid)
        self.worker.connect()
        connected = self.worker.get_live_weight()
        self.assertEqual(connected.connection_status, "connected")
        self.assertFalse(connected.sample_valid)
        self.assertIsNone(connected.sampled_at)
        self.assertEqual(self.samples, [connected])

    def test_serial_reconnect_invalidates_previous_sample_until_new_response(self):
        self.serial.send_and_receive.return_value = ResponsePacket(weight_value=Decimal("5"))
        self.worker.exchange(DisplayRequestPacket())
        previous = self.worker.get_live_weight()

        self.worker.connect()

        connected = self.worker.get_live_weight()
        self.assertEqual(connected.connection_status, "connected")
        self.assertFalse(connected.sample_valid)
        self.assertEqual(connected.weight, previous.weight)
        self.assertEqual(connected.sampled_at, previous.sampled_at)
        self.assertEqual(connected.sampled_monotonic, previous.sampled_monotonic)
        self.assertEqual(connected.sample_seq, previous.sample_seq)

        self.worker.exchange(DisplayRequestPacket())
        self.assertEqual([sample.sample_valid for sample in self.samples], [True, False, True])
        self.assertEqual(self.worker.get_live_weight().sample_seq, 2)

    def test_idle_voice_and_print_responses_all_refresh_snapshot(self):
        self.serial.send_and_receive.side_effect = [
            ResponsePacket(weight_value=Decimal("1")),
            ResponsePacket(weight_value=Decimal("2"), voice_code=VoiceCode.PLEASE_WAIT),
            ResponsePacket(weight_value=Decimal("3")),
            ResponsePacket(weight_value=Decimal("4")),
            ResponsePacket(weight_value=Decimal("5"), voice_code=VoiceCode.THANK_YOU),
            ResponsePacket(weight_value=Decimal("6")),
            ResponsePacket(weight_value=Decimal("7")),
        ]
        self.worker.idle()
        self.assertEqual(self.worker.get_live_weight().sample_seq, 1)
        self.worker.poll_until_voice_ends(DisplayRequestPacket(voice_code=VoiceCode.PLEASE_WAIT))
        self.assertEqual(self.worker.get_live_weight().sample_seq, 3)
        completed = WeighingCompletedEvent(rfid_card_uid="12345678", weight=3)
        self.worker.last_event = completed
        self.worker._saved_event_uuid = completed.uuid
        self.worker._visit_saved = True
        self.worker.print()

        sample = self.worker.get_live_weight()
        self.assertEqual(sample.weight, Decimal("7"))
        self.assertEqual(sample.sample_seq, 7)
        self.assertEqual(sample.connection_status, "connected")
        self.assertTrue(sample.sample_valid)
        self.assertEqual(completed.weight, 3)
        self.assertEqual([snapshot.sample_seq for snapshot in self.samples], [1, 2, 3, 4, 5, 6, 7])
        self.assertIsInstance(self.serial.send_and_receive.call_args_list[3].args[0], PrinterRequestPacket)
        cleared_lights = self.serial.send_and_receive.call_args.args[0]
        self.assertFalse(cleared_lights.green_blink or cleared_lights.red_blink)

    def test_same_weight_is_published_for_every_new_response(self):
        self.serial.send_and_receive.return_value = ResponsePacket(weight_value=Decimal("12.5"))

        for _ in range(3):
            self.worker.exchange(DisplayRequestPacket())

        self.assertEqual([sample.weight for sample in self.samples], [Decimal("12.5")] * 3)
        self.assertEqual([sample.sample_seq for sample in self.samples], [1, 2, 3])

    def test_error_preserves_last_sample_time_and_sequence_then_recovers(self):
        self.serial.send_and_receive.return_value = ResponsePacket(
            weight_value=Decimal("-1.25"), weight_status=WeightStatus.UNSTABLE,
            weight_type=WeightType.GROSS,
        )
        with patch("suwol1000.time.monotonic", return_value=10):
            self.worker.exchange(DisplayRequestPacket())
        first = self.worker.get_live_weight()
        with self.assertRaises(FrozenInstanceError):
            first.weight = Decimal("99")

        self.serial.send_and_receive.side_effect = ValueError("bad frame")
        with self.assertRaises(ValueError):
            self.worker.exchange(DisplayRequestPacket())
        with self.assertRaises(ValueError):
            self.worker.exchange(DisplayRequestPacket())
        self.worker.record_failure("hw.protocol.parse_error", ValueError("bad frame"))
        error = self.worker.get_live_weight()
        self.assertEqual(error.connection_status, "error")
        self.assertFalse(error.sample_valid)
        self.assertEqual(error.sample_seq, first.sample_seq)
        self.assertEqual(error.sampled_at, first.sampled_at)
        self.assertEqual(error.sampled_monotonic, 10)
        self.assertEqual(error.weight, Decimal("-1.25"))

        self.serial.send_and_receive.side_effect = serial.SerialException("unplugged")
        with self.assertRaises(serial.SerialException):
            self.worker.exchange(DisplayRequestPacket())
        self.assertEqual(self.worker.get_live_weight().connection_status, "disconnected")
        self.assertFalse(self.worker.get_live_weight().sample_valid)
        self.worker.connect()
        self.assertEqual(self.worker.get_live_weight().connection_status, "connected")
        self.assertFalse(self.worker.get_live_weight().sample_valid)
        self.assertEqual(self.worker.get_live_weight().sampled_at, first.sampled_at)
        self.serial.send_and_receive.side_effect = None
        self.worker.exchange(DisplayRequestPacket())
        self.assertEqual(self.worker.get_live_weight().sample_seq, 2)
        self.assertEqual(self.worker.get_live_weight().connection_status, "connected")
        self.assertTrue(self.worker.get_live_weight().sample_valid)
        self.assertEqual(first.sample_seq, 1)
        self.worker.stop()
        self.worker.stop()
        self.assertEqual(
            [sample.connection_status for sample in self.samples],
            ["connected", "error", "disconnected", "connected", "connected", "disconnected"],
        )
        self.assertEqual([sample.sample_valid for sample in self.samples], [True, False, False, False, True, False])

    def test_non_numeric_overload_preserves_parser_failure_with_no_false_measurement(self):
        self.serial.send_and_receive.side_effect = lambda request: ResponsePacket.from_bytes(response_bytes(b"     OL"))
        with self.assertRaises(InvalidOperation):
            self.worker.exchange(DisplayRequestPacket())
        sample = self.worker.get_live_weight()
        self.assertIsNone(sample.weight)
        self.assertEqual(sample.connection_status, "error")
        self.assertEqual(sample.sample_seq, 0)
        self.assertFalse(sample.sample_valid)

    def test_numeric_overload_and_no_signal_are_actual_samples(self):
        for weight_status in (WeightStatus.OVERLOAD, WeightStatus.NONE):
            with self.subTest(weight_status=weight_status):
                response = ResponsePacket(weight_value=Decimal("123"), weight_status=weight_status)
                self.serial.send_and_receive.return_value = response
                self.assertIs(self.worker.exchange(DisplayRequestPacket()), response)
                current = self.worker.get_live_weight()
                self.assertTrue(current.sample_valid)
                self.assertEqual(current.weight, Decimal("123"))
                self.assertEqual(current.weight_status, weight_status)
                self.assertIsNotNone(current.sampled_at)
        self.assertEqual(self.worker.get_live_weight().sample_seq, 2)

    def test_nonfinite_value_is_not_published_and_business_response_is_unchanged(self):
        self.serial.send_and_receive.return_value = ResponsePacket(weight_value=Decimal("2"))
        self.worker.exchange(DisplayRequestPacket())
        previous = self.worker.get_live_weight()
        invalid = ResponsePacket.from_bytes(response_bytes(b"    NaN"))
        self.serial.send_and_receive.return_value = invalid

        response = self.worker.exchange(DisplayRequestPacket())
        current = self.worker.get_live_weight()

        self.assertIs(response, invalid)
        self.assertTrue(response.weight_value.is_nan())
        self.assertEqual(current.weight, Decimal("2"))
        self.assertEqual(current.sample_seq, previous.sample_seq)
        self.assertEqual(current.sampled_at, previous.sampled_at)
        self.assertEqual(current.connection_status, "error")
        self.assertFalse(current.sample_valid)
        self.worker.exchange(DisplayRequestPacket())
        self.assertEqual([sample.connection_status for sample in self.samples], ["connected", "error"])

    def test_live_api_invalid_values_report_error_without_replacing_previous_sample(self):
        invalid_weights = [
            Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"), Decimal("-Infinity"),
            Decimal("1000000000.1"), Decimal("-1000000000.1"),
            Decimal("1e-31"), Decimal("-1e-30"), Decimal("1e-999999"),
        ]
        invalid_responses = [ResponsePacket(weight_value=weight) for weight in invalid_weights]
        invalid_responses.extend(ResponsePacket(weight_unit=unit) for unit in ("", "  ", "123456789"))
        for invalid in invalid_responses:
            with self.subTest(response=invalid):
                self.serial.send_and_receive.return_value = ResponsePacket(weight_value=Decimal("2"))
                self.worker.exchange(DisplayRequestPacket())
                previous = self.worker.get_live_weight()
                self.serial.send_and_receive.return_value = invalid

                self.assertIs(self.worker.exchange(DisplayRequestPacket()), invalid)

                current = self.worker.get_live_weight()
                self.assertEqual(current, replace(previous, connection_status="error", sample_valid=False))

    def test_live_api_boundary_values_remain_exact(self):
        weights = (Decimal("1000000000"), Decimal("-1000000000"), Decimal("1e-30"), Decimal("-1e-29"))
        for weight in weights:
            with self.subTest(weight=weight):
                self.serial.send_and_receive.return_value = ResponsePacket(weight_value=weight)
                self.worker.exchange(DisplayRequestPacket())
                current = self.worker.get_live_weight()
                self.assertTrue(current.sample_valid)
                self.assertEqual(current.weight, weight)

    def test_callback_failures_do_not_interrupt_samples_or_weighing(self):
        self.worker.on_live_weight = Mock(side_effect=RuntimeError("Callback failed"))
        self.worker.live_weight_failures.logger = Mock()
        events = []

        def submit(event):
            events.append(event)
            return Future()

        self.worker.on_event = submit
        self.serial.send_and_receive.return_value = ResponsePacket(
            weight_value=Decimal("23"), rfid_card_uid="12345678",
        )

        for _ in range(3):
            self.worker.exchange(DisplayRequestPacket())
        prepare_vehicle(self.worker, reserve=False)
        self.worker.idle()
        self.worker.poll_until_voice_ends = Mock()
        self.worker.measure()

        self.assertEqual(self.worker.get_live_weight().sample_seq, 4)
        self.assertEqual(len(events), 2)
        self.assertIsInstance(events[-1], WeighingCompletedEvent)
        self.assertEqual(events[-1].weight, 23)
        self.assertEqual(self.worker.on_live_weight.call_count, 4)
        self.worker.live_weight_failures.logger.log.assert_called_once()
        self.assertEqual(self.worker.live_weight_failures.count, 4)

    def test_stop_cannot_publish_disconnected_before_an_older_received_sample(self):
        callback_started = threading.Event()
        release_callback = threading.Event()

        def receive(sample):
            if sample.connection_status == "connected":
                callback_started.set()
                release_callback.wait(timeout=5)
            self.samples.append(sample)

        self.worker.on_live_weight = receive
        self.serial.send_and_receive.return_value = ResponsePacket(weight_value=Decimal("5"))
        receiving = threading.Thread(target=self.worker.exchange, args=(DisplayRequestPacket(),))
        stopping = threading.Thread(target=self.worker.stop)
        receiving.start()
        try:
            self.assertTrue(callback_started.wait(timeout=5))
            stopping.start()
            self.assertTrue(self.worker.stop_event.wait(timeout=5))
        finally:
            release_callback.set()
            receiving.join(timeout=5)
            if stopping.ident is not None:
                stopping.join(timeout=5)

        self.assertFalse(receiving.is_alive())
        self.assertFalse(stopping.is_alive())
        self.worker.exchange(DisplayRequestPacket())
        self.assertEqual([sample.connection_status for sample in self.samples], ["connected", "disconnected"])
        self.assertEqual(self.worker.get_live_weight().sample_seq, 1)
        self.assertFalse(self.worker.get_live_weight().sample_valid)

    def test_restart_keeps_sample_sequence_but_waits_for_a_new_measurement(self):
        worker = WeighingStationWorker(self.serial, initial_sample_seq=27)
        self.assertEqual(worker.get_live_weight().sample_seq, 0)
        self.serial.send_and_receive.return_value = ResponsePacket()
        worker.exchange(DisplayRequestPacket())
        self.assertEqual(worker.get_live_weight().sample_seq, 28)
        worker.stop()
        self.assertEqual(worker.get_live_weight().connection_status, "disconnected")
        worker.exchange(DisplayRequestPacket())
        self.assertEqual(worker.get_live_weight().sample_seq, 28)
        self.assertEqual(worker.get_live_weight().connection_status, "disconnected")


class LiveWeightPayloadTests(unittest.TestCase):
    def test_decimal_scientific_notation_is_sent_as_exact_fixed_point(self):
        snapshot = LiveWeightSnapshot(
            weight=Decimal("1E-7"), weight_status=WeightStatus.STABLE, weight_type=WeightType.NET,
            sampled_at=datetime.now(UTC), sampled_monotonic=20, sample_seq=1,
            connection_status="connected", sample_valid=True,
        )
        self.assertEqual(weight_payload(3, snapshot)["weight"], "0.0000001")

    def test_sample_fields_are_preserved_and_age_uses_monotonic_not_wall_clock(self):
        now = datetime(2026, 10, 13, tzinfo=UTC)
        snapshot = LiveWeightSnapshot(
            weight=Decimal("0.00"), unit="kg", weight_status=WeightStatus.STABLE,
            weight_type=WeightType.NET, sampled_at=now, sampled_monotonic=20,
            sample_seq=7, connection_status="connected", sample_valid=True,
        )
        with patch("telemetry.time.monotonic", return_value=22.125):
            payload = weight_payload(3, snapshot)

        self.assertEqual(payload, {
            "station_id": 3, "weight": "0.00", "unit": "kg", "weight_status": "ST",
            "weight_type": "NT", "sampled_at": now.isoformat(), "sample_age_ms": 2125,
            "sample_seq": 7, "connection_status": "connected",
        })

    def test_missing_or_invalidated_sample_cannot_create_weight_message(self):
        for snapshot in (
            LiveWeightSnapshot(),
            LiveWeightSnapshot(
                weight=Decimal("10"), weight_status=WeightStatus.STABLE,
                sampled_at=datetime.now(UTC), sampled_monotonic=20, sample_seq=1,
                connection_status="disconnected", sample_valid=False,
            ),
        ):
            with self.subTest(snapshot=snapshot), self.assertRaises(ValueError):
                weight_payload(9, snapshot)
