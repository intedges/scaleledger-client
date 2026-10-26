from concurrent.futures import Future
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import threading
import unittest
from unittest.mock import Mock, patch

import serial

from events import WeighingCompletedEvent
from telemetry import telemetry_payload
from suwol1000 import (
    DisplayRequestPacket, StationTelemetrySnapshot, PrinterRequestPacket, PrinterStatus, ResponsePacket,
    VoiceCode, WeighingStationWorker, WeightStatus, WeightType,
)
from tests.helpers import prepare_vehicle
from tests.test_station_protocol import frame


def response_bytes(weight: bytes) -> bytes:
    return (
        b"\x02" b"0" b"D" b"00000000" b"0" b"000000" b"00" b"00" b"00" b"025" b"30" b"05" b"0"
        b"0000" b"ST" b"," b"NT" b"," b"+" + weight + b"kg" b"\x03"
    )


class StationTelemetrySnapshotTests(unittest.TestCase):
    def setUp(self):
        self.serial = Mock(port="test-port")
        self.samples = []
        self.worker = WeighingStationWorker(
            self.serial, station_id=7, polling_interval=0, receipt_builder=lambda event: b"receipt",
            on_telemetry=self.samples.append,
            vehicle_threshold=Decimal("1"),
        )
        self.worker.logger = Mock()
        self.worker.failures.logger = self.worker.logger

    def test_initial_snapshot_has_no_measurement_or_assumed_connection(self):
        sample = self.worker.get_telemetry()
        self.assertIsNone(sample.weight)
        self.assertIsNone(sample.sampled_at)
        self.assertIsNone(sample.sampled_monotonic)
        self.assertEqual(sample.sample_seq, 0)
        self.assertEqual(sample.connection_status, "waiting")
        self.assertFalse(sample.sample_valid)
        self.assertFalse(sample.weight_valid)
        self.assertIsNone(sample.unit)
        self.assertIsNone(sample.weight_status)
        self.assertIsNone(sample.weight_type)
        self.assertIsNone(sample.voice_code)
        self.assertIsNone(sample.green_blink)
        self.assertIsNone(sample.red_blink)
        self.assertIsNone(sample.fan_on)
        self.assertIsNone(sample.heater_on)
        self.assertIsNone(sample.temperature)
        self.assertIsNone(sample.fan_trigger_temp)
        self.assertIsNone(sample.heater_trigger_temp)
        self.worker.connect()
        connected = self.worker.get_telemetry()
        self.assertEqual(connected.connection_status, "connected")
        self.assertFalse(connected.sample_valid)
        self.assertIsNone(connected.sampled_at)
        self.assertEqual(self.samples, [connected])

    def test_serial_reconnect_invalidates_previous_sample_until_new_response(self):
        self.serial.send_and_receive.return_value = ResponsePacket(weight_value=Decimal("5"), inner_temperature=28)
        self.worker.exchange(DisplayRequestPacket())
        previous = self.worker.get_telemetry()

        self.worker.connect()

        connected = self.worker.get_telemetry()
        self.assertEqual(connected.connection_status, "connected")
        self.assertFalse(connected.sample_valid)
        self.assertIsNone(connected.weight)
        self.assertIsNone(connected.sampled_at)
        self.assertIsNone(connected.sampled_monotonic)
        self.assertIsNone(connected.temperature)
        self.assertIsNone(connected.fan_trigger_temp)
        self.assertIsNone(connected.heater_trigger_temp)
        self.assertEqual(connected.sample_seq, previous.sample_seq)

        self.worker.exchange(DisplayRequestPacket())
        self.assertEqual([sample.sample_valid for sample in self.samples], [True, False, True])
        self.assertEqual(self.worker.get_telemetry().sample_seq, 2)

    def test_idle_voice_and_print_responses_all_refresh_snapshot(self):
        self.serial.send_and_receive.side_effect = [
            ResponsePacket(weight_value=Decimal("1")),
            ResponsePacket(weight_value=Decimal("2"), voice_code=VoiceCode.TAG_CARD),
            ResponsePacket(weight_value=Decimal("3")),
            ResponsePacket(weight_value=Decimal("4"), voice_code=VoiceCode.WEIGHT_COMPLETE),
            ResponsePacket(weight_value=Decimal("5")),
        ]
        self.worker.idle()
        self.worker.idle()
        self.assertEqual(self.worker.get_telemetry().sample_seq, 2)
        completed = WeighingCompletedEvent(rfid_card_uid="12345678", station_id=7, weight=3)
        self.worker.last_event = completed
        self.worker._saved_event_uuid = completed.uuid
        self.worker._processing = True
        self.worker.print()
        self.worker.idle()
        self.worker.idle()

        sample = self.worker.get_telemetry()
        self.assertEqual(sample.weight, Decimal("5"))
        self.assertEqual(sample.sample_seq, 5)
        self.assertEqual(sample.connection_status, "connected")
        self.assertTrue(sample.sample_valid)
        self.assertTrue(sample.weight_valid)
        self.assertEqual(completed.weight, 3)
        self.assertEqual([snapshot.sample_seq for snapshot in self.samples], [1, 2, 3, 4, 5])
        self.assertIsInstance(self.serial.send_and_receive.call_args_list[2].args[0], PrinterRequestPacket)
        cleared_lights = self.serial.send_and_receive.call_args.args[0]
        self.assertFalse(cleared_lights.green_blink or cleared_lights.red_blink)

    def test_same_weight_is_published_for_every_new_response(self):
        self.serial.send_and_receive.return_value = ResponsePacket(weight_value=Decimal("12.5"))

        for _ in range(3):
            self.worker.exchange(DisplayRequestPacket())

        self.assertEqual([sample.weight for sample in self.samples], [Decimal("12.5")] * 3)
        self.assertEqual([sample.sample_seq for sample in self.samples], [1, 2, 3])

    def test_raw_frame_fields_share_one_observation_even_when_weight_is_invalid(self):
        observed_at = datetime(2026, 10, 26, tzinfo=UTC)
        raw = frame(
            relay=b"19", voice=b"04", printer=b"1", temperature=b"+29", fan=b"32", heater=b"06",
            weight=b"OL,NT,+     OLkg",
        )
        self.serial.send_and_receive.side_effect = lambda request: ResponsePacket.from_bytes(raw)
        with patch("suwol1000.datetime") as clock, patch("suwol1000.time.monotonic", return_value=10):
            clock.now.return_value = observed_at
            response = self.worker.exchange(DisplayRequestPacket(
                voice_code=VoiceCode.THANK_YOU, green_blink=False, red_blink=True,
            ))

        snapshot = self.worker.get_telemetry()
        self.assertTrue(snapshot.sample_valid)
        self.assertFalse(snapshot.weight_valid)
        self.assertEqual(snapshot.sample_seq, 1)
        self.assertEqual(snapshot.sampled_at, observed_at)
        self.assertEqual(snapshot.sampled_monotonic, 10)
        self.assertEqual(snapshot.connection_status, "connected")
        self.assertEqual(snapshot.weight_status, WeightStatus.OVERLOAD)
        self.assertEqual(snapshot.weight_type, WeightType.NET)
        self.assertIsNone(snapshot.weight)
        self.assertIsNone(snapshot.unit)
        self.assertEqual(snapshot.printer_status, PrinterStatus.NO_PAPER)
        self.assertEqual(snapshot.voice_code, VoiceCode.TAG_CARD)
        self.assertEqual((snapshot.green_blink, snapshot.red_blink), (True, False))
        self.assertEqual((snapshot.fan_on, snapshot.heater_on), (True, True))
        self.assertEqual(snapshot.temperature, 29)
        self.assertEqual((snapshot.fan_trigger_temp, snapshot.heater_trigger_temp), (32, 6))
        self.assertEqual(response.invalid_fields, ("weight_value",))
        self.assertEqual(self.samples, [snapshot])

    def test_unreadable_fields_in_a_new_frame_do_not_reuse_previous_device_states(self):
        self.serial.send_and_receive.return_value = ResponsePacket.from_bytes(
            frame(relay=b"18", voice=b"04", printer=b"1"),
        )
        self.worker.exchange(DisplayRequestPacket())
        self.serial.send_and_receive.return_value = ResponsePacket.from_bytes(
            frame(
                relay=b"\x00A", voice=b"13", printer=b"9", temperature=b"\x00\x00\x00",
                fan=b"\x00\x00", heater=b"xx", weight=b"XX,XX,+    120kg",
            ),
        )
        self.worker.exchange(DisplayRequestPacket())
        snapshot = self.worker.get_telemetry()
        self.assertTrue(snapshot.sample_valid)
        self.assertFalse(snapshot.weight_valid)
        self.assertEqual(snapshot.sample_seq, 2)
        for name in (
            "weight", "unit", "weight_status", "weight_type", "printer_status", "voice_code",
            "green_blink", "red_blink", "fan_on", "heater_on", "temperature",
            "fan_trigger_temp", "heater_trigger_temp",
        ):
            with self.subTest(field=name):
                self.assertIsNone(getattr(snapshot, name))

    def test_raw_zero_voice_and_inactive_outputs_are_known_values(self):
        self.serial.send_and_receive.return_value = ResponsePacket.from_bytes(frame(relay=b"00", voice=b"00"))
        self.worker.exchange(DisplayRequestPacket())
        snapshot = self.worker.get_telemetry()
        self.assertTrue(snapshot.sample_valid)
        self.assertTrue(snapshot.weight_valid)
        self.assertEqual(snapshot.voice_code, VoiceCode.NONE)
        self.assertEqual((snapshot.green_blink, snapshot.red_blink), (False, False))
        self.assertEqual((snapshot.fan_on, snapshot.heater_on), (False, False))

    def test_reported_temperature_settings_keep_zero_and_independent_unknown_values(self):
        cases = (
            (b"30", b"05", 30, 5), (b"00", b"00", 0, 0),
            (b"\x00\x00", b"07", None, 7), (b"40", b"  ", 40, None),
            (b"99", b"99", 99, 99), (b"31", b"06", 31, 6),
        )
        for sequence, (fan, heater, expected_fan, expected_heater) in enumerate(cases, start=1):
            with self.subTest(fan=fan, heater=heater):
                self.serial.send_and_receive.return_value = ResponsePacket.from_bytes(
                    frame(fan=fan, heater=heater, relay=b"00"),
                )
                self.worker.exchange(DisplayRequestPacket())
                snapshot = self.worker.get_telemetry()
                self.assertEqual((snapshot.fan_trigger_temp, snapshot.heater_trigger_temp), (expected_fan, expected_heater))
                self.assertEqual(snapshot.sample_seq, sequence)
                self.assertTrue(snapshot.sample_valid)
                self.assertTrue(snapshot.weight_valid)
                self.assertEqual((snapshot.fan_on, snapshot.heater_on), (False, False))

    def test_raw_temperature_uses_each_response_without_inventing_fault_sentinels(self):
        first_observed_at = datetime(2026, 10, 26, tzinfo=UTC)
        cases = (
            (b"+28", 28), (b"+29", 29), (b"-05", -5), (b"+00", 0),
            (b"-99", -99), (b"\x00\x00\x00", None), (b"+28", 28),
        )
        for sequence, (raw, expected) in enumerate(cases, start=1):
            with self.subTest(raw=raw):
                self.serial.send_and_receive.return_value = ResponsePacket.from_bytes(frame(temperature=raw))
                observed_at = first_observed_at + timedelta(seconds=sequence)
                with patch("suwol1000.datetime") as clock, patch("suwol1000.time.monotonic", return_value=sequence):
                    clock.now.return_value = observed_at
                    self.worker.exchange(DisplayRequestPacket())
                snapshot = self.worker.get_telemetry()
                self.assertTrue(snapshot.sample_valid)
                self.assertTrue(snapshot.weight_valid)
                self.assertEqual(snapshot.temperature, expected)
                self.assertEqual(snapshot.sampled_at, observed_at)
                self.assertEqual(snapshot.sampled_monotonic, sequence)
                self.assertEqual(snapshot.sample_seq, sequence)
        self.assertEqual([sample.temperature for sample in self.samples], [value for _, value in cases])

        self.serial.send_and_receive.side_effect = serial.SerialException("disconnected")
        with self.assertRaises(serial.SerialException):
            self.worker.exchange(DisplayRequestPacket())
        disconnected = self.worker.get_telemetry()
        self.assertIsNone(disconnected.temperature)
        self.assertIsNone(disconnected.fan_trigger_temp)
        self.assertIsNone(disconnected.heater_trigger_temp)
        self.assertIsNone(disconnected.sampled_at)
        self.assertIsNone(disconnected.sampled_monotonic)
        self.assertFalse(disconnected.sample_valid)
        self.assertEqual(disconnected.sample_seq, len(cases))

    def test_error_clears_observation_and_preserves_sequence_then_recovers(self):
        self.serial.send_and_receive.return_value = ResponsePacket(
            weight_value=Decimal("-1.25"), weight_status=WeightStatus.UNSTABLE,
            weight_type=WeightType.GROSS, green_blink=True, red_blink=True, inner_temperature=28,
        )
        with patch("suwol1000.time.monotonic", return_value=10):
            self.worker.exchange(DisplayRequestPacket())
        first = self.worker.get_telemetry()
        with self.assertRaises(FrozenInstanceError):
            first.weight = Decimal("99")

        self.serial.send_and_receive.side_effect = ValueError("bad frame")
        with self.assertRaises(ValueError):
            self.worker.exchange(DisplayRequestPacket())
        with self.assertRaises(ValueError):
            self.worker.exchange(DisplayRequestPacket())
        self.worker.record_failure("hw.protocol.parse_error", ValueError("bad frame"))
        error = self.worker.get_telemetry()
        self.assertEqual(error.connection_status, "error")
        self.assertFalse(error.sample_valid)
        self.assertEqual(error.sample_seq, first.sample_seq)
        self.assertIsNone(error.sampled_at)
        self.assertIsNone(error.sampled_monotonic)
        self.assertIsNone(error.weight)
        self.assertIsNone(error.voice_code)
        self.assertIsNone(error.green_blink)
        self.assertIsNone(error.red_blink)
        self.assertIsNone(error.fan_on)
        self.assertIsNone(error.heater_on)
        self.assertIsNone(error.temperature)
        self.assertIsNone(error.fan_trigger_temp)
        self.assertIsNone(error.heater_trigger_temp)
        self.assertFalse(error.weight_valid)

        self.serial.send_and_receive.side_effect = serial.SerialException("unplugged")
        with self.assertRaises(serial.SerialException):
            self.worker.exchange(DisplayRequestPacket())
        self.assertEqual(self.worker.get_telemetry().connection_status, "disconnected")
        self.assertFalse(self.worker.get_telemetry().sample_valid)
        self.worker.connect()
        self.assertEqual(self.worker.get_telemetry().connection_status, "connected")
        self.assertFalse(self.worker.get_telemetry().sample_valid)
        self.assertIsNone(self.worker.get_telemetry().sampled_at)
        self.serial.send_and_receive.side_effect = None
        self.worker.exchange(DisplayRequestPacket())
        self.assertEqual(self.worker.get_telemetry().sample_seq, 2)
        self.assertEqual(self.worker.get_telemetry().connection_status, "connected")
        self.assertTrue(self.worker.get_telemetry().sample_valid)
        self.assertEqual(first.sample_seq, 1)
        self.worker.stop()
        self.worker.stop()
        self.assertEqual(
            [sample.connection_status for sample in self.samples],
            ["connected", "error", "disconnected", "connected", "connected", "disconnected"],
        )
        self.assertEqual([sample.sample_valid for sample in self.samples], [True, False, False, False, True, False])

    def test_non_numeric_overload_keeps_frame_observation_with_no_false_measurement(self):
        self.serial.send_and_receive.side_effect = lambda request: ResponsePacket.from_bytes(response_bytes(b"     OL"))
        response = self.worker.exchange(DisplayRequestPacket())
        self.assertIsNone(response.weight_value)
        self.assertEqual(response.invalid_fields, ("weight_value",))
        sample = self.worker.get_telemetry()
        self.assertIsNone(sample.weight)
        self.assertEqual(sample.connection_status, "connected")
        self.assertEqual(sample.sample_seq, 1)
        self.assertTrue(sample.sample_valid)
        self.assertFalse(sample.weight_valid)
        self.assertIsNone(sample.unit)
        self.assertEqual(sample.weight_status, WeightStatus.STABLE)

    def test_numeric_overload_and_no_signal_are_actual_samples(self):
        for weight_status in (WeightStatus.OVERLOAD, WeightStatus.NONE):
            with self.subTest(weight_status=weight_status):
                response = ResponsePacket(weight_value=Decimal("123"), weight_status=weight_status)
                self.serial.send_and_receive.return_value = response
                self.assertIs(self.worker.exchange(DisplayRequestPacket()), response)
                current = self.worker.get_telemetry()
                self.assertTrue(current.sample_valid)
                self.assertTrue(current.weight_valid)
                self.assertEqual(current.weight, Decimal("123"))
                self.assertEqual(current.weight_status, weight_status)
                self.assertIsNotNone(current.sampled_at)
        self.assertEqual(self.worker.get_telemetry().sample_seq, 2)

    def test_nan_weight_is_unknown_in_new_observation_and_business_response_is_unchanged(self):
        self.serial.send_and_receive.return_value = ResponsePacket(weight_value=Decimal("2"))
        self.worker.exchange(DisplayRequestPacket())
        previous = self.worker.get_telemetry()
        invalid = ResponsePacket.from_bytes(response_bytes(b"    NaN"))
        self.serial.send_and_receive.return_value = invalid

        response = self.worker.exchange(DisplayRequestPacket())
        current = self.worker.get_telemetry()

        self.assertIs(response, invalid)
        self.assertIsNone(response.weight_value)
        self.assertIsNone(current.weight)
        self.assertIsNone(current.unit)
        self.assertEqual(current.sample_seq, previous.sample_seq + 1)
        self.assertGreaterEqual(current.sampled_at, previous.sampled_at)
        self.assertEqual(current.connection_status, "connected")
        self.assertTrue(current.sample_valid)
        self.assertFalse(current.weight_valid)
        self.worker.exchange(DisplayRequestPacket())
        self.assertEqual([sample.connection_status for sample in self.samples], ["connected"] * 3)
        self.assertEqual([sample.sample_seq for sample in self.samples], [1, 2, 3])

    def test_live_api_invalid_weights_never_reuse_previous_measurement(self):
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
                previous = self.worker.get_telemetry()
                self.serial.send_and_receive.return_value = invalid

                self.assertIs(self.worker.exchange(DisplayRequestPacket()), invalid)

                current = self.worker.get_telemetry()
                self.assertIsNone(current.weight)
                self.assertIsNone(current.unit)
                self.assertFalse(current.weight_valid)
                self.assertTrue(current.sample_valid)
                self.assertEqual(current.connection_status, "connected")
                self.assertEqual(current.sample_seq, previous.sample_seq + 1)
                self.assertEqual(current.weight_status, invalid.weight_status)
                self.assertEqual(current.weight_type, invalid.weight_type)
                self.assertEqual(current.printer_status, invalid.printer_status)

    def test_live_api_boundary_values_remain_exact(self):
        weights = (Decimal("1000000000"), Decimal("-1000000000"), Decimal("1e-30"), Decimal("-1e-29"))
        for weight in weights:
            with self.subTest(weight=weight):
                self.serial.send_and_receive.return_value = ResponsePacket(weight_value=weight)
                self.worker.exchange(DisplayRequestPacket())
                current = self.worker.get_telemetry()
                self.assertTrue(current.sample_valid)
                self.assertEqual(current.weight, weight)

    def test_callback_failures_do_not_interrupt_samples_or_weighing(self):
        self.worker.on_telemetry = Mock(side_effect=RuntimeError("Callback failed"))
        self.worker.telemetry_failures.logger = Mock()
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
        self.worker.measure()

        self.assertEqual(self.worker.get_telemetry().sample_seq, 4)
        self.assertEqual(len(events), 2)
        self.assertIsInstance(events[-1], WeighingCompletedEvent)
        self.assertEqual(events[-1].weight, 23)
        self.assertEqual(self.worker.on_telemetry.call_count, 4)
        self.worker.telemetry_failures.logger.log.assert_called_once()
        self.assertEqual(self.worker.telemetry_failures.count, 4)

    def test_stop_cannot_publish_disconnected_before_an_older_received_sample(self):
        callback_started = threading.Event()
        release_callback = threading.Event()

        def receive(sample):
            if sample.connection_status == "connected":
                callback_started.set()
                release_callback.wait(timeout=5)
            self.samples.append(sample)

        self.worker.on_telemetry = receive
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
        self.assertEqual(self.worker.get_telemetry().sample_seq, 1)
        self.assertFalse(self.worker.get_telemetry().sample_valid)
        self.assertIsNone(self.worker.get_telemetry().printer_status)
        self.assertIsNone(self.worker.get_telemetry().sampled_at)
        self.assertIsNone(self.worker.get_telemetry().sampled_monotonic)

    def test_restart_keeps_sample_sequence_but_waits_for_a_new_measurement(self):
        worker = WeighingStationWorker(self.serial, station_id=7, initial_sample_seq=27)
        self.assertEqual(worker.get_telemetry().sample_seq, 27)
        self.serial.send_and_receive.return_value = ResponsePacket()
        worker.exchange(DisplayRequestPacket())
        self.assertEqual(worker.get_telemetry().sample_seq, 28)
        worker.stop()
        self.assertEqual(worker.get_telemetry().connection_status, "disconnected")
        worker.exchange(DisplayRequestPacket())
        self.assertEqual(worker.get_telemetry().sample_seq, 28)
        self.assertEqual(worker.get_telemetry().connection_status, "disconnected")


class StationTelemetryPayloadTests(unittest.TestCase):
    def test_decimal_scientific_notation_is_sent_as_exact_fixed_point(self):
        snapshot = StationTelemetrySnapshot(
            weight=Decimal("1E-7"), unit="kg", weight_status=WeightStatus.STABLE, weight_type=WeightType.NET,
            sampled_at=datetime.now(UTC), sampled_monotonic=20, sample_seq=1,
            connection_status="connected", sample_valid=True, weight_valid=True,
        )
        self.assertEqual(telemetry_payload(3, snapshot)["weight"], "0.0000001")

    def test_sample_fields_are_preserved_and_age_uses_monotonic_not_wall_clock(self):
        now = datetime(2026, 10, 13, tzinfo=UTC)
        snapshot = StationTelemetrySnapshot(
            weight=Decimal("0.00"), unit="kg", weight_status=WeightStatus.STABLE,
            weight_type=WeightType.NET, sampled_at=now, sampled_monotonic=20,
            sample_seq=7, connection_status="connected", sample_valid=True, weight_valid=True,
        )
        with patch("telemetry.time.monotonic", return_value=22.125):
            payload = telemetry_payload(3, snapshot)

        self.assertEqual(payload, {
            "station_id": 3, "weight": "0.00", "unit": "kg", "weight_status": "ST",
            "weight_type": "NT", "sampled_at": now.isoformat(), "sample_age_ms": 2125,
            "sample_seq": 7, "weight_valid": True, "printer_status": "unknown",
            "voice_code": None, "fan_on": None, "heater_on": None,
            "green_blink": None, "red_blink": None,
            "temperature": None,
            "fan_trigger_temp": None, "heater_trigger_temp": None,
        })

    def test_missing_or_invalidated_sample_cannot_create_telemetry_message(self):
        for snapshot in (
            StationTelemetrySnapshot(),
            StationTelemetrySnapshot(
                weight=Decimal("10"), weight_status=WeightStatus.STABLE,
                sampled_at=datetime.now(UTC), sampled_monotonic=20, sample_seq=1,
                connection_status="disconnected", sample_valid=False,
            ),
        ):
            with self.subTest(snapshot=snapshot), self.assertRaises(ValueError):
                telemetry_payload(9, snapshot)
