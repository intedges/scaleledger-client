from datetime import UTC, datetime, timedelta
from decimal import Decimal
import unittest
from unittest.mock import Mock, patch

import serial

from suwol1000 import (
    DisplayRequestPacket, PrinterStatus, ResponsePacket, WeighingStationWorker,
    printer_status_label,
)
from tests.test_station_protocol import frame


class PrinterObservationTests(unittest.TestCase):
    def setUp(self):
        self.serial = Mock(port="test-port")
        self.snapshots = []
        self.worker = WeighingStationWorker(
            self.serial, station_id=7, on_telemetry=self.snapshots.append,
        )
        self.worker.logger = Mock()
        self.started_at = datetime(2026, 10, 26, tzinfo=UTC)

    def observe(self, printer, *, second=0, weight=b"ST,NT,+    120kg"):
        response = ResponsePacket.from_bytes(frame(printer=printer, weight=weight))
        self.serial.send_and_receive.return_value = response
        with (
            patch("suwol1000.datetime") as clock,
            patch("suwol1000.time.monotonic", return_value=100 + second),
        ):
            clock.now.return_value = self.started_at + timedelta(seconds=second)
            self.worker.exchange(DisplayRequestPacket())
        return self.worker.get_telemetry()

    def assert_unobserved(self, snapshot):
        self.assertIsNone(snapshot.printer_status)
        self.assertIsNone(snapshot.sampled_at)
        self.assertIsNone(snapshot.sampled_monotonic)
        self.assertFalse(snapshot.sample_valid)

    def printer_logs(self):
        return [
            (call[0], call.kwargs)
            for call in self.worker.logger.method_calls
            if call.args and call.args[0] == "hw.printer.status_changed"
        ]

    def test_start_and_port_open_do_not_assume_a_healthy_printer(self):
        self.assert_unobserved(self.worker.get_telemetry())
        self.worker.connect()
        self.assert_unobserved(self.worker.get_telemetry())
        self.assertEqual(self.printer_logs(), [])

    def test_frame_status_changes_are_published_even_when_weight_stays_equal(self):
        cases = ((b"0", PrinterStatus.NORMAL), (b"1", PrinterStatus.NO_PAPER),
                 (b"2", PrinterStatus.TRANSMITTING), (b"9", None))
        for second, (raw, expected) in enumerate(cases):
            with self.subTest(raw=raw):
                snapshot = self.observe(raw, second=second)
                self.assertEqual(snapshot.printer_status, expected)
                self.assertEqual(snapshot.sampled_at, self.started_at + timedelta(seconds=second))
                self.assertEqual(snapshot.sampled_monotonic, 100 + second)
                self.assertEqual(snapshot.sample_seq, second + 1)
                self.assertEqual(snapshot.weight, Decimal("120"))
                self.assertTrue(snapshot.sample_valid)
                self.assertTrue(snapshot.weight_valid)
        self.assertEqual(len(self.snapshots), 4)
        self.assertEqual([printer_status_label(s.printer_status) for s in self.snapshots],
                         ["normal", "no_paper", "transmitting", "unknown"])

    def test_invalid_weight_does_not_block_new_frame_and_printer_observation(self):
        original = self.observe(b"0")
        for second, raw in enumerate((b"1", b"1", b"0", b"\x00"), start=1):
            snapshot = self.observe(raw, second=second, weight=b"OL,NT,+     OLkg")
            self.assertEqual(snapshot.connection_status, "connected")
            self.assertTrue(snapshot.sample_valid)
            self.assertFalse(snapshot.weight_valid)
            self.assertIsNone(snapshot.weight)
            self.assertIsNone(snapshot.unit)
            self.assertEqual(snapshot.sample_seq, original.sample_seq + second)
            self.assertEqual(snapshot.sampled_at, self.started_at + timedelta(seconds=second))
            self.assertEqual(snapshot.sampled_monotonic, 100 + second)
        self.assertEqual([s.printer_status for s in self.snapshots], [
            PrinterStatus.NORMAL, PrinterStatus.NO_PAPER, PrinterStatus.NO_PAPER,
            PrinterStatus.NORMAL, None,
        ])

    def test_unreadable_printer_status_is_an_observation_without_a_status_change_log(self):
        snapshot = self.observe(b"9")
        self.assertIsNone(snapshot.printer_status)
        self.assertEqual(snapshot.sampled_at, self.started_at)
        self.assertEqual(snapshot.sampled_monotonic, 100)
        self.assertTrue(snapshot.sample_valid)
        self.assertEqual(self.printer_logs(), [])

    def test_repeated_status_is_logged_only_on_transitions(self):
        for raw in (b"0", b"0", b"1", b"1", b"2", b"2", b"9", b"9", b"0"):
            self.observe(raw)
        self.assertEqual(self.printer_logs(), [
            ("info", {"previous": "unknown", "status": "normal"}),
            ("warning", {"previous": "normal", "status": "no_paper"}),
            ("info", {"previous": "no_paper", "status": "transmitting"}),
            ("warning", {"previous": "transmitting", "status": "unknown"}),
            ("info", {"previous": "unknown", "status": "normal"}),
        ])

    def test_frame_failure_clears_printer_after_an_invalid_weight_observation(self):
        for raw in (b"1", b"9"):
            with self.subTest(raw=raw):
                snapshot = self.observe(raw, weight=b"OL,NT,+     OLkg")
                self.assertEqual(snapshot.connection_status, "connected")
                self.assertFalse(snapshot.weight_valid)
                count = len(self.snapshots)
                self.serial.send_and_receive.side_effect = ValueError("invalid frame")
                try:
                    for _ in range(2):
                        with self.assertRaises(ValueError):
                            self.worker.exchange(DisplayRequestPacket())
                finally:
                    self.serial.send_and_receive.side_effect = None
                self.assert_unobserved(self.worker.get_telemetry())
                self.assertEqual(self.worker.get_telemetry().connection_status, "error")
                self.assertEqual(len(self.snapshots), count + 1)

    def test_serial_failure_and_reconnect_wait_for_a_fresh_printer_observation(self):
        self.observe(b"1")
        self.serial.send_and_receive.side_effect = serial.SerialTimeoutException("no response")
        with self.assertRaises(serial.SerialTimeoutException):
            self.worker.exchange(DisplayRequestPacket())
        self.assert_unobserved(self.worker.get_telemetry())
        self.assertEqual(self.worker.get_telemetry().connection_status, "disconnected")
        self.worker.connect()
        self.assert_unobserved(self.worker.get_telemetry())
        self.assertEqual(self.worker.get_telemetry().connection_status, "connected")
        self.assertIn(("warning", {"previous": "no_paper", "status": "unknown"}), self.printer_logs())
        self.serial.send_and_receive.side_effect = None
        self.assertEqual(self.observe(b"0", second=1).printer_status, PrinterStatus.NORMAL)

    def test_reopening_a_connected_port_invalidates_printer_observation(self):
        self.observe(b"0")
        self.worker.connect()
        self.assert_unobserved(self.worker.get_telemetry())
        self.assertEqual(len(self.snapshots), 2)

    def test_stop_clears_printer_and_late_responses_cannot_restore_it(self):
        self.observe(b"0")
        logs = self.printer_logs()
        self.worker.stop()
        self.assert_unobserved(self.worker.get_telemetry())
        self.assertEqual(self.printer_logs(), logs)
        count = len(self.snapshots)
        self.observe(b"1", second=1)
        self.assert_unobserved(self.worker.get_telemetry())
        self.assertEqual(len(self.snapshots), count)


if __name__ == "__main__":
    unittest.main()
