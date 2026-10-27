"""Relay 8 wire encoding and bounded serial I/O for one-shot zero requests."""

import unittest
from unittest.mock import Mock, patch

import serial

from suwol1000 import DeviceResponseTimeout, DisplayRequestPacket, ResponsePacket, SerialClient
from tests.test_station_protocol import frame


class ZeroRelayPacketTests(unittest.TestCase):
    def test_zero_output_preserves_each_lamp_combination_and_packet_size(self):
        for green, red, off, on in (
            (False, False, b"00", b"80"),
            (True, False, b"01", b"81"),
            (False, True, b"02", b"82"),
            (True, True, b"03", b"83"),
        ):
            with self.subTest(green=green, red=red):
                ordinary = DisplayRequestPacket(green_blink=green, red_blink=red).to_bytes()
                zero = DisplayRequestPacket(green_blink=green, red_blink=red, zero_output=True).to_bytes()
                self.assertEqual((len(ordinary), len(zero)), (32, 32))
                self.assertEqual(ordinary[23:25], off)
                self.assertEqual(zero[23:25], on)
                self.assertEqual(ordinary[:23] + ordinary[25:], zero[:23] + zero[25:])

    def test_zero_output_feedback_is_independent_of_lamps_and_fan(self):
        for relay, zero, lamps, fan in (
            (b"00", False, (False, False), False),
            (b"80", True, (False, False), False),
            (b"83", True, (True, True), False),
            (b"08", False, (False, False), True),
            (b"8;", True, (True, True), True),
        ):
            with self.subTest(relay=relay):
                response = ResponsePacket.from_bytes(frame(relay=relay))
                self.assertIs(response.zero_output_on, zero)
                self.assertEqual((response.green_blink, response.red_blink), lamps)
                self.assertIs(response.fan_on, fan)

    def test_invalid_feedback_is_unknown_instead_of_off(self):
        response = ResponsePacket.from_bytes(frame(relay=b"\x00A"))
        self.assertIsNone(response.zero_output_on)
        self.assertIn("zero_output_on", response.invalid_fields)


class SerialDeadlineTests(unittest.TestCase):
    def setUp(self):
        self.now = 10.0
        self.enterContext(patch("suwol1000.time.monotonic", side_effect=lambda: self.now))
        self.client = SerialClient("zero-deadline-test")
        self.client.logger = Mock()
        self.port = self.client.serial = Mock(is_open=True, timeout=2.0, write_timeout=2.0)
        self.observed_limits = []
        self.write_seconds = 0.4
        self.read_seconds = 0.3
        self.port.write.side_effect = self.write
        self.port.read.side_effect = self.read
        self.request = DisplayRequestPacket(zero_output=True)

    def write(self, data):
        self.observed_limits.append(("write", self.port.write_timeout))
        self.now += self.write_seconds
        return len(data)

    def read(self, size):
        self.observed_limits.append(("read", self.port.timeout))
        self.now += self.read_seconds
        return frame(relay=b"80")

    def assert_limits_restored(self):
        self.assertEqual((self.port.timeout, self.port.write_timeout), (2.0, 2.0))

    def test_write_and_read_share_the_deadline_and_restore_normal_limits(self):
        response = self.client.send_and_receive(self.request, deadline=11.5)

        self.assertTrue(response.zero_output_on)
        self.assertEqual(self.observed_limits[0], ("write", 1.5))
        self.assertEqual(self.observed_limits[1][0], "read")
        self.assertAlmostEqual(self.observed_limits[1][1], 1.1)
        self.assert_limits_restored()
        self.port.write.assert_called_once_with(self.request.to_bytes())
        self.port.read.assert_called_once_with(53)

    def test_long_deadline_does_not_extend_normal_timeouts(self):
        self.client.send_and_receive(self.request, deadline=20.0)

        self.assertEqual(self.observed_limits, [("write", 2.0), ("read", 2.0)])
        self.assert_limits_restored()

    def test_expired_deadline_does_not_write_or_read(self):
        with self.assertRaisesRegex(serial.SerialTimeoutException, "before write"):
            self.client.send_and_receive(self.request, deadline=10.0)

        self.port.write.assert_not_called()
        self.port.read.assert_not_called()
        self.assert_limits_restored()

    def test_write_exhausting_deadline_does_not_start_a_read_or_resend(self):
        self.write_seconds = 1.5
        with self.assertRaisesRegex(DeviceResponseTimeout, "after write"):
            self.client.send_and_receive(self.request, deadline=11.5)

        self.port.write.assert_called_once()
        self.port.read.assert_not_called()
        self.assert_limits_restored()

    def test_late_complete_response_is_not_accepted(self):
        self.read_seconds = 1.2
        with self.assertRaisesRegex(DeviceResponseTimeout, "during read"):
            self.client.send_and_receive(self.request, deadline=11.5)

        self.port.write.assert_called_once()
        self.port.read.assert_called_once()
        self.assert_limits_restored()

    def test_partial_write_does_not_read_or_resend(self):
        self.port.write.side_effect = lambda _: 12
        with self.assertRaisesRegex(serial.SerialTimeoutException, "sent 12/32 bytes"):
            self.client.send_and_receive(self.request, deadline=11.5)

        self.port.write.assert_called_once()
        self.port.read.assert_not_called()
        self.assert_limits_restored()

    def test_partial_write_without_deadline_is_also_a_failure(self):
        self.port.write.side_effect = lambda _: 0
        with self.assertRaisesRegex(serial.SerialTimeoutException, "sent 0/32 bytes"):
            self.client.send_and_receive(self.request)

        self.port.write.assert_called_once()
        self.port.read.assert_not_called()
        self.assert_limits_restored()

    def test_timeouts_are_restored_when_write_or_read_fails(self):
        for method in ("write", "read"):
            with self.subTest(method=method):
                self.now = 10.0
                self.port.write.side_effect = self.write
                self.port.read.side_effect = self.read
                getattr(self.port, method).side_effect = serial.SerialException("disconnected")
                with self.assertRaisesRegex(serial.SerialException, "disconnected"):
                    self.client.send_and_receive(self.request, deadline=11.5)
                self.assert_limits_restored()

    def test_timeouts_are_restored_after_partial_or_invalid_response(self):
        for response, error in ((b"\x02", DeviceResponseTimeout), (b"\x00" + frame()[1:], ValueError)):
            with self.subTest(response=response):
                self.now = 10.0
                self.port.read.side_effect = lambda _: response
                with self.assertRaises(error):
                    self.client.send_and_receive(self.request, deadline=11.5)
                self.assert_limits_restored()

    def test_invalid_deadline_does_not_touch_the_port(self):
        for deadline in (True, float("nan"), float("inf"), "later"):
            with self.subTest(deadline=deadline):
                with self.assertRaisesRegex(ValueError, "finite monotonic timestamp"):
                    self.client.send_and_receive(self.request, deadline=deadline)
        self.assertEqual(self.port.method_calls, [])


if __name__ == "__main__":
    unittest.main()
