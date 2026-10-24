from concurrent.futures import Future
from decimal import Decimal
import threading
import unittest
from unittest.mock import Mock, patch

from events import RFIDTaggedEvent, WeighingCompletedEvent
from suwol1000 import (
    RESPONSE_LENGTH, CommandCode, DisplayRequestPacket, InputCode, PrinterStatus,
    ResponsePacket, SerialClient, VoiceCode, WeighingStationWorker, WeightStatus, WeightType,
)


# Captured from a SUW-1000 on 2026-10-23: no card, fan running, +28 degrees inside.
CAPTURED = bytes.fromhex(
    "02 30 44 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 38 30 30 30 30 2b 32 38"
    " 33 30 30 35 30 30 30 30 30 53 54 2c 4e 54 2c 2d 20 30 30 31 36 36 30 6b 67 03"
)


def frame(
    *, header=b"0D", card=b"00000000", pin=b"0", relay=b"00", voice=b"00",
    temperature=b"+25", fan=b"30", heater=b"05", printer=b"0", reserved=b"0000",
    weight=b"ST,NT,+    120kg",
) -> bytes:
    return (
        b"\x02" + header + card + pin + b"000000" + relay + b"00" + voice + temperature
        + fan + heater + printer + reserved + weight + b"\x03"
    )


class ResponsePacketTests(unittest.TestCase):
    def test_captured_device_frame(self):
        response = ResponsePacket.from_bytes(CAPTURED)

        self.assertEqual(response.invalid_fields, ())
        self.assertEqual((response.device_id, response.command_code), (0, CommandCode.DISPLAY))
        self.assertFalse(response.has_card)
        self.assertEqual(response.user_command_code, InputCode.NONE)
        self.assertEqual(
            (response.green_blink, response.red_blink, response.fan_on, response.heater_on),
            (False, False, True, False),
        )
        self.assertEqual(response.voice_code, VoiceCode.NONE)
        self.assertEqual(
            (response.inner_temperature, response.fan_trigger_temp, response.heater_trigger_temp),
            (28, 30, 5),
        )
        self.assertEqual(response.printer_status, PrinterStatus.NORMAL)
        self.assertEqual(
            (response.weight_status, response.weight_type, response.weight_value, response.weight_unit),
            (WeightStatus.STABLE, WeightType.NET, Decimal("-1660"), "kg"),
        )
        self.assertEqual(response.raw, CAPTURED)

    def test_unreadable_field_does_not_discard_card_voice_or_weight(self):
        cases = {
            # The field unit of 2026-10-22 sent NUL bytes as its fan setting.
            "fan_trigger_temp": dict(fan=b"\x00\x00"),
            "heater_trigger_temp": dict(heater=b"  "),
            "inner_temperature": dict(temperature=b"\xff\xff\xff"),
            "printer_status": dict(printer=b"9"),
            "user_command_code": dict(pin=b"\x00"),
            "device_id": dict(header=b"\x00D"),
            "command_code": dict(header=b"0\x00"),
        }
        for name, fields in cases.items():
            with self.subTest(name):
                response = ResponsePacket.from_bytes(frame(card=b"12345678", voice=b"04", **fields))

                self.assertEqual(response.invalid_fields, (name,))
                self.assertIsNone(getattr(response, name))
                self.assertEqual(response.rfid_card_uid, "12345678")
                self.assertEqual(response.voice_code, VoiceCode.TAG_CARD)
                self.assertEqual(response.weight_value, Decimal("120"))

    def test_temperature_keeps_its_sign(self):
        for text, expected in ((b"+28", 28), (b"-05", -5), (b"- 5", -5), (b"025", 25)):
            with self.subTest(text=text):
                self.assertEqual(ResponsePacket.from_bytes(frame(temperature=text)).inner_temperature, expected)

    def test_unreadable_relay_status_only_clears_the_relay_flags(self):
        response = ResponsePacket.from_bytes(frame(relay=b"\x00A"))

        self.assertEqual(response.invalid_fields, ("green_blink", "red_blink", "fan_on", "heater_on"))
        self.assertEqual(response.weight_value, Decimal("120"))

    def test_unknown_voice_status_is_none(self):
        for voice in (b"13", b"\x00\x00", b"-1"):
            with self.subTest(voice=voice):
                response = ResponsePacket.from_bytes(frame(voice=voice))
                self.assertIsNone(response.voice_code)
                self.assertEqual(response.invalid_fields, ("voice_code",))

    def test_only_printable_cards_are_tags(self):
        cases = {
            b"12345678": True, b"00000000": False, b"        ": False,
            b"\x00" * 8: False, b"1234\xff678": False,
        }
        for card, has_card in cases.items():
            with self.subTest(card=card):
                response = ResponsePacket.from_bytes(frame(card=card))
                self.assertEqual(response.has_card, has_card)
                self.assertEqual(response.invalid_fields, () if has_card or card == b"00000000" else ("rfid_card_uid",))

    def test_weight_must_be_a_plain_number(self):
        cases = {
            b"- 001660": Decimal("-1660"),
            b"+12345.6": Decimal("12345.6"),
            b"+      0": Decimal("0"),
            b"+     OL": None,
            b"+    NaN": None,
            b"+1e5    ": None,
            b"+ 1 2   ": None,
            b"\x00" * 8: None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                response = ResponsePacket.from_bytes(frame(weight=b"ST,NT," + text + b"kg"))
                self.assertEqual(response.weight_value, expected)
                self.assertEqual(response.invalid_fields, () if expected is not None else ("weight_value",))

    def test_unreadable_weight_status_type_or_unit(self):
        cases = {
            "weight_status": b"XX,NT,+    120kg",
            "weight_type": b"ST,XX,+    120kg",
            "weight_unit": b"ST,NT,+    120\x00\x00",
        }
        for name, weight in cases.items():
            with self.subTest(name):
                self.assertEqual(ResponsePacket.from_bytes(frame(weight=weight)).invalid_fields, (name,))

    def test_no_indicator_data_is_a_no_signal_reading(self):
        # The device fills the weight with "0" when the indicator sends nothing.
        response = ResponsePacket.from_bytes(frame(weight=b"0" * 16))

        self.assertEqual(response.invalid_fields, ())
        self.assertEqual(
            (response.weight_status, response.weight_type, response.weight_value, response.weight_unit),
            (WeightStatus.NONE, WeightType.NONE, Decimal("0"), "00"),
        )

    def test_field_offsets_do_not_shift_on_non_ascii_or_etx_bytes(self):
        for reserved in (b"\xc3\xa9\x00\x00", b"\xe0\xa0\xff\x80", b"\x03\x03\x03\x03"):
            with self.subTest(reserved=reserved):
                response = ResponsePacket.from_bytes(frame(reserved=reserved))
                self.assertEqual(response.invalid_fields, ())
                self.assertEqual(response.weight_status, WeightStatus.STABLE)
                self.assertEqual(response.weight_value, Decimal("120"))

    def test_broken_frame_still_fails_with_its_raw_bytes(self):
        with self.assertRaisesRegex(ValueError, r"^Invalid response packet length: 0 bytes$"):
            ResponsePacket.from_bytes(b"")
        with self.assertRaisesRegex(ValueError, "length: 3 bytes 02 30 03$"):
            ResponsePacket.from_bytes(b"\x020\x03")
        with self.assertRaisesRegex(ValueError, "^Invalid STX/ETX: 00 30 44"):
            ResponsePacket.from_bytes(b"\x00" + CAPTURED[1:])


class SerialClientTests(unittest.TestCase):
    def test_drops_stale_bytes_and_reads_one_fixed_length_frame(self):
        client = SerialClient("test-port")
        client.serial = Mock(is_open=True)
        # read_until(ETX) would have cut this frame at the fan setting.
        client.serial.read.return_value = frame(fan=b"\x03\x00")
        request = DisplayRequestPacket()

        response = client.send_and_receive(request)

        self.assertEqual([name for name, *_ in client.serial.method_calls], ["reset_input_buffer", "write", "read"])
        client.serial.write.assert_called_once_with(request.to_bytes())
        client.serial.read.assert_called_once_with(RESPONSE_LENGTH)
        self.assertEqual(response.invalid_fields, ("fan_trigger_temp",))
        self.assertEqual(response.weight_value, Decimal("120"))


class DisplayWeightTests(unittest.TestCase):
    def test_weight_beyond_the_display_is_not_used_so_requests_stay_32_bytes(self):
        client = Mock(port="test-port")
        with patch("suwol1000.get_logger", return_value=Mock()):
            worker = WeighingStationWorker(client, station_id=7, polling_interval=0)
        cases = (
            (Decimal("999999"), True), (Decimal("-999999"), True),
            (Decimal("999999.7"), False), (Decimal("1234567"), False), (Decimal("-1000000"), False),
        )
        for weight, used in cases:
            with self.subTest(weight=weight):
                worker.last_weight = Decimal("120")
                client.send_and_receive.return_value = ResponsePacket(weight_value=weight)

                worker.exchange(DisplayRequestPacket())

                self.assertEqual(worker.last_weight, weight if used else Decimal("120"))
                self.assertEqual(worker._indicator_fault, not used)
                self.assertEqual(len(worker._display_request().to_bytes()), 32)


class RawFrameScale:
    """Answers with wire frames so the production parser runs on every exchange."""

    port = "raw-frame-test"

    def __init__(self, **fields):
        self.fields = fields
        self.clock = 0.0
        self.requests = []
        self.connects = 0
        self.card_pending = True
        self.stop_event: threading.Event | None = None

    def connect(self):
        self.connects += 1

    def disconnect(self):
        pass

    def interrupt(self):
        pass

    def send_and_receive(self, request):
        self.requests.append(request)
        self.clock += 1
        if len(self.requests) >= 80:
            self.stop_event.set()
        card = b"00000000"
        if self.card_pending and request.green_blink and not request.red_blink:
            self.card_pending = False
            card = b"12345678"
        # Echo each voice once, like the device reporting its playback.
        wire = dict(
            card=card, voice=f"{request.voice_code:02d}".encode(),
            weight=b"ST,NT,+      0kg" if self.clock <= 8 else b"ST,NT,+    120kg",
        )
        return ResponsePacket.from_bytes(frame(**wire | self.fields))


class UnreadableFieldStationTests(unittest.TestCase):
    def run_station(self, scale: RawFrameScale):
        events, samples = [], []

        def save(event):
            events.append(event)
            result = Future()
            result.set_result(None)
            return result

        logger = Mock()
        with patch("suwol1000.get_logger") as get_logger:
            get_logger.return_value.bind.return_value = logger
            worker = WeighingStationWorker(
                scale, station_id=7, on_event=save, on_live_weight=samples.append,
                polling_interval=0, retry_interval=0,
            )
        scale.stop_event = worker.stop_event
        with patch("suwol1000.time.monotonic", side_effect=lambda: scale.clock):
            thread = threading.Thread(target=worker.run)
            thread.start()
            thread.join(timeout=10)
        self.assertFalse(thread.is_alive())
        logger.log.assert_not_called()
        self.assertEqual(scale.connects, 1)
        warnings = [c for c in logger.warning.call_args_list if c.args[0] == "hw.protocol.field_invalid"]
        self.assertEqual(len(warnings), 1)
        self.assertEqual(len(bytes.fromhex(warnings[0].kwargs["raw"])), RESPONSE_LENGTH)
        return events, samples, warnings[0].kwargs["fields"]

    @staticmethod
    def voices(scale: RawFrameScale) -> list[VoiceCode]:
        return [request.voice_code for request in scale.requests if request.voice_code != VoiceCode.NONE]

    def test_unreadable_fan_and_heater_settings_keep_the_station_weighing(self):
        scale = RawFrameScale(fan=b"\x00\x00", heater=b"\x00\x00")

        events, samples, fields = self.run_station(scale)

        self.assertEqual([type(event) for event in events], [RFIDTaggedEvent, WeighingCompletedEvent])
        self.assertEqual((events[1].rfid_card_uid, events[1].weight), ("12345678", 120))
        self.assertEqual(
            self.voices(scale),
            [VoiceCode.TAG_CARD, VoiceCode.PLEASE_WAIT, VoiceCode.WEIGHT_COMPLETE, VoiceCode.THANK_YOU],
        )
        self.assertEqual(fields, ["fan_trigger_temp", "heater_trigger_temp"])
        self.assertNotIn("error", [sample.connection_status for sample in samples])

    def test_unreadable_voice_status_skips_guidance_but_completes_weighing(self):
        scale = RawFrameScale(voice=b"\x00\x00")

        events, _, fields = self.run_station(scale)

        self.assertEqual([type(event) for event in events], [RFIDTaggedEvent, WeighingCompletedEvent])
        self.assertEqual(self.voices(scale), [])
        self.assertEqual(fields, ["voice_code"])

    def test_unreadable_weight_pauses_weighing_without_reconnecting(self):
        scale = RawFrameScale(weight=b"OL,GS,+     OLkg")

        events, samples, fields = self.run_station(scale)

        self.assertEqual(events, [])
        self.assertEqual(fields, ["weight_value"])
        last = scale.requests[-1]
        self.assertEqual((last.green_blink, last.red_blink), (False, True))
        self.assertIn("error", [sample.connection_status for sample in samples])
        self.assertFalse(any(sample.sample_valid for sample in samples))


if __name__ == "__main__":
    unittest.main()
