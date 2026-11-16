"""Recover an unobserved playback through fresh protocol responses, not retries."""

from decimal import Decimal
import unittest
from unittest.mock import Mock, patch

from suwol1000 import (
    DeviceResponseTimeout, DisplayRequestPacket, VoiceCode, WeighingStationWorker,
)
from tests.test_station_cycle import Clock, CycleScale


class StationVoiceResyncTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.enterContext(patch("suwol1000.time.monotonic", self.clock))
        self.enterContext(patch("suwol1000.get_logger", return_value=Mock()))
        self.reset_station()

    def reset_station(self):
        self.clock.now = 0.0
        self.scale = CycleScale(self.clock)
        self.scale.weight = Decimal("0")
        self.scale.fixed_voice = VoiceCode.NONE
        self.events = Mock()
        self.worker = WeighingStationWorker(
            self.scale, station_id=7, on_event=self.events,
            polling_interval=0, retry_interval=0, voice_timeout=1,
        )

    def observe(self, at, voice=VoiceCode.NONE):
        self.clock.now = at
        self.scale.queue(weight=str(self.scale.weight), voice=voice)
        return self.worker.exchange(DisplayRequestPacket())

    def send_unobserved_voice(self):
        self.observe(0)
        self.worker.exchange(DisplayRequestPacket(voice_code=VoiceCode.TAG_CARD))
        self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
        self.assertFalse(self.worker._voice_started)

    def voice_commands(self):
        return [
            (at, request.voice_code)
            for at, request in zip(self.scale.times, self.scale.requests)
            if isinstance(request, DisplayRequestPacket)
            and request.voice_code != VoiceCode.NONE
        ]

    def recovery_logs(self):
        return [
            call.kwargs for call in self.worker.logger.info.call_args_list
            if call.args[0] == "hw.speaker.recovered"
        ]

    def test_recovery_requires_grace_then_a_full_idle_window_despite_short_warning_timeout(self):
        self.send_unobserved_voice()
        # The one-second warning timeout must not shorten the recovery grace.
        for at in range(1, 16):
            self.observe(at)
            self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
        warnings = [
            call for call in self.worker.logger.warning.call_args_list
            if call.args[0] == "hw.speaker.timeout"
        ]
        self.assertEqual(len(warnings), 1)
        for at in (16, 17, 18, 19, 20, 20.48):
            self.observe(at)
            self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)

        self.observe(20.50)

        self.assertIsNone(self.worker._pending_voice)
        self.assertEqual(self.voice_commands(), [(0, VoiceCode.TAG_CARD)])
        self.assertEqual(len(self.recovery_logs()), 1)
        self.assertEqual(self.recovery_logs()[0].get("reason"), "idle_resynchronized")
        self.events.assert_not_called()

    def test_unknown_or_other_busy_voice_restarts_idle_confirmation(self):
        for interruption in (None, VoiceCode.THANK_YOU):
            with self.subTest(interruption=interruption):
                self.reset_station()
                self.send_unobserved_voice()
                for at in range(15, 20):
                    self.observe(at)
                self.observe(19.5, interruption)

                for at in (20, 21, 22, 23, 24, 25):
                    self.observe(at)
                    self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
                self.observe(25.5)

                self.assertIsNone(self.worker._pending_voice)
                self.assertEqual(self.voice_commands(), [(0, VoiceCode.TAG_CARD)])

    def test_exchange_failure_or_reconnect_restarts_idle_confirmation(self):
        for interruption in ("lost_response", "reconnect"):
            with self.subTest(interruption=interruption):
                self.reset_station()
                self.send_unobserved_voice()
                for at in range(15, 20):
                    self.observe(at)
                self.clock.now = 19.5
                if interruption == "lost_response":
                    self.scale.responses.append((0, DeviceResponseTimeout("response lost")))
                    with self.assertRaises(DeviceResponseTimeout):
                        self.worker.exchange(DisplayRequestPacket())
                else:
                    self.worker.connect()
                self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)

                for at in (20, 21, 22, 23, 24, 25):
                    self.observe(at)
                    self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
                self.observe(25.5)

                self.assertIsNone(self.worker._pending_voice)
                self.assertEqual(self.voice_commands(), [(0, VoiceCode.TAG_CARD)])

    def test_five_second_observation_gap_does_not_count_as_idle_time(self):
        self.send_unobserved_voice()
        for at in range(15, 20):
            self.observe(at)
        # Exactly the existing observation-expiry boundary breaks continuity.
        for at in (24, 25, 26, 27, 28, 29):
            self.observe(at)
            self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)

        self.observe(29.5)

        self.assertIsNone(self.worker._pending_voice)
        self.assertEqual(self.recovery_logs()[0].get("reason"), "idle_resynchronized")

    def test_elapsed_time_and_reopening_alone_never_release_pending_voice(self):
        self.send_unobserved_voice()
        self.clock.now = 3600
        self.worker.connect()
        self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
        self.assertNotEqual(self.worker.get_telemetry().connection_status, "connected")
        self.observe(3600)
        self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
        self.assertEqual(self.worker.get_telemetry().connection_status, "connected")
        # Another long gap is not evidence that the speaker stayed idle.
        self.observe(3606)
        self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
        for at in (3607, 3608, 3609, 3610, 3611):
            self.observe(at)
            self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
        self.observe(3611.5)
        self.assertIsNone(self.worker._pending_voice)

    def test_observed_matching_busy_then_idle_uses_normal_completion_path(self):
        self.send_unobserved_voice()
        for at in range(15, 20):
            self.observe(at)
        self.observe(19.5, VoiceCode.TAG_CARD)
        self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
        self.assertTrue(self.worker._voice_started)

        self.observe(19.6)

        self.assertIsNone(self.worker._pending_voice)
        self.assertEqual(self.recovery_logs()[0].get("reason"), "busy_then_idle")
        self.assertEqual(self.voice_commands(), [(0, VoiceCode.TAG_CARD)])

    def test_any_persistent_busy_prevents_recovery_and_new_commands(self):
        for busy in (VoiceCode.TAG_CARD, VoiceCode.THANK_YOU):
            with self.subTest(busy=busy):
                self.reset_station()
                self.send_unobserved_voice()
                self.scale.fixed_voice = busy
                for at in range(1, 121):
                    self.clock.now = at
                    self.worker._poll()
                self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
                self.assertEqual(self.voice_commands(), [(0, VoiceCode.TAG_CARD)])
                self.assertEqual(self.recovery_logs(), [])

    def test_one_hour_of_idle_after_lost_prompt_response_resumes_current_guidance(self):
        self.observe(0)
        self.observe(1)
        self.scale.weight = Decimal("120")
        self.observe(2)
        self.observe(3)
        self.scale.responses.append((0, DeviceResponseTimeout("prompt response lost")))
        with self.assertRaises(DeviceResponseTimeout):
            self.worker._poll()
        first_command_at = self.voice_commands()[0][0]
        self.clock.now = 4
        self.worker.connect()
        self.assertNotEqual(self.worker.get_telemetry().connection_status, "connected")

        for at in range(4, 3605):
            self.clock.now = at
            self.worker._poll()

        commands = self.voice_commands()
        self.assertGreater(len(commands), 2)
        self.assertTrue(all(code == VoiceCode.TAG_CARD for _, code in commands))
        self.assertGreaterEqual(commands[1][0] - first_command_at, 20.49)
        self.assertLessEqual(commands[1][0] - first_command_at, 22)
        self.assertTrue(self.recovery_logs())
        self.assertEqual(self.worker.get_telemetry().connection_status, "connected")
        self.assertEqual(self.worker.get_telemetry().sample_seq, 3605)
        self.events.assert_not_called()


if __name__ == "__main__":
    unittest.main()
