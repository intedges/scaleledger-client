"""Exercise voice faults through run(), with playback independent of commands."""

from collections import Counter
from concurrent.futures import Future
from dataclasses import replace
from decimal import Decimal
import unittest
from unittest.mock import Mock, patch

import serial

from events import RFIDTaggedEvent, WeighingCompletedEvent
from suwol1000 import (
    DisplayRequestPacket, PrinterRequestPacket, VoiceCode, WeighingStationWorker,
)
from tests.test_station_liveness import (
    BoundedSimulationStop, ContinuousScale, SimulatedClock,
)


class VoiceProtocolScale(ContinuousScale):
    """Report playback snapshots rather than echoing requested voice numbers.

    A command can start after its immediate response. While playback or a delayed
    start is pending, further voice commands are ignored by this simulated device
    and recorded as overlaps; no particular real firmware overwrite behavior is
    assumed. Vehicles and single RFID tags follow the existing physical timeline.
    """

    def __init__(self, clock, *, missing_voices=(), long_voice=None,
                 long_seconds=20.0, long_once=False, first_start_delay=0.0,
                 tag_while_playing=False, fault=None, response_filter=None,
                 initial_busy_until=0.0):
        super().__init__(clock, repeat_tags=False, fault=fault)
        self.hidden_voices = set(missing_voices)
        self.long_voice = long_voice
        self.long_seconds = long_seconds
        self.long_once = long_once
        self.long_played = False
        self.first_start_delay = first_start_delay
        self.tag_while_playing = tag_while_playing
        self.response_filter = response_filter
        self.playing = VoiceCode.THANK_YOU if initial_busy_until else VoiceCode.NONE
        self.starts_at = 0.0
        self.ends_at = initial_busy_until
        self.last_reported_voice = None
        self.voice_commands = []
        self.overlaps = []
        self.commands_without_idle = []
        self.playbacks = []
        self.responses = []
        self.tag_requests = []
        self.live_snapshots = []

    def send_and_receive(self, request):
        previous_voice = self.last_reported_voice
        try:
            response = super().send_and_receive(request)
        except serial.SerialException:
            self.last_reported_voice = None
            raise
        at = self.clock.now
        voice = getattr(request, "voice_code", VoiceCode.NONE)
        if voice != VoiceCode.NONE:
            command = (at, self.visit_at(), voice)
            self.voice_commands.append(command)
            if previous_voice != VoiceCode.NONE:
                self.commands_without_idle.append(command)
            if at < self.ends_at:
                self.overlaps.append(command)
            else:
                use_long = voice == self.long_voice and not (self.long_once and self.long_played)
                duration = self.long_seconds if use_long else 0.8
                self.long_played |= use_long
                delay = self.first_start_delay if len(self.voice_commands) == 1 else 0.0
                self.playing = voice
                self.starts_at = at + delay
                self.ends_at = self.starts_at + duration
                self.playbacks.append((at, self.starts_at, self.ends_at, voice))
        current_voice = (
            self.playing
            if self.starts_at <= at < self.ends_at
            and self.playing not in self.hidden_voices
            else VoiceCode.NONE
        )
        card = response.rfid_card_uid
        if self.tag_while_playing and card != "00000000" and not (
            voice == VoiceCode.NONE and current_voice == VoiceCode.TAG_CARD
        ):
            self.tagged_visits.discard(self.visit_at())
            card = "00000000"
        response = replace(response, voice_code=current_voice, rfid_card_uid=card)
        if self.response_filter is not None:
            response = self.response_filter(at, response)
        self.last_reported_voice = current_voice
        self.responses.append((at, response))
        if response.rfid_card_uid != "00000000":
            self.tag_requests.append((at, self.visit_at(), request))
        return response


class StationVoiceRecoveryTests(unittest.TestCase):
    def simulate(self, **device_options):
        clock = SimulatedClock()
        scale = VoiceProtocolScale(clock, **device_options)
        submissions = []
        accepted = []
        logger = Mock()

        def on_event(event):
            if isinstance(event, RFIDTaggedEvent):
                accepted.append((clock.now, scale.visit_at(), event))
                return None
            self.assertIsInstance(event, WeighingCompletedEvent)
            submissions.append((clock.now, scale.visit_at(), event))
            result = Future()
            result.set_result(None)
            return result

        with patch("suwol1000.time.monotonic", clock), patch(
            "suwol1000.get_logger", return_value=logger,
        ):
            worker = WeighingStationWorker(
                scale, station_id=7, on_event=on_event, rfid_validator=lambda event: True,
                receipt_builder=lambda event: event.uuid.encode("ascii"),
                on_live_weight=scale.live_snapshots.append,
                polling_interval=0.1, retry_interval=0.2,
                vehicle_confirm_seconds=3,
            )
            worker.stop_event = BoundedSimulationStop(clock, 88)
            scale.worker = worker
            worker.run()
        self.assertLess(len(scale.requests), 2000)
        self.assertTrue(scale.responses)
        gaps = [
            later[0] - earlier[0]
            for earlier, later in zip(scale.responses, scale.responses[1:])
        ]
        max_gap = 0.51 if scale.fault_used else 0.21
        self.assertLessEqual(max(gaps), max_gap, "voice handling stopped weight polling")
        self.assertEqual(scale.commands_without_idle, [], "a new voice needs a prior 00 response")
        self.assertEqual(scale.overlaps, [], "a new command overlapped outstanding playback")
        return scale, submissions, accepted

    def assert_two_visits_saved_and_printed(self, scale, submissions, accepted):
        self.assertEqual(Counter(visit for _, visit, _ in accepted), {1: 1, 2: 1})
        self.assertEqual(Counter(visit for _, visit, _ in submissions), {1: 1, 2: 1})
        self.assertEqual(Counter(visit for visit, _ in scale.printed), {1: 1, 2: 1})
        self.assertEqual(len({event.uuid for _, _, event in submissions}), 2)
        for _, visit, event in submissions:
            self.assertEqual(event.rfid_card_uid, "12345678" if visit == 1 else "87654321")
            self.assertEqual(event.weight, 120)
            self.assertIn((visit, event.uuid.encode("ascii")), scale.printed)

    def assert_voice_only_failure_keeps_station_healthy(self, scale):
        self.assertTrue(scale.live_snapshots)
        self.assertTrue(all(
            snapshot.connection_status == "connected"
            for snapshot in scale.live_snapshots[:-1]
        ))
        self.assertEqual(scale.live_snapshots[-1].connection_status, "disconnected")
        self.assertFalse(any(
            isinstance(request, DisplayRequestPacket)
            and request.red_blink and not request.green_blink
            for _, _, _, request in scale.requests
        ), "a voice-only failure must not advertise a weighing/connection fault")

    def test_normal_playback_preserves_guidance_and_print_order(self):
        scale, submissions, accepted = self.simulate()
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        for visit in (1, 2):
            actions = []
            for _, request_visit, _, request in scale.requests:
                if request_visit != visit:
                    continue
                if isinstance(request, PrinterRequestPacket):
                    actions.append("print")
                elif request.voice_code != VoiceCode.NONE:
                    actions.append(request.voice_code)
            self.assertEqual(actions, [
                VoiceCode.TAG_CARD, VoiceCode.PLEASE_WAIT,
                VoiceCode.WEIGHT_COMPLETE, "print", VoiceCode.THANK_YOU,
            ])

    def test_single_card_during_ready_guidance_is_preserved(self):
        scale, submissions, accepted = self.simulate(tag_while_playing=True)
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        self.assertEqual(len(scale.tag_requests), 2)
        self.assertTrue(all(
            isinstance(request, DisplayRequestPacket)
            and request.voice_code == VoiceCode.NONE
            for _, _, request in scale.tag_requests
        ))
        self.assertTrue(all(
            response.voice_code == VoiceCode.TAG_CARD
            for _, response in scale.responses if response.rfid_card_uid != "00000000"
        ))

    def test_unobserved_voice_start_does_not_cancel_cards_or_skip_printing(self):
        for missing_voice in (
            VoiceCode.TAG_CARD, VoiceCode.PLEASE_WAIT, VoiceCode.WEIGHT_COMPLETE,
        ):
            with self.subTest(missing_voice=missing_voice):
                scale, submissions, accepted = self.simulate(missing_voices={missing_voice})
                self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
                commands = [code for _, _, code in scale.voice_commands]
                self.assertIn(missing_voice, commands)
                self.assertEqual(commands[-1], missing_voice)
                self.assertEqual(commands.count(missing_voice), 1)

    def test_twenty_second_busy_keeps_work_moving_without_another_voice(self):
        scale, submissions, accepted = self.simulate(long_voice=VoiceCode.TAG_CARD)
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        first_started, _, first_ended, _ = scale.playbacks[0]
        first_saved_at = next(at for at, visit, _ in submissions if visit == 1)
        self.assertLess(first_saved_at, first_ended)
        self.assertGreaterEqual(first_saved_at - first_started, 15)
        self.assertFalse(any(
            first_started < at < first_ended
            for at, _, _ in scale.voice_commands
        ))

    def test_permanent_busy_does_not_block_either_vehicle_or_send_more_voices(self):
        scale, submissions, accepted = self.simulate(
            long_voice=VoiceCode.TAG_CARD, long_seconds=float("inf"),
        )
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        self.assertEqual([voice for _, _, voice in scale.voice_commands], [VoiceCode.TAG_CARD])
        self.assert_voice_only_failure_keeps_station_healthy(scale)

    def test_completion_voice_timeout_still_prints_the_saved_record(self):
        scale, submissions, accepted = self.simulate(
            long_voice=VoiceCode.WEIGHT_COMPLETE, long_seconds=float("inf"),
        )
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        self.assertEqual([voice for _, _, voice in scale.voice_commands], [
            VoiceCode.TAG_CARD, VoiceCode.PLEASE_WAIT, VoiceCode.WEIGHT_COMPLETE,
        ])
        first_print_at = next(
            at for at, visit, _, request in scale.requests
            if visit == 1 and isinstance(request, PrinterRequestPacket)
        )
        complete_at = scale.voice_commands[-1][0]
        self.assertGreaterEqual(first_print_at - complete_at, 15)

    def test_busy_recovery_does_not_replay_guidance_from_the_previous_visit(self):
        scale, submissions, accepted = self.simulate(
            long_voice=VoiceCode.WEIGHT_COMPLETE, long_seconds=20, long_once=True,
        )
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        first_voices = [voice for _, visit, voice in scale.voice_commands if visit == 1]
        second_voices = [voice for _, visit, voice in scale.voice_commands if visit == 2]
        self.assertEqual(first_voices, [
            VoiceCode.TAG_CARD, VoiceCode.PLEASE_WAIT, VoiceCode.WEIGHT_COMPLETE,
        ])
        self.assertEqual(second_voices, [
            VoiceCode.TAG_CARD, VoiceCode.PLEASE_WAIT, VoiceCode.WEIGHT_COMPLETE,
            VoiceCode.THANK_YOU,
        ])
        self.assertFalse(any(visit == 0 for _, visit, _ in scale.voice_commands))

    def test_delayed_start_00_is_not_permission_for_a_second_voice(self):
        scale, submissions, accepted = self.simulate(first_start_delay=20)
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        first_sent, first_started, first_ended, _ = scale.playbacks[0]
        first_saved_at = next(at for at, visit, _ in submissions if visit == 1)
        self.assertLess(first_saved_at, first_started)
        self.assertTrue(all(
            response.voice_code == VoiceCode.NONE
            for at, response in scale.responses if first_sent <= at < first_started
        ))
        self.assertFalse(any(
            first_sent < at < first_ended for at, _, _ in scale.voice_commands
        ))
        self.assertEqual(
            [voice for _, visit, voice in scale.voice_commands if visit == 1],
            [VoiceCode.TAG_CARD],
        )
        self.assertEqual(
            [voice for _, visit, voice in scale.voice_commands if visit == 2],
            [VoiceCode.TAG_CARD, VoiceCode.PLEASE_WAIT,
             VoiceCode.WEIGHT_COMPLETE, VoiceCode.THANK_YOU],
        )

    def test_voice_failure_does_not_bypass_invalid_weight_departure_or_connection_loss(self):
        for condition in ("invalid_weight", "departure", "connection_loss"):
            with self.subTest(condition=condition):
                def filter_response(at, response):
                    if 12 <= at < 40 and condition != "connection_loss":
                        weight = Decimal("NaN") if condition == "invalid_weight" else Decimal("0")
                        return replace(response, weight_value=weight)
                    return response

                def fault(request, scale):
                    return condition == "connection_loss" and scale.clock.now >= 12

                scale, submissions, accepted = self.simulate(
                    missing_voices={VoiceCode.TAG_CARD},
                    response_filter=filter_response, fault=fault,
                )
                self.assertEqual(Counter(visit for _, visit, _ in accepted), {1: 1, 2: 1})
                self.assertEqual(len(submissions), 1)
                _, visit, event = submissions[0]
                self.assertEqual(visit, 2)
                self.assertEqual(event.rfid_card_uid, "87654321")
                self.assertEqual(event.weight, 120)
                self.assertEqual(scale.printed, [(2, event.uuid.encode("ascii"))])
                self.assertEqual(
                    [voice for _, _, voice in scale.voice_commands], [VoiceCode.TAG_CARD],
                )
                if condition == "connection_loss":
                    self.assertTrue(scale.fault_used)
                    self.assertEqual(scale.connect_calls, 2)

    def test_reconnect_keeps_unobserved_voice_pending_and_saved_receipt_unchanged(self):
        scale, submissions, accepted = self.simulate(
            missing_voices={VoiceCode.TAG_CARD},
            fault=lambda request, scale: scale.clock.now >= 30,
        )
        self.assertTrue(scale.fault_used)
        self.assertEqual(scale.connect_calls, 2)
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        self.assertEqual([voice for _, _, voice in scale.voice_commands], [VoiceCode.TAG_CARD])
        first_saved_at = next(at for at, visit, _ in submissions if visit == 1)
        first_print_at = next(
            at for at, visit, _, request in scale.requests
            if visit == 1 and isinstance(request, PrinterRequestPacket)
        )
        self.assertLess(first_saved_at, 30)
        self.assertLess(first_print_at, 30)

    def test_existing_busy_at_startup_only_suppresses_audio_not_station_health(self):
        scale, submissions, accepted = self.simulate(initial_busy_until=float("inf"))
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        self.assertEqual(scale.voice_commands, [])
        self.assert_voice_only_failure_keeps_station_healthy(scale)


if __name__ == "__main__":
    unittest.main()
