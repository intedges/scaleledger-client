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
    BoundedSimulationStop, ContinuousScale, SimulatedClock, simulated_wait,
)


VISIT_STARTS = {1: 6, 2: 46}


class VoiceProtocolScale(ContinuousScale):
    """Report playback snapshots rather than echoing requested voice numbers.

    A command can start after its immediate response. While playback or a delayed
    start is pending, further voice commands are ignored by this simulated device
    and recorded as overlaps; no particular real firmware overwrite behavior is
    assumed. Vehicles and single RFID tags follow the existing physical timeline.
    """

    def __init__(self, clock, *, missing_voices=(), long_voice=None,
                 long_seconds=20.0, long_once=False, first_start_delay=0.0,
                 tag_while_playing=False, tag_after=0.0, blind_tags=False, fault=None,
                 response_filter=None, initial_busy_until=0.0):
        super().__init__(clock, repeat_tags=False, fault=fault)
        # A driver who taps on every poll of a visit, whatever the lamps show.
        self.blind_tags = blind_tags
        self.hidden_voices = set(missing_voices)
        self.long_voice = long_voice
        self.long_seconds = long_seconds
        self.long_once = long_once
        self.long_played = False
        self.first_start_delay = first_start_delay
        self.tag_while_playing = tag_while_playing
        self.tag_after = tag_after
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
        visit = self.visit_at()
        if self.blind_tags and visit:
            card = "12345678" if visit == 1 else "87654321"
        if card != "00000000" and (
            at - VISIT_STARTS.get(visit, 0) < self.tag_after
            or self.tag_while_playing
            and not (voice == VoiceCode.NONE and current_voice == VoiceCode.TAG_CARD)
        ):
            self.tagged_visits.discard(visit)
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
        ), patch("suwol1000.wait", side_effect=simulated_wait(clock)):
            worker = WeighingStationWorker(
                scale, station_id=7, on_event=on_event, rfid_validator=lambda event: True,
                receipt_builder=lambda event: event.uuid.encode("ascii"),
                on_telemetry=scale.live_snapshots.append,
                polling_interval=0.1, retry_interval=0.2,
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

    @staticmethod
    def guidance(scale):
        """Voices and receipts of each visit, including the thanks after it leaves."""
        actions = {1: [], 2: []}
        for at, _, _, request in scale.requests:
            if at < VISIT_STARTS[1]:
                continue
            visit = 1 if at < VISIT_STARTS[2] else 2
            if isinstance(request, PrinterRequestPacket):
                actions[visit].append("print")
            elif request.voice_code != VoiceCode.NONE:
                actions[visit].append(request.voice_code)
        return actions

    @staticmethod
    def voice_times(scale, voice, visit):
        return [at for at, command_visit, code in scale.voice_commands
                if code == voice and command_visit == visit]

    def test_normal_guidance_prompts_prints_then_repeats_completion_until_departure(self):
        scale, submissions, accepted = self.simulate()
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        for visit, actions in self.guidance(scale).items():
            with self.subTest(visit=visit):
                self.assertEqual(actions[:3], [VoiceCode.TAG_CARD, "print", VoiceCode.WEIGHT_COMPLETE])
                self.assertEqual(set(actions[2:-1]), {VoiceCode.WEIGHT_COMPLETE})
                self.assertGreaterEqual(len(actions[2:-1]), 6)
                self.assertEqual(actions[-1], VoiceCode.THANK_YOU)
                completions = self.voice_times(scale, VoiceCode.WEIGHT_COMPLETE, visit)
                gaps = [later - earlier for earlier, later in zip(completions, completions[1:])]
                self.assertTrue(all(4.99 <= gap <= 5.25 for gap in gaps), gaps)
        departures = [40, 80]
        farewells = [at for at, _, voice in scale.voice_commands if voice == VoiceCode.THANK_YOU]
        self.assertEqual(len(farewells), 2)
        for departure, farewell in zip(departures, farewells):
            self.assertTrue(departure <= farewell < departure + 1, farewells)

    def test_card_prompt_repeats_every_five_seconds_until_the_card_is_tagged(self):
        scale, submissions, accepted = self.simulate(tag_after=12)
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        for visit in (1, 2):
            with self.subTest(visit=visit):
                tagged_at = next(at for at, accepted_visit, _ in accepted if accepted_visit == visit)
                self.assertGreaterEqual(tagged_at, VISIT_STARTS[visit] + 12)
                prompts = self.voice_times(scale, VoiceCode.TAG_CARD, visit)
                self.assertEqual(len(prompts), 3)
                self.assertTrue(all(at < tagged_at for at in prompts))
                gaps = [later - earlier for earlier, later in zip(prompts, prompts[1:])]
                self.assertTrue(all(4.99 <= gap <= 5.25 for gap in gaps), gaps)
                printed_at = next(
                    at for at, request_visit, _, request in scale.requests
                    if request_visit == visit and isinstance(request, PrinterRequestPacket)
                )
                self.assertLess(printed_at - tagged_at, 0.25)

    def test_card_prompt_stops_when_the_vehicle_leaves_without_a_card(self):
        scale, submissions, accepted = self.simulate(tag_after=float("inf"))
        self.assertEqual((accepted, submissions, scale.printed), ([], [], []))
        for visit, departure in ((1, 40), (2, 80)):
            prompts = self.voice_times(scale, VoiceCode.TAG_CARD, visit)
            self.assertGreaterEqual(len(prompts), 6)
            self.assertTrue(all(at < departure for at in prompts))
        self.assertFalse([at for at, visit, _ in scale.voice_commands if visit == 0])
        self.assertEqual({voice for _, _, voice in scale.voice_commands}, {VoiceCode.TAG_CARD})

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
            VoiceCode.TAG_CARD, VoiceCode.WEIGHT_COMPLETE, VoiceCode.THANK_YOU,
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
        first_sent, _, first_ended, _ = scale.playbacks[0]
        first_saved_at = next(at for at, visit, _ in submissions if visit == 1)
        self.assertLess(first_saved_at - first_sent, 0.25)
        self.assertFalse(any(
            first_sent < at < first_ended
            for at, _, _ in scale.voice_commands
        ))
        # The vehicle is still on the platform, so completion follows the long prompt.
        self.assertEqual(
            next(voice for at, _, voice in scale.voice_commands if at >= first_ended),
            VoiceCode.WEIGHT_COMPLETE,
        )

    def test_permanent_busy_does_not_block_either_vehicle_or_send_more_voices(self):
        scale, submissions, accepted = self.simulate(
            long_voice=VoiceCode.TAG_CARD, long_seconds=float("inf"),
        )
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        self.assertEqual([voice for _, _, voice in scale.voice_commands], [VoiceCode.TAG_CARD])
        self.assert_voice_only_failure_keeps_station_healthy(scale)

    def test_endless_completion_voice_still_prints_the_next_vehicle(self):
        scale, submissions, accepted = self.simulate(
            long_voice=VoiceCode.WEIGHT_COMPLETE, long_seconds=float("inf"),
        )
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        self.assertEqual([voice for _, _, voice in scale.voice_commands], [
            VoiceCode.TAG_CARD, VoiceCode.WEIGHT_COMPLETE,
        ])
        first_print_at = next(
            at for at, visit, _, request in scale.requests
            if visit == 1 and isinstance(request, PrinterRequestPacket)
        )
        self.assertLess(first_print_at, scale.voice_commands[-1][0])
        self.assert_voice_only_failure_keeps_station_healthy(scale)

    def test_busy_farewell_does_not_replay_guidance_from_the_previous_visit(self):
        scale, submissions, accepted = self.simulate(
            long_voice=VoiceCode.THANK_YOU, long_seconds=20, long_once=True,
        )
        self.assert_two_visits_saved_and_printed(scale, submissions, accepted)
        _, _, farewell_ended, _ = next(
            playback for playback in scale.playbacks if playback[3] == VoiceCode.THANK_YOU
        )
        second_print_at = next(
            at for at, visit, _, request in scale.requests
            if visit == 2 and isinstance(request, PrinterRequestPacket)
        )
        self.assertLess(second_print_at, farewell_ended)
        # The next vehicle tagged during the long thanks; only its own completion follows.
        after = [voice for at, _, voice in scale.voice_commands if at >= farewell_ended]
        self.assertEqual(after[0], VoiceCode.WEIGHT_COMPLETE)
        self.assertNotIn(VoiceCode.TAG_CARD, after)
        self.assertEqual(after[-1], VoiceCode.THANK_YOU)

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
        # Guidance resumes for the weighed vehicle once its playback is seen to end.
        resumed = [voice for at, _, voice in scale.voice_commands if first_ended <= at < 40]
        self.assertTrue(resumed)
        self.assertEqual(set(resumed), {VoiceCode.WEIGHT_COMPLETE})

    def test_suppressed_guidance_does_not_admit_cards_without_a_valid_vehicle(self):
        for condition in ("invalid_weight", "empty_platform"):
            with self.subTest(condition=condition):
                def filter_response(at, response):
                    if at >= VISIT_STARTS[2]:
                        weight = Decimal("NaN") if condition == "invalid_weight" else Decimal("0")
                        return replace(response, weight_value=weight)
                    return response

                # The first completion notice is never seen to play, so guidance stays
                # suppressed while the second visit's tags keep arriving.
                scale, submissions, accepted = self.simulate(
                    missing_voices={VoiceCode.WEIGHT_COMPLETE}, blind_tags=True,
                    response_filter=filter_response,
                )
                self.assertEqual([voice for _, _, voice in scale.voice_commands], [
                    VoiceCode.WEIGHT_COMPLETE,
                ])
                self.assertGreater(len([at for at, visit, _ in scale.tag_requests if visit == 2]), 100)
                self.assertEqual(Counter(visit for _, visit, _ in accepted), {1: 1})
                self.assertEqual(Counter(visit for _, visit, _ in submissions), {1: 1})
                self.assertEqual([visit for visit, _ in scale.printed], [1])
                faults = [
                    request for at, _, _, request in scale.requests
                    if at >= VISIT_STARTS[2] + 1 and isinstance(request, DisplayRequestPacket)
                    and request.red_blink and not request.green_blink
                ]
                self.assertEqual(bool(faults), condition == "invalid_weight")

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
