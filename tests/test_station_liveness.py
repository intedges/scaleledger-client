"""Run the real station loop against a time-driven, multi-vehicle device.

These scenarios deliberately do not prime the cycle or call state methods by
hand: polling, voice waits, exceptions and reconnects all use run(). The fake
clock replaces waiting only, leaving the worker's transitions untouched.
"""

from concurrent.futures import Future
from decimal import Decimal
import unittest
from unittest.mock import Mock, patch

import serial

from events import RFIDTaggedEvent, WeighingCompletedEvent
from suwol1000 import (
    DisplayRequestPacket, PrinterRequestPacket, ResponsePacket, VoiceCode,
    WeighingStationWorker, WorkerState,
)


class SimulatedClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now = round(self.now + seconds, 6)


class BoundedSimulationStop:
    def __init__(self, clock, deadline):
        self.clock = clock
        self.deadline = deadline
        self.stopped = False

    def is_set(self):
        return self.stopped or self.clock.now >= self.deadline

    def set(self):
        self.stopped = True

    def wait(self, seconds):
        self.clock.advance(max(seconds, 0.01))
        return self.is_set()


class ContinuousScale:
    port = "continuous-liveness-simulation"

    def __init__(self, clock, *, missing_voices=(), busy_seconds=0.8,
                 fault=None, jitter=False, repeat_tags=True):
        self.clock = clock
        self.missing_voices = set(missing_voices)
        self.busy_seconds = busy_seconds
        self.fault = fault
        self.fault_used = False
        self.jitter = jitter
        self.repeat_tags = repeat_tags
        self.tagged_visits = set()
        self.active_voice = VoiceCode.NONE
        self.voice_until = 0.0
        self.requests = []
        self.printed = []
        self.connect_calls = 0
        self.worker = None

    def connect(self):
        self.connect_calls += 1

    def disconnect(self):
        pass

    def interrupt(self):
        pass

    def visit_at(self, at=None):
        at = self.clock.now if at is None else at
        if 6 <= at < 40:
            return 1
        if 46 <= at < 80:
            return 2
        return 0

    def send_and_receive(self, request):
        self.clock.advance(0.1)
        visit = self.visit_at()
        self.requests.append((self.clock.now, visit, self.worker.state, request))
        if self.fault and not self.fault_used and self.fault(request, self):
            self.fault_used = True
            raise serial.SerialException("one simulated device disconnect")
        if isinstance(request, PrinterRequestPacket):
            self.printed.append((visit, request.document_bytes))
        voice = getattr(request, "voice_code", VoiceCode.NONE)
        if voice != VoiceCode.NONE:
            self.active_voice = voice
            self.voice_until = self.clock.now + self.busy_seconds
        current_voice = (
            self.active_voice
            if self.clock.now < self.voice_until
            and self.active_voice not in self.missing_voices
            else VoiceCode.NONE
        )
        weight = Decimal("120") if visit else Decimal("0")
        if self.jitter and 9.0 <= self.clock.now < 10.0:
            weight = Decimal("50")
        # A driver tags whenever the reader advertises readiness, then repeats
        # the tag through processing/exit. The worker, not this device, decides
        # which tags to consume. Distinct visit IDs expose delayed-card mistakes.
        card = "00000000"
        ready_lamp = (
            isinstance(request, DisplayRequestPacket)
            and request.green_blink and not request.red_blink
        )
        tag = visit and (
            (ready_lamp and visit not in self.tagged_visits)
            or (self.repeat_tags and visit in self.tagged_visits)
        )
        if tag:
            self.tagged_visits.add(visit)
            card = "12345678" if visit == 1 else "87654321"
        return ResponsePacket(
            weight_value=weight, rfid_card_uid=card, voice_code=current_voice,
        )


class StationLivenessTests(unittest.TestCase):
    def simulate(self, *, missing_voices=(), busy_seconds=0.8, fault=None,
                 jitter=False, fail_first_save=False, save_delay=0.0,
                 rejected_first_card=False, repeat_tags=True):
        clock = SimulatedClock()
        scale = ContinuousScale(
            clock, missing_voices=missing_voices, busy_seconds=busy_seconds,
            fault=fault, jitter=jitter, repeat_tags=repeat_tags,
        )
        submissions = []
        accepted = []
        pending = []

        def on_event(event):
            if isinstance(event, RFIDTaggedEvent):
                accepted.append((clock.now, scale.visit_at(), event))
                return None
            submissions.append((clock.now, scale.visit_at(), event))
            result = Future()
            if fail_first_save and len(submissions) == 1:
                result.set_exception(OSError("simulated full disk, recovered before retry"))
            elif save_delay:
                pending.append((clock.now + save_delay, result))
            else:
                result.set_result(None)
            return result

        original_send = scale.send_and_receive

        def send(request):
            response = original_send(request)
            for due, result in pending:
                if due <= clock.now and not result.done():
                    result.set_result(None)
            return response

        scale.send_and_receive = send
        checks = 0

        def validator(event):
            nonlocal checks
            checks += 1
            return not (rejected_first_card and checks == 1)

        with patch("suwol1000.time.monotonic", clock), patch(
            "suwol1000.get_logger", return_value=Mock(),
        ):
            worker = WeighingStationWorker(
                scale, on_event=on_event, rfid_validator=validator,
                receipt_builder=lambda event: event.uuid.encode(),
                polling_interval=0.1, retry_interval=0.2,
                vehicle_confirm_seconds=3,
                voice_timeout=2,
            )
            worker.stop_event = BoundedSimulationStop(clock, 88)
            scale.worker = worker
            worker.run()
        self.assertLess(len(scale.requests), 2000, "simulation failed to make time progress")
        self.assertTrue(all(isinstance(item[2], WeighingCompletedEvent)
                            for item in submissions))
        return scale, submissions, accepted

    def assert_two_visits_once(self, submissions):
        by_visit = {}
        for at, visit, event in submissions:
            self.assertIn(visit, (1, 2), f"record created without a vehicle at {at}")
            by_visit.setdefault(visit, set()).add(event.uuid)
            expected_card = "12345678" if visit == 1 else "87654321"
            self.assertEqual(event.rfid_card_uid, expected_card)
        self.assertEqual(set(by_visit), {1, 2}, "a valid vehicle could not finish weighing")
        self.assertEqual([len(by_visit[visit]) for visit in (1, 2)], [1, 1])
        self.assertNotEqual(by_visit[1], by_visit[2])

    def test_real_run_loop_handles_two_vehicles_and_repeated_tags(self):
        scale, submissions, accepted = self.simulate()
        self.assert_two_visits_once(submissions)
        self.assertEqual(len(accepted), 2)
        self.assertEqual(len(scale.printed), 2)

    def test_busy_voice_ends_and_both_vehicles_eventually_finish(self):
        scale, submissions, _ = self.simulate(busy_seconds=1.6)
        self.assert_two_visits_once(submissions)
        self.assertEqual(len(scale.printed), 2)

    def test_single_tag_during_ready_voice_finishes_each_vehicle(self):
        scale, submissions, accepted = self.simulate(repeat_tags=False)
        self.assert_two_visits_once(submissions)
        self.assertEqual(len(accepted), 2)
        self.assertEqual(len(scale.printed), 2)

    def test_ready_weight_jitter_requalifies_without_a_permanent_block(self):
        scale, submissions, _ = self.simulate(jitter=True)
        self.assert_two_visits_once(submissions)
        self.assertGreaterEqual(submissions[0][0], 13)
        self.assertEqual(len(scale.printed), 2)

    def test_ready_disconnect_requalifies_without_a_permanent_block(self):
        def fault(request, scale):
            return isinstance(request, DisplayRequestPacket) and request.green_blink

        scale, submissions, _ = self.simulate(fault=fault)
        self.assertTrue(scale.fault_used)
        self.assertGreater(scale.connect_calls, 1)
        self.assert_two_visits_once(submissions)

    def test_failed_save_retries_same_uuid_and_allows_the_next_vehicle(self):
        scale, submissions, _ = self.simulate(fail_first_save=True)
        self.assert_two_visits_once(submissions)
        self.assertEqual(len(submissions), 3)
        self.assertEqual(submissions[0][2].uuid, submissions[1][2].uuid)
        self.assertEqual(len(scale.printed), 2)

    def test_delayed_save_ignores_tags_then_allows_next_vehicle(self):
        scale, submissions, _ = self.simulate(save_delay=4)
        self.assert_two_visits_once(submissions)
        self.assertEqual(len(scale.printed), 2)

    def test_unregistered_card_can_be_corrected_without_departure(self):
        scale, submissions, accepted = self.simulate(rejected_first_card=True)
        self.assert_two_visits_once(submissions)
        self.assertEqual(len(accepted), 3)
        self.assertEqual(len(scale.printed), 2)

    def test_complete_disconnect_preserves_one_record_and_next_vehicle_progress(self):
        def fault(request, scale):
            return getattr(request, "voice_code", None) == VoiceCode.WEIGHT_COMPLETE

        scale, submissions, _ = self.simulate(fault=fault)
        self.assertTrue(scale.fault_used)
        self.assert_two_visits_once(submissions)
        self.assertEqual(len(scale.printed), 1)

    def test_printer_disconnect_preserves_one_record_and_next_vehicle_progress(self):
        scale, submissions, _ = self.simulate(
            fault=lambda request, scale: isinstance(request, PrinterRequestPacket),
        )
        self.assertTrue(scale.fault_used)
        self.assert_two_visits_once(submissions)
        self.assertEqual(len(scale.printed), 1)

    def test_missing_ready_voice_start_does_not_permanently_block_valid_cards(self):
        scale, submissions, _ = self.simulate(
            missing_voices={VoiceCode.TAG_CARD}, repeat_tags=False,
        )
        self.assertTrue(any(getattr(request, "voice_code", None) == VoiceCode.TAG_CARD
                            for _, _, _, request in scale.requests))
        self.assert_two_visits_once(submissions)

    def test_missing_wait_voice_start_does_not_permanently_block_valid_cards(self):
        _, submissions, _ = self.simulate(missing_voices={VoiceCode.PLEASE_WAIT})
        self.assert_two_visits_once(submissions)

    def test_unobserved_short_voices_do_not_permanently_block_valid_cards(self):
        _, submissions, _ = self.simulate(missing_voices=set(VoiceCode))
        self.assert_two_visits_once(submissions)


if __name__ == "__main__":
    unittest.main()
