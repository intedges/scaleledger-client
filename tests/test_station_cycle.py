from collections import deque
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
from weighing_cycle import CycleState


NO_CARD = "00000000"
FIRST_CARD = "12345678"
SECOND_CARD = "87654321"


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class CycleScale:
    """Echo each voice once, with explicit responses for arrival and tag races."""

    port = "vehicle-cycle-test"

    def __init__(self, clock):
        self.clock = clock
        self.weight = Decimal("120")
        self.card = NO_CARD
        self.requests = []
        self.times = []
        self.responses = deque()
        self.step = 0.0
        self.fixed_voice = None
        self.tag_when_ready = NO_CARD
        self.connect_calls = 0

    def connect(self):
        self.connect_calls += 1

    def disconnect(self):
        pass

    def interrupt(self):
        pass

    def queue(self, *, weight="120", card=NO_CARD, voice=VoiceCode.NONE, advance=0.0):
        self.responses.append((advance, ResponsePacket(
            weight_value=Decimal(weight), rfid_card_uid=card, voice_code=voice,
        )))

    def send_and_receive(self, request):
        self.requests.append(request)
        self.times.append(self.clock.now)
        self.clock.now += self.step
        if self.responses:
            advance, response = self.responses.popleft()
            self.clock.now += advance
            if isinstance(response, Exception):
                raise response
            self.weight = response.weight_value
            return response
        card, self.card = self.card, NO_CARD
        if (
            isinstance(request, DisplayRequestPacket)
            and request.green_blink and not request.red_blink
            and self.tag_when_ready != NO_CARD
        ):
            card, self.tag_when_ready = self.tag_when_ready, NO_CARD
        voice = self.fixed_voice
        if voice is None:
            voice = getattr(request, "voice_code", VoiceCode.NONE)
        return ResponsePacket(weight_value=self.weight, rfid_card_uid=card, voice_code=voice)


class StationCycleTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.enterContext(patch("suwol1000.time.monotonic", self.clock))
        self.enterContext(patch("suwol1000.get_logger", return_value=Mock()))
        self.scale = CycleScale(self.clock)
        self.events = []
        self.result = Future()
        self.result.set_result(None)
        self.validator = Mock(return_value=True)
        self.worker = WeighingStationWorker(
            self.scale, station_id=7, on_event=self.submit, rfid_validator=self.validator,
            receipt_builder=lambda event: b"receipt", polling_interval=0,
            retry_interval=0, voice_timeout=1,
        )

    def submit(self, event):
        self.events.append(event)
        return self.result

    def invoke(self, name):
        self.worker.state = WorkerState[name.upper()]
        state = getattr(self.worker, name)()
        self.worker.state = state
        return state

    def idle_at(self, *times):
        for at in times:
            self.clock.now = at
            self.assertEqual(self.invoke("idle"), WorkerState.IDLE)

    def observe(self, weight, at):
        self.clock.now = at
        self.scale.weight = Decimal(weight)
        return self.worker.exchange(DisplayRequestPacket())

    def sustain(self, weight):
        start = self.clock.now + 1
        for offset in range(4):
            self.observe(weight, start + offset)

    def ready(self):
        self.sustain("0")
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
        self.sustain("120")
        self.assertEqual(self.worker.cycle.state, CycleState.READY)

    def accept(self, card=FIRST_CARD):
        self.scale.card = card
        self.assertEqual(self.invoke("idle"), WorkerState.VERIFY)
        self.assertEqual(self.worker.cycle.state, CycleState.RESERVED)

    def measure(self):
        self.assertEqual(self.invoke("verify"), WorkerState.MEASURE)
        self.assertEqual(self.invoke("measure"), WorkerState.SAVE)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)

    def finish(self):
        self.assertEqual(self.invoke("save"), WorkerState.PRINT)
        self.assertEqual(self.invoke("print"), WorkerState.IDLE)

    def measurements(self):
        return [event for event in self.events if isinstance(event, WeighingCompletedEvent)]

    def prints(self):
        return sum(isinstance(request, PrinterRequestPacket) for request in self.scale.requests)

    def voices(self, code):
        return [request for request in self.scale.requests
                if isinstance(request, DisplayRequestPacket) and request.voice_code == code]

    def voice_times(self, code):
        return [at for at, request in zip(self.scale.times, self.scale.requests)
                if isinstance(request, DisplayRequestPacket) and request.voice_code == code]

    def commands_since(self, begin):
        return [
            "print" if isinstance(request, PrinterRequestPacket) else request.voice_code
            for request in self.scale.requests[begin:]
            if isinstance(request, PrinterRequestPacket) or request.voice_code != VoiceCode.NONE
        ]

    def lights(self):
        request = self.worker._display_request()
        return request.green_blink, request.red_blink

    def test_starting_under_a_vehicle_requires_an_observed_empty_platform(self):
        self.sustain("120")
        self.scale.card = FIRST_CARD
        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_EMPTY)
        self.assertEqual(self.lights(), (False, False))
        self.assertFalse(self.events)

        self.sustain("50")
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
        self.sustain("50")
        self.scale.card = FIRST_CARD
        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        self.assertFalse(self.events)
        self.sustain("50.1")
        self.accept()

    def test_entry_needs_one_second_and_the_card_prompt_repeats_every_five_seconds(self):
        self.sustain("0")
        start = self.clock.now + 1
        for offset in (0, 0.5, 0.99):
            self.observe("120", start + offset)
        self.scale.card = FIRST_CARD
        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        self.assertFalse(self.events)
        self.assertEqual(self.lights(), (False, False))
        self.assertFalse(self.voices(VoiceCode.TAG_CARD))

        self.observe("120", start + 1)
        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.idle_at(*(start + offset for offset in (1, 2, 3, 4, 5, 5.99, 6, 7, 8, 9, 10, 11)))
        self.assertEqual(self.lights(), (True, False))
        self.assertEqual(self.voice_times(VoiceCode.TAG_CARD), [start + 1, start + 6, start + 11])

        self.clock.now = start + 12
        self.accept()
        self.measure()
        self.finish()
        self.idle_at(start + 13, start + 15, start + 16, start + 17, start + 18, start + 21)
        self.assertEqual(self.voice_times(VoiceCode.TAG_CARD), [start + 1, start + 6, start + 11])

    def test_card_tag_prints_right_away_with_the_weight_of_that_response(self):
        self.ready()
        begin = len(self.scale.requests)
        # The prompt carries the card, and its playback continues through printing.
        self.scale.queue(weight="123.7", card=FIRST_CARD, voice=VoiceCode.TAG_CARD)
        self.scale.fixed_voice = VoiceCode.TAG_CARD

        self.assertEqual(self.invoke("idle"), WorkerState.VERIFY)
        prompt = self.scale.requests[begin]
        self.assertEqual(prompt.voice_code, VoiceCode.TAG_CARD)
        self.assertEqual((prompt.green_blink, prompt.red_blink), (True, False))
        self.assertEqual(self.lights(), (True, True))
        self.measure()
        self.finish()

        self.assertEqual(
            [(event.rfid_card_uid, event.weight) for event in self.measurements()],
            [(FIRST_CARD, 123)],
        )
        self.assertEqual(len(self.scale.requests), begin + 2)
        self.assertIsInstance(self.scale.requests[begin + 1], PrinterRequestPacket)
        self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
        self.assertEqual(self.lights(), (False, False))

    def test_early_tags_are_ignored_without_guidance(self):
        self.sustain("0")
        self.observe("120", self.clock.now + 1)
        self.scale.card = FIRST_CARD

        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)

        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
        self.assertFalse(self.events)
        self.assertEqual(self.commands_since(0), [])
        self.assertEqual(self.lights(), (False, False))
        # The tag is not kept for the ready light; the driver tags again then.
        self.observe("120", self.clock.now + 1)
        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        self.assertFalse(self.events)
        self.accept(SECOND_CARD)
        self.assertEqual(self.worker.last_plate, SECOND_CARD)

    def test_card_prompt_stops_when_the_vehicle_leaves_and_restarts_on_the_next_entry(self):
        self.ready()
        start = self.clock.now
        self.idle_at(start)
        self.observe("50", start + 1)
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)

        self.idle_at(*(start + offset for offset in range(1, 13)))

        self.assertEqual(self.voice_times(VoiceCode.TAG_CARD), [start])
        self.sustain("120")
        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.idle_at(self.clock.now)
        self.assertEqual(len(self.voices(VoiceCode.TAG_CARD)), 2)
        self.assertFalse(self.measurements())

    def test_cards_are_accepted_and_printed_while_the_speaker_stays_busy(self):
        self.ready()
        start = self.clock.now
        self.idle_at(start)
        self.scale.fixed_voice = VoiceCode.TAG_CARD
        self.scale.card = FIRST_CARD
        self.clock.now = start + 0.5

        self.assertEqual(self.invoke("idle"), WorkerState.VERIFY)
        self.measure()
        self.finish()
        self.idle_at(*(start + offset for offset in range(1, 12)))

        self.assertEqual(self.prints(), 1)
        self.assertEqual(self.commands_since(0), [VoiceCode.TAG_CARD, "print"])
        self.worker.logger.warning.assert_called_once_with(
            "hw.speaker.timeout", voice_code="TAG_CARD", reason="still_busy",
            action="continue_weighing_suppress_voice_until_busy_then_idle",
        )

    def test_unregistered_card_keeps_red_until_its_notice_ends_then_accepts_another_card(self):
        self.ready()
        self.accept()
        # The card arrived during the prompt, which keeps playing for a while.
        self.scale.fixed_voice = VoiceCode.TAG_CARD
        self.validator.return_value = False
        rejected_at = self.clock.now

        self.assertEqual(self.invoke("verify"), WorkerState.IDLE)

        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.assertEqual(self.lights(), (False, True))
        self.idle_at(rejected_at + 1)
        self.assertFalse(self.voices(VoiceCode.UNREGISTERED_CARD))
        self.scale.fixed_voice = None
        self.idle_at(rejected_at + 1.5, rejected_at + 1.5)
        notice = self.voices(VoiceCode.UNREGISTERED_CARD)
        self.assertEqual(len(notice), 1)
        self.assertEqual((notice[0].green_blink, notice[0].red_blink), (False, True))
        # Red alone never accepts a card, even after the first two seconds.
        self.scale.fixed_voice = VoiceCode.UNREGISTERED_CARD
        self.scale.card = SECOND_CARD
        self.idle_at(rejected_at + 3)
        self.assertEqual(self.lights(), (False, True))
        self.assertEqual(len([event for event in self.events if isinstance(event, RFIDTaggedEvent)]), 1)

        self.scale.fixed_voice = None
        self.idle_at(rejected_at + 3.5)
        self.assertEqual(self.lights(), (True, False))
        self.idle_at(rejected_at + 3.5)
        self.assertEqual(len(self.voices(VoiceCode.TAG_CARD)), 2)
        self.assertFalse(self.measurements())
        self.validator.return_value = True
        self.accept(SECOND_CARD)
        self.measure()
        self.assertEqual([event.rfid_card_uid for event in self.measurements()], [SECOND_CARD])

    def test_unregistered_card_red_ends_after_five_seconds_if_the_notice_cannot_play(self):
        self.ready()
        self.accept()
        self.scale.fixed_voice = VoiceCode.TAG_CARD
        self.validator.return_value = False
        rejected_at = self.clock.now
        self.assertEqual(self.invoke("verify"), WorkerState.IDLE)

        self.idle_at(rejected_at + 2, rejected_at + 4, rejected_at + 4.99)
        self.assertEqual(self.lights(), (False, True))
        self.idle_at(rejected_at + 5)

        self.assertEqual(self.lights(), (True, False))
        self.assertFalse(self.voices(VoiceCode.UNREGISTERED_CARD))
        self.scale.card = SECOND_CARD
        self.assertEqual(self.invoke("idle"), WorkerState.VERIFY)
        self.assertEqual(self.worker.last_plate, SECOND_CARD)

    def test_one_record_and_receipt_per_vehicle_until_three_seconds_at_or_below_limit(self):
        self.ready()
        self.accept()
        self.measure()
        self.finish()
        self.assertEqual(self.lights(), (False, False))
        first = self.measurements()[0]

        for card in (FIRST_CARD, SECOND_CARD, FIRST_CARD, SECOND_CARD):
            self.scale.card = card
            self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        self.assertEqual(self.measurements(), [first])
        self.assertEqual(self.prints(), 1)
        self.assertEqual(len(self.voices(VoiceCode.WEIGHT_COMPLETE)), 1)

        start = self.clock.now + 1
        for offset in (0, 1, 2, 2.99):
            self.observe("50", start + offset)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.observe("50", start + 3)
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
        self.sustain("120")
        self.accept(SECOND_CARD)
        self.measure()
        self.finish()
        self.assertEqual(len(self.measurements()), 2)
        self.assertNotEqual(self.measurements()[0].uuid, self.measurements()[1].uuid)
        self.assertEqual(self.prints(), 2)

    def test_completion_repeats_every_five_seconds_until_departure_then_thanks_once(self):
        self.ready()
        self.accept()
        self.measure()
        self.finish()
        printed_at = self.clock.now

        self.idle_at(*(printed_at + offset for offset in (0, 1, 2, 3, 4, 4.99, 5, 6, 7, 8, 9, 10)))
        self.observe("50", printed_at + 11)
        self.idle_at(*(printed_at + offset for offset in range(11, 20)))

        self.assertEqual(
            self.voice_times(VoiceCode.WEIGHT_COMPLETE),
            [printed_at, printed_at + 5, printed_at + 10],
        )
        self.assertEqual(self.voice_times(VoiceCode.THANK_YOU), [printed_at + 11])
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
        self.assertEqual(self.lights(), (False, False))

    def test_departure_before_the_completion_notice_still_hears_it_then_thanks(self):
        self.ready()
        self.accept()
        self.scale.fixed_voice = VoiceCode.TAG_CARD
        self.measure()
        self.finish()
        begin = len(self.scale.requests)
        self.observe("50", self.clock.now + 0.5)
        self.scale.fixed_voice = None

        for _ in range(6):
            self.assertEqual(self.invoke("idle"), WorkerState.IDLE)

        self.assertEqual(self.commands_since(begin), [VoiceCode.WEIGHT_COMPLETE, VoiceCode.THANK_YOU])
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)

    def test_completion_reminders_stop_when_the_weight_becomes_unreadable(self):
        self.ready()
        self.accept()
        self.measure()
        self.finish()
        printed_at = self.clock.now
        self.idle_at(printed_at)
        # The indicator stops reporting a usable weight while the vehicle drives off.
        self.scale.weight = Decimal("NaN")
        self.idle_at(*(printed_at + offset for offset in range(1, 12)))
        self.assertEqual(self.lights(), (False, True))
        self.scale.weight = Decimal("0")
        self.idle_at(*(printed_at + offset for offset in range(12, 20)))

        self.assertEqual(self.voice_times(VoiceCode.WEIGHT_COMPLETE), [printed_at])
        self.assertFalse(self.voices(VoiceCode.THANK_YOU))
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)

    def test_reconnect_does_not_remind_a_vehicle_that_was_not_weighed(self):
        self.ready()
        self.accept()
        self.measure()
        self.finish()
        printed_at = self.clock.now
        self.idle_at(printed_at)
        self.scale.responses.append((0, serial.SerialException("disconnected")))
        with self.assertRaises(serial.SerialException):
            self.idle_at(printed_at + 1)

        # After a long outage another vehicle stands on the platform.
        self.clock.now = printed_at + 600
        self.assertEqual(self.invoke("connect"), WorkerState.IDLE)
        self.idle_at(*(printed_at + 600 + offset for offset in range(12)))

        self.assertEqual(self.voice_times(VoiceCode.WEIGHT_COMPLETE), [printed_at])
        self.assertFalse(self.voices(VoiceCode.THANK_YOU))
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.assertEqual(self.lights(), (False, False))

    def test_next_vehicle_ready_drops_guidance_left_from_the_previous_one(self):
        self.ready()
        self.accept()
        self.measure()
        self.finish()
        self.idle_at(self.clock.now)
        # A long completion notice outlasts the departure and the next entry.
        self.scale.fixed_voice = VoiceCode.WEIGHT_COMPLETE
        self.sustain("0")
        self.sustain("120")
        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.scale.fixed_voice = None
        begin = len(self.scale.requests)

        for _ in range(4):
            self.assertEqual(self.invoke("idle"), WorkerState.IDLE)

        self.assertEqual(self.commands_since(begin), [VoiceCode.TAG_CARD])
        self.assertFalse(self.voices(VoiceCode.THANK_YOU))

    def test_processing_lights_cover_verification_save_and_print(self):
        self.ready()
        self.accept()
        self.assertEqual(self.lights(), (True, True))
        self.result = Future()
        self.measure()
        begin = len(self.scale.requests)
        self.assertEqual(self.invoke("save"), WorkerState.SAVE)
        self.assertEqual(self.invoke("save"), WorkerState.SAVE)
        self.assertEqual(self.lights(), (True, True))
        waiting = self.scale.requests[begin:]
        self.assertEqual(len(waiting), 2)
        self.assertTrue(all(
            (request.green_blink, request.red_blink, request.voice_code) == (True, True, VoiceCode.NONE)
            for request in waiting
        ))
        self.result.set_result(None)
        self.finish()
        self.assertIsInstance(self.scale.requests[-1], PrinterRequestPacket)

        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        notice = self.scale.requests[-1]
        self.assertEqual(notice.voice_code, VoiceCode.WEIGHT_COMPLETE)
        self.assertEqual((notice.green_blink, notice.red_blink), (False, False))

    def test_failed_save_keeps_red_and_the_original_event_when_new_cards_arrive(self):
        self.ready()
        self.accept()
        self.result = Future()
        self.measure()
        original = self.measurements()[0]
        self.result.set_exception(OSError("disk full"))
        self.scale.card = SECOND_CARD

        self.assertEqual(self.invoke("save"), WorkerState.SAVE)
        self.assertEqual(self.invoke("save"), WorkerState.SAVE)

        self.assertEqual(self.lights(), (False, True))
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.assertEqual(self.measurements(), [original])
        self.assertIs(self.worker._pending_save, original)
        notices = self.voices(VoiceCode.CHECK_ADMIN)
        self.assertEqual(len(notices), 1)
        self.assertEqual((notices[0].green_blink, notices[0].red_blink), (False, True))
        self.assertFalse(self.voices(VoiceCode.WEIGHT_COMPLETE))
        self.assertEqual(self.prints(), 0)

    def test_unobserved_voice_start_suppresses_guidance_but_not_weighing(self):
        self.ready()
        self.scale.fixed_voice = VoiceCode.NONE
        start = self.clock.now

        self.idle_at(*(start + offset for offset in (0, 0.5, 1, 1.5)))

        self.worker.logger.warning.assert_called_once_with(
            "hw.speaker.timeout", voice_code="TAG_CARD", reason="start_unobserved",
            action="continue_weighing_suppress_voice_until_busy_then_idle",
        )
        self.idle_at(*(start + offset for offset in (3, 5, 6, 7)))
        self.assertEqual(len(self.voices(VoiceCode.TAG_CARD)), 1)
        sample = self.worker.get_live_weight()
        self.assertTrue(sample.sample_valid)
        self.assertEqual(sample.connection_status, "connected")
        self.assertFalse(self.worker._indicator_fault)
        self.accept()
        self.measure()
        self.finish()
        self.idle_at(start + 8)
        self.assertEqual(self.prints(), 1)
        self.assertEqual(self.commands_since(0), [VoiceCode.TAG_CARD, "print"])

    def test_lost_prompt_response_is_not_resent_after_requalifying(self):
        self.ready()
        self.scale.responses.append((0, ValueError("truncated response")))

        # A corrupt packet remains a protocol fault, not a voice-only failure.
        with self.assertRaises(ValueError):
            self.invoke("idle")

        self.assertTrue(self.worker._indicator_fault)
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
        self.scale.fixed_voice = VoiceCode.NONE
        self.sustain("120")
        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.idle_at(self.clock.now, self.clock.now + 1, self.clock.now + 2)
        self.assertEqual(len(self.voices(VoiceCode.TAG_CARD)), 1)
        self.worker.logger.warning.assert_called_once_with(
            "hw.speaker.playback_uncertain", voice_code="TAG_CARD",
            reason="response_lost", action="suppress_voice_until_busy_then_idle",
        )

    def test_saved_vehicle_departure_skips_stale_guidance_but_prints_original(self):
        self.ready()
        self.accept()
        self.result = Future()
        self.measure()
        original = self.measurements()[0]
        self.sustain("0")
        self.sustain("120")
        self.result.set_result(None)

        self.finish()
        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)

        self.assertEqual(self.measurements(), [original])
        self.assertEqual(self.prints(), 1)
        self.assertFalse(self.voices(VoiceCode.WEIGHT_COMPLETE))
        self.assertFalse(self.voices(VoiceCode.THANK_YOU))
        # The vehicle now on the platform is prompted instead.
        self.assertEqual(self.scale.requests[-1].voice_code, VoiceCode.TAG_CARD)

    def test_serial_reconnect_does_not_unlock_a_consumed_vehicle(self):
        self.ready()
        self.accept()
        self.measure()
        self.finish()
        self.scale.responses.append((0, serial.SerialException("disconnected")))
        with self.assertRaises(serial.SerialException):
            self.worker.exchange(DisplayRequestPacket())

        self.assertEqual(self.invoke("connect"), WorkerState.IDLE)
        self.scale.card = SECOND_CARD
        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)

        self.assertEqual(self.scale.connect_calls, 1)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.assertEqual(len(self.measurements()), 1)
        self.assertEqual(self.lights(), (False, False))

    def test_departure_while_saving_does_not_admit_a_second_card_before_completion(self):
        self.ready()
        self.accept()
        self.result = Future()
        self.measure()
        original = self.measurements()[0]
        self.sustain("0")
        self.sustain("120")
        self.scale.card = SECOND_CARD

        self.assertEqual(self.invoke("save"), WorkerState.SAVE)
        self.assertEqual(self.invoke("save"), WorkerState.SAVE)

        self.assertEqual(self.measurements(), [original])
        self.assertEqual(self.worker.last_plate, FIRST_CARD)
        self.assertEqual(self.lights(), (True, True))
        # The next vehicle is not prompted for a card it could not use yet.
        self.assertEqual(len(self.voices(VoiceCode.TAG_CARD)), 1)

    def test_first_green_after_printing_accepts_the_next_vehicles_card(self):
        self.ready()
        self.accept()
        self.result = Future()
        self.measure()
        self.sustain("0")
        self.sustain("120")
        self.result.set_result(None)
        self.assertEqual(self.invoke("save"), WorkerState.PRINT)
        # The next driver tags as soon as green alone signals that cards are accepted.
        self.scale.tag_when_ready = SECOND_CARD

        self.assertEqual(self.invoke("print"), WorkerState.IDLE)
        self.assertEqual(self.invoke("idle"), WorkerState.VERIFY)

        self.assertEqual(self.worker.last_plate, SECOND_CARD)
        self.assertEqual(self.worker.cycle.state, CycleState.RESERVED)
        self.assertEqual(len(self.measurements()), 1)


if __name__ == "__main__":
    unittest.main()
