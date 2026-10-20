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
            self.scale, on_event=self.submit, rfid_validator=self.validator,
            receipt_builder=lambda event: b"receipt", polling_interval=0,
            retry_interval=0,
            vehicle_confirm_seconds=3, voice_timeout=1,
        )

    def submit(self, event):
        self.events.append(event)
        return self.result

    def invoke(self, name):
        self.worker.state = WorkerState[name.upper()]
        state = getattr(self.worker, name)()
        self.worker.state = state
        return state

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
        self.assertEqual(self.invoke("save"), WorkerState.COMPLETE)
        self.assertEqual(self.invoke("complete"), WorkerState.PRINT)
        self.assertEqual(self.invoke("print"), WorkerState.IDLE)

    def measurements(self):
        return [event for event in self.events if isinstance(event, WeighingCompletedEvent)]

    def voices(self, code):
        return [request for request in self.scale.requests
                if isinstance(request, DisplayRequestPacket) and request.voice_code == code]

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

    def test_entry_needs_three_seconds_and_the_ready_prompt_is_not_repeated(self):
        self.sustain("0")
        start = self.clock.now + 1
        for offset in (0, 1, 2, 2.99):
            self.observe("120", start + offset)
        self.scale.card = FIRST_CARD
        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        self.assertFalse(self.events)
        self.assertEqual(self.lights(), (False, False))

        self.observe("120", start + 3)
        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        self.assertEqual(self.lights(), (True, False))
        self.assertEqual(len(self.voices(VoiceCode.TAG_CARD)), 1)
        for _ in range(4):
            self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        self.assertEqual(len(self.voices(VoiceCode.TAG_CARD)), 1)

    def test_first_card_during_the_ready_prompt_is_preserved_and_lights_change(self):
        self.ready()
        self.scale.queue()
        self.scale.queue(card=FIRST_CARD, voice=VoiceCode.TAG_CARD)
        self.scale.queue(card=SECOND_CARD)

        self.assertEqual(self.invoke("idle"), WorkerState.VERIFY)

        tags = [event for event in self.events if isinstance(event, RFIDTaggedEvent)]
        self.assertEqual([event.rfid_card_uid for event in tags], [FIRST_CARD])
        self.assertEqual(self.worker.last_plate, FIRST_CARD)
        self.assertEqual(self.worker.cycle.state, CycleState.RESERVED)
        prompt = self.voices(VoiceCode.TAG_CARD)
        self.assertEqual(len(prompt), 1)
        self.assertEqual((prompt[0].green_blink, prompt[0].red_blink), (True, False))
        final_poll = self.scale.requests[-1]
        self.assertEqual((final_poll.green_blink, final_poll.red_blink), (True, True))
        self.measure()
        self.assertEqual([event.rfid_card_uid for event in self.measurements()], [FIRST_CARD])

    def test_rejected_card_announcement_never_shows_ready_green_when_cards_are_ignored(self):
        self.sustain("0")
        self.observe("120", self.clock.now + 1)
        self.worker.voice_timeout = 10
        self.scale.queue(card=FIRST_CARD)
        self.scale.queue(voice=VoiceCode.STAND_BY, advance=1)
        self.scale.queue(voice=VoiceCode.STAND_BY, advance=1)
        self.scale.queue(voice=VoiceCode.STAND_BY, advance=1)
        self.scale.queue()
        begin = len(self.scale.requests)

        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)

        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.assertFalse(self.events)
        self.assertEqual(len(self.voices(VoiceCode.STAND_BY)), 1)
        self.assertTrue(all(
            not (request.green_blink and not request.red_blink)
            for request in self.scale.requests[begin:]
        ))
        # Once the announcement ends, the first true ready light accepts a fresh tag.
        self.scale.tag_when_ready = SECOND_CARD
        self.assertEqual(self.invoke("idle"), WorkerState.VERIFY)
        self.assertEqual(self.worker.last_plate, SECOND_CARD)

    def test_departure_during_ready_prompt_cancels_the_preserved_card(self):
        self.ready()
        self.scale.queue()
        self.scale.queue(card=FIRST_CARD, voice=VoiceCode.TAG_CARD)
        self.scale.queue(weight="50", voice=VoiceCode.TAG_CARD, advance=0.1)
        self.scale.queue(weight="120", advance=0.1)

        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        self.assertNotEqual(self.worker.cycle.state, CycleState.RESERVED)
        self.assertEqual(self.invoke("measure"), WorkerState.IDLE)
        self.assertFalse(self.measurements())

    def test_next_vehicle_card_is_not_lost_during_previous_ready_voice(self):
        self.ready()
        self.worker.voice_timeout = 20
        self.scale.step = 0.25
        self.scale.fixed_voice = VoiceCode.TAG_CARD
        original_sequence = self.worker.cycle.ready_sequence
        self.scale.queue()
        self.scale.queue(voice=VoiceCode.TAG_CARD)
        for _ in range(4):
            self.scale.queue(weight="0", voice=VoiceCode.TAG_CARD, advance=1)
        for _ in range(4):
            self.scale.queue(weight="120", voice=VoiceCode.TAG_CARD, advance=1)
        self.scale.tag_when_ready = SECOND_CARD

        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)

        self.assertGreater(self.worker.cycle.ready_sequence, original_sequence)
        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.assertEqual(self.scale.tag_when_ready, SECOND_CARD)
        self.assertFalse(self.events)
        self.assertEqual(len(self.voices(VoiceCode.TAG_CARD)), 1)

        self.assertEqual(self.invoke("idle"), WorkerState.VERIFY)
        self.assertEqual(self.worker.last_plate, SECOND_CARD)
        self.assertEqual(self.scale.tag_when_ready, NO_CARD)
        self.assertEqual(len(self.events), 1)

    def test_unregistered_card_shows_red_and_releases_the_same_vehicle_for_retry(self):
        self.ready()
        self.accept()
        self.validator.return_value = False

        self.assertEqual(self.invoke("verify"), WorkerState.IDLE)

        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        errors = self.voices(VoiceCode.UNREGISTERED_CARD)
        self.assertEqual(len(errors), 1)
        self.assertEqual((errors[0].green_blink, errors[0].red_blink), (False, True))
        self.assertEqual(self.lights(), (True, False))
        self.assertFalse(self.measurements())
        self.validator.return_value = True
        self.accept(SECOND_CARD)
        self.measure()
        self.assertEqual([event.rfid_card_uid for event in self.measurements()], [SECOND_CARD])

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
        self.assertEqual(sum(isinstance(request, PrinterRequestPacket)
                             for request in self.scale.requests), 1)
        self.assertLessEqual(len(self.voices(VoiceCode.WEIGHT_COMPLETE)), 2)

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
        self.assertEqual(sum(isinstance(request, PrinterRequestPacket)
                             for request in self.scale.requests), 2)

    def test_processing_lights_cover_verification_save_completion_and_print(self):
        self.ready()
        self.accept()
        self.assertEqual(self.lights(), (True, True))
        self.result = Future()
        self.measure()
        self.assertEqual(self.invoke("save"), WorkerState.SAVE)
        self.assertEqual(self.lights(), (True, True))
        waiting = self.voices(VoiceCode.PLEASE_WAIT)
        self.assertTrue(waiting)
        self.assertTrue(all((request.green_blink, request.red_blink) == (True, True)
                            for request in waiting))
        self.result.set_result(None)
        self.finish()
        for voice in (VoiceCode.WEIGHT_COMPLETE, VoiceCode.THANK_YOU):
            requests = self.voices(voice)
            self.assertEqual(len(requests), 1)
            self.assertEqual((requests[0].green_blink, requests[0].red_blink), (True, True))
        self.assertEqual(self.invoke("idle"), WorkerState.IDLE)
        off = self.scale.requests[-1]
        self.assertEqual((off.green_blink, off.red_blink), (False, False))

    def test_failed_save_keeps_red_and_the_original_event_when_new_cards_arrive(self):
        self.ready()
        self.accept()
        self.result = Future()
        self.measure()
        original = self.measurements()[0]
        self.result.set_exception(OSError("disk full"))
        self.scale.card = SECOND_CARD

        self.assertEqual(self.invoke("save"), WorkerState.SAVE)

        self.assertEqual(self.lights(), (False, True))
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.assertEqual(self.measurements(), [original])
        self.assertIs(self.worker._pending_save, original)
        self.assertFalse(self.voices(VoiceCode.WEIGHT_COMPLETE))
        self.assertFalse(any(isinstance(request, PrinterRequestPacket) for request in self.scale.requests))

    def test_unobserved_voice_has_a_deadline_without_invalidating_weight(self):
        self.scale.fixed_voice = VoiceCode.NONE
        self.scale.step = 0.25

        self.worker.poll_until_voice_ends(DisplayRequestPacket(voice_code=VoiceCode.TAG_CARD))

        self.assertLessEqual(self.clock.now, 1.5)
        self.assertEqual(len(self.voices(VoiceCode.TAG_CARD)), 1)
        self.assertLessEqual(len(self.scale.requests), 6)
        sample = self.worker.get_live_weight()
        self.assertTrue(sample.sample_valid)
        self.assertEqual(sample.connection_status, "connected")
        self.assertFalse(self.worker._indicator_fault)
        self.worker.logger.warning.assert_called_once_with(
            "hw.speaker.timeout", voice_code="TAG_CARD", reason="start_unobserved",
            action="continue_weighing_suppress_voice_until_busy_then_idle",
        )

    def test_lost_initial_voice_response_does_not_resend_the_command(self):
        self.worker.exchange(DisplayRequestPacket())
        self.scale.responses.append((0, ValueError("truncated response")))
        self.scale.fixed_voice = VoiceCode.NONE
        self.scale.step = 0.25

        # A corrupt packet remains a protocol fault, not a voice-only timeout.
        with self.assertRaises(ValueError):
            self.worker.poll_until_voice_ends(DisplayRequestPacket(voice_code=VoiceCode.TAG_CARD))

        self.assertTrue(self.worker._indicator_fault)
        self.worker.poll_until_voice_ends(DisplayRequestPacket(voice_code=VoiceCode.PLEASE_WAIT))
        self.assertEqual(len(self.voices(VoiceCode.TAG_CARD)), 1)
        self.assertFalse(self.voices(VoiceCode.PLEASE_WAIT))
        self.assertLessEqual(self.clock.now, 1.5)

    def test_busy_voice_timeout_preserves_saved_event_and_advances_to_print(self):
        self.ready()
        self.accept()
        self.measure()
        self.assertEqual(self.invoke("save"), WorkerState.COMPLETE)
        original = self.measurements()[0]
        self.scale.fixed_voice = VoiceCode.WEIGHT_COMPLETE
        self.scale.step = 0.25
        started = self.clock.now

        self.assertEqual(self.invoke("complete"), WorkerState.PRINT)

        self.assertLessEqual(self.clock.now - started, 1.5)
        self.assertEqual(len(self.voices(VoiceCode.WEIGHT_COMPLETE)), 1)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.assertEqual(self.measurements(), [original])
        self.assertFalse(any(isinstance(request, PrinterRequestPacket) for request in self.scale.requests))
        self.assertEqual(self.invoke("print"), WorkerState.IDLE)
        self.assertEqual(sum(isinstance(request, PrinterRequestPacket) for request in self.scale.requests), 1)
        self.assertFalse(self.voices(VoiceCode.THANK_YOU))

    def test_saved_vehicle_departure_skips_stale_guidance_but_prints_original(self):
        self.ready()
        self.accept()
        self.result = Future()
        self.measure()
        original = self.measurements()[0]
        self.sustain("0")
        self.sustain("120")
        self.result.set_result(None)

        self.assertEqual(self.invoke("save"), WorkerState.COMPLETE)
        self.assertEqual(self.invoke("complete"), WorkerState.PRINT)
        self.assertEqual(self.invoke("print"), WorkerState.IDLE)

        self.assertEqual(self.measurements(), [original])
        self.assertFalse(self.voices(VoiceCode.WEIGHT_COMPLETE))
        self.assertFalse(self.voices(VoiceCode.THANK_YOU))
        self.assertEqual(sum(isinstance(request, PrinterRequestPacket) for request in self.scale.requests), 1)

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

        self.assertEqual(self.measurements(), [original])
        self.assertEqual(self.worker.last_plate, FIRST_CARD)
        self.assertEqual(self.lights(), (True, True))

    def test_first_green_after_printing_accepts_the_next_vehicles_card(self):
        self.ready()
        self.accept()
        self.result = Future()
        self.measure()
        self.sustain("0")
        self.sustain("120")
        self.result.set_result(None)
        self.assertEqual(self.invoke("save"), WorkerState.COMPLETE)
        self.assertEqual(self.invoke("complete"), WorkerState.PRINT)
        # The next driver tags as soon as green alone signals that cards are accepted.
        self.scale.tag_when_ready = SECOND_CARD

        state = self.invoke("print")
        if state == WorkerState.IDLE:
            state = self.invoke("idle")

        self.assertEqual(state, WorkerState.VERIFY)
        self.assertEqual(self.worker.last_plate, SECOND_CARD)
        self.assertEqual(self.worker.cycle.state, CycleState.RESERVED)
        self.assertEqual(len(self.measurements()), 1)


if __name__ == "__main__":
    unittest.main()
