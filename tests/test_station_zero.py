"""Exercise zero requests through the real worker loop, without hardware waits."""

from concurrent.futures import Future
from dataclasses import replace
from decimal import Decimal
import unittest
from unittest.mock import Mock, patch

from events import RFIDTaggedEvent, WeighingCompletedEvent
from suwol1000 import (
    DeviceResponseTimeout, DisplayRequestPacket, ResponsePacket, VoiceCode,
    WeighingStationWorker, WeightStatus, WeightType, WorkerState, ZeroRequestError,
)
from weighing_cycle import CycleState
from tests.helpers import prepare_vehicle


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now = round(self.now + max(0.0, seconds), 6)


class SimulatedStop:
    def __init__(self, clock):
        self.clock = clock
        self.stopped = False
        self.after_wait = None

    def is_set(self):
        return self.stopped or self.clock.now >= 10

    def set(self):
        self.stopped = True

    def wait(self, seconds):
        if not self.stopped:
            self.clock.advance(max(0.01, seconds))
        if self.after_wait is not None:
            self.after_wait()
        return self.is_set()


class ZeroScale:
    port = "zero-test-port"
    timeout = 2.0
    write_timeout = 2.0

    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.connections = 0
        self.interruptions = 0
        self.after_on = None
        self.before_off = None
        self.on_error = None
        self.off_error = None
        self.on_feedback = True
        self.off_feedback = False
        self.after_weight = Decimal("0")
        self.after_status = WeightStatus.STABLE
        self.after_type = WeightType.NET
        # Default successful pulse: the ON reply observes sound 12 and the OFF
        # reply observes playback finished. Busy/unknown cases override these.
        self.on_voice = VoiceCode.ALERT_SOUND
        self.off_voice = VoiceCode.NONE
        self.pulse_started = False
        self.off_attempts = 0

    def connect(self):
        self.connections += 1

    def disconnect(self):
        pass

    def interrupt(self):
        self.interruptions += 1

    def send_and_receive(self, request, *, deadline=None):
        self.calls.append((self.clock.now, self.connections, request, deadline))
        self.clock.advance(0.089)
        if request.zero_output:
            self.pulse_started = True
            if self.after_on is not None:
                self.after_on()
            if self.on_error is not None:
                self.clock.advance(max(0, deadline - self.clock.now))
                raise self.on_error
            return ResponsePacket(
                zero_output_on=self.on_feedback, weight_value=Decimal("-990"),
                rfid_card_uid="12345678", voice_code=self.on_voice,
            )
        if self.pulse_started:
            self.off_attempts += 1
            if self.before_off is not None:
                self.before_off()
            if self.off_error is not None:
                error, self.off_error = self.off_error, None
                raise error
            return ResponsePacket(
                zero_output_on=self.off_feedback, weight_value=self.after_weight,
                weight_status=self.after_status, weight_type=self.after_type,
                rfid_card_uid="87654321", voice_code=self.off_voice,
            )
        return ResponsePacket(weight_value=Decimal("-990"))


class StationZeroTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.enterContext(patch("suwol1000.time.monotonic", self.clock))
        self.enterContext(patch("suwol1000.get_logger", return_value=Mock()))
        self.reset_station()

    def reset_station(self):
        self.clock.now = 0.0
        self.scale = ZeroScale(self.clock)
        self.events = Mock()
        self.worker = WeighingStationWorker(
            self.scale, station_id=7, on_event=self.events,
            polling_interval=0.1, retry_interval=0.1,
        )
        self.stop = SimulatedStop(self.clock)
        self.worker.stop_event = self.stop
        self.future = None
        self.completed_state = None

    def run_request(self, *, before_request=None, on_result=None, stop_on_result=True):
        def completed(future):
            self.completed_state = self.worker.cycle.state
            if on_result is not None:
                on_result(future)
            if stop_on_result and not self.stop.is_set():
                self.worker.stop()

        def enqueue():
            if self.future is None:
                if before_request is not None:
                    before_request()
                self.future = self.worker.request_zero()
                self.future.add_done_callback(completed)

        self.stop.after_wait = enqueue
        self.worker.run()
        self.assertIsNotNone(self.future)
        self.assertTrue(self.future.done())
        return self.future

    def prime(self):
        self.worker.state = WorkerState.IDLE
        self.worker.exchange(DisplayRequestPacket())

    def prepare_card(self, state=WorkerState.MEASURE):
        self.prime()
        self.worker.last_weight = Decimal("120")
        self.worker._telemetry = replace(self.worker.get_telemetry(), weight=Decimal("120"))
        prepare_vehicle(self.worker)
        self.worker.state = state
        self.worker._processing = True
        self.worker.last_plate = "12345678"
        self.worker.last_event = RFIDTaggedEvent(rfid_card_uid="12345678")
        return self.worker.last_event

    def execute_queued_zero(self):
        self.future = self.worker.request_zero()
        self.assertFalse(self.future.done())
        self.assertTrue(self.worker._run_zero_request())
        return self.future.result()

    def test_loop_sends_one_on_then_off_after_one_point_five_seconds(self):
        result = self.run_request().result()
        pulse_calls = [call for call in self.scale.calls if call[3] is not None]
        self.assertEqual(len(pulse_calls), 2)
        on, off = pulse_calls
        self.assertTrue(on[2].zero_output)
        self.assertFalse(off[2].zero_output)
        self.assertAlmostEqual(off[0] - on[0], 1.5)
        self.assertAlmostEqual(on[3] - on[0], 1.5)
        self.assertAlmostEqual(off[3] - off[0], 2.0)
        self.assertTrue(on[2].green_blink and on[2].red_blink)
        self.assertEqual(on[2].voice_code, VoiceCode.ALERT_SOUND)
        self.assertEqual(off[2].voice_code, VoiceCode.NONE)
        self.assertEqual(on[2].display_weight, Decimal("-990"))
        self.assertEqual(result.weight_before, Decimal("-990"))
        self.assertEqual(result.weight_after, Decimal("0"))
        self.assertTrue(result.output_released)
        self.assertTrue(result.zero_observed)
        self.assertIsNone(result.error)
        self.events.assert_not_called()

    def test_output_release_is_separate_from_zero_weight_observation(self):
        for weight, status, kind in (
            (Decimal("10"), WeightStatus.STABLE, WeightType.NET),
            (Decimal("0"), WeightStatus.UNSTABLE, WeightType.NET),
            (Decimal("0"), WeightStatus.STABLE, WeightType.GROSS),
        ):
            with self.subTest(weight=weight, status=status, kind=kind):
                self.reset_station()
                self.scale.after_weight, self.scale.after_status, self.scale.after_type = weight, status, kind
                result = self.run_request().result()
                self.assertTrue(result.output_released)
                self.assertFalse(result.zero_observed)
                self.assertEqual(result.weight_after, weight if kind == WeightType.NET else None)

    def test_duplicate_request_shares_running_future_and_cannot_cancel_output(self):
        def during_on():
            self.assertIs(self.worker.request_zero(), self.future)
            self.assertFalse(self.future.cancel())

        self.scale.after_on = during_on
        self.run_request().result()
        self.assertEqual(sum(call[2].zero_output for call in self.scale.calls), 1)
        self.assertEqual(self.scale.off_attempts, 1)

    def test_cancelled_pending_request_never_outputs_on(self):
        self.prime()
        future = self.worker.request_zero()
        self.assertTrue(future.cancel())
        self.worker._run_zero_request()
        self.assertTrue(future.cancelled())
        self.assertFalse(any(call[2].zero_output for call in self.scale.calls))
        self.assertIsNone(self.worker._zero_request)

    def test_stop_resolves_pending_request_without_output(self):
        self.prime()
        future = self.worker.request_zero()
        future.add_done_callback(lambda _: self.worker.request_zero())
        self.worker.stop()
        with self.assertRaises(ZeroRequestError) as caught:
            future.result()
        self.assertEqual(caught.exception.reason, "stopped")
        self.assertFalse(any(call[2].zero_output for call in self.scale.calls))

    def test_busy_or_invalid_weight_requests_are_rejected_before_output(self):
        self.prime()
        for state in (WorkerState.SAVE, WorkerState.PRINT):
            with self.subTest(state=state):
                self.worker.state = state
                self.assertEqual(self.worker.request_zero().exception().reason, "busy")
        self.worker.state = WorkerState.IDLE
        valid = self.worker.get_telemetry()
        for changes in (
            {"weight_status": WeightStatus.UNSTABLE},
            {"weight_type": WeightType.TARE},
            {"weight_valid": False},
            {"sampled_monotonic": -10},
        ):
            with self.subTest(changes=changes):
                self.worker._telemetry = replace(valid, **changes)
                self.assertEqual(self.worker.request_zero().exception().reason, "invalid_weight")
        self.assertFalse(any(call[2].zero_output for call in self.scale.calls))

    def test_worker_rechecks_state_and_weight_before_executing_pending_request(self):
        self.prime()
        future = self.worker.request_zero()
        self.worker.state = WorkerState.SAVE
        self.worker._run_zero_request()
        self.assertEqual(future.exception().reason, "busy")
        self.worker.state = WorkerState.IDLE
        future = self.worker.request_zero()
        self.worker._telemetry = replace(self.worker.get_telemetry(), weight=Decimal("120"))
        self.worker._run_zero_request()
        self.assertEqual(future.exception().reason, "weight_changed")
        self.assertFalse(any(call[2].zero_output for call in self.scale.calls))

    def test_missing_on_response_still_sends_off_without_retrying_on(self):
        self.scale.on_error = DeviceResponseTimeout("ON reply lost")
        result = self.run_request().result()
        self.assertEqual(result.error, "on_failed")
        self.assertTrue(result.output_released)
        self.assertEqual(sum(call[2].zero_output for call in self.scale.calls), 1)
        self.assertEqual(self.scale.off_attempts, 1)

    def test_poll_failure_after_request_rejects_it_instead_of_replaying_on_reconnect(self):
        original_exchange = self.scale.send_and_receive
        polls = 0

        def exchange(request, *, deadline=None):
            nonlocal polls
            polls += 1
            if polls == 2:
                self.future = self.worker.request_zero()
                self.assertFalse(self.future.done())
                self.future.add_done_callback(lambda _: self.worker.stop())
                raise DeviceResponseTimeout("poll failed after the request arrived")
            return original_exchange(request, deadline=deadline)

        self.scale.send_and_receive = exchange
        self.worker.run()

        self.assertIsInstance(self.future.exception(), ZeroRequestError)
        self.assertEqual(self.future.exception().reason, "busy")
        self.assertFalse(any(call[2].zero_output for call in self.scale.calls))

    def test_active_or_unknown_off_feedback_does_not_release_card_interlock(self):
        for feedback in (True, None):
            with self.subTest(feedback=feedback):
                self.reset_station()
                self.scale.off_feedback = feedback
                result = self.run_request().result()
                self.assertFalse(result.output_released)
                self.assertFalse(result.zero_observed)
                self.assertEqual(result.error, "off_unconfirmed")
                self.assertTrue(self.worker._zero_release_pending)

    def test_unconfirmed_on_feedback_releases_output_immediately(self):
        self.scale.on_feedback = None
        result = self.run_request().result()
        pulse_calls = [call for call in self.scale.calls if call[3] is not None]
        self.assertLess(pulse_calls[1][0] - pulse_calls[0][0], 1.5)
        self.assertEqual(result.error, "on_unconfirmed")
        self.assertTrue(result.output_released)

    def test_stop_during_on_or_off_does_not_cancel_the_cleanup_exchange(self):
        for stage in ("after_on", "before_off"):
            with self.subTest(stage=stage):
                self.reset_station()
                setattr(self.scale, stage, lambda: (self.worker.stop(), self.worker.stop()))
                result = self.run_request().result()
                self.assertEqual(result.error, "stopped")
                self.assertTrue(result.output_released)
                self.assertEqual(self.scale.off_attempts, 1)
                self.assertEqual(self.scale.interruptions, 0)

    def test_off_failure_blocks_cards_until_off_is_confirmed_after_reconnect(self):
        self.scale.off_error = DeviceResponseTimeout("OFF reply lost")
        seen = []

        def result_received(future):
            result = future.result()
            self.assertFalse(result.output_released)
            self.assertEqual(result.error, "off_failed")
            self.assertTrue(self.worker._zero_release_pending)
            self.assertFalse(self.worker._accept_card(ResponsePacket(rfid_card_uid="12345678")))
            self.assertEqual(self.worker.request_zero().exception().reason, "release_pending")

        def after_off():
            if self.scale.connections > 1:
                seen.append(self.worker._zero_release_pending)

        self.scale.before_off = after_off
        self.run_request(on_result=result_received, stop_on_result=False)
        self.assertTrue(seen)
        self.assertTrue(seen[0])
        self.assertFalse(self.worker._zero_release_pending)
        self.assertGreaterEqual(self.scale.connections, 2)
        self.assertEqual(sum(call[2].zero_output for call in self.scale.calls), 1)
        self.events.assert_not_called()
        reconnect_calls = [call for call in self.scale.calls if call[1] > 1]
        self.assertFalse(reconnect_calls[0][2].zero_output)
        self.assertEqual(reconnect_calls[0][2].voice_code, VoiceCode.NONE)

    def test_explicit_zero_resets_consumed_visit_without_departure_or_card_reuse(self):
        def lock_visit():
            self.worker.cycle.state = CycleState.LOCKED
            self.worker._weighed_vehicle_on = True
            self.worker._weighed_invalidations = self.worker.cycle.invalidations
            self.worker._completion_announced = True

        self.run_request(before_request=lock_visit).result()
        self.assertEqual(self.completed_state, CycleState.WAIT_EMPTY)
        self.events.assert_not_called()
        self.assertFalse(self.worker._farewell_pending)
        self.assertEqual(
            [call[2].voice_code for call in self.scale.calls if call[3] is not None],
            [VoiceCode.ALERT_SOUND, VoiceCode.NONE],
        )

    def test_zero_cancels_verify_or_measure_card_before_output_but_preserves_playing_voice(self):
        for state in (WorkerState.VERIFY, WorkerState.MEASURE):
            with self.subTest(state=state):
                self.reset_station()
                self.prepare_card(state)
                self.worker._rejected_until = self.worker._rejection_notice_until = 30
                self.worker._tag_prompt_at = self.worker._completion_at = 30
                self.worker._farewell_pending = self.worker._weighed_vehicle_on = True
                self.worker._pending_voice = VoiceCode.TAG_CARD
                self.worker._voice_status = VoiceCode.TAG_CARD
                self.worker._voice_started = True
                self.scale.on_voice = self.scale.off_voice = VoiceCode.TAG_CARD

                def during_on():
                    self.assertEqual(self.worker.state, WorkerState.IDLE)
                    self.assertEqual(self.worker.cycle.state, CycleState.WAIT_EMPTY)
                    self.assertIsNone(self.worker.last_event)
                    self.assertEqual(self.worker.last_plate, "")
                    self.assertFalse(self.worker._processing)
                    self.assertEqual(self.worker._rejected_until, 0)
                    self.assertEqual(self.worker._rejection_notice_until, 0)
                    self.assertIsNone(self.worker._tag_prompt_at)
                    self.assertIsNone(self.worker._completion_at)
                    self.assertFalse(self.worker._farewell_pending)

                self.scale.after_on = during_on
                result = self.execute_queued_zero()

                self.assertIsNone(result.error)
                self.assertIsNone(self.worker.last_event)
                self.assertIsNone(self.worker._pending_save)
                self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
                self.assertTrue(self.worker._voice_started)
                self.events.assert_not_called()

    def test_existing_save_original_or_result_is_never_discarded_by_zero(self):
        original = WeighingCompletedEvent(station_id=7, rfid_card_uid="12345678", weight=120)
        save_result = Future()
        for protected in (
            {"_pending_save": original},
            {"_save_result": save_result},
            {"last_event": original},
            {"_save_fault": True},
        ):
            with self.subTest(protected=tuple(protected)):
                self.reset_station()
                self.prepare_card()
                for name, value in protected.items():
                    setattr(self.worker, name, value)

                result = self.worker.request_zero()

                self.assertTrue(result.done())
                self.assertEqual(result.exception().reason, "busy")
                for name, value in protected.items():
                    self.assertIs(getattr(self.worker, name), value)
                self.events.assert_not_called()
                self.assertFalse(any(call[2].zero_output for call in self.scale.calls))

    def test_pending_zero_wins_before_measurement_original_is_created(self):
        self.prepare_card()
        pending = self.worker.request_zero()
        self.assertFalse(pending.done())

        self.assertEqual(self.worker.measure(), WorkerState.MEASURE)
        self.assertEqual(self.worker.cycle.state, CycleState.RESERVED)
        self.assertIsNone(self.worker._pending_save)
        self.events.assert_not_called()

        self.worker._run_zero_request()
        self.assertTrue(pending.result().output_released)
        self.assertIsNone(self.worker.last_event)
        self.assertIsNone(self.worker._pending_save)
        self.events.assert_not_called()

    def test_created_measurement_wins_before_reentrant_save_callback_zero_request(self):
        self.prepare_card()
        observed = []
        saved = Future()

        def save(event):
            # Guard against deadlock and prove the user callback runs outside
            # the same lock that serializes zero acceptance and consumption.
            unlocked = self.worker._zero_lock.acquire(blocking=False)
            if unlocked:
                self.worker._zero_lock.release()
                zero = self.worker.request_zero()
                observed.append((event, unlocked, zero.exception().reason))
            else:
                observed.append((event, unlocked, None))
            return saved

        self.events.side_effect = save
        self.assertEqual(self.worker.measure(), WorkerState.SAVE)

        self.assertEqual(len(observed), 1)
        original, unlocked, reason = observed[0]
        self.assertTrue(unlocked)
        self.assertEqual(reason, "busy")
        self.assertIs(self.worker._pending_save, original)
        self.assertIs(self.worker.last_event, original)
        self.assertIs(self.worker._save_result, saved)
        self.assertEqual((original.rfid_card_uid, original.weight), ("12345678", 120))
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.assertFalse(any(call[2].zero_output for call in self.scale.calls))

    def test_cancelled_or_rejected_zero_leaves_original_card_available_to_measure(self):
        for outcome in ("cancelled", "weight_changed"):
            with self.subTest(outcome=outcome):
                self.reset_station()
                card = self.prepare_card()
                saved = Future()
                self.events.return_value = saved
                zero = self.worker.request_zero()
                if outcome == "cancelled":
                    self.assertTrue(zero.cancel())
                    # measure() must ignore a cancelled request even before
                    # the next loop has removed its request container.
                else:
                    self.worker._telemetry = replace(self.worker.get_telemetry(), weight=Decimal("121"))
                    self.worker._run_zero_request()
                    self.assertEqual(zero.exception().reason, "weight_changed")
                self.assertIs(self.worker.last_event, card)
                self.assertTrue(self.worker._processing)
                self.assertEqual(self.worker.measure(), WorkerState.SAVE)
                self.assertEqual(self.worker._pending_save.rfid_card_uid, card.rfid_card_uid)
                self.assertEqual(self.worker._pending_save.weight, 120)
                self.events.assert_called_once()
                self.assertFalse(any(call[2].zero_output for call in self.scale.calls))

    def test_off_failure_cannot_restore_card_discarded_before_zero(self):
        self.prepare_card()
        self.scale.off_error = DeviceResponseTimeout("OFF reply lost")

        result = self.execute_queued_zero()

        self.assertFalse(result.output_released)
        self.assertEqual(result.error, "off_failed")
        self.assertIsNone(self.worker.last_event)
        self.assertEqual(self.worker.last_plate, "")
        self.assertFalse(self.worker._processing)
        self.assertIsNone(self.worker._pending_save)
        self.assertEqual(self.worker._resume_state(), WorkerState.IDLE)
        self.events.assert_not_called()

    def test_zero_alert_is_sent_with_on_once_and_completion_allows_new_guidance(self):
        self.prime()
        self.execute_queued_zero()
        self.assertIsNone(self.worker._pending_voice)
        self.assertEqual(self.worker._voice_status, VoiceCode.NONE)
        for _ in range(3):
            self.worker._poll()

        voices = [call[2].voice_code for call in self.scale.calls]
        self.assertEqual(voices.count(VoiceCode.ALERT_SOUND), 1)
        on = next(index for index, call in enumerate(self.scale.calls) if call[2].zero_output)
        self.assertEqual(voices[on], VoiceCode.ALERT_SOUND)
        self.assertEqual(voices[on + 1], VoiceCode.NONE)
        self.assertIsNone(self.worker._zero_alert_until)

        self.worker._rejection_notice_until = self.clock.now + 1
        self.worker._poll()
        self.assertEqual(self.scale.calls[-1][2].voice_code, VoiceCode.UNREGISTERED_CARD)

    def test_unobserved_or_lost_on_voice_reply_keeps_tracking_without_resending_alert(self):
        for feedback, error in (
            (VoiceCode.NONE, None),
            (None, None),
            (VoiceCode.ALERT_SOUND, DeviceResponseTimeout("ON response lost")),
        ):
            with self.subTest(feedback=feedback, error=error):
                self.reset_station()
                self.prime()
                self.scale.on_voice = feedback
                self.scale.on_error = error
                self.execute_queued_zero()
                self.assertEqual(self.worker._pending_voice, VoiceCode.ALERT_SOUND)
                self.assertFalse(self.worker._voice_started)
                self.assertIsNone(self.worker._zero_alert_until)
                self.worker._rejection_notice_until = self.clock.now + 1
                for _ in range(3):
                    self.worker._poll()

                voices = [call[2].voice_code for call in self.scale.calls]
                self.assertEqual(voices.count(VoiceCode.ALERT_SOUND), 1)
                self.assertNotIn(VoiceCode.UNREGISTERED_CARD, voices)
                self.assertEqual(self.worker._pending_voice, VoiceCode.ALERT_SOUND)
                self.assertFalse(self.worker._voice_started)

    def test_busy_speaker_finishing_in_on_reply_allows_alert_with_off(self):
        self.prime()
        self.worker._pending_voice = VoiceCode.TAG_CARD
        self.worker._voice_status = VoiceCode.TAG_CARD
        self.worker._voice_started = True
        self.scale.on_voice = VoiceCode.NONE

        self.execute_queued_zero()

        pulse = [call for call in self.scale.calls if call[3] is not None]
        self.assertAlmostEqual(pulse[1][0] - pulse[0][0], 1.5)
        self.assertEqual(
            [call[2].voice_code for call in pulse],
            [VoiceCode.NONE, VoiceCode.ALERT_SOUND],
        )

    def test_busy_speaker_delays_alert_without_delaying_zero_or_forgetting_old_playback(self):
        self.prime()
        self.worker._pending_voice = VoiceCode.TAG_CARD
        self.worker._voice_status = VoiceCode.TAG_CARD
        self.worker._voice_started = False
        self.scale.on_voice = self.scale.off_voice = VoiceCode.TAG_CARD

        self.execute_queued_zero()

        pulse = [call for call in self.scale.calls if call[3] is not None]
        self.assertAlmostEqual(pulse[1][0] - pulse[0][0], 1.5)
        self.assertEqual(pulse[1][2].voice_code, VoiceCode.NONE)
        self.assertEqual(self.worker._pending_voice, VoiceCode.TAG_CARD)
        self.assertTrue(self.worker._voice_started)
        self.scale.off_voice = VoiceCode.NONE
        self.worker._poll()  # Observe the old voice's Busy -> 00 completion.
        self.assertIsNone(self.worker._pending_voice)
        self.worker._poll()  # The first subsequent idle send can contain 12.
        self.worker._poll()

        voices = [call[2].voice_code for call in self.scale.calls]
        self.assertEqual(voices.count(VoiceCode.ALERT_SOUND), 1)
        self.assertEqual(voices[-2], VoiceCode.ALERT_SOUND)

    def test_unknown_speaker_defers_alert_until_an_idle_response_is_observed(self):
        self.prime()
        self.worker._voice_status = None
        self.scale.on_voice = self.scale.off_voice = None
        self.execute_queued_zero()
        self.assertFalse(any(call[2].voice_code == VoiceCode.ALERT_SOUND for call in self.scale.calls))

        self.scale.off_voice = VoiceCode.NONE
        self.worker._poll()
        self.assertEqual(self.scale.calls[-1][2].voice_code, VoiceCode.NONE)
        self.worker._poll()
        self.assertEqual(self.scale.calls[-1][2].voice_code, VoiceCode.ALERT_SOUND)

    def test_delayed_alert_expires_and_is_not_replayed_after_speaker_becomes_idle(self):
        self.prime()
        self.worker._pending_voice = VoiceCode.TAG_CARD
        self.worker._voice_status = VoiceCode.TAG_CARD
        self.worker._voice_started = True
        self.scale.on_voice = self.scale.off_voice = VoiceCode.TAG_CARD
        self.execute_queued_zero()
        self.clock.advance(self.worker.guidance_interval)

        self.scale.off_voice = VoiceCode.NONE
        self.worker._poll()
        self.worker._poll()

        self.assertIsNone(self.worker._zero_alert_until)
        self.assertFalse(any(call[2].voice_code == VoiceCode.ALERT_SOUND for call in self.scale.calls))

    def test_accepting_a_new_card_discards_delayed_alert_from_the_previous_zero(self):
        self.prime()
        self.worker._voice_status = None
        self.scale.on_voice = self.scale.off_voice = None
        self.execute_queued_zero()
        self.worker._poll()
        self.clock.advance(1.1)
        self.worker._poll()
        self.scale.after_weight = Decimal("120")
        self.clock.advance(0.1)
        self.worker._poll()
        self.clock.advance(1.1)
        self.worker._poll()
        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.assertIsNotNone(self.worker._zero_alert_until)
        self.assertLess(self.clock.now, self.worker._zero_alert_until)

        self.assertTrue(self.worker._accept_card(ResponsePacket(rfid_card_uid="24681357")))

        self.assertIsNone(self.worker._zero_alert_until)
        self.scale.off_voice = VoiceCode.NONE
        self.worker._poll()
        self.worker._poll()
        self.assertFalse(any(call[2].voice_code == VoiceCode.ALERT_SOUND for call in self.scale.calls))

    def test_stop_after_on_suppresses_only_unsent_alert_and_still_sends_off(self):
        for busy in (False, True):
            with self.subTest(busy=busy):
                self.reset_station()

                def before_request():
                    if busy:
                        self.worker._voice_status = self.worker._pending_voice = VoiceCode.TAG_CARD
                        self.worker._voice_started = True
                        self.scale.on_voice = VoiceCode.TAG_CARD

                self.scale.after_on = self.worker.stop
                result = self.run_request(before_request=before_request).result()

                self.assertEqual(result.error, "stopped")
                self.assertTrue(result.output_released)
                self.assertEqual(self.scale.off_attempts, 1)
                voices = [call[2].voice_code for call in self.scale.calls if call[3] is not None]
                self.assertEqual(
                    voices,
                    [VoiceCode.NONE if busy else VoiceCode.ALERT_SOUND, VoiceCode.NONE],
                )

    def test_unexpected_output_recovery_keeps_consumed_visit_locked(self):
        self.prime()
        self.worker.cycle.state = CycleState.LOCKED
        self.scale.pulse_started = True
        self.scale.off_feedback = True
        self.worker._poll()
        self.assertTrue(self.worker._zero_release_pending)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)

        self.scale.off_feedback = False
        self.worker._poll()
        self.assertFalse(self.worker._zero_release_pending)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.events.assert_not_called()
        self.assertFalse(any(call[2].voice_code == VoiceCode.ALERT_SOUND for call in self.scale.calls))

    def test_new_worker_blocks_lingering_output_until_off_and_fresh_empty_then_entry(self):
        # The previous process may have died with relay 8 active. Even though
        # every new request asks for OFF, the actual feedback can remain ON.
        samples = iter((
            (0.1, "0", True),
            (1.2, "0", True),
            (1.4, "120", True),
            (2.5, "120", True),
            (2.7, "120", False),
            (3.8, "120", False),
            (4.0, "0", False),
            (5.1, "0", False),
            (5.3, "120", False),
            (6.4, "120", False),
        ))
        observed = []
        accepted_at = []

        def exchange(request, *, deadline=None):
            sample = next(samples, None)
            if sample is None:
                self.stop.set()
                return ResponsePacket(weight_value=Decimal("120"))
            at, weight, output_on = sample
            self.clock.now = at
            self.scale.calls.append((at, self.scale.connections, request, deadline))
            return ResponsePacket(
                weight_value=Decimal(weight), zero_output_on=output_on,
                rfid_card_uid="12345678",
            )

        def accepted(event):
            accepted_at.append(self.clock.now)
            self.worker.stop()

        self.scale.send_and_receive = exchange
        self.events.side_effect = accepted
        self.stop.after_wait = lambda: observed.append((
            self.scale.calls[-1][0], self.worker.cycle.state, self.worker._zero_release_pending,
        ))
        self.worker.run()

        self.assertEqual(accepted_at, [6.4])
        self.events.assert_called_once()
        self.assertTrue(all(not call[2].zero_output for call in self.scale.calls))
        self.assertTrue(all(pending for at, _, pending in observed if at <= 2.5))
        self.assertTrue(all(state == CycleState.WAIT_EMPTY for at, state, _ in observed if at <= 4.0))
        self.assertIn((2.7, CycleState.WAIT_EMPTY, False), observed)
        self.assertIn((5.1, CycleState.WAIT_ENTRY, False), observed)
