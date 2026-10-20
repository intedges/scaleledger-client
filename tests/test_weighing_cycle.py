from decimal import Decimal
import unittest

from weighing_cycle import CycleState, WeighingCycle


class WeighingCycleTests(unittest.TestCase):
    def setUp(self):
        self.cycle = WeighingCycle(Decimal("50"), ready_seconds=1, empty_seconds=2)

    def observe(self, weight, now):
        self.cycle.observe(None if weight is None else Decimal(weight), now=now)

    def empty(self, start=0):
        self.observe("0", start)
        self.observe("0", start + 1)
        self.observe("0", start + 2)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)

    def ready(self, start=0):
        self.empty(start)
        self.observe("120", start + 3)
        self.observe("120", start + 4)
        self.assertEqual(self.cycle.state, CycleState.READY)

    def lock(self):
        self.ready()
        self.assertTrue(self.cycle.reserve(now=4))
        self.assertTrue(self.cycle.consume(now=4))
        self.assertEqual(self.cycle.state, CycleState.LOCKED)

    def test_restart_with_vehicle_present_requires_empty_then_new_entry(self):
        for now in range(10):
            self.observe("120", now)
            self.assertEqual(self.cycle.state, CycleState.WAIT_EMPTY)
            self.assertFalse(self.cycle.reserve(now=now))
        self.empty(10)
        self.observe("120", 13)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)
        self.observe("120", 14)
        self.assertEqual(self.cycle.state, CycleState.READY)
        self.assertEqual(self.cycle.ready_sequence, 1)

    def test_exact_threshold_is_empty_and_cannot_authorize_measurement(self):
        self.observe("50", 0)
        self.observe("50", 2)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)
        for now in (3, 4, 5):
            self.observe("50", now)
            self.assertFalse(self.cycle.reserve(now=now))
        self.observe("50.01", 6)
        self.observe("50.01", 7)
        self.assertTrue(self.cycle.reserve(now=7))

    def test_brief_entry_below_duration_does_not_accept_card(self):
        self.empty()
        self.observe("120", 3)
        self.observe("120", 3.9)
        self.assertFalse(self.cycle.reserve(now=3.9))
        self.observe("0", 4)
        self.observe("120", 5)
        self.observe("120", 5.9)
        self.assertFalse(self.cycle.reserve(now=5.9))
        self.observe("120", 6)
        self.assertTrue(self.cycle.reserve(now=6))

    def test_each_vehicle_visit_can_be_consumed_only_once(self):
        self.lock()
        for now in (5, 6, 7):
            self.observe("120", now)
            self.assertFalse(self.cycle.reserve(now=now))
            self.assertFalse(self.cycle.consume(now=now))
            self.cycle.release()
            self.assertEqual(self.cycle.state, CycleState.LOCKED)
        self.empty(8)
        self.observe("120", 11)
        self.observe("120", 12)
        self.assertEqual(self.cycle.ready_sequence, 2)
        self.assertTrue(self.cycle.reserve(now=12))
        self.assertTrue(self.cycle.consume(now=12))

    def test_short_empty_sample_does_not_unlock_consumed_visit(self):
        self.lock()
        self.observe("0", 5)
        self.observe("0", 6)
        self.observe("120", 6.5)
        self.observe("0", 7)
        self.observe("0", 8)
        self.assertEqual(self.cycle.state, CycleState.LOCKED)
        self.observe("0", 9)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)

    def test_ready_visit_must_qualify_again_after_a_low_sample(self):
        self.ready()
        self.observe("50", 5)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)
        self.observe("120", 5.1)
        self.assertFalse(self.cycle.reserve(now=5.1))
        self.observe("120", 6.2)
        self.assertTrue(self.cycle.reserve(now=6.2))
        self.assertEqual(self.cycle.ready_sequence, 2)

    def test_low_sample_cancels_reserved_card_even_if_vehicle_returns(self):
        self.ready()
        self.assertTrue(self.cycle.reserve(now=4))
        self.observe("0", 5)
        self.observe("120", 6)
        self.observe("120", 7)
        self.assertEqual(self.cycle.state, CycleState.READY)
        self.assertFalse(self.cycle.consume(now=7))
        self.assertTrue(self.cycle.reserve(now=7))
        self.assertTrue(self.cycle.consume(now=7))

    def test_invalid_card_release_allows_retry_without_another_entry(self):
        self.ready()
        self.assertFalse(self.cycle.consume(now=4))
        self.assertTrue(self.cycle.reserve(now=4))
        self.assertFalse(self.cycle.reserve(now=4))
        self.cycle.release()
        self.assertEqual(self.cycle.state, CycleState.READY)
        self.assertEqual(self.cycle.ready_sequence, 2)
        self.assertTrue(self.cycle.reserve(now=4))
        self.assertTrue(self.cycle.consume(now=4))

    def test_invalid_samples_cancel_reservation_without_becoming_empty(self):
        for invalid in (None, "NaN", "sNaN", "Infinity", "-Infinity"):
            with self.subTest(weight=invalid):
                self.setUp()
                self.ready()
                self.assertTrue(self.cycle.reserve(now=4))
                self.observe(invalid, 5)
                self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)
                self.assertFalse(self.cycle.consume(now=5))
                self.observe("120", 6)
                self.assertFalse(self.cycle.reserve(now=6))
                self.observe("120", 7)
                self.assertTrue(self.cycle.reserve(now=7))

    def test_faults_never_unlock_and_interrupt_empty_confirmation(self):
        self.lock()
        self.observe("0", 5)
        self.cycle.invalidate()
        self.assertEqual(self.cycle.state, CycleState.LOCKED)
        self.observe("0", 6)
        self.observe(None, 7)
        self.observe("0", 8)
        self.observe("0", 9)
        self.assertEqual(self.cycle.state, CycleState.LOCKED)
        self.observe("0", 10)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)

    def test_missing_samples_reset_initial_empty_and_entry_confirmation(self):
        self.observe("0", 0)
        self.observe("0", 3)
        self.assertEqual(self.cycle.state, CycleState.WAIT_EMPTY)
        self.observe("0", 5)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)
        self.observe("120", 6)
        self.observe("120", 9)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)
        self.observe("120", 10)
        self.assertEqual(self.cycle.state, CycleState.READY)

    def test_observation_gap_cancels_reserved_card_and_requires_new_high_duration(self):
        self.ready()
        self.assertTrue(self.cycle.reserve(now=4))
        self.observe("120", 7)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)
        self.observe("120", 8)
        self.assertEqual(self.cycle.state, CycleState.READY)
        self.assertFalse(self.cycle.consume(now=8))

    def test_observation_gap_keeps_lock_and_restarts_empty_confirmation(self):
        self.lock()
        self.observe("0", 5)
        self.observe("0", 8)
        self.assertEqual(self.cycle.state, CycleState.LOCKED)
        self.assertFalse(self.cycle.reserve(now=8))
        self.observe("0", 9)
        self.assertEqual(self.cycle.state, CycleState.LOCKED)
        self.observe("0", 10)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)

    def test_stale_ready_or_reserved_weight_cannot_be_used_without_new_samples(self):
        for reserve_first in (False, True):
            with self.subTest(reserved=reserve_first):
                self.setUp()
                self.ready()
                if reserve_first:
                    self.assertTrue(self.cycle.reserve(now=4))
                    self.assertFalse(self.cycle.consume(now=6.1))
                else:
                    self.assertFalse(self.cycle.reserve(now=6.1))
                self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)

    def test_latest_sample_at_maximum_gap_is_still_usable(self):
        self.ready()
        self.assertTrue(self.cycle.reserve(now=6))
        self.assertTrue(self.cycle.consume(now=6))

    def test_time_reversal_breaks_observation_continuity_and_reservation(self):
        self.ready()
        self.assertTrue(self.cycle.reserve(now=4))
        self.observe("120", 3)
        self.assertEqual(self.cycle.state, CycleState.WAIT_ENTRY)
        self.observe("120", 4)
        self.assertFalse(self.cycle.consume(now=4))
        self.assertTrue(self.cycle.reserve(now=4))
        self.assertFalse(self.cycle.consume(now=3))

    def test_independent_stations_do_not_share_cycle_state(self):
        other = WeighingCycle(Decimal("50"), ready_seconds=1, empty_seconds=2)
        self.lock()
        other.observe(Decimal("0"), now=0)
        other.observe(Decimal("0"), now=2)
        other.observe(Decimal("120"), now=3)
        other.observe(Decimal("120"), now=4)
        self.assertEqual(self.cycle.state, CycleState.LOCKED)
        self.assertTrue(other.reserve(now=4))

    def test_constructor_rejects_invalid_threshold_and_durations(self):
        for threshold in (Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"), 100):
            with self.subTest(threshold=threshold), self.assertRaises(ValueError):
                WeighingCycle(threshold, ready_seconds=1, empty_seconds=2)
        for name in ("ready_seconds", "empty_seconds", "max_sample_gap"):
            for invalid in (0, -1, float("nan"), float("inf"), True, "1"):
                arguments = {"ready_seconds": 1, "empty_seconds": 2, "max_sample_gap": 2}
                arguments[name] = invalid
                with self.subTest(setting=name, value=invalid), self.assertRaises(ValueError):
                    WeighingCycle(Decimal("50"), **arguments)

    def test_nonfinite_clock_values_are_rejected(self):
        for invalid in (float("nan"), float("inf"), float("-inf"), True, "1"):
            for action in ("observe", "reserve", "consume"):
                with self.subTest(now=invalid, action=action), self.assertRaises(ValueError):
                    if action == "observe":
                        self.cycle.observe(Decimal("120"), now=invalid)
                    else:
                        getattr(self.cycle, action)(now=invalid)
