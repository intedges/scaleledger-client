"""Accelerated, deterministic worker runs; these are not real-time hardware soaks."""

from collections import Counter, defaultdict
from concurrent.futures import Future
import threading
import unittest
from unittest.mock import patch

import serial

from events import RFIDTaggedEvent, WeighingCompletedEvent
from logconfig import RepeatedFailureLog
from suwol1000 import (
    DisplayRequestPacket, PrinterRequestPacket, ResponsePacket, VoiceCode,
    WeighingStationWorker, WorkerState,
)
from weighing_cycle import CycleState


NO_CARD = "00000000"
INVALID_CARD = "99999999"


class VirtualClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class CountingLogger:
    """Keep counters instead of retaining tens of thousands of mock calls."""

    def __init__(self):
        self.events = Counter()

    def bind(self, **kwargs):
        return self

    def info(self, event, **kwargs):
        self.events[event] += 1

    debug = warning = critical = info

    def log(self, level, event, **kwargs):
        self.info(event, **kwargs)


class ScheduledTraffic:
    """A physical timeline independent of the worker's internal state.

    Each vehicle stays for 24 seconds, followed by six seconds at or below
    50 kg. Every exchange consumes 0.5 virtual seconds. A vehicle present
    during startup must leave before the counted visits begin.
    """

    port = "virtual-soak-scale"
    startup_occupied_seconds = 6.0
    initial_empty_seconds = 6.0
    occupied_seconds = 24.0
    visit_seconds = 30.0

    def __init__(self, clock, vehicles):
        self.clock = clock
        self.vehicles = vehicles
        self.stop_event = None
        self.connect_calls = 0
        self.exchanges = 0
        self.faults = set()
        self.voice_commands = Counter()
        self.card_tags = Counter()
        self.prints = defaultdict(list)
        self.submissions = defaultdict(list)
        self.saved = defaultdict(list)
        self.pending = []
        self.tagged = []
        self.invalid_tags = 0

    @property
    def start(self):
        return self.startup_occupied_seconds + self.initial_empty_seconds

    @property
    def end(self):
        return self.start + self.vehicles * self.visit_seconds

    def position(self):
        elapsed = self.clock.now - self.start
        if elapsed < 0 or elapsed >= self.vehicles * self.visit_seconds:
            return None, None
        return int(elapsed // self.visit_seconds), elapsed % self.visit_seconds

    def connect(self):
        self.connect_calls += 1

    def disconnect(self):
        pass

    def interrupt(self):
        pass

    def submit(self, event):
        if isinstance(event, RFIDTaggedEvent):
            self.tagged.append(event)
            return None
        if not isinstance(event, WeighingCompletedEvent):
            raise TypeError(type(event).__name__)
        vehicle, age = self.position()
        self.submissions[vehicle].append(event)
        future = Future()
        first_attempt = len(self.submissions[vehicle]) == 1
        if vehicle is not None and vehicle % 8 == 4 and first_attempt:
            future.set_exception(OSError("simulated temporary local storage error"))
        else:
            delay = 8.0 if vehicle is not None and vehicle % 8 == 5 else 1.0
            self.pending.append((self.clock.now + delay, future, vehicle, event))
        return future

    def validate(self, event):
        if event.rfid_card_uid == INVALID_CARD:
            self.invalid_tags += 1
            return False
        return True

    def settle_saves(self):
        still_pending = []
        for due, future, vehicle, event in self.pending:
            if self.clock.now >= due:
                self.saved[vehicle].append(event)
                future.set_result(None)
            else:
                still_pending.append((due, future, vehicle, event))
        self.pending = still_pending

    @staticmethod
    def wire_response(request, *, weight, card, unstable):
        # Keep the real 53-byte response parser and request serializers in the run.
        voice = getattr(request, "voice_code", VoiceCode.NONE)
        relay = (bool(getattr(request, "green_blink", False))
                 + 2 * bool(getattr(request, "red_blink", False)))
        raw = (
            b"\x020D" + card.encode("ascii") + b"0000000"
            + f"0{relay}00{voice:02d}025300500000".encode("ascii")
            + (b"US" if unstable else b"ST") + b",GS,"
            + f"+{weight:7d}kg".encode("ascii") + b"\x03"
        )
        return ResponsePacket.from_bytes(raw)

    def send_and_receive(self, request):
        self.exchanges += 1
        self.clock.now += 0.5
        self.settle_saves()
        request.to_bytes()
        vehicle, age = self.position()
        if self.clock.now >= self.end:
            self.stop_event.set()
        weight, card, unstable = 0, NO_CARD, False
        if self.clock.now < self.startup_occupied_seconds:
            weight, card = 180, "12345678"
        if vehicle is not None:
            profile = vehicle % 8
            # Both 0 and the exact 50 kg boundary must qualify as empty.
            weight = 50 if int(age * 2) % 2 else 0
            if age < self.occupied_seconds:
                weight = 70 + (vehicle % 5) * 10
                unstable = profile == 1
                if age >= 4:
                    # Keep tagging alternate cards after acceptance and completion.
                    card = f"{10000000 + vehicle * 2 + int(age * 2) % 2:08d}"
                    if vehicle % 7 == 0 and age < 7:
                        card = INVALID_CARD
                    self.card_tags[vehicle] += 1
            fault_time = 1.0 if profile in (2, 3) else 21.0
            if profile in (2, 3, 6, 7) and age >= fault_time and vehicle not in self.faults:
                self.faults.add(vehicle)
                if profile in (3, 7):
                    raise serial.SerialException("simulated transient cable disconnection")
                # A truncated actual protocol frame fails in the production parser.
                return ResponsePacket.from_bytes(b"\x020D\x03")
        if isinstance(request, PrinterRequestPacket):
            self.prints[vehicle].append(request.document_bytes)
        if isinstance(request, DisplayRequestPacket) and request.voice_code != VoiceCode.NONE:
            self.voice_commands[request.voice_code] += 1
        return self.wire_response(request, weight=weight, card=card, unstable=unstable)


class StationSoakTests(unittest.TestCase):
    def test_500_visits_through_worker_run_recover_without_duplicate_or_stuck_cycle(self):
        clock = VirtualClock()
        traffic = ScheduledTraffic(clock, vehicles=500)
        logger = CountingLogger()
        with patch("suwol1000.time.monotonic", clock), patch("suwol1000.get_logger", return_value=logger):
            worker = WeighingStationWorker(
                traffic, station_id=7, on_event=traffic.submit, rfid_validator=traffic.validate,
                receipt_builder=lambda event: event.uuid.encode("ascii"),
                polling_interval=0, retry_interval=0,
            )
            worker.failures = RepeatedFailureLog(logger, "hw.station", clock=clock)
            traffic.stop_event = worker.stop_event
            thread = threading.Thread(target=worker.run)
            thread.start()
            thread.join(timeout=10)
            if thread.is_alive():
                worker.stop()
                thread.join(timeout=2)
                self.fail("The accelerated worker run did not finish within its wall-time bound")

        expected_vehicles = set(range(500))
        self.assertEqual(set(traffic.saved), expected_vehicles)
        self.assertEqual(set(traffic.prints), expected_vehicles)
        uuids = set()
        for vehicle in expected_vehicles:
            with self.subTest(vehicle=vehicle, profile=vehicle % 8):
                self.assertEqual(len(traffic.saved[vehicle]), 1)
                event = traffic.saved[vehicle][0]
                self.assertEqual(event.station_id, 7)
                self.assertEqual(event.weight, 70 + (vehicle % 5) * 10)
                self.assertNotEqual(event.rfid_card_uid, INVALID_CARD)
                self.assertEqual(traffic.prints[vehicle], [event.uuid.encode("ascii")])
                self.assertEqual({attempt.uuid for attempt in traffic.submissions[vehicle]}, {event.uuid})
                self.assertEqual({attempt.station_id for attempt in traffic.submissions[vehicle]}, {7})
                self.assertEqual(len(traffic.submissions[vehicle]), 2 if vehicle % 8 == 4 else 1)
                self.assertGreater(traffic.card_tags[vehicle], 20)
                uuids.add(event.uuid)
        self.assertEqual(len(uuids), 500)
        self.assertGreater(traffic.invalid_tags, 0)
        self.assertGreater(traffic.voice_commands[VoiceCode.UNREGISTERED_CARD], 0)
        self.assertGreater(traffic.voice_commands[VoiceCode.CHECK_ADMIN], 0)
        self.assertEqual(traffic.faults, {v for v in expected_vehicles if v % 8 in (2, 3, 6, 7)})
        self.assertEqual(traffic.connect_calls, 1 + sum(v % 8 in (3, 7) for v in expected_vehicles))
        self.assertEqual(logger.events["sys.worker.unexpected_error"], 0)
        self.assertEqual(logger.events["hw.speaker.timeout"], 0)
        self.assertFalse(traffic.pending)
        self.assertIsNone(worker._pending_save)
        self.assertIsNone(worker._save_result)
        self.assertIsNone(worker.last_event)
        self.assertFalse(worker._processing)
        self.assertEqual(worker.state, WorkerState.IDLE)
        self.assertEqual(worker.cycle.state, CycleState.WAIT_ENTRY)
        self.assertEqual(clock.now, 15012.0)


if __name__ == "__main__":
    unittest.main()
