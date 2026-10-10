import unittest
from unittest.mock import Mock, patch

from cache import MarketDataCache, RFIDInfo
from events import WeighingCompletedEvent
from managers import StationRuntime, WeighingStationManager
from models import WeighingStation


class WeighingStationManagerTests(unittest.TestCase):
    def setUp(self):
        self.manager = WeighingStationManager(Mock(), MarketDataCache())
        self.manager.logger = Mock()

    def test_station_rename_updates_receipts_without_restarting_worker(self):
        self.manager.market_cache.update_rfid_data({
            "12345678": RFIDInfo(True, "Producer", "Species"),
        })
        event = WeighingCompletedEvent(rfid_card_uid="12345678", weight=23)
        cached = WeighingStation(id=1, name="Cached station", serial_port="test-port")
        updated = WeighingStation(id=1, name="Updated station", serial_port="test-port")

        with patch("managers.WeighingStationWorker") as worker, patch("managers.threading.Thread") as thread:
            self.manager.sync([cached])
            runtime = self.manager.workers[1]
            build_receipt = worker.call_args.kwargs["receipt_builder"]
            self.assertIn(b"Cached station", build_receipt(event))

            self.manager.sync([updated])
            receipt = build_receipt(event)

        self.assertIn(b"Updated station", receipt)
        self.assertNotIn(b"Cached station", receipt)
        self.assertIs(self.manager.workers[1], runtime)
        self.assertEqual(worker.call_count, 1)
        self.assertEqual(thread.call_count, 1)
        runtime.worker.stop.assert_not_called()
        runtime.thread.join.assert_not_called()
        runtime.thread.start.assert_called_once_with()

    def test_port_change_restarts_worker_with_updated_receipt_name(self):
        self.manager.market_cache.update_rfid_data({
            "12345678": RFIDInfo(True, "Producer", "Species"),
        })
        event = WeighingCompletedEvent(rfid_card_uid="12345678", weight=23)
        cached = WeighingStation(id=1, name="Cached station", serial_port="old-port")
        updated = WeighingStation(id=1, name="Updated station", serial_port="new-port")
        workers = [Mock(), Mock()]
        threads = [Mock(), Mock()]

        with patch("managers.WeighingStationWorker", side_effect=workers) as worker, patch(
            "managers.threading.Thread", side_effect=threads,
        ):
            self.manager.sync([cached])
            self.manager.sync([updated])
            receipt = worker.call_args.kwargs["receipt_builder"](event)

        workers[0].stop.assert_called_once_with()
        threads[0].join.assert_called_once_with()
        threads[1].start.assert_called_once_with()
        self.assertIs(self.manager.workers[1].worker, workers[1])
        self.assertEqual(self.manager.workers[1].port, "new-port")
        self.assertIn(b"Updated station", receipt)

    def test_stop_all_signals_every_worker_before_waiting_for_threads(self):
        actions = []
        runtimes = []
        for station_id in (1, 2):
            worker = Mock()
            thread = Mock()
            worker.stop.side_effect = lambda i=station_id: actions.append(("stop", i))

            def join(i=station_id):
                self.assertIn(i, self.manager.workers)
                actions.append(("join", i))

            thread.join.side_effect = join
            runtime = StationRuntime(worker, thread, f"test-port-{station_id}", f"Station {station_id}")
            self.manager.workers[station_id] = runtime
            runtimes.append(runtime)

        self.manager.stop_all()

        self.assertEqual(actions, [("stop", 1), ("stop", 2), ("join", 1), ("join", 2)])
        self.assertEqual(self.manager.workers, {})
        for runtime in runtimes:
            runtime.thread.join.assert_called_once_with()

    def test_stop_worker_remains_registered_until_thread_terminates(self):
        runtime = StationRuntime(Mock(), Mock(), "test-port", "Station")
        self.manager.workers[1] = runtime

        def join():
            runtime.worker.stop.assert_called_once_with()
            self.assertIs(self.manager.workers[1], runtime)

        runtime.thread.join.side_effect = join

        self.manager.stop_worker(1)

        runtime.thread.join.assert_called_once_with()
        self.assertNotIn(1, self.manager.workers)

    def test_failed_join_does_not_forget_the_worker(self):
        runtime = StationRuntime(Mock(), Mock(), "test-port", "Station")
        runtime.thread.join.side_effect = RuntimeError("Simulated join failure")
        self.manager.workers[1] = runtime

        with self.assertRaises(RuntimeError):
            self.manager.stop_worker(1)

        self.assertIs(self.manager.workers[1], runtime)
