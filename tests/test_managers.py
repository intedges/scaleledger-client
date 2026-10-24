from concurrent.futures import Future
from datetime import UTC, datetime
from decimal import Decimal
import unittest
from unittest.mock import Mock, patch

from cache import MarketDataCache, RFIDInfo
from events import WeighingCompletedEvent
from managers import StationRuntime, WeighingStationManager
from models import WeighingStation
from suwol1000 import DisplayRequestPacket, LiveWeightSnapshot, ResponsePacket, WorkerState
from tests.helpers import prepare_vehicle


class WeighingStationManagerTests(unittest.TestCase):
    def setUp(self):
        self.manager = WeighingStationManager(Mock(), MarketDataCache())
        self.manager.logger = Mock()

    def test_each_worker_records_the_station_id_from_server_settings(self):
        stations = [
            WeighingStation(id=7, name="Station A", serial_port="first-port"),
            WeighingStation(id=11, name="Station B", serial_port="second-port"),
        ]
        self.manager.on_event.side_effect = lambda event: Future()

        with patch("managers.threading.Thread"), patch("suwol1000.get_logger", return_value=Mock()):
            self.manager.sync(stations)
            for station in stations:
                worker = self.manager.workers[station.id].worker
                worker.last_plate = "12345678"
                worker.last_weight = Decimal("120")
                prepare_vehicle(worker)
                self.assertEqual(worker.measure(), WorkerState.SAVE)

        events = [call.args[0] for call in self.manager.on_event.call_args_list]
        self.assertEqual([event.station_id for event in events], [7, 11])
        self.assertEqual(len({event.uuid for event in events}), 2)

    def test_station_rename_updates_receipts_without_restarting_worker(self):
        self.manager.market_cache.update_rfid_data({
            "12345678": RFIDInfo(True, "Producer", "Species"),
        })
        event = WeighingCompletedEvent(station_id=1, rfid_card_uid="12345678", weight=23)
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
        event = WeighingCompletedEvent(station_id=1, rfid_card_uid="12345678", weight=23)
        cached = WeighingStation(id=1, name="Cached station", serial_port="old-port")
        updated = WeighingStation(id=1, name="Updated station", serial_port="new-port")
        workers = [Mock(), Mock()]
        workers[0].get_live_weight.return_value = LiveWeightSnapshot(sample_seq=12)
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
        self.assertEqual(worker.call_args.kwargs["initial_sample_seq"], 12)
        self.assertEqual(worker.call_args.kwargs["station_id"], 1)

    def test_receipt_prints_the_measurement_time_in_local_time(self):
        self.manager.market_cache.update_rfid_data({
            "12345678": RFIDInfo(True, "Producer", "Species"),
        })
        event = WeighingCompletedEvent(
            station_id=1,
            rfid_card_uid="12345678", weight=23,
            timestamp=datetime(2026, 10, 11, 5, 0, tzinfo=UTC),
        )
        station = WeighingStation(id=1, name="Station", serial_port="test-port")

        with patch("managers.WeighingStationWorker") as worker, patch("managers.threading.Thread"):
            self.manager.sync([station])
            receipt = worker.call_args.kwargs["receipt_builder"](event)

        local_time = event.timestamp.astimezone().strftime("%Y-%m-%d %H:%M:%S")
        self.assertIn(local_time.encode(), receipt)

    def test_stop_all_signals_every_worker_before_waiting_for_threads(self):
        actions = []
        self.manager.on_station_removed = lambda station_id: actions.append(("remove", station_id))
        runtimes = []
        for station_id in (1, 2):
            worker = Mock()
            worker.get_live_weight.return_value = LiveWeightSnapshot()
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

        self.assertEqual(actions, [
            ("stop", 1), ("stop", 2), ("join", 1), ("remove", 1), ("join", 2), ("remove", 2),
        ])
        self.assertEqual(self.manager.workers, {})
        for runtime in runtimes:
            runtime.thread.join.assert_called_once_with()

    def test_stop_worker_remains_registered_until_thread_terminates(self):
        runtime = StationRuntime(Mock(), Mock(), "test-port", "Station")
        runtime.worker.get_live_weight.return_value = LiveWeightSnapshot()
        self.manager.workers[1] = runtime

        def join():
            runtime.worker.stop.assert_called_once_with()
            self.assertIs(self.manager.workers[1], runtime)

        runtime.thread.join.side_effect = join

        self.manager.stop_worker(1)

        runtime.thread.join.assert_called_once_with()
        self.assertNotIn(1, self.manager.workers)

    def test_failed_join_does_not_forget_the_worker(self):
        self.manager.on_station_removed = Mock()
        runtime = StationRuntime(Mock(), Mock(), "test-port", "Station")
        runtime.thread.join.side_effect = RuntimeError("Simulated join failure")
        self.manager.workers[1] = runtime

        with self.assertRaises(RuntimeError):
            self.manager.stop_worker(1)

        self.assertIs(self.manager.workers[1], runtime)
        self.manager.on_station_removed.assert_not_called()

    def test_station_registers_waiting_before_start_and_forwards_each_sample(self):
        updates = []

        def receive(station_id, sample):
            self.assertIn(station_id, self.manager.workers)
            updates.append((station_id, sample))

        self.manager.on_live_weight = receive
        station = WeighingStation(id=7, name="Station", serial_port="test-port")
        client = Mock(port="test-port")
        client.send_and_receive.return_value = ResponsePacket()
        with patch("managers.SerialClient", return_value=client), patch("managers.threading.Thread") as thread:
            def start():
                self.assertEqual(len(updates), 1)
                self.assertEqual(updates[0][1].connection_status, "waiting")
                self.manager.workers[7].worker.exchange(DisplayRequestPacket())
                self.manager.workers[7].worker.exchange(DisplayRequestPacket())

            thread.return_value.start.side_effect = start
            self.manager.sync([station])

        self.assertEqual([station_id for station_id, _ in updates], [7, 7, 7])
        self.assertEqual([sample.sample_seq for _, sample in updates], [0, 1, 2])

    def test_port_restart_removes_old_snapshot_before_waiting_and_new_sample(self):
        actions = []
        self.manager.on_live_weight = lambda station_id, sample: actions.append(
            (station_id, sample.connection_status, sample.sample_seq),
        )
        self.manager.on_station_removed = lambda station_id: actions.append((station_id, "removed"))
        cached = WeighingStation(id=1, name="Station", serial_port="old-port")
        updated = WeighingStation(id=1, name="Station", serial_port="new-port")

        def client(port):
            result = Mock(port=port)
            result.send_and_receive.return_value = ResponsePacket()
            return result

        with patch("managers.SerialClient", side_effect=client), patch("managers.threading.Thread"):
            self.manager.sync([cached])
            self.manager.workers[1].worker.exchange(DisplayRequestPacket())
            self.manager.sync([updated])
            self.manager.workers[1].worker.exchange(DisplayRequestPacket())
            self.manager.sync([])

        self.assertEqual(actions, [
            (1, "waiting", 0), (1, "connected", 1), (1, "disconnected", 1), (1, "removed"),
            (1, "waiting", 0), (1, "connected", 2), (1, "disconnected", 2), (1, "removed"),
        ])
        self.assertEqual(self.manager.workers, {})

    def test_failed_initial_callback_does_not_prevent_worker_start(self):
        self.manager.on_live_weight = Mock(side_effect=RuntimeError("Callback failed"))
        station = WeighingStation(id=1, name="Station", serial_port="test-port")

        with patch("managers.threading.Thread") as thread:
            self.manager.start_worker(station)

        thread.return_value.start.assert_called_once_with()
        self.assertIn(1, self.manager.workers)
        self.manager.logger.exception.assert_called_once()

    def test_failed_thread_start_removes_waiting_snapshot_and_registration(self):
        self.manager.on_live_weight = Mock()
        self.manager.on_station_removed = Mock()
        station = WeighingStation(id=1, name="Station", serial_port="test-port")

        with patch("managers.threading.Thread") as thread:
            thread.return_value.start.side_effect = RuntimeError("Cannot start thread")
            with self.assertRaises(RuntimeError):
                self.manager.start_worker(station)

        self.assertEqual(self.manager.workers, {})
        self.manager.on_station_removed.assert_called_once_with(1)
        self.manager.stop_all()
        thread.return_value.join.assert_not_called()
