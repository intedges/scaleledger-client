from concurrent.futures import Future
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
import threading
import time
import unittest
from unittest.mock import Mock, patch

import serial

from device_configuration import DeviceConfiguration
from cache import MarketDataCache
from managers import StationRuntime, WeighingStationManager
from suwol1000 import (
    DisplayRequestPacket, PrinterRequestPacket, ResponsePacket, SerialClient,
    WeighingStationWorker, WorkerState,
)
from tests.helpers import prepare_vehicle
from tests.test_station_cycle import Clock, CycleScale
from tests.test_station_protocol import frame
from weighing_cycle import CycleState


class ConfiguredScale(CycleScale):
    serial = None
    configure_timeouts = SerialClient.configure_timeouts


class StationConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.enterContext(patch("suwol1000.time.monotonic", self.clock))
        self.enterContext(patch("suwol1000.get_logger", return_value=Mock()))
        self.make_worker()

    def make_worker(self):
        self.clock.now = 0
        self.scale = ConfiguredScale(self.clock)
        self.saved = Future()
        self.on_event = Mock(return_value=self.saved)
        self.configuration = DeviceConfiguration(auction_name="원래 경매장", print_copies=2)
        self.worker = WeighingStationWorker(
            self.scale, station_id=7, configuration=self.configuration,
            on_event=self.on_event, polling_interval=0,
            receipt_builder=lambda event: self.worker.configuration.auction_name.encode("utf-8"),
        )
        self.worker.state = WorkerState.IDLE

    def observe(self, weight, now):
        self.clock.now = now
        self.scale.weight = Decimal(weight)
        return self.worker.exchange(DisplayRequestPacket())

    def measure(self):
        self.worker.last_plate = "12345678"
        self.worker.last_weight = Decimal("120")
        prepare_vehicle(self.worker)
        self.worker.state = WorkerState.MEASURE
        self.worker.state = self.worker.measure()
        return self.on_event.call_args.args[0]

    def finish(self):
        self.saved.set_result(None)
        self.worker.state = self.worker.save()
        self.assertEqual(self.worker.state, WorkerState.PRINT)
        self.worker.state = self.worker.print()

    def prints(self):
        return [request for request in self.scale.requests if isinstance(request, PrinterRequestPacket)]

    def test_current_load_and_removal_keep_old_values_then_next_load_uses_latest_snapshot(self):
        self.observe("0", 0)
        self.observe("0", 1)
        self.observe("120", 2)
        updated = replace(
            self.configuration, auction_name="새 경매장", vehicle_threshold=200,
            vehicle_ready_seconds=2, vehicle_empty_seconds=2,
            card_prompt_interval=12, completion_interval=20, print_copies=9,
            serial_read_timeout=3, serial_write_timeout=4,
        )
        self.worker.request_configuration(updated)
        self.observe("120", 2.5)
        self.assertEqual(self.worker.configuration, self.configuration)
        self.assertEqual((self.scale.timeout, self.scale.write_timeout), (3, 4))
        self.observe("120", 3)
        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.assertEqual((self.worker.card_prompt_interval, self.worker.completion_interval), (5, 10))

        self.scale.card = "12345678"
        self.worker.state = self.worker.idle()
        self.worker.state = self.worker.verify()
        self.worker.state = self.worker.measure()
        original = self.on_event.call_args.args[0]
        self.worker.state = self.worker.save()
        self.assertIs(self.worker.last_event, original)
        self.assertEqual(self.prints(), [])
        self.finish()
        self.assertEqual(len(self.prints()), 1)
        self.assertEqual(self.prints()[0].copies, 2)
        self.assertEqual(self.prints()[0].document_bytes, "원래 경매장".encode("utf-8"))

        self.observe("120", 4)
        self.observe("120", 5)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.assertEqual(self.worker.configuration, self.configuration)
        self.observe("0", 6)
        self.observe("0", 6.5)
        self.assertEqual(self.worker.cycle.state, CycleState.LOCKED)
        self.observe("0", 7)
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
        self.observe("0", 7.1)
        self.assertEqual(self.worker.configuration, updated)
        self.assertEqual((self.worker.card_prompt_interval, self.worker.completion_interval), (12, 20))
        self.assertEqual(self.worker.cycle.empty_seconds, 2)
        self.observe("250", 8)
        self.observe("250", 9)
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)
        self.observe("250", 10)
        self.assertEqual(self.worker.cycle.state, CycleState.READY)
        self.assertEqual(len(self.prints()), 1)
        self.on_event.assert_any_call(original)

    def test_aborted_entry_and_observation_gap_require_old_empty_confirmation(self):
        self.observe("0", 0)
        self.observe("0", 1)
        self.observe("120", 2)
        updated = replace(self.configuration, vehicle_threshold=200, vehicle_empty_seconds=2)
        self.worker.request_configuration(updated)
        self.worker.cycle.invalidate()
        self.observe("0", 10)
        self.observe("0", 10.5)
        self.assertEqual(self.worker.configuration, self.configuration)
        self.observe("0", 11)
        self.observe("0", 11.1)
        self.assertEqual(self.worker.configuration, updated)

    def test_latest_request_replaces_pending_without_changing_network_thread_state(self):
        self.observe("0", 0)
        self.observe("0", 1)
        middle = replace(self.configuration, print_copies=3)
        newest = replace(self.configuration, print_copies=9)
        self.worker.request_configuration(middle)
        self.worker.request_configuration(newest)
        self.assertEqual(self.worker.configuration, self.configuration)
        self.observe("0", 1.1)
        self.assertEqual(self.worker.configuration, newest)

    def test_equal_values_and_new_timestamp_do_not_reset_empty_observations(self):
        self.observe("0", 0)
        self.observe("0", 1)
        initial_invalidations = self.worker.cycle.invalidations
        self.worker.request_configuration(self.configuration)
        self.observe("0", 1.1)
        self.worker.request_configuration(replace(
            self.configuration, updated_at=datetime(2026, 11, 15, tzinfo=UTC),
        ))
        self.observe("0", 1.2)
        self.assertEqual(self.worker.cycle.invalidations, initial_invalidations)
        self.assertEqual(self.worker.cycle._configuration_empty_since, 0)
        self.assertEqual(self.worker.cycle.state, CycleState.WAIT_ENTRY)

    def test_delayed_and_failed_save_keep_receipt_and_copies_but_next_load_changes(self):
        event = self.measure()
        rendered = "원래 경매장".encode("utf-8")
        self.worker.request_configuration(replace(self.configuration, auction_name="새 이름", print_copies=9))
        self.worker.receipt_builder = Mock(return_value=b"changed after capture")
        self.worker.state = self.worker.save()
        self.assertEqual(self.prints(), [])
        self.saved.set_exception(OSError("disk full"))
        self.worker.state = self.worker.save()
        retry = Future()
        self.on_event.return_value = retry
        self.clock.now += 5
        self.worker.state = self.worker.save()
        self.assertIs(self.on_event.call_args.args[0], event)
        retry.set_result(None)
        self.worker.state = self.worker.save()
        self.worker.state = self.worker.print()
        self.assertEqual([(item.copies, item.document_bytes) for item in self.prints()], [(2, rendered)])
        self.worker.receipt_builder.assert_not_called()
        self.assertEqual(self.on_event.call_count, 2)

    def test_render_failure_does_not_prevent_storage_or_retry_rendering(self):
        for result in (None, ValueError("cannot render")):
            with self.subTest(result=result):
                self.make_worker()
                self.worker.receipt_builder = Mock(
                    side_effect=result if isinstance(result, Exception) else None, return_value=None,
                )
                event = self.measure()
                self.on_event.assert_called_once_with(event)
                self.finish()
                self.assertEqual(self.prints(), [])
                self.worker.receipt_builder.assert_called_once_with(event)

    def test_print_failure_does_not_retry_and_next_cycle_can_apply_configuration(self):
        self.measure()
        self.saved.set_result(None)
        self.worker.state = self.worker.save()
        original_send = self.scale.send_and_receive
        attempted = []

        def fail_print(request):
            attempted.append(request)
            raise serial.SerialException("response lost after write")

        self.scale.send_and_receive = fail_print
        with self.assertRaises(serial.SerialException):
            self.worker.print()
        self.scale.send_and_receive = original_send
        self.worker.state = self.worker._resume_state()
        updated = replace(self.configuration, print_copies=9)
        self.worker.request_configuration(updated)
        self.worker.print()
        self.assertEqual(len(attempted), 1)
        self.observe("0", 1)
        self.observe("0", 2)
        self.observe("0", 2.1)
        self.assertEqual(self.worker.configuration, updated)

    def test_supported_copies_are_one_command_and_one_record(self):
        for copies in (1, 2, 9):
            with self.subTest(copies=copies):
                self.make_worker()
                self.worker.request_configuration(replace(self.configuration, print_copies=copies))
                self.worker._apply_configuration()
                self.measure()
                self.finish()
                self.assertEqual(len(self.prints()), 1)
                self.assertEqual(self.prints()[0].copies, copies)
                self.assertEqual(self.prints()[0].to_bytes()[7:8], str(copies).encode("ascii"))
                self.on_event.assert_called_once()


class SerialConfigurationTests(unittest.TestCase):
    def test_open_port_changes_only_after_inflight_exchange_and_equal_values_are_noop(self):
        entered = threading.Event()
        release = threading.Event()
        settings = []
        client = SerialClient("configuration-test")

        class Port:
            is_open = True
            timeout = 2.0
            write_timeout = 2.0

            def __setattr__(self, key, value):
                settings.append((key, value))
                object.__setattr__(self, key, value)

            def reset_input_buffer(self):
                pass

            def write(self, data):
                return len(data)

            def read(self, length):
                entered.set()
                self_test.assertTrue(release.wait(2))
                return frame()

        self_test = self
        client.serial = Port()
        worker = WeighingStationWorker(client, station_id=7, configuration=DeviceConfiguration())
        failures = []

        def exchange():
            try:
                worker.exchange(DisplayRequestPacket())
            except Exception as exc:
                failures.append(exc)

        thread = threading.Thread(target=exchange)
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            configuration = DeviceConfiguration(serial_read_timeout=3, serial_write_timeout=4)
            worker.request_configuration(configuration)
            self.assertEqual((client.serial.timeout, client.serial.write_timeout), (2, 2))
            self.assertEqual(settings, [])
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        worker.exchange(DisplayRequestPacket())
        self.assertEqual((client.timeout, client.write_timeout), (3, 4))
        self.assertEqual((client.serial.timeout, client.serial.write_timeout), (3, 4))
        self.assertEqual(settings, [("timeout", 3), ("write_timeout", 4)])
        worker.request_configuration(configuration)
        worker.exchange(DisplayRequestPacket())
        self.assertEqual(len(settings), 2)

    def test_new_timeout_respects_short_exchange_deadline_and_restores_values(self):
        client = SerialClient("configuration-test")
        client.serial = Mock(is_open=True, timeout=2.0, write_timeout=2.0)
        client.serial.write.side_effect = len
        client.serial.read.return_value = frame()
        worker = WeighingStationWorker(client, station_id=7)
        worker.request_configuration(DeviceConfiguration(serial_read_timeout=9999.9, serial_write_timeout=9999.9))
        observed = []

        def write(data):
            observed.append(client.serial.write_timeout)
            return len(data)

        def read(length):
            observed.append(client.serial.timeout)
            return frame()

        client.serial.write.side_effect = write
        client.serial.read.side_effect = read
        with patch("suwol1000.time.monotonic", return_value=10):
            worker.exchange(DisplayRequestPacket(), deadline=11.5)
        self.assertEqual(observed, [1.5, 1.5])
        self.assertEqual((client.serial.timeout, client.serial.write_timeout), (9999.9, 9999.9))

    def test_packet_rejects_invalid_copy_values(self):
        for copies in (True, False, 0, -1, 10, 1.0, "1", None):
            with self.subTest(copies=copies), self.assertRaises(ValueError):
                PrinterRequestPacket(document_bytes=b"receipt", copies=copies).to_bytes()


class StationShutdownConfigurationTests(unittest.TestCase):
    def test_stop_after_write_prevents_read_and_later_exchanges(self):
        client = SerialClient("shutdown-test", timeout=9999.9, write_timeout=9999.9)
        client.serial = Mock(is_open=True)
        worker = WeighingStationWorker(client, station_id=7)

        def write(data):
            worker.stop()
            return len(data)

        client.serial.write.side_effect = write
        with self.assertRaisesRegex(serial.SerialException, "interrupted"):
            worker.exchange(PrinterRequestPacket(document_bytes=b"receipt", copies=9))
        with self.assertRaisesRegex(serial.SerialException, "interrupted"):
            worker.exchange(DisplayRequestPacket())
        client.serial.write.assert_called_once()
        client.serial.read.assert_not_called()

    def test_manager_cancels_io_that_windows_submits_after_first_cancel(self):
        for stage in ("write", "read"):
            with self.subTest(stage=stage):
                window = threading.Event()
                submit = threading.Event()
                pending = threading.Event()
                cancelled = threading.Event()

                class WindowsPort:
                    is_open = True
                    timeout = 9999.9
                    write_timeout = 9999.9
                    writes = 0
                    reads = 0
                    cancels = 0
                    missed_cancel = False

                    def reset_input_buffer(self):
                        pass

                    def await_driver(self):
                        # The Python stop check passed, but Windows hasn't yet
                        # submitted the overlapped request that cancel can see.
                        window.set()
                        if not submit.wait(2):
                            raise AssertionError("shutdown did not reach the submission race")
                        pending.set()
                        if not cancelled.wait(2):
                            self.missed_cancel = True
                            raise serial.SerialException("test driver remained pending")

                    def write(self, data):
                        self.writes += 1
                        if stage == "write":
                            self.await_driver()
                            return 0
                        return len(data)

                    def read(self, length):
                        self.reads += 1
                        self.await_driver()
                        return b""

                    def cancel(self):
                        self.cancels += 1
                        if pending.is_set():
                            cancelled.set()
                        else:
                            submit.set()

                    def cancel_read(self):
                        if stage == "read":
                            self.cancel()

                    def cancel_write(self):
                        if stage == "write":
                            self.cancel()

                    def close(self):
                        self.is_open = False

                port = WindowsPort()
                configuration = DeviceConfiguration(serial_read_timeout=9999.9, serial_write_timeout=9999.9)
                with patch("suwol1000.serial.Serial", return_value=port), patch(
                    "suwol1000.get_logger", return_value=Mock(),
                ):
                    worker = WeighingStationWorker(
                        SerialClient("shutdown-race"), station_id=7, configuration=configuration,
                        polling_interval=0, retry_interval=0,
                    )
                    thread = threading.Thread(target=worker.run, daemon=True)
                    manager = WeighingStationManager(Mock(), MarketDataCache())
                    manager.logger = Mock()
                    manager.workers[7] = StationRuntime(worker, thread, "shutdown-race", "저울")
                    thread.start()
                    try:
                        self.assertTrue(window.wait(2))
                        started_at = time.monotonic()
                        manager.stop_worker(7)
                        self.assertLess(time.monotonic() - started_at, 1)
                    finally:
                        submit.set()
                        cancelled.set()
                        worker.stop()
                        thread.join(2)
                self.assertFalse(thread.is_alive())
                self.assertFalse(port.missed_cancel)
                self.assertGreaterEqual(port.cancels, 2)
                self.assertEqual(port.writes, 1)
                self.assertEqual(port.reads, int(stage == "read"))
                self.assertFalse(port.is_open)
                self.assertNotIn(7, manager.workers)
                worker.logger.log.assert_not_called()

    def test_repeated_shutdown_cancellation_preserves_active_zero_off_exchange(self):
        client = SerialClient("zero-shutdown", timeout=9999.9, write_timeout=9999.9)
        client.serial = Mock(is_open=True, timeout=9999.9, write_timeout=9999.9)
        client.serial.write.side_effect = len
        client.serial.read.return_value = frame()
        worker = WeighingStationWorker(client, station_id=7)
        worker._zero_active = True
        worker.stop()
        for _ in range(3):
            worker.interrupt_pending_io()

        worker.exchange(DisplayRequestPacket(zero_output=False), deadline=time.monotonic() + 0.2)

        client.serial.write.assert_called_once()
        client.serial.read.assert_called_once()
        client.serial.cancel_read.assert_not_called()
        client.serial.cancel_write.assert_not_called()
        self.assertEqual((client.serial.timeout, client.serial.write_timeout), (9999.9, 9999.9))
        worker._zero_active = False
        worker.interrupt_pending_io()
        with self.assertRaisesRegex(serial.SerialException, "interrupted"):
            worker.exchange(DisplayRequestPacket())
