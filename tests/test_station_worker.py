from concurrent.futures import Future
from decimal import Decimal
import logging
import threading
import unittest
from unittest.mock import Mock, patch

import serial

from events import WeighingCompletedEvent
from logconfig import RepeatedFailureLog
from suwol1000 import (
    DeviceResponseTimeout, DisplayRequestPacket, PrinterRequestPacket,
    ResponsePacket, SerialClient, VoiceCode, WeighingStationWorker, WorkerState,
)
from tests.test_station_protocol import frame
from weighing_cycle import CycleState


class ScriptedSerialClient:
    """Fails a fixed number of exchanges like a powered-off scale, then answers."""

    port = "test-port"
    timeout = 2.0
    write_timeout = 2.0

    def __init__(self, *, failed_exchanges=0, failed_connects=0, answers_before_stop=60):
        self.failed_exchanges = failed_exchanges
        self.failed_connects = failed_connects
        self.answers_before_stop = answers_before_stop
        self.stop_event: threading.Event | None = None

    def connect(self):
        if self.failed_connects:
            self.failed_connects -= 1
            raise serial.SerialException("could not open port 'test-port'")

    def disconnect(self):
        pass

    def interrupt(self):
        pass

    def send_and_receive(self, request):
        if self.failed_exchanges:
            self.failed_exchanges -= 1
            raise DeviceResponseTimeout("Serial response timeout: received 0/53 bytes")
        self.answers_before_stop -= 1
        if not self.answers_before_stop:
            self.stop_event.set()
        return ResponsePacket()


class WeighingScale(ScriptedSerialClient):
    """Plays each requested voice once and reports one card tag after the noise."""

    def __init__(self, *, noise: int, fail_print: bool, **kwargs):
        super().__init__(failed_exchanges=noise, **kwargs)
        self.fail_print = fail_print
        self.card_pending = True
        self.clock = 0.0
        self.print_requests = []

    def send_and_receive(self, request):
        self.clock += 1
        if self.failed_exchanges:
            return super().send_and_receive(request)
        if isinstance(request, PrinterRequestPacket):
            self.print_requests.append(request)
            if self.fail_print:
                raise DeviceResponseTimeout("Serial response timeout: received 3/53 bytes")
        rfid = "00000000"
        if self.card_pending and request.green_blink and not request.red_blink:
            self.card_pending = False
            rfid = "12345678"
        voice = getattr(request, "voice_code", VoiceCode.NONE)
        super().send_and_receive(request)
        weight = Decimal("0") if self.clock <= 8 else Decimal("120")
        return ResponsePacket(rfid_card_uid=rfid, voice_code=voice, weight_value=weight)


class BlockingSerialClient(ScriptedSerialClient):
    """Blocks in a read until stop() cancels it, like pyserial's cancel_read()."""

    def __init__(self):
        super().__init__()
        self.reading = threading.Event()
        self.cancelled = threading.Event()

    def interrupt(self):
        self.cancelled.set()

    def send_and_receive(self, request):
        self.reading.set()
        self.cancelled.wait(timeout=5)
        raise DeviceResponseTimeout("Serial response timeout: received 0/53 bytes")


class PoweredOffStationTests(unittest.TestCase):
    def run_worker(self, client: ScriptedSerialClient, **worker_kwargs) -> Mock:
        logger = Mock()
        with patch("suwol1000.get_logger") as get_logger:
            get_logger.return_value.bind.return_value = logger
            worker = WeighingStationWorker(client, station_id=7, polling_interval=0, retry_interval=0, **worker_kwargs)
        # Each clock reading stands for one second of the polling loop.
        ticks = iter(range(1_000_000))
        worker.failures = RepeatedFailureLog(logger, "hw.station", clock=lambda: float(next(ticks)))
        client.stop_event = worker.stop_event

        thread = threading.Thread(target=worker.run)
        if isinstance(client, WeighingScale):
            with patch("suwol1000.time.monotonic", side_effect=lambda: client.clock):
                thread.start()
                thread.join(timeout=10)
        else:
            thread.start()
            thread.join(timeout=10)
        self.assertFalse(thread.is_alive())
        return logger

    @staticmethod
    def events(method: Mock) -> list[str]:
        return [c.args[0] for c in method.call_args_list]

    def test_unresponsive_scale_logs_summaries_instead_of_every_timeout(self):
        # An hour of read timeouts at about one per second.
        logger = self.run_worker(ScriptedSerialClient(failed_exchanges=3600))

        self.assertEqual([c.args[1] for c in logger.log.call_args_list], ["hw.serial.timeout"])
        self.assertEqual(logger.log.call_args.args[0], logging.WARNING)
        self.assertNotIn("exc_info", logger.log.call_args.kwargs)
        self.assertIn("0/53 bytes", logger.log.call_args.kwargs["error"])
        self.assertEqual(self.events(logger.warning), ["hw.station.failure_repeating"] * 5)
        recovered = [c for c in logger.info.call_args_list if c.args[0] == "hw.station.recovered"]
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].kwargs["total_failures"], 3600)
        logger.exception.assert_not_called()

    def test_missing_port_logs_the_first_failure_and_quiet_reconnects(self):
        logger = self.run_worker(ScriptedSerialClient(failed_connects=1200))

        self.assertEqual([c.args[1] for c in logger.log.call_args_list], ["hw.serial.connection_lost"])
        self.assertEqual(logger.log.call_args.args[0], logging.WARNING)
        self.assertNotIn("exc_info", logger.log.call_args.kwargs)
        self.assertEqual(self.events(logger.warning), ["hw.station.failure_repeating"])
        self.assertNotIn("sys.worker.recovery_scheduled", self.events(logger.info))
        self.assertNotIn("hw.serial.connected", self.events(logger.info))
        self.assertIn("hw.serial.connected", self.events(logger.debug))
        self.assertIn("hw.station.recovered", self.events(logger.info))

    def test_failed_print_during_a_failure_episode_is_logged_with_its_card(self):
        events = []

        def saved(event):
            events.append(event)
            result = Future()
            result.set_result(None)
            return result

        scale = WeighingScale(noise=3, fail_print=True, answers_before_stop=40)
        logger = self.run_worker(
            scale,
            on_event=saved,
            receipt_builder=lambda event: b"receipt",
        )

        completed = [e for e in events if isinstance(e, WeighingCompletedEvent)]
        self.assertEqual(len(completed), 1)
        failures = [c for c in logger.log.call_args_list if c.args[1] == "hw.serial.timeout"]
        self.assertEqual(len(failures), 2)
        self.assertEqual(failures[1].kwargs["rfid_card_uid"], "12345678")
        self.assertEqual(failures[1].kwargs["event_id"], completed[0].uuid)
        self.assertEqual([call.args[0] for call in failures], [logging.WARNING, logging.ERROR])
        self.assertNotIn("exc_info", failures[1].kwargs)
        self.assertIn("3/53 bytes", failures[1].kwargs["error"])
        self.assertEqual(len(scale.print_requests), 1)

    def test_stop_during_a_read_is_not_logged_as_a_device_failure(self):
        client = BlockingSerialClient()
        logger = Mock()
        with patch("suwol1000.get_logger") as get_logger:
            get_logger.return_value.bind.return_value = logger
            worker = WeighingStationWorker(client, station_id=7, polling_interval=0, retry_interval=0)
        thread = threading.Thread(target=worker.run)
        thread.start()
        self.assertTrue(client.reading.wait(timeout=5))

        worker.stop()
        thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        logger.log.assert_not_called()
        self.assertIn("sys.worker.terminated", self.events(logger.info))

    def test_healthy_start_logs_the_connection(self):
        logger = self.run_worker(ScriptedSerialClient())

        self.assertIn("hw.serial.connected", self.events(logger.info))
        logger.log.assert_not_called()
        logger.warning.assert_not_called()


class SerialClientRecoveryTests(unittest.TestCase):
    def test_observation_gap_is_five_seconds_independent_of_io_timeouts_and_polling(self):
        for read_timeout, write_timeout, polling_interval in (
            (2.0, 2.0, 0.1),
            (3.0, 3.0, 0.1),
            (0.75, 1.5, 0.25),
            (1.5, 1.0, 0.5),
        ):
            with self.subTest(read=read_timeout, write=write_timeout, polling=polling_interval):
                worker = WeighingStationWorker(
                    SerialClient("test-port", timeout=read_timeout, write_timeout=write_timeout),
                    station_id=7, polling_interval=polling_interval,
                )

                self.assertEqual(worker.cycle.max_sample_gap, 5.0)
                self.assertEqual(worker.cycle.max_sample_age, 2.0)

    def test_run_expires_observation_while_saving_without_new_serial_responses(self):
        now = [0.0]
        client = Mock(port="test-port", timeout=2.0, write_timeout=2.0)
        checks = []
        pending_result = Future()
        pending_event = WeighingCompletedEvent(
            station_id=7, rfid_card_uid="12345678", weight=120,
        )

        with patch("suwol1000.time.monotonic", side_effect=lambda: now[0]), patch(
            "suwol1000.get_logger", return_value=Mock(),
        ):
            worker = WeighingStationWorker(client, station_id=7)
            for at, weight in ((0, "0"), (1, "0"), (2, "120"), (3, "120")):
                now[0] = at
                client.send_and_receive.return_value = ResponsePacket(weight_value=Decimal(weight))
                worker.exchange(DisplayRequestPacket())
            self.assertTrue(worker.cycle.reserve(now=3))
            self.assertTrue(worker.cycle.consume(now=3))
            worker._pending_save = worker.last_event = pending_event
            worker._save_result = pending_result
            worker.state = WorkerState.SAVE

            def saving_without_io():
                snapshot = worker.get_telemetry()
                checks.append((
                    now[0], worker.cycle.invalidations, worker.cycle.state,
                    snapshot.sample_valid, snapshot.connection_status, worker._indicator_fault,
                ))
                if len(checks) == 4:
                    worker.stop_event.set()
                else:
                    now[0] = (7.999, 8.0, 8.001)[len(checks) - 1]
                return WorkerState.SAVE

            with patch.object(worker, "save", side_effect=saving_without_io):
                worker.run()

        self.assertEqual(checks, [
            (3, 0, CycleState.LOCKED, True, "connected", False),
            (7.999, 0, CycleState.LOCKED, True, "connected", False),
            (8.0, 1, CycleState.LOCKED, False, "error", True),
            (8.001, 1, CycleState.LOCKED, False, "error", True),
        ])
        self.assertEqual(client.send_and_receive.call_count, 4)
        self.assertIs(worker._pending_save, pending_event)
        self.assertIs(worker._save_result, pending_result)
        self.assertFalse(pending_result.done())
        client.connect.assert_not_called()
        client.disconnect.assert_called_once_with()

    def test_write_timeout_reconnects_with_a_distinct_warning_without_traceback(self):
        self.assert_failed_write_recovers(
            serial.SerialTimeoutException("Write timeout"),
            event="hw.serial.write_timeout", level=logging.WARNING, traceback=False,
        )

    def test_unexpected_failure_reconnects_and_preserves_error_traceback(self):
        self.assert_failed_write_recovers(
            RuntimeError("unexpected driver failure"),
            event="sys.worker.unexpected_error", level=logging.ERROR, traceback=True,
        )

    def assert_failed_write_recovers(self, failure, *, event, level, traceback):
        old_port = Mock(is_open=True)
        new_port = Mock(is_open=True)
        old_port.write.side_effect = failure
        new_port.read.return_value = frame()
        logger = Mock()

        def receive_sample(snapshot):
            if snapshot.sample_valid:
                worker.stop_event.set()

        with patch("suwol1000.get_logger") as get_logger, patch(
            "suwol1000.serial.Serial", side_effect=[old_port, new_port],
        ) as open_serial:
            get_logger.return_value.bind.return_value = logger
            worker = WeighingStationWorker(
                SerialClient("test-port"), station_id=7, polling_interval=0,
                retry_interval=0, on_telemetry=receive_sample,
            )
            worker.run()

        self.assertEqual(open_serial.call_count, 2)
        old_port.write.assert_called_once()
        old_port.read.assert_not_called()
        old_port.close.assert_called_once_with()
        new_port.write.assert_called_once()
        new_port.read.assert_called_once_with(53)
        new_port.close.assert_called_once_with()
        logger.log.assert_called_once()
        self.assertEqual(logger.log.call_args.args, (level, event))
        if traceback:
            self.assertIs(logger.log.call_args.kwargs["exc_info"], failure)
        else:
            self.assertNotIn("exc_info", logger.log.call_args.kwargs)
            self.assertIn(str(failure), logger.log.call_args.kwargs["error"])

    def test_empty_read_closes_and_reopens_port_before_publishing_fresh_weight(self):
        old_port = Mock(is_open=True)
        new_port = Mock(is_open=True)
        old_port.read.side_effect = [frame(weight=b"ST,NT,+     95kg"), b"", frame()]
        new_port.read.return_value = frame()
        samples = []
        logger = Mock()
        port_events = []
        old_port.close.side_effect = lambda: port_events.append("old_closed")

        def receive_sample(snapshot):
            samples.append(snapshot)
            if snapshot.sample_valid and snapshot.sample_seq == 2:
                worker.stop_event.set()

        def open_port(*args, **kwargs):
            port_events.append("open")
            if not old_port.read.called:
                return old_port
            return new_port

        with patch("suwol1000.get_logger") as get_logger, patch(
            "suwol1000.serial.Serial", side_effect=open_port,
        ) as open_serial:
            get_logger.return_value.bind.return_value = logger
            worker = WeighingStationWorker(
                SerialClient("test-port"), station_id=7, polling_interval=0,
                retry_interval=0, on_telemetry=receive_sample,
            )
            worker.run()

        self.assertEqual(open_serial.call_count, 2)
        self.assertEqual(port_events, ["open", "old_closed", "open"])
        old_port.close.assert_called_once_with()
        self.assertEqual(old_port.read.call_count, 2)
        new_port.read.assert_called_once_with(53)
        new_port.close.assert_called_once_with()
        self.assertEqual(
            [(sample.connection_status, sample.sample_valid) for sample in samples],
            [
                ("connected", False), ("connected", True), ("disconnected", False),
                ("connected", False), ("connected", True), ("disconnected", False),
            ],
        )
        self.assertEqual(
            [(sample.weight, sample.sample_seq) for sample in samples if sample.sample_valid],
            [(Decimal("95"), 1), (Decimal("120"), 2)],
        )
        self.assertEqual([c.args[1] for c in logger.log.call_args_list], ["hw.serial.timeout"])
        self.assertEqual(logger.log.call_args.args[0], logging.WARNING)
        self.assertNotIn("exc_info", logger.log.call_args.kwargs)

    def test_cancelled_empty_read_stops_without_logging_failure_or_reconnecting(self):
        port = Mock(is_open=True)
        reading = threading.Event()
        cancelled = threading.Event()
        logger = Mock()

        def read(size):
            reading.set()
            cancelled.wait(timeout=5)
            return b""

        port.read.side_effect = read
        port.cancel_read.side_effect = cancelled.set
        with patch("suwol1000.get_logger") as get_logger, patch(
            "suwol1000.serial.Serial", return_value=port,
        ) as open_serial:
            get_logger.return_value.bind.return_value = logger
            worker = WeighingStationWorker(
                SerialClient("test-port"), station_id=7, polling_interval=0, retry_interval=0,
            )
            thread = threading.Thread(target=worker.run, daemon=True)
            thread.start()
            try:
                self.assertTrue(reading.wait(timeout=5))
            finally:
                worker.stop()
                thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(open_serial.call_count, 1)
        port.cancel_read.assert_called_once_with()
        port.cancel_write.assert_called_once_with()
        port.close.assert_called_once_with()
        logger.log.assert_not_called()
        self.assertFalse(worker.get_telemetry().sample_valid)
        self.assertEqual(worker.get_telemetry().connection_status, "disconnected")
