from concurrent.futures import Future
from decimal import Decimal
import threading
import unittest
from unittest.mock import Mock, patch

import serial

from events import WeighingCompletedEvent
from logconfig import RepeatedFailureLog
from suwol1000 import (
    PrinterRequestPacket, ResponsePacket, VoiceCode, WeighingStationWorker,
)


class ScriptedSerialClient:
    """Fails a fixed number of exchanges like a powered-off scale, then answers."""

    port = "test-port"

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
            raise ValueError("Invalid response packet length: 0 bytes")
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

    def send_and_receive(self, request):
        self.clock += 1
        if self.failed_exchanges:
            return super().send_and_receive(request)
        if isinstance(request, PrinterRequestPacket) and self.fail_print:
            raise ValueError("Invalid response packet length: 3 bytes")
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
        raise ValueError("Invalid response packet length: 0 bytes")


class PoweredOffStationTests(unittest.TestCase):
    def run_worker(self, client: ScriptedSerialClient, **worker_kwargs) -> Mock:
        logger = Mock()
        with patch("suwol1000.get_logger") as get_logger:
            get_logger.return_value.bind.return_value = logger
            worker = WeighingStationWorker(client, polling_interval=0, retry_interval=0, **worker_kwargs)
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

        self.assertEqual([c.args[1] for c in logger.log.call_args_list], ["hw.protocol.parse_error"])
        self.assertEqual(self.events(logger.warning), ["hw.station.failure_repeating"] * 5)
        recovered = [c for c in logger.info.call_args_list if c.args[0] == "hw.station.recovered"]
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].kwargs["total_failures"], 3600)
        logger.exception.assert_not_called()

    def test_missing_port_logs_the_first_failure_and_quiet_reconnects(self):
        logger = self.run_worker(ScriptedSerialClient(failed_connects=1200))

        self.assertEqual([c.args[1] for c in logger.log.call_args_list], ["hw.serial.connection_lost"])
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

        logger = self.run_worker(
            WeighingScale(noise=3, fail_print=True, answers_before_stop=40),
            on_event=saved,
            receipt_builder=lambda event: b"receipt",
        )

        completed = [e for e in events if isinstance(e, WeighingCompletedEvent)]
        self.assertEqual(len(completed), 1)
        failures = [c for c in logger.log.call_args_list if c.args[1] == "hw.protocol.parse_error"]
        self.assertEqual(len(failures), 2)
        self.assertEqual(failures[1].kwargs["rfid_card_uid"], "12345678")
        self.assertEqual(failures[1].kwargs["event_id"], completed[0].uuid)
        self.assertIn("3 bytes", str(failures[1].kwargs["exc_info"]))

    def test_stop_during_a_read_is_not_logged_as_a_device_failure(self):
        client = BlockingSerialClient()
        logger = Mock()
        with patch("suwol1000.get_logger") as get_logger:
            get_logger.return_value.bind.return_value = logger
            worker = WeighingStationWorker(client, polling_interval=0, retry_interval=0)
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
