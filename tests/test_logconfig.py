import asyncio
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import re
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, call, patch
import warnings

import structlog

from config import LoggingSettings
from logconfig import (
    LOG_BACKUP_COUNT, LOG_MAX_BYTES, RepeatedFailureLog, setup_logging,
)


class SetupLoggingTests(unittest.TestCase):
    def setUp(self):
        self.log_dir = Path(self.enterContext(tempfile.TemporaryDirectory())) / "logs"
        root = logging.getLogger()
        saved = (root.handlers[:], root.level, sys.excepthook, threading.excepthook)

        def restore():
            for handler in root.handlers[:]:
                root.removeHandler(handler)
                handler.close()
            handlers, level, sys.excepthook, threading.excepthook = saved
            for handler in handlers:
                root.addHandler(handler)
            root.setLevel(level)
            logging.getLogger("httpx2").setLevel(logging.NOTSET)
            logging.captureWarnings(False)
            structlog.reset_defaults()

        # Close the log file before the directory is removed (required on Windows).
        self.addCleanup(restore)
        # pythonw.exe starts without a console.
        with patch.object(sys, "stderr", None):
            setup_logging(self.log_dir)

    def read_log(self) -> str:
        for handler in logging.getLogger().handlers:
            handler.flush()
        return (self.log_dir / "client.log").read_text(encoding="utf-8")

    def test_writes_application_and_library_records_to_a_rotating_file(self):
        logger = structlog.stdlib.get_logger()
        logger.info("test.event", port="COM3")
        logger.debug("test.debug_event")
        logging.getLogger("websockets.client").info("connect failed; reconnecting in 3.1 seconds")
        logging.getLogger("httpx2").info("HTTP Request: POST http://localhost:8000/ 200 OK")
        logging.getLogger("tortoise.db_client").debug("SELECT 1")
        try:
            raise ValueError("Invalid response packet length: 0 bytes")
        except ValueError:
            logger.exception("test.failed")
        with warnings.catch_warnings():
            warnings.simplefilter("always")
            warnings.warn("naive datetime", RuntimeWarning)

        text = self.read_log()
        self.assertRegex(text, r"test\.event\s+.*port=COM3")
        self.assertIn("connect failed; reconnecting", text)
        self.assertIn("ValueError: Invalid response packet length: 0 bytes", text)
        self.assertIn("RuntimeWarning: naive datetime", text)
        for unwanted in ("test.debug_event", "HTTP Request", "SELECT 1", "\x1b["):
            self.assertNotIn(unwanted, text)
        handlers = logging.getLogger().handlers
        self.assertEqual(len(handlers), 1)
        self.assertIs(type(handlers[0]), RotatingFileHandler)
        self.assertEqual((handlers[0].maxBytes, handlers[0].backupCount), (LOG_MAX_BYTES, LOG_BACKUP_COUNT))

    def test_timestamps_use_local_time_with_offset(self):
        structlog.stdlib.get_logger().info("test.event")
        logging.getLogger("asyncio").warning("library record")

        offset = re.escape(datetime.now().astimezone().strftime("%:z"))
        lines = self.read_log().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            self.assertRegex(line, rf"^\d{{4}}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{{6}}{offset} ")

    def test_configured_level_filters_application_and_explicit_library_levels(self):
        with patch.object(sys, "stderr", None):
            setup_logging(self.log_dir, settings=LoggingSettings(level="ERROR"))
        logger = structlog.stdlib.get_logger()
        logger.warning("test.hidden_application_warning")
        logger.error("test.visible_application_error")
        # Child loggers with explicit levels bypass the root logger's level.
        logging.getLogger("httpx2").warning("test.hidden_library_warning")
        logging.getLogger("httpx2").error("test.visible_library_error")

        text = self.read_log()
        self.assertIn("test.visible_application_error", text)
        self.assertIn("test.visible_library_error", text)
        self.assertNotIn("test.hidden_application_warning", text)
        self.assertNotIn("test.hidden_library_warning", text)

    def test_debug_level_includes_application_diagnostics(self):
        with patch.object(sys, "stderr", None):
            setup_logging(self.log_dir, settings=LoggingSettings(level="DEBUG"))
        structlog.stdlib.get_logger().debug("test.visible_diagnostic")
        self.assertIn("test.visible_diagnostic", self.read_log())

    def test_configured_size_and_retention_limit_log_files(self):
        with patch.object(sys, "stderr", None):
            setup_logging(self.log_dir, settings=LoggingSettings(max_bytes=220, backup_count=2))
        handler = logging.getLogger().handlers[0]
        self.assertEqual((handler.maxBytes, handler.backupCount), (220, 2))
        logger = structlog.stdlib.get_logger()
        for number in range(20):
            logger.info(f"record-{number:03d}", detail="x" * 100)

        self.assertIn("record-019", self.read_log())
        self.assertEqual(
            sorted(path.name for path in self.log_dir.iterdir()),
            ["client.log", "client.log.1", "client.log.2"],
        )
        retained = "".join(path.read_text(encoding="utf-8") for path in self.log_dir.iterdir())
        self.assertIn("record-018", retained)
        self.assertNotIn("record-000", retained)

    def test_uncaught_exceptions_are_logged(self):
        def fail():
            raise RuntimeError("thread failed")

        thread = threading.Thread(target=fail, name="WeighingStation-1-COM3")
        thread.start()
        thread.join()
        try:
            raise RuntimeError("process failed")
        except RuntimeError:
            sys.excepthook(*sys.exc_info())

        text = self.read_log()
        self.assertRegex(text, r"sys\.thread\.uncaught_exception\s+.*thread=WeighingStation-1-COM3")
        self.assertIn("RuntimeError: thread failed", text)
        self.assertIn("sys.process.uncaught_exception", text)
        self.assertIn("RuntimeError: process failed", text)

    def test_unhandled_asyncio_exceptions_reach_the_file(self):
        loop = asyncio.new_event_loop()
        try:
            # What the default handler receives for a task whose exception nobody awaited.
            loop.call_exception_handler({
                "message": "Task exception was never retrieved",
                "exception": RuntimeError("task failed"),
            })
        finally:
            loop.close()

        text = self.read_log()
        self.assertRegex(text, r"\[error\s*\] Task exception was never retrieved")
        self.assertIn("RuntimeError: task failed", text)


class RepeatedFailureLogTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.logger = Mock()
        self.failures = RepeatedFailureLog(
            self.logger, "hw.station", report_interval=600, settle_time=30, clock=lambda: self.now,
        )

    def fail_every_second(self, seconds: int, event="hw.protocol.parse_error", exc=None):
        for _ in range(seconds):
            self.failures.failure(event, exc or ValueError("Invalid response packet length: 0 bytes"))
            self.now += 1

    def test_logs_first_failure_then_periodic_summaries(self):
        error = ValueError("Invalid response packet length: 0 bytes")
        self.failures.failure("hw.protocol.parse_error", error, port="COM3")
        self.now += 1
        self.fail_every_second(3600)

        self.logger.log.assert_called_once_with(
            logging.ERROR, "hw.protocol.parse_error", exc_info=error, port="COM3",
        )
        summaries = self.logger.warning.call_args_list
        self.assertEqual(len(summaries), 6)
        self.assertEqual(summaries[0], call(
            "hw.station.failure_repeating",
            failures=600,
            total_failures=601,
            failing_seconds=600,
            last_error="hw.protocol.parse_error: ValueError('Invalid response packet length: 0 bytes')",
        ))
        self.assertEqual(summaries[-1].kwargs["total_failures"], 3601)
        self.assertTrue(self.failures.failing)

    def test_logs_each_new_kind_of_failure_once(self):
        self.fail_every_second(5, "hw.protocol.parse_error")
        self.fail_every_second(5, "hw.serial.connection_lost", OSError("port gone"))
        self.fail_every_second(5, "hw.protocol.parse_error")

        self.assertEqual(
            [c.args[1] for c in self.logger.log.call_args_list],
            ["hw.protocol.parse_error", "hw.serial.connection_lost"],
        )
        self.assertEqual(self.failures.count, 15)

    def test_recovers_only_after_failures_settle(self):
        self.fail_every_second(120)
        self.failures.success()
        self.now += 28  # 29 seconds after the last failure
        self.failures.success()
        self.logger.info.assert_not_called()

        self.now += 1
        self.failures.success()
        self.logger.info.assert_called_once_with(
            "hw.station.recovered", total_failures=120, failing_seconds=119,
        )
        self.assertFalse(self.failures.failing)

        self.failures.success()
        self.fail_every_second(1)
        self.assertEqual(self.logger.log.call_count, 2)

    def test_exception_groups_are_told_apart_by_their_causes(self):
        self.failures.failure("net.worker.failed", ExceptionGroup("tasks", [OSError("closed")]))
        self.failures.failure("net.worker.failed", ExceptionGroup("tasks", [OSError("closed again")]))
        self.failures.failure("net.worker.failed", ExceptionGroup("tasks", [KeyError("id")]))

        self.assertEqual(self.logger.log.call_count, 2)
        self.assertIsInstance(self.logger.log.call_args.kwargs["exc_info"].exceptions[0], KeyError)

    def test_failures_without_traceback_log_the_error_text(self):
        error = OSError("All connection attempts failed")
        self.failures.failure("sys.boot.network.offline", error, level=logging.WARNING, traceback=False)

        self.logger.log.assert_called_once_with(
            logging.WARNING, "sys.boot.network.offline", error=repr(error),
        )

    def test_always_logged_failures_do_not_hide_later_ones(self):
        self.fail_every_second(5)
        print_error = ValueError("Invalid response packet length: 3 bytes")
        for _ in range(2):
            self.failures.failure("hw.protocol.parse_error", print_error, always_log=True, rfid_card_uid="12345678")

        self.assertEqual(self.logger.log.call_count, 3)
        self.assertEqual(self.logger.log.call_args.kwargs["rfid_card_uid"], "12345678")
        self.assertEqual(self.failures.count, 7)

    def test_intermittent_failures_stay_one_episode(self):
        for _ in range(100):
            self.fail_every_second(1)
            self.failures.success()
            self.now += 10

        self.assertEqual(self.logger.log.call_count, 1)
        self.logger.info.assert_not_called()
        self.assertEqual(self.failures.count, 100)
