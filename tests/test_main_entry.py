import asyncio
import io
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import main
from api import AuthDegradedError
from config import LoggingSettings
from events import WeighingCompletedEvent
from paths import AppPaths
from utils import acquire_instance_lock


class MainEntryTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.logger = Mock()
        self.run_client = AsyncMock()
        for patcher in (
            patch("main.program_dir", return_value=self.root),
            patch("main.setup_logging"),
            patch("main.get_logger", return_value=self.logger),
            patch("main.run_client", self.run_client),
            patch("main.WindowsStopEvent", return_value=None),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def write_config(self, name="config.toml", url="http://127.0.0.1:8000") -> Path:
        path = self.root / name
        path.write_text(f'server_url = "{url}"\n', encoding="utf-8")
        return path

    def test_runs_with_the_server_url_from_the_program_folder(self):
        self.write_config()

        self.assertEqual(main.main([]), 0)

        self.run_client.assert_awaited_once_with("http://127.0.0.1:8000", self.root / "data" / "db.sqlite3", None)
        main.setup_logging.assert_called_once_with(self.root / "logs", settings=LoggingSettings())

    def test_development_config_option_overrides_the_default_file(self):
        self.write_config()
        other = self.write_config("dev.toml", "http://192.168.0.10:8000")

        self.assertEqual(main.main(["--config", str(other)]), 0)

        self.assertEqual(self.run_client.await_args.args[0], "http://192.168.0.10:8000")

    def test_invalid_config_keeps_running_without_network(self):
        for content in (None, 'server_url = "localhost:8000"\n'):
            with self.subTest(content=content):
                if content is not None:
                    (self.root / "config.toml").write_text(content, encoding="utf-8")
                self.run_client.reset_mock()

                self.assertEqual(main.main([]), 0)

                self.run_client.assert_awaited_once_with(None, self.root / "data" / "db.sqlite3", None)
                event, kwargs = self.logger.error.call_args.args[0], self.logger.error.call_args.kwargs
                self.assertEqual((event, kwargs["action"]), ("sys.config.invalid", "disable_network"))
                self.assertIn("config.toml", kwargs["error"])

    def test_second_instance_exits_without_starting_or_opening_logs(self):
        self.write_config()
        held = acquire_instance_lock(self.root / "data" / "client.lock")
        self.addCleanup(held.close)

        console = io.StringIO()
        for stderr in (console, None):
            with self.subTest(console=stderr is not None), patch("main.sys.stderr", stderr):
                self.assertEqual(main.main([]), 1)

        self.assertIn("이미 실행 중", console.getvalue())
        # Both console and pythonw launches leave the running process's log alone.
        main.setup_logging.assert_not_called()
        main.WindowsStopEvent.assert_not_called()
        self.run_client.assert_not_awaited()
        self.logger.error.assert_not_called()
        self.assertFalse((self.root / "logs").exists())

        held.close()
        self.assertEqual(main.main([]), 0)
        self.run_client.assert_awaited_once()

    def test_passes_configured_logging_policy(self):
        path = self.write_config()
        with path.open("a") as stream:
            stream.write('[logging]\nlevel = "WARNING"\nmax_bytes = 2048\nbackup_count = 2\n')
        self.assertEqual(main.main([]), 0)
        main.setup_logging.assert_called_once_with(
            self.root / "logs",
            settings=LoggingSettings(level="WARNING", max_bytes=2048, backup_count=2),
        )

    def test_check_config_has_no_runtime_side_effects(self):
        self.write_config()
        self.assertEqual(main.main(["--check-config"]), 0)
        self.run_client.assert_not_awaited()
        main.setup_logging.assert_not_called()
        self.assertFalse((self.root / "data").exists())
        self.assertFalse((self.root / "logs").exists())

    def test_check_config_reports_errors_without_starting_offline(self):
        with patch("main.sys.stderr", None):
            self.assertEqual(main.main(["--check-config"]), 2)
        self.run_client.assert_not_awaited()
        main.setup_logging.assert_not_called()
        self.assertFalse((self.root / "data").exists())

    def test_logging_failure_releases_instance_lock(self):
        self.write_config()
        main.setup_logging.side_effect = PermissionError("logs not writable")
        with self.assertRaises(PermissionError):
            main.main([])
        lock = acquire_instance_lock(self.root / "data" / "client.lock")
        self.assertIsNotNone(lock)
        lock.close()

    def test_stop_does_not_require_valid_config(self):
        with patch("main.stop_client", return_value=0) as stop:
            self.assertEqual(main.main(["--stop"]), 0)
        stop.assert_called_once_with(AppPaths(self.root))
        self.run_client.assert_not_awaited()
        main.setup_logging.assert_not_called()


class CooperativeShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_waits_for_pending_writes_before_closing_the_database(self):
        started = asyncio.Event()
        stop = asyncio.Event()
        order = []

        async def run():
            started.set()
            try:
                await asyncio.Future()
            finally:
                await asyncio.sleep(0)
                order.append("writes_drained")

        async def close():
            order.append("database_closed")

        client = Mock(run=run, close=AsyncMock(side_effect=close), _cleanup_started=False)
        with patch("main.HeadlessClient", return_value=client):
            task = asyncio.create_task(main.run_client(None, Path("unused.sqlite3"), stop))
            await asyncio.wait_for(started.wait(), timeout=1)
            stop.set()
            await asyncio.wait_for(task, timeout=1)
        self.assertEqual(order, ["writes_drained", "database_closed"])
        client.close.assert_awaited_once()

    async def test_client_failure_propagates_after_closing(self):
        client = Mock(
            run=AsyncMock(side_effect=RuntimeError("database failed")),
            close=AsyncMock(), _cleanup_started=False,
        )
        with patch("main.HeadlessClient", return_value=client):
            with self.assertRaisesRegex(RuntimeError, "database failed"):
                await main.run_client(None, Path("unused.sqlite3"), asyncio.Event())
        client.close.assert_awaited_once()

    async def test_stop_during_cleanup_waits_and_preserves_the_client_failure(self):
        closing = asyncio.Event()
        finish_cleanup = asyncio.Event()
        stop = asyncio.Event()
        stop_observed = asyncio.Event()
        closed = []

        async def close():
            closing.set()
            await finish_cleanup.wait()
            closed.append("database_closed")

        client = Mock(
            run=AsyncMock(side_effect=RuntimeError("database failed")),
            close=AsyncMock(side_effect=close),
            _cleanup_started=False,
        )
        logger = Mock(info=Mock(side_effect=lambda *args: stop_observed.set()))
        with patch("main.HeadlessClient", return_value=client), patch("main.get_logger", return_value=logger):
            task = asyncio.create_task(main.run_client(None, Path("unused.sqlite3"), stop))
            try:
                await asyncio.wait_for(closing.wait(), timeout=1)
                stop.set()
                # The stop message confirms run_client has observed the request
                # while close is still awaiting its database cleanup.
                await asyncio.wait_for(stop_observed.wait(), timeout=1)
            finally:
                finish_cleanup.set()
            with self.assertRaisesRegex(RuntimeError, "database failed"):
                await asyncio.wait_for(task, timeout=1)
        self.assertEqual(closed, ["database_closed"])
        client.close.assert_awaited_once()

    async def test_stop_during_failed_client_drain_preserves_records_and_failure(self):
        write_started = asyncio.Event()
        finish_write = asyncio.Event()
        draining = asyncio.Event()
        stop = asyncio.Event()
        stop_observed = asyncio.Event()
        order = []
        consumers = []
        failure = RuntimeError("auth reset database unavailable")
        client = main.HeadlessClient(None, Path("unused.sqlite3"))
        client.logger = Mock()
        client.setup = AsyncMock()
        client.api_client = Mock()
        client.access_token = "cached"
        client.gateway_id = 1
        client.bootstrap = AsyncMock(return_value=True)
        client.run_active_loop = AsyncMock(side_effect=AuthDegradedError("token rejected"))
        client.wipe_local_auth = AsyncMock(side_effect=failure)
        client.station_manager.stop_all = Mock()
        event = WeighingCompletedEvent(rfid_card_uid="12345678", weight=41)

        async def save_record(**kwargs):
            consumers.append(asyncio.current_task())
            write_started.set()
            await finish_write.wait()
            order.append("record_saved")
            return SimpleNamespace(uuid=event.uuid, weight=event.weight)

        async def restore():
            client.handle_hardware_event(event)
            await write_started.wait()

        join_records = client.event_queue.join

        async def drain_records():
            draining.set()
            await join_records()

        async def close():
            order.append("database_closed")

        client.restore_local_state = restore
        client.close = AsyncMock(side_effect=close)
        logger = Mock(info=Mock(side_effect=lambda *args: stop_observed.set()))
        with patch("main.HeadlessClient", return_value=client), patch(
            "main.Record.create", side_effect=save_record,
        ), patch.object(client.event_queue, "join", side_effect=drain_records), patch(
            "main.get_logger", return_value=logger,
        ):
            task = asyncio.create_task(main.run_client(None, Path("unused.sqlite3"), stop))
            try:
                # Use the real run, network error handler and event consumer.
                # Wait until the fatal auth-reset error has started draining.
                await asyncio.wait_for(draining.wait(), timeout=1)
                stop.set()
                await asyncio.wait_for(stop_observed.wait(), timeout=1)
                finish_write.set()
                with self.assertRaises(RuntimeError) as raised:
                    await asyncio.wait_for(task, timeout=1)
                self.assertIs(raised.exception, failure)
                self.assertEqual(order, ["record_saved", "database_closed"])
                self.assertTrue(consumers[0].cancelled())
                await asyncio.wait_for(join_records(), timeout=1)
                client.close.assert_awaited_once()
                client.wipe_local_auth.assert_awaited_once()
            finally:
                finish_write.set()
                task.cancel()
                for consumer in consumers:
                    consumer.cancel()
                await asyncio.gather(task, *consumers, return_exceptions=True)

    async def test_event_failure_still_waits_for_client_cleanup(self):
        order = []

        async def run():
            try:
                await asyncio.Future()
            finally:
                await asyncio.sleep(0)
                order.append("writes_drained")

        async def close():
            order.append("database_closed")

        client = Mock(run=run, close=AsyncMock(side_effect=close), _cleanup_started=False)
        stop = Mock(is_set=Mock(side_effect=OSError("event handle failed")))
        with patch("main.HeadlessClient", return_value=client):
            with self.assertRaisesRegex(OSError, "event handle failed"):
                await main.run_client(None, Path("unused.sqlite3"), stop)
        self.assertEqual(order, ["writes_drained", "database_closed"])


class ProcessShutdownTests(unittest.TestCase):
    def test_stop_closes_sqlite_worker_before_process_exits(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        database = root / "data" / "db.sqlite3"
        # Start the real ORM in run_client's child task without a pre-existing
        # Tortoise context. A leaked non-daemon SQLite thread prevents process exit
        # even after run_client returns and its shutdown message has been logged.
        script = """
import asyncio
from pathlib import Path
import sys
from main import run_client

database = Path(sys.argv[1])
class StopAfterDatabaseCreated:
    def is_set(self):
        return database.exists()

asyncio.run(run_client(None, database, StopAfterDatabaseCreated()))
print("client-stopped", flush=True)
"""
        result = subprocess.run(
            [sys.executable, "-W", "error", "-c", script, str(database)],
            cwd=Path(main.__file__).resolve().parent,
            capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(database.exists())
        self.assertIn("client-stopped", result.stdout)


class StopCommandTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.paths = AppPaths(self.root)
        self.paths.data_dir.mkdir()
        self.paths.lock_file.touch()
        self.stream = io.StringIO()
        self.enterContext(patch("main.sys", SimpleNamespace(
            platform="win32", stdout=self.stream, stderr=self.stream,
        )))

    def test_stop_waits_for_the_running_instance_to_release_its_lock(self):
        released = Mock()
        with patch("main.request_stop", return_value=True), patch(
            "main.acquire_instance_lock", side_effect=[None, released],
        ), patch("main.time.sleep") as sleep:
            self.assertEqual(main.stop_client(self.paths), 0)
        sleep.assert_called_once_with(0.2)
        released.close.assert_called_once()

    def test_a_missing_event_with_a_held_lock_is_not_reported_as_stopped(self):
        with patch("main.request_stop", return_value=False), patch(
            "main.acquire_instance_lock", return_value=None,
        ):
            self.assertEqual(main.stop_client(self.paths), 1)
        self.assertIn("시작 중", self.stream.getvalue())

    def test_shutdown_timeout_reports_failure_without_releasing_the_running_lock(self):
        with patch("main.request_stop", return_value=True), patch(
            "main.acquire_instance_lock", return_value=None,
        ):
            self.assertEqual(main.stop_client(self.paths, timeout=0), 1)
        self.assertIn("종료가 아직 완료되지 않았습니다", self.stream.getvalue())


class InstanceLockTests(unittest.TestCase):
    def test_lock_is_exclusive_until_released(self):
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "data" / "client.lock"

        first = acquire_instance_lock(path)
        self.assertIsNotNone(first)
        self.assertIsNone(acquire_instance_lock(path))
        first.close()

        second = acquire_instance_lock(path)
        self.assertIsNotNone(second)
        second.close()
