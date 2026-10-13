from collections.abc import Callable
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys
import threading
import time
from types import TracebackType
from typing import Any

import structlog

from config import LoggingSettings


LOG_FILE_NAME = "client.log"
LOG_MAX_BYTES = LoggingSettings().max_bytes
LOG_BACKUP_COUNT = LoggingSettings().backup_count

# Third-party loggers that report every request at INFO.
QUIET_LOGGERS = ("httpx2",)


def setup_logging(
    log_dir: Path, *, settings: LoggingSettings | None = None,
) -> None:
    settings = settings or LoggingSettings()
    log_dir.mkdir(parents=True, exist_ok=True)

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        # Local time with its UTC offset, as printed on the receipts.
        structlog.processors.TimeStamper(fmt="%Y-%m-%dT%H:%M:%S.%f%:z", utc=False),
        structlog.processors.StackInfoRenderer(),
    ]
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    def formatter(colors: bool) -> logging.Formatter:
        # Records from the standard library (websockets, asyncio, ...) share the format.
        return structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.dev.ConsoleRenderer(colors=colors),
            ],
        )

    file_handler = RotatingFileHandler(
        log_dir / LOG_FILE_NAME,
        maxBytes=settings.max_bytes,
        backupCount=settings.backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(settings.level)
    file_handler.setFormatter(formatter(colors=False))
    handlers: list[logging.Handler] = [file_handler]

    # pythonw.exe runs without a console and leaves sys.stderr as None.
    if sys.stderr is not None:
        console_formatter = formatter(colors=sys.stderr.isatty())
        # Read sys.stderr after creating the renderer: colorama may wrap it on Windows.
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(settings.level)
        console_handler.setFormatter(console_formatter)
        handlers.append(console_handler)

    logging.basicConfig(level=settings.level, handlers=handlers, force=True)
    for name in QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
    logging.captureWarnings(True)
    _install_exception_hooks()


def _install_exception_hooks() -> None:
    def log_uncaught(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: TracebackType | None,
    ) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        structlog.stdlib.get_logger().critical(
            "sys.process.uncaught_exception",
            exc_info=(exc_type, exc_value, exc_traceback),
        )

    def log_uncaught_in_thread(args: threading.ExceptHookArgs) -> None:
        if issubclass(args.exc_type, SystemExit):
            return
        structlog.stdlib.get_logger().critical(
            "sys.thread.uncaught_exception",
            thread=args.thread.name if args.thread else None,
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    sys.excepthook = log_uncaught
    threading.excepthook = log_uncaught_in_thread


def _exception_types(exc: BaseException) -> frozenset[type[BaseException]]:
    # A TaskGroup failure arrives as a group; tell its causes apart.
    if isinstance(exc, BaseExceptionGroup):
        return frozenset().union(*(_exception_types(e) for e in exc.exceptions))
    return frozenset((type(exc),))


class RepeatedFailureLog:
    """Log a failure once, then periodic summaries until the failures stop.

    A powered-off scale fails the same way about once a second, and an
    unreachable server every few seconds; logging every occurrence would fill
    the log with identical tracebacks.
    """

    def __init__(
        self,
        logger: Any,
        scope: str,
        *,
        report_interval: float = 600.0,
        settle_time: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.logger = logger
        self.scope = scope
        self.report_interval = report_interval
        # Intermittent failures stay one episode instead of logging each recovery.
        self.settle_time = settle_time
        self.clock = clock
        self._reset()

    def _reset(self):
        self.count = 0
        self._logged_kinds: set[tuple[str, frozenset[type[BaseException]]]] = set()
        self._started_at = self._last_failure_at = self._reported_at = 0.0
        self._unreported = 0
        self._last_error = ""

    @property
    def failing(self) -> bool:
        return self.count > 0

    def failure(
        self,
        event: str,
        exc: BaseException,
        *,
        level: int = logging.ERROR,
        traceback: bool = True,
        always_log: bool = False,
        **kw: Any,
    ):
        now = self.clock()
        if not self.count:
            self._started_at = self._reported_at = now
        self.count += 1
        self._last_failure_at = now

        kind = (event, _exception_types(exc))
        if always_log or kind not in self._logged_kinds:
            if not always_log:
                self._logged_kinds.add(kind)
            if traceback:
                self.logger.log(level, event, exc_info=exc, **kw)
            else:
                self.logger.log(level, event, error=repr(exc), **kw)
            return

        self._unreported += 1
        self._last_error = f"{event}: {exc!r}"
        if now - self._reported_at >= self.report_interval:
            self.logger.warning(
                f"{self.scope}.failure_repeating",
                failures=self._unreported,
                total_failures=self.count,
                failing_seconds=round(now - self._started_at),
                last_error=self._last_error,
            )
            self._reported_at = now
            self._unreported = 0

    def success(self):
        if not self.count or self.clock() - self._last_failure_at < self.settle_time:
            return
        self.logger.info(
            f"{self.scope}.recovered",
            total_failures=self.count,
            failing_seconds=round(self._last_failure_at - self._started_at),
        )
        self._reset()
