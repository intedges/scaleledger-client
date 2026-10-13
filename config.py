from dataclasses import dataclass, field
from pathlib import Path
import re
import tomllib
from urllib.parse import urlsplit, urlunsplit

import httpx2

from paths import AppPaths, program_dir


# DNS-style names; IP literals are left to the HTTP client's own checks.
HOST_NAME = re.compile(r"[a-z0-9_-]+(\.[a-z0-9_-]+)*\.?")


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class LoggingSettings:
    level: str = "INFO"
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 10


@dataclass(frozen=True)
class Settings:
    server_url: str
    logging: LoggingSettings = field(default_factory=LoggingSettings)


def load_settings(path: Path) -> Settings:
    try:
        # Accept the BOM that Windows Notepad may add when saving as UTF-8.
        data = tomllib.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        hint = ""
        if path.with_name(f"{path.name}.txt").exists():
            # Notepad appends .txt, and Explorer hides the extension.
            hint = f" ({path.name}.txt exists: remove its .txt extension)"
        raise ConfigError(f"{path} not found; copy config.example.toml and set server_url{hint}") from None
    except OSError as e:
        raise ConfigError(f"{path} cannot be read: {e}") from None
    except UnicodeDecodeError:
        raise ConfigError(f"{path} must be saved as UTF-8") from None
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"{path} is not valid TOML: {e}") from None

    match data:
        case {"server_url": str(url)}:
            try:
                server_url = normalize_server_url(url)
            except ValueError as e:
                raise ConfigError(f"{path}: server_url {e}") from None
        case _:
            raise ConfigError(f"{path} must set server_url to a string")

    try:
        logging = _load_logging_settings(data.get("logging", {}))
    except ValueError as e:
        raise ConfigError(f"{path}: [logging] {e}") from None
    return Settings(server_url=server_url, logging=logging)


def _load_logging_settings(data: object) -> LoggingSettings:
    if not isinstance(data, dict):
        raise ValueError("must be a table")
    unknown = data.keys() - {"level", "max_bytes", "backup_count"}
    if unknown:
        raise ValueError(f"contains unknown keys: {', '.join(sorted(unknown))}")

    defaults = LoggingSettings()
    level = data.get("level", defaults.level)
    if not isinstance(level, str) or level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError("level must be DEBUG, INFO, WARNING, ERROR or CRITICAL")
    max_bytes = data.get("max_bytes", defaults.max_bytes)
    backup_count = data.get("backup_count", defaults.backup_count)
    for name, value in (("max_bytes", max_bytes), ("backup_count", backup_count)):
        # bool is an int subclass, but TOML true/false is not a size or a count.
        if type(value) is not int or value < 1:
            raise ValueError(f"{name} must be an integer of at least 1")
    return LoggingSettings(level=level, max_bytes=max_bytes, backup_count=backup_count)


def normalize_server_url(url: str) -> str:
    try:
        parts = urlsplit(url.strip())
        parts.port  # Raises ValueError on a malformed port.
    except ValueError:
        raise ValueError(f"is malformed: {url!r}") from None
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError(f"must start with http:// or https:// and name a host: {url!r}")
    if parts.username is not None or parts.query or parts.fragment:
        raise ValueError(f"must not contain credentials, a query or a fragment: {url!r}")
    if ":" not in parts.hostname and not HOST_NAME.fullmatch(parts.hostname):
        raise ValueError(f"has an invalid host name: {parts.hostname!r}")
    # urlsplit lowercases the scheme; keep any path prefix without its trailing slash.
    normalized = urlunsplit(parts._replace(path=parts.path.rstrip("/")))
    try:
        # The HTTP client is stricter than urlsplit, e.g. about 192.168.0.256 or 192.168.001.010.
        # Share its percent-encoded path with WebSocket handshakes as well.
        normalized = str(httpx2.URL(normalized))
    except httpx2.InvalidURL as e:
        raise ValueError(f"is invalid: {e}") from None
    return normalized


def websocket_url(server_url: str) -> str:
    parts = urlsplit(server_url)
    return urlunsplit(parts._replace(scheme={"http": "ws", "https": "wss"}[parts.scheme]))
