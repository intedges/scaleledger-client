from pathlib import Path
import tempfile
import unittest

import httpx2
from websockets.client import ClientProtocol
from websockets.server import ServerProtocol
from websockets.uri import parse_uri

import config
from config import (
    AppPaths, ConfigError, LoggingSettings, Settings, load_settings, normalize_server_url, program_dir,
    websocket_url,
)


class LoadSettingsTests(unittest.TestCase):
    def setUp(self):
        self.path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "config.toml"

    def load(self, content: str | bytes) -> Settings:
        if isinstance(content, str):
            content = content.encode()
        self.path.write_bytes(content)
        return load_settings(self.path)

    def test_reads_and_normalizes_server_url(self):
        for url, expected in (
            ("http://127.0.0.1:8000", "http://127.0.0.1:8000"),
            (" http://192.168.0.10:8000/\\n", "http://192.168.0.10:8000"),
            ("HTTP://Server-PC:8000", "http://server-pc:8000"),
            ("https://scaleledger.example/prefix/", "https://scaleledger.example/prefix"),
            ("http://[::1]:8000", "http://[::1]:8000"),
        ):
            with self.subTest(url=url):
                self.assertEqual(self.load(f'server_url = "{url}"\n').server_url, expected)

    def test_accepts_utf8_with_bom_and_korean_comments(self):
        content = '\ufeff# 관리 프로그램 주소\nserver_url = "http://127.0.0.1:8000"\n'
        self.assertEqual(self.load(content.encode()).server_url, "http://127.0.0.1:8000")

    def test_rejects_missing_or_unreadable_file(self):
        with self.assertRaisesRegex(ConfigError, "not found"):
            load_settings(self.path)
        self.path.with_name("config.toml.txt").write_text('server_url = "http://127.0.0.1:8000"\n')
        with self.assertRaisesRegex(ConfigError, r"config\.toml\.txt exists"):
            load_settings(self.path)
        self.path.mkdir()
        with self.assertRaisesRegex(ConfigError, "cannot be read"):
            load_settings(self.path)
        self.path.rmdir()
        with self.assertRaisesRegex(ConfigError, "UTF-8"):
            self.load('# 관리 프로그램 주소\nserver_url = "http://127.0.0.1:8000"\n'.encode("cp949"))
        with self.assertRaisesRegex(ConfigError, r"not valid TOML: .*line 2"):
            self.load('# comment\nserver_url = "http://127.0.0.1:8000\n')

    def test_rejects_missing_or_invalid_server_url(self):
        for content in (
            "",
            "server_url = 8000\n",
            '[server]\nurl = "http://127.0.0.1:8000"\n',
        ):
            with self.subTest(content=content), self.assertRaisesRegex(ConfigError, "server_url"):
                self.load(content)
        for url in (
            "",
            "127.0.0.1:8000",
            "ws://127.0.0.1:8000",
            "http://",
            "http://127.0.0.1:port",
            "http://admin:secret@127.0.0.1:8000",
            "http://127.0.0.1:8000/?debug=1",
            "http://127.0.0.1:8000/#top",
            # Accepted by urlsplit but refused by the HTTP client or never resolvable.
            "http://192.168.0.256:8000",
            "http://192.168.001.010:8000",
            "http://192.168.0.1\u200b:8000",
            "http://\uff11\uff19\uff12.\uff11\uff16\uff18.\uff10.\uff11:8000",
            "http://192,168,0,10:8000",
            "http://server pc:8000",
        ):
            with self.subTest(url=url), self.assertRaisesRegex(ConfigError, "server_url"):
                self.load(f'server_url = "{url}"\n')

    def test_example_file_is_valid(self):
        example = Path(config.__file__).with_name("config.example.toml")
        settings = load_settings(example)
        self.assertEqual(settings.server_url, "http://127.0.0.1:8000")
        self.assertEqual(settings.logging, LoggingSettings())

    def test_logging_uses_defaults_for_missing_or_partial_table(self):
        for suffix, expected in (
            ("", LoggingSettings()),
            ("[logging]\n", LoggingSettings()),
            ('[logging]\nlevel = "DEBUG"\n', LoggingSettings(level="DEBUG")),
            ("[logging]\nbackup_count = 1\n", LoggingSettings(backup_count=1)),
        ):
            with self.subTest(suffix=suffix):
                settings = self.load('server_url = "http://127.0.0.1:8000"\n' + suffix)
                self.assertEqual(settings.logging, expected)

    def test_reads_logging_level_size_and_retention(self):
        for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            with self.subTest(level=level):
                settings = self.load(
                    'server_url = "http://127.0.0.1:8000"\n'
                    f'[logging]\nlevel = "{level}"\nmax_bytes = 2048\nbackup_count = 2\n'
                )
                self.assertEqual(settings.logging, LoggingSettings(level, 2048, 2))

    def test_rejects_invalid_logging_table_or_unknown_keys(self):
        for suffix, error in (
            ('logging = "INFO"\n', "must be a table"),
            ("logging = []\n", "must be a table"),
            ('[logging]\nleve1 = "INFO"\n', "unknown keys: leve1"),
            ('[logging.file]\nlevel = "INFO"\n', "unknown keys: file"),
        ):
            with self.subTest(suffix=suffix), self.assertRaisesRegex(ConfigError, error):
                self.load('server_url = "http://127.0.0.1:8000"\n' + suffix)

    def test_rejects_invalid_logging_level_and_numeric_values(self):
        cases = [("level", value) for value in ('"debug"', '"WARN"', '"NOTSET"', "20", "true")]
        cases += [
            (name, value)
            for name in ("max_bytes", "backup_count")
            for value in ("0", "-1", "1.5", '"10"', "true", "false")
        ]
        for name, value in cases:
            with self.subTest(name=name, value=value), self.assertRaisesRegex(
                ConfigError, rf"config\.toml: \[logging\] {name}",
            ):
                self.load('server_url = "http://127.0.0.1:8000"\n' f"[logging]\n{name} = {value}\n")


class WebsocketUrlTests(unittest.TestCase):
    def test_maps_the_scheme_and_keeps_the_rest(self):
        self.assertEqual(websocket_url("http://127.0.0.1:8000"), "ws://127.0.0.1:8000")
        self.assertEqual(websocket_url("https://host/prefix"), "wss://host/prefix")
        # Only the scheme changes, even when the host name contains it.
        self.assertEqual(websocket_url("http://http-server:8000"), "ws://http-server:8000")

    def test_http_and_websocket_use_the_same_encoded_path(self):
        endpoint = "/ws/devices/gateways/1/"
        for prefix, expected in (
            ("my prefix", "/my%20prefix"),
            ("한글", "/%ED%95%9C%EA%B8%80"),
            ("my%20prefix", "/my%20prefix"),
            ("한글/a%20b", "/%ED%95%9C%EA%B8%80/a%20b"),
        ):
            with self.subTest(prefix=prefix):
                url = normalize_server_url(f"http://127.0.0.1:8000/{prefix}")
                http_request = httpx2.Request("GET", url + endpoint)
                self.assertEqual(http_request.url.raw_path.decode(), expected + endpoint)

                # Exercise the actual handshake encoding without opening a socket.
                client = ClientProtocol(parse_uri(websocket_url(url) + endpoint))
                client.send_request(client.connect())
                server = ServerProtocol()
                server.receive_data(b"".join(client.data_to_send()))
                self.assertIsNone(server.handshake_exc)
                self.assertEqual(server.events_received()[0].path, expected + endpoint)


class AppPathsTests(unittest.TestCase):
    def test_paths_are_relative_to_the_program_folder(self):
        root = Path("/opt/scaleledger-client")
        paths = AppPaths(root)
        self.assertEqual(paths.config_file, root / "config.toml")
        self.assertEqual(paths.db_file, root / "data" / "db.sqlite3")
        self.assertEqual(paths.credential_file, root / "data" / "credential.json")
        self.assertEqual(paths.lock_file, root / "data" / "client.lock")
        self.assertEqual(paths.log_dir, root / "logs")

    def test_program_folder_is_the_source_folder(self):
        self.assertEqual(program_dir(), Path(config.__file__).resolve().parent)
