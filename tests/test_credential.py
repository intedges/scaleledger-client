import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from credential import CredentialError, CredentialStore, StoredCredential


SERVER = "http://127.0.0.1:8000"


class CredentialStoreTests(unittest.TestCase):
    def setUp(self):
        self.path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "data" / "credential.json"
        self.store = CredentialStore(self.path)

    def test_first_run_has_no_credential_and_create_stores_one_for_the_server(self):
        self.assertIsNone(self.store.load())
        credential = self.store.create(SERVER)

        self.assertRegex(credential, r"^[A-Za-z0-9_-]{43}$")
        self.assertEqual(
            json.loads(self.path.read_text(encoding="utf-8")), {"server_url": SERVER, "credential": credential},
        )
        self.assertEqual(self.store.load(), StoredCredential(SERVER, credential))
        # The replacement is complete; no temporary file remains.
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_create_replaces_the_previous_credential(self):
        first = self.store.create("http://previous-server:8000")
        second = self.store.create(SERVER)

        self.assertNotEqual(first, second)
        self.assertEqual(self.store.load(), StoredCredential(SERVER, second))

    def test_staged_switch_keeps_active_fields_and_reuses_pending_token(self):
        previous_url = "http://previous-server:8000"
        previous = self.store.create(previous_url)
        pending = self.store.stage_switch(self.store.load(), previous_url, SERVER)
        document = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual((document["server_url"], document["credential"]), (previous_url, previous))
        self.assertEqual(self.store.load().pending, pending)
        self.assertEqual(self.store.stage_switch(self.store.load(), previous_url, SERVER), pending)
        self.store.save(pending)
        self.assertEqual(self.store.load(), StoredCredential(SERVER, pending.credential))

    def test_invalid_pending_entry_keeps_active_credential(self):
        data = {"server_url": SERVER, "credential": "active", "pending": {"credential": "candidate"}}
        self.path.parent.mkdir(parents=True)
        self.path.write_text(json.dumps(data), encoding="utf-8")
        self.assertEqual(self.store.load(), StoredCredential(SERVER, "active"))

    def test_stored_server_address_is_normalized(self):
        self.path.parent.mkdir(parents=True)
        self.path.write_text(
            json.dumps({"server_url": "HTTP://127.0.0.1:8000/", "credential": "edited"}), encoding="utf-8",
        )
        self.assertEqual(self.store.load(), StoredCredential(SERVER, "edited"))

    def test_invalid_content_is_reported_without_being_replaced(self):
        self.path.parent.mkdir(parents=True)
        for content in (
            "", "not json", "[]", '{"server_url": "http://127.0.0.1:8000"}', '{"credential": "x"}',
            "[" * 2000 + "]" * 2000,
            '{"server_url": "ftp://127.0.0.1", "credential": "x"}',
            '{"server_url": "http://127.0.0.1:8000", "credential": ""}',
            '{"server_url": 8000, "credential": "x"}',
        ):
            with self.subTest(content=content):
                self.path.write_text(content, encoding="utf-8")
                with self.assertRaises(CredentialError):
                    self.store.load()
                self.assertEqual(self.path.read_text(encoding="utf-8"), content)

    def test_unreadable_file_is_not_mistaken_for_invalid_content(self):
        with patch.object(Path, "read_text", side_effect=PermissionError("denied")):
            with self.assertRaisesRegex(PermissionError, "denied"):
                self.store.load()

    def test_failed_write_keeps_the_previous_credential(self):
        credential = self.store.create(SERVER)
        with patch("credential.os.replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.store.create(SERVER)
        self.assertEqual(self.store.load(), StoredCredential(SERVER, credential))


if __name__ == "__main__":
    unittest.main()
