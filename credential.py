"""The secret this PC presents to the one server that approves it."""

from dataclasses import dataclass
import json
import os
from pathlib import Path
import secrets

from config import normalize_server_url


class CredentialError(Exception):
    """The credential file exists but its content cannot be used."""


@dataclass(frozen=True)
class StoredCredential:
    server_url: str
    credential: str


class CredentialStore:
    def __init__(self, path: Path):
        self.path = path

    def load(self) -> StoredCredential | None:
        """Return the stored credential and its server, or None before the first run.

        An unreadable file raises OSError: it may still hold a valid credential.
        """
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except ValueError as exc:
            raise CredentialError(f"{self.path} is not valid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise CredentialError(f"{self.path} must contain an object")
        credential, server_url = data.get("credential"), data.get("server_url")
        if not isinstance(credential, str) or not credential or not isinstance(server_url, str):
            raise CredentialError(f"{self.path} needs a credential and its server_url")
        try:
            return StoredCredential(normalize_server_url(server_url), credential)
        except ValueError as exc:
            raise CredentialError(f"{self.path} server_url {exc}") from exc

    def create(self, server_url: str) -> str:
        """Store and return a new credential for *server_url*, replacing any previous one."""
        credential = secrets.token_urlsafe(32)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f"{self.path.name}.tmp")
        with temporary.open("w", encoding="utf-8") as file:
            json.dump({"server_url": server_url, "credential": credential}, file)
            file.flush()
            os.fsync(file.fileno())
        # Swap in only a complete file; Python guarantees the rename is atomic on POSIX only.
        os.replace(temporary, self.path)
        return credential
