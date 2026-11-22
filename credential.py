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
    pending: "StoredCredential | None" = None


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
        except (ValueError, RecursionError) as exc:
            raise CredentialError(f"{self.path} is not valid JSON: {exc}") from exc
        active = self._read_identity(data)
        if "pending" not in data:
            return active
        try:
            pending = self._read_identity(data["pending"])
        except CredentialError:
            # A damaged candidate does not revoke the active credential.
            return active
        return StoredCredential(active.server_url, active.credential, pending)

    def _read_identity(self, data: object) -> StoredCredential:
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
        identity = StoredCredential(server_url, secrets.token_urlsafe(32))
        self.save(identity)
        return identity.credential

    def stage_switch(self, stored: StoredCredential | None, previous_url: str, new_url: str) -> StoredCredential:
        # Legacy clients read just the top-level server URL and token.
        known = {stored.server_url: stored.credential} if stored else {}
        if stored and stored.pending:
            known.setdefault(stored.pending.server_url, stored.pending.credential)
        previous = StoredCredential(previous_url, known.get(previous_url) or secrets.token_urlsafe(32))
        pending = StoredCredential(new_url, known.get(new_url) or secrets.token_urlsafe(32))
        self.save(previous, pending)
        return pending

    def save(self, identity: StoredCredential, pending: StoredCredential | None = None) -> None:
        data = {"server_url": identity.server_url, "credential": identity.credential}
        if pending is not None:
            data["pending"] = {"server_url": pending.server_url, "credential": pending.credential}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f"{self.path.name}.tmp")
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(data, file)
            file.flush()
            os.fsync(file.fileno())
        # Swap in only a complete file; Python guarantees the rename is atomic on POSIX only.
        os.replace(temporary, self.path)
