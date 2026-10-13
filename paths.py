from dataclasses import dataclass
from pathlib import Path
import sys


def program_dir() -> Path:
    # A frozen build unpacks its sources elsewhere; keep data next to the executable.
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


@dataclass(frozen=True)
class AppPaths:
    """Locations under the program folder, independent of the working directory."""

    root: Path

    @property
    def config_file(self) -> Path:
        return self.root / "config.toml"

    @property
    def data_dir(self) -> Path:
        return self.root / "data"

    @property
    def db_file(self) -> Path:
        return self.data_dir / "db.sqlite3"

    @property
    def lock_file(self) -> Path:
        return self.data_dir / "client.lock"

    @property
    def log_dir(self) -> Path:
        return self.root / "logs"
