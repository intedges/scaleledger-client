from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import math
import re


_TIME_FIELDS = (
    "vehicle_ready_seconds", "vehicle_empty_seconds", "card_prompt_interval",
    "completion_interval", "serial_read_timeout", "serial_write_timeout",
)


@dataclass(frozen=True)
class DeviceConfiguration:
    """One validated set of server-wide operating values, independent of a port."""

    auction_name: str = ""
    vehicle_threshold: int = 50
    vehicle_ready_seconds: float = 1.0
    vehicle_empty_seconds: float = 1.0
    card_prompt_interval: float = 5.0
    completion_interval: float = 10.0
    print_copies: int = 1
    serial_read_timeout: float = 2.0
    serial_write_timeout: float = 2.0
    updated_at: datetime | None = None

    def __post_init__(self):
        if type(self.auction_name) is not str or len(self.auction_name) > 100:
            raise ValueError("auction_name must be a string of at most 100 characters")
        if type(self.vehicle_threshold) is not int or not 1 <= self.vehicle_threshold <= 2_147_483_647:
            raise ValueError("vehicle_threshold must be an integer between 1 and 2147483647")
        if type(self.print_copies) is not int or not 1 <= self.print_copies <= 9:
            raise ValueError("print_copies must be an integer between 1 and 9")
        for name in _TIME_FIELDS:
            value = getattr(self, name)
            if (
                type(value) not in (int, float) or not 0.1 <= value <= 9999.9
                or not math.isfinite(value)
                or Decimal(str(value)) * 10 != (Decimal(str(value)) * 10).to_integral_value()
            ):
                raise ValueError(f"{name} must be between 0.1 and 9999.9 with one decimal place")
            object.__setattr__(self, name, float(value))
        if self.updated_at is not None and (
            not isinstance(self.updated_at, datetime) or self.updated_at.utcoffset() is None
        ):
            raise ValueError("updated_at must be a timezone-aware datetime")

    @classmethod
    def from_response(cls, value) -> "DeviceConfiguration":
        if not isinstance(value, dict):
            raise ValueError("configuration must be an object")
        try:
            values = {
                "auction_name": value["auction_name"],
                "vehicle_threshold": value["vehicle_threshold"],
                "print_copies": value["print_copies"],
            }
            for name in _TIME_FIELDS:
                raw = value[name]
                if type(raw) is not str or re.fullmatch(r"[0-9]+\.[0-9]", raw) is None:
                    raise ValueError(f"{name} must be a decimal string with one decimal place")
                values[name] = float(raw)
            updated_at = value["updated_at"]
        except KeyError as exc:
            raise ValueError(f"configuration.{exc.args[0]} is required") from None
        if type(updated_at) is not str:
            raise ValueError("configuration.updated_at must be a timezone-aware ISO 8601 string")
        try:
            values["updated_at"] = datetime.fromisoformat(updated_at)
        except ValueError:
            raise ValueError("configuration.updated_at must be a timezone-aware ISO 8601 string") from None
        return cls(**values)
