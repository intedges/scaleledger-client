from decimal import Decimal
from enum import Enum, auto
import math


class CycleState(Enum):
    WAIT_EMPTY = auto()
    WAIT_ENTRY = auto()
    READY = auto()
    RESERVED = auto()
    LOCKED = auto()


class WeighingCycle:
    """Allow one measurement per observed vehicle visit, including after restart."""

    def __init__(
        self,
        threshold: Decimal,
        ready_seconds: float,
        empty_seconds: float,
        max_sample_gap: float = 2.0,
    ):
        if not isinstance(threshold, Decimal) or not threshold.is_finite() or threshold <= 0:
            raise ValueError("threshold must be a finite positive Decimal")
        self.threshold = threshold
        self.ready_seconds = self._positive_seconds(ready_seconds, "ready_seconds")
        self.empty_seconds = self._positive_seconds(empty_seconds, "empty_seconds")
        self.max_sample_gap = self._positive_seconds(max_sample_gap, "max_sample_gap")
        self.state = CycleState.WAIT_EMPTY
        self.ready_sequence = 0
        self._high_since: float | None = None
        self._empty_since: float | None = None
        self._last_sample_at: float | None = None
        self._last_sample_high = False

    @staticmethod
    def _finite_seconds(value: float, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{name} must be a finite number")
        try:
            result = float(value)
        except OverflowError:
            raise ValueError(f"{name} must be a finite number") from None
        if not math.isfinite(result):
            raise ValueError(f"{name} must be a finite number")
        return result

    @classmethod
    def _positive_seconds(cls, value: float, name: str) -> float:
        result = cls._finite_seconds(value, name)
        if result <= 0:
            raise ValueError(f"{name} must be positive")
        return result

    def invalidate(self):
        """Discard observation continuity without unlocking a consumed visit."""
        self._high_since = None
        self._empty_since = None
        self._last_sample_at = None
        self._last_sample_high = False
        if self.state in (CycleState.READY, CycleState.RESERVED):
            self.state = CycleState.WAIT_ENTRY

    def _expire_sample(self, now: float):
        if self._last_sample_at is not None and (
            now < self._last_sample_at or now - self._last_sample_at > self.max_sample_gap
        ):
            self.invalidate()

    def _become_ready(self):
        self.state = CycleState.READY
        self.ready_sequence += 1

    def observe(self, weight: Decimal | None, *, now: float):
        now = self._finite_seconds(now, "now")
        self._expire_sample(now)
        if not isinstance(weight, Decimal) or not weight.is_finite():
            self.invalidate()
            return

        self._last_sample_at = now
        self._last_sample_high = weight > self.threshold
        if self._last_sample_high:
            self._empty_since = None
            if self.state == CycleState.WAIT_ENTRY:
                if self._high_since is None:
                    self._high_since = now
                if now - self._high_since >= self.ready_seconds:
                    self._become_ready()
            return

        self._high_since = None
        if self.state in (CycleState.READY, CycleState.RESERVED):
            self.state = CycleState.WAIT_ENTRY
        if self.state in (CycleState.WAIT_EMPTY, CycleState.LOCKED):
            if self._empty_since is None:
                self._empty_since = now
            if now - self._empty_since >= self.empty_seconds:
                self.state = CycleState.WAIT_ENTRY
                self._empty_since = None

    def reserve(self, *, now: float) -> bool:
        now = self._finite_seconds(now, "now")
        self._expire_sample(now)
        if self.state != CycleState.READY or not self._last_sample_high:
            return False
        self.state = CycleState.RESERVED
        return True

    def release(self):
        if self.state == CycleState.RESERVED:
            self._become_ready()

    def consume(self, *, now: float) -> bool:
        now = self._finite_seconds(now, "now")
        self._expire_sample(now)
        if self.state != CycleState.RESERVED or not self._last_sample_high:
            return False
        self.state = CycleState.LOCKED
        self._high_since = None
        self._empty_since = None
        return True
