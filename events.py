from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import datetime
import time
import uuid

from api import RecordReceiptDTO


@dataclass(frozen=True)
class ReceiptConfirmation:
    receipt: RecordReceiptDTO
    # Monotonic time when the validated server response became available to the station.
    received_at: float


class ReceiptFuture(Future[ReceiptConfirmation | None]):
    def __init__(self):
        super().__init__()
        self.saved_at = time.monotonic()
        # A transport failure can end the wait before a retry returns. Keep
        # observing this UUID until the station releases its working receipt.
        self.confirmation: ReceiptConfirmation | None = None
        self.set_running_or_notify_cancel()

    def observe(self, receipt: RecordReceiptDTO | None):
        # Keep the first number's arrival time when a cache write is retried.
        # A later cancellation always takes precedence before printer handoff.
        if receipt is not None and (self.confirmation is None or receipt.cancelled_at is not None):
            self.confirmation = ReceiptConfirmation(receipt, time.monotonic())
        if not self.done():
            self.set_result(self.confirmation)


type SaveFuture = Future[ReceiptFuture | None]


@dataclass(frozen=True, kw_only=True)
class BaseEvent:
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now().astimezone())


@dataclass(frozen=True)
class RFIDTaggedEvent(BaseEvent):
    rfid_card_uid: str


@dataclass(frozen=True)
class WeighingCompletedEvent(BaseEvent):
    rfid_card_uid: str
    weight: int
    station_id: int
