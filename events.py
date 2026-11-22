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
        self.set_running_or_notify_cancel()


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
