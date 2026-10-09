from dataclasses import dataclass, field
from datetime import datetime
import uuid


@dataclass(frozen=True, kw_only=True)
class BaseEvent:
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class RFIDTaggedEvent(BaseEvent):
    rfid_card_uid: str


@dataclass(frozen=True)
class WeighingCompletedEvent(BaseEvent):
    rfid_card_uid: str
    weight: int
