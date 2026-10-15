from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class RFIDInfo:
    is_active: bool
    producer_name: str
    species_name: str


@dataclass(frozen=True)
class MarketSnapshot:
    rfid_map: Mapping[str, RFIDInfo]
    gateway_name: str

    def get_rfid_info(self, uid: str) -> RFIDInfo | None:
        return self.rfid_map.get(uid)


class MarketDataCache:
    def __init__(self):
        self.snapshot = MarketSnapshot(MappingProxyType({}), "Gateway")

    @property
    def gateway_name(self) -> str:
        return self.snapshot.gateway_name

    def replace(self, data: dict[str, RFIDInfo], gateway_name: str):
        self.snapshot = MarketSnapshot(MappingProxyType(dict(data)), gateway_name)

    def update_rfid_data(self, data: dict[str, RFIDInfo]):
        self.replace(data, self.gateway_name)

    def get_rfid_info(self, uid: str) -> RFIDInfo | None:
        return self.snapshot.get_rfid_info(uid)
