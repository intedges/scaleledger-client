from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class RFIDInfo:
    is_active: bool
    producer_name: str
    species_name: str


class MarketDataCache:
    def __init__(self):
        self.rfid_map: Dict[str, RFIDInfo] = {}
        self.gateway_name: str = "Gateway"

    def update_rfid_data(self, data: Dict[str, RFIDInfo]):
        self.rfid_map = data

    def get_rfid_info(self, uid: str) -> Optional[RFIDInfo]:
        return self.rfid_map.get(uid)
