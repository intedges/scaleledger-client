from dataclasses import dataclass


@dataclass
class RFIDInfo:
    is_active: bool
    producer_name: str
    species_name: str


class MarketDataCache:
    def __init__(self):
        self.rfid_map: dict[str, RFIDInfo] = {}
        self.gateway_name: str = "Gateway"

    def update_rfid_data(self, data: dict[str, RFIDInfo]):
        self.rfid_map = data

    def get_rfid_info(self, uid: str) -> RFIDInfo | None:
        return self.rfid_map.get(uid)
