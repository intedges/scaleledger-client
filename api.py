from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List
import uuid

import httpx


@dataclass
class RecordCreateDTO:
    uuid: uuid.UUID
    rfid_card_uid: str
    weight: int
    measured_at: datetime


class AuthDegradedError(Exception):
    pass


class APIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=base_url, timeout=10.0)

    async def close(self):
        await self.client.aclose()

    async def retrieve_gateway_self(self, access_token: str) -> Dict[str, Any]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.get("devices/api/gateways/self/", headers=headers)
        response.raise_for_status()
        return response.json()

    async def list_gateway_stations(self, access_token: str) -> List[Dict[str, Any]]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.get("devices/api/gateways/self/stations/", headers=headers)
        response.raise_for_status()
        return response.json()

    async def send_heartbeat(self, access_token: str) -> Dict[str, Any]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.post("devices/api/gateways/heartbeat/", headers=headers)
        response.raise_for_status()
        return response.json()

    async def create_record(self, access_token: str, record: RecordCreateDTO) -> dict:
        headers = {"Authorization": f"Gateway {access_token}"}
        payload = {
            "uuid": str(record.uuid),
            "rfid_card_uid": record.rfid_card_uid,
            "weight": record.weight,
            "measured_at": record.measured_at.isoformat(),
        }
        response = await self.client.post("weighing/api/records/", json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    async def fetch_species(self, access_token: str) -> List[Dict[str, Any]]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.get("market/api/species/", headers=headers)
        response.raise_for_status()
        return response.json()
    
    async def fetch_producers(self, access_token: str) -> List[Dict[str, Any]]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.get("market/api/producers/", headers=headers)
        response.raise_for_status()
        return response.json()

    async def fetch_rfid_cards(self, access_token: str) -> List[Dict[str, Any]]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.get("market/api/rfid-cards/", headers=headers)
        response.raise_for_status()
        return response.json()
