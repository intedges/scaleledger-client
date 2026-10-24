from dataclasses import dataclass
from datetime import datetime
from typing import Any
import uuid

import httpx2


@dataclass
class RecordCreateDTO:
    uuid: uuid.UUID
    rfid_card_uid: str
    station_id: int
    weight: int
    measured_at: datetime


class AuthDegradedError(Exception):
    pass


class APIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx2.AsyncClient(base_url=base_url, timeout=10.0)

    async def close(self):
        await self.client.aclose()

    async def request_enrollment(self, access_token: str, hostname: str, ip_address: str) -> str:
        headers = {"Authorization": f"Gateway {access_token}"}
        payload = {"hostname": hostname, "ip_address": ip_address}
        response = await self.client.post("devices/api/gateways/enrollment/", json=payload, headers=headers)
        # A revoked credential is an answer, not a failure: the caller replaces it.
        expected = {200: "approved", 202: "pending", 403: "revoked"}.get(response.status_code)
        if expected is not None:
            try:
                body = response.json()
            except ValueError:
                body = None
            if isinstance(body, dict) and body.get("status") == expected:
                return expected
        response.raise_for_status()
        raise ValueError(f"Unexpected enrollment response: HTTP {response.status_code}")

    async def retrieve_gateway_self(self, access_token: str) -> dict[str, Any]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.get("devices/api/gateways/self/", headers=headers)
        response.raise_for_status()
        return response.json()

    async def list_gateway_stations(self, access_token: str) -> list[dict[str, Any]]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.get("devices/api/gateways/self/stations/", headers=headers)
        response.raise_for_status()
        return response.json()

    async def send_heartbeat(self, access_token: str) -> dict[str, Any]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.post("devices/api/gateways/heartbeat/", headers=headers)
        response.raise_for_status()
        return response.json()

    async def create_record(self, access_token: str, record: RecordCreateDTO) -> dict:
        headers = {"Authorization": f"Gateway {access_token}"}
        payload = {
            "uuid": str(record.uuid),
            "rfid_card_uid": record.rfid_card_uid,
            "station_id": record.station_id,
            "weight": record.weight,
            "measured_at": record.measured_at.isoformat(),
        }
        response = await self.client.post("weighing/api/records/", json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    async def fetch_species(self, access_token: str) -> list[dict[str, Any]]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.get("market/api/species/", headers=headers)
        response.raise_for_status()
        return response.json()
    
    async def fetch_producers(self, access_token: str) -> list[dict[str, Any]]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.get("market/api/producers/", headers=headers)
        response.raise_for_status()
        return response.json()

    async def fetch_rfid_cards(self, access_token: str) -> list[dict[str, Any]]:
        headers = {"Authorization": f"Gateway {access_token}"}
        response = await self.client.get("market/api/rfid-cards/", headers=headers)
        response.raise_for_status()
        return response.json()
