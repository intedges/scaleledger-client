from dataclasses import dataclass
from datetime import UTC, date, datetime
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


class InvalidRecordResponse(ValueError):
    """The server did not acknowledge the submitted measurement as expected."""


def _receipt_timestamp(value, field: str) -> datetime:
    try:
        timestamp = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise InvalidRecordResponse(f"Invalid {field} in record response") from None
    if timestamp.utcoffset() is None:
        raise InvalidRecordResponse(f"Missing timezone for {field} in record response")
    try:
        return timestamp.astimezone(UTC)
    except OverflowError:
        raise InvalidRecordResponse(f"{field} is outside the supported UTC range") from None


@dataclass(frozen=True)
class RecordReceiptDTO:
    uuid: uuid.UUID
    station_id: int
    weight: int
    measured_at: datetime
    cancelled_at: datetime | None
    daily_number: int | None
    number_date: date | None

    @classmethod
    def from_response(cls, body, record: RecordCreateDTO) -> "RecordReceiptDTO":
        if not isinstance(body, dict):
            raise InvalidRecordResponse("Record response must be an object")
        if not isinstance(body.get("uuid"), str):
            raise InvalidRecordResponse("Invalid UUID in record response")
        try:
            response_uuid = uuid.UUID(body["uuid"])
        except ValueError:
            raise InvalidRecordResponse("Invalid UUID in record response") from None
        if response_uuid != record.uuid:
            raise InvalidRecordResponse("Record response UUID does not match the request")
        station_id = body.get("station_id")
        if type(station_id) is not int or station_id != record.station_id:
            raise InvalidRecordResponse("Record response station_id does not match the request")
        weight = body.get("weight")
        if type(weight) is not int or not -2_147_483_648 <= weight <= 2_147_483_647:
            raise InvalidRecordResponse("Invalid weight in record response")
        measured_at = _receipt_timestamp(body.get("measured_at"), "measured_at")
        if "cancelled_at" not in body:
            raise InvalidRecordResponse("Missing cancelled_at in record response")
        cancelled_at = body["cancelled_at"]
        if cancelled_at is not None:
            cancelled_at = _receipt_timestamp(cancelled_at, "cancelled_at")

        # Earlier servers acknowledged a saved record without issuing a number.
        daily_number = number_date = None
        if "daily_number" in body or "number_date" in body:
            daily_number = body.get("daily_number")
            if type(daily_number) is not int or not 1 <= daily_number <= 2_147_483_647:
                raise InvalidRecordResponse("Invalid daily_number in record response")
            try:
                number_date = date.fromisoformat(body.get("number_date"))
            except (TypeError, ValueError):
                raise InvalidRecordResponse("Invalid number_date in record response") from None
            if number_date.isoformat() != body["number_date"]:
                raise InvalidRecordResponse("Expected YYYY-MM-DD for number_date in record response")
        return cls(response_uuid, station_id, weight, measured_at, cancelled_at, daily_number, number_date)


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

    async def create_record(self, access_token: str, record: RecordCreateDTO) -> RecordReceiptDTO:
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
        if response.status_code not in (200, 201):
            raise InvalidRecordResponse(f"Unexpected record response: HTTP {response.status_code}")
        try:
            body = response.json()
        except ValueError:
            raise InvalidRecordResponse("Record response contains invalid JSON") from None
        return RecordReceiptDTO.from_response(body, record)

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
