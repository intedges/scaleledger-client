from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from printer import Receipt


class ReprintRequestError(RuntimeError):
    """A reprint was rejected before its printer command was sent."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class ReprintRequest:
    station_id: int
    copies: int
    receipt: Receipt

    @classmethod
    def from_payload(cls, payload) -> "ReprintRequest":
        if not isinstance(payload, dict):
            raise ReprintRequestError("invalid_payload")
        station_id = payload.get("station_id")
        if type(station_id) is not int or station_id <= 0:
            raise ReprintRequestError("invalid_station_id")
        copies = payload.get("copies")
        if type(copies) is not int or not 1 <= copies <= 9:
            raise ReprintRequestError("invalid_copies")
        receipt = payload.get("receipt")
        if not isinstance(receipt, dict):
            raise ReprintRequestError("invalid_receipt")
        text_fields = (
            "gateway_name", "station_name", "rfid_card_uid", "producer_name",
            "species_name", "auction_name",
        )
        if any(not isinstance(receipt.get(field), str) for field in text_fields):
            raise ReprintRequestError("invalid_receipt_text")
        for field in text_fields:
            value = receipt[field]
            if value and not value.isprintable():
                raise ReprintRequestError("invalid_receipt_text")
            try:
                # The renderer normally replaces unsupported characters. A web
                # reprint must not silently change the server-confirmed names.
                value.encode("cp949")
            except UnicodeEncodeError:
                raise ReprintRequestError("unsupported_receipt_text") from None
        weight = receipt.get("weight")
        if type(weight) is not int:
            raise ReprintRequestError("invalid_weight")
        record_uuid = receipt.get("record_uuid")
        if not isinstance(record_uuid, str):
            raise ReprintRequestError("invalid_record_uuid")
        try:
            record_uuid = UUID(record_uuid)
        except ValueError:
            raise ReprintRequestError("invalid_record_uuid") from None
        measured_at = receipt.get("measured_at")
        if not isinstance(measured_at, str):
            raise ReprintRequestError("invalid_measured_at")
        try:
            measured_at = datetime.fromisoformat(measured_at)
        except ValueError:
            raise ReprintRequestError("invalid_measured_at") from None
        if measured_at.utcoffset() is None:
            raise ReprintRequestError("invalid_measured_at")
        return cls(
            station_id=station_id,
            copies=copies,
            receipt=Receipt(
                record_uuid=record_uuid, weight=Decimal(weight), measured_at=measured_at,
                **{field: receipt[field] for field in text_fields},
            ),
        )
