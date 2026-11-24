from decimal import Decimal
import json
import time
from zoneinfo import ZoneInfo

from api import RecordCreateDTO, RecordReceiptDTO
from weighing_cycle import CycleState


def receipt_text_and_qrs(document: bytes) -> tuple[str, list[dict]]:
    """Separate receipt text from length-prefixed ESC/POS QR commands."""
    text = bytearray()
    qrs = []
    stored = None
    offset = 0
    while (start := document.find(b"\x1d(k", offset)) != -1:
        text.extend(document[offset:start])
        body = start + 5
        length = int.from_bytes(document[start + 3:body], "little")
        offset = body + length
        command = document[body:offset]
        if command.startswith(b"1P0"):
            stored = command[3:]
        elif command == b"1Q0":
            assert stored is not None, "QR print command has no stored data"
            qrs.append(json.loads(stored))
            stored = None
    assert stored is None, "Stored QR data was never printed"
    text.extend(document[offset:])
    return text.decode("cp949"), qrs


def accepted_receipt(*, access_token: str, record: RecordCreateDTO) -> RecordReceiptDTO:
    return RecordReceiptDTO(
        uuid=record.uuid, station_id=record.station_id,
        weight=record.weight, measured_at=record.measured_at, cancelled_at=None,
        daily_number=18, number_date=record.measured_at.astimezone(ZoneInfo("Asia/Seoul")).date(),
    )


def prepare_vehicle(worker, *, reserve=True):
    """Provide a confirmed visit without adding hardware or telemetry responses."""
    now = time.monotonic()
    for age in (8, 7, 6, 5):
        worker.cycle.observe(Decimal("0"), now=now - age)
    for age in (3, 2, 1, 0):
        worker.cycle.observe(worker.last_weight, now=now - age)
    assert worker.cycle.state == CycleState.READY
    if reserve:
        assert worker.cycle.reserve(now=now)
