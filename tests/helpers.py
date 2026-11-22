from decimal import Decimal
import time
from zoneinfo import ZoneInfo

from api import RecordCreateDTO, RecordReceiptDTO
from weighing_cycle import CycleState


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
