from decimal import Decimal
import time

from weighing_cycle import CycleState


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
