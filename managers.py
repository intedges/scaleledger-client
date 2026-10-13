from collections.abc import Callable
from dataclasses import dataclass
import threading

from structlog.stdlib import get_logger

from cache import MarketDataCache
from events import BaseEvent, RFIDTaggedEvent, WeighingCompletedEvent
from printer import Receipt, ReceiptTemplate
from models import WeighingStation
from suwol1000 import LiveWeightSnapshot, SerialClient, WeighingStationWorker


@dataclass
class StationRuntime:
    worker: WeighingStationWorker
    thread: threading.Thread
    port: str
    station_name: str


class WeighingStationManager:
    def __init__(
        self,
        on_event: Callable[[BaseEvent], None],
        market_cache: MarketDataCache,
        *,
        on_live_weight: Callable[[int, LiveWeightSnapshot], None] | None = None,
        on_station_removed: Callable[[int], None] | None = None,
    ):
        self.on_event = on_event
        self.market_cache = market_cache
        self.on_live_weight = on_live_weight
        self.on_station_removed = on_station_removed
        self.workers: dict[int, StationRuntime] = {}
        self._sample_sequences: dict[int, int] = {}
        self.logger = get_logger()

    def sync(self, stations: list[WeighingStation]):
        self.logger.info("sys.manager.station.sync_evalutaing", current_count=len(self.workers), target_count=len(stations))

        target_ids = {station.id for station in stations}
        current_ids = set(self.workers.keys())

        to_stop_ids = current_ids - target_ids
        for station_id in to_stop_ids:
            self.stop_worker(station_id)

        for station in stations:
            if station.id not in self.workers:
                self.start_worker(station)
            else:
                runtime = self.workers[station.id]
                if runtime.port != station.serial_port:
                    self.logger.info(
                        "sys.manager.station.config_changed", 
                        station_id=station.id, 
                        current_port=runtime.port,
                        target_port=station.serial_port,
                        action="restart_worker"
                    )
                    self.stop_worker(station.id)
                    self.start_worker(station)
                else:
                    runtime.station_name = station.name
        self.logger.info("sys.manager.station.sync_completed", running_workers=len(self.workers))

    def start_worker(self, station: WeighingStation):
        self.logger.info("sys.manager.station.start", station_id=station.id, port=station.serial_port)
        
        def validate_rfid(event: RFIDTaggedEvent) -> bool:
            info = self.market_cache.get_rfid_info(event.rfid_card_uid)
            if not info:
                return False
            return info.is_active

        def build_receipt(event: WeighingCompletedEvent) -> bytes | None:
            info = self.market_cache.get_rfid_info(event.rfid_card_uid)
            if not info:
                return None
                
            receipt = Receipt(
                record_uuid=event.uuid,
                gateway_name=self.market_cache.gateway_name,
                station_name=runtime.station_name,
                rfid_card_uid=event.rfid_card_uid,
                producer_name=info.producer_name,
                species_name=info.species_name,
                weight=event.weight,
                measured_at=event.timestamp
            )
            return ReceiptTemplate.render(receipt)

        def on_live_weight(snapshot: LiveWeightSnapshot):
            self.on_live_weight(station.id, snapshot)

        worker = WeighingStationWorker(
            serial_client=SerialClient(port=station.serial_port),
            on_event=self.on_event,
            rfid_validator=validate_rfid,
            receipt_builder=build_receipt,
            initial_sample_seq=self._sample_sequences.get(station.id, 0),
            on_live_weight=on_live_weight if self.on_live_weight is not None else None,
        )

        thread = threading.Thread(target=worker.run, name=f"WeighingStation-{station.id}-{station.serial_port}", daemon=True)
        runtime = StationRuntime(worker=worker, thread=thread, port=station.serial_port, station_name=station.name)
        self.workers[station.id] = runtime
        if self.on_live_weight is not None:
            try:
                on_live_weight(worker.get_live_weight())
            except Exception:
                self.logger.exception("sys.manager.station.live_weight.callback_failed", station_id=station.id)
        try:
            thread.start()
        except Exception:
            del self.workers[station.id]
            self._notify_station_removed(station.id)
            raise

    def stop_worker(self, station_id: int):
        runtime = self.workers[station_id]
        self.logger.info("sys.manager.station.stop_worker", station_id=station_id, port=runtime.port)
        runtime.worker.stop()
        runtime.thread.join()
        self._remember_sample_sequence(station_id, runtime)
        del self.workers[station_id]
        self._notify_station_removed(station_id)

    def _notify_station_removed(self, station_id: int):
        if self.on_station_removed is not None:
            try:
                self.on_station_removed(station_id)
            except Exception:
                self.logger.exception("sys.manager.station.removed.callback_failed", station_id=station_id)

    def _remember_sample_sequence(self, station_id: int, runtime: StationRuntime):
        self._sample_sequences[station_id] = max(
            self._sample_sequences.get(station_id, 0), runtime.worker.get_live_weight().sample_seq,
        )

    def stop_all(self):
        self.logger.info("sys.manager.station.stop_all.requested")
        runtimes = list(self.workers.items())
        for station_id, runtime in runtimes:
            self.logger.info("sys.manager.station.stop_worker", station_id=station_id, port=runtime.port)
            runtime.worker.stop()
        for station_id, runtime in runtimes:
            runtime.thread.join()
            self._remember_sample_sequence(station_id, runtime)
            del self.workers[station_id]
            self._notify_station_removed(station_id)
