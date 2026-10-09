import asyncio
import json
import ssl

import certifi
import httpx
import structlog
from structlog.stdlib import get_logger
from tortoise import Tortoise
from tortoise.transactions import in_transaction
from websockets.exceptions import ConnectionClosed
import websockets

from api import APIClient, AuthDegradedError
from cache import MarketDataCache, RFIDInfo
from events import BaseEvent, WeighingCompletedEvent
from managers import WeighingStationManager
from models import Gateway, Record, WeighingStation, Species, Producer, RFIDCard
from utils import get_hostname, get_ip_address, get_mac_address, scan_peripherals
from workers import HeartbeatWorker, RecordUploadWorker


def setup_logging():
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # 개발 환경에서는 ConsoleRenderer, 배포 환경에서는 JSONRenderer 권장
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


class HeadlessClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

        self.api_client = APIClient(base_url=self.base_url)
        self.ws_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        self.provisioning_url = f"{self.ws_url}/ws/devices/gateways/provisioning/"

        self.mac_address = get_mac_address()
        self.ip_address = get_ip_address()
        self.hostname = get_hostname()

        self.logger = get_logger()

        self.retry_interval = 5

        self.access_token: str | None = None
        self.gateway_id: int | None = None

        self.market_cache = MarketDataCache()
        self.upload_queue: asyncio.Queue[str] = asyncio.Queue()
        self.event_queue: asyncio.Queue[BaseEvent] = asyncio.Queue()
        self.station_manager = WeighingStationManager(
            on_event=self.handle_hardware_event,
            market_cache = self.market_cache,
        )
        self.main_loop = None

        self.ws_kwargs = {}
        if self.ws_url.startswith("wss://"):
            self.ws_kwargs["ssl"] = ssl.create_default_context(cafile=certifi.where())


    def handle_hardware_event(self, event: BaseEvent):
        self.main_loop.call_soon_threadsafe(self.event_queue.put_nowait, event)
    
    async def event_consumer_worker(self):
        self.logger.info("sys.worker.event_consumer.started")
        while True:
            event = await self.event_queue.get()
            try:
                if isinstance(event, WeighingCompletedEvent):
                    self.logger.info(
                        "biz.weighing.completed", 
                        event_id=event.uuid,
                        rfid=event.rfid_card_uid, 
                        weight=event.weight
                    )

                    record = await Record.create(
                        uuid=event.uuid,
                        rfid_card_uid=event.rfid_card_uid,
                        weight=event.weight,
                        measured_at=event.timestamp,
                    )

                    self.logger.info("biz.record.created", uuid=str(record.uuid), weight=event.weight)

                    await self.upload_queue.put(str(event.uuid))

                    self.logger.debug("biz.record.queued_for_upload", queue_size=self.upload_queue.qsize())

            except Exception:
                event_id = getattr(event, 'uuid', 'unknown')
                self.logger.exception("biz.record.local_save_failed", event_id=event_id)
            finally:
                self.event_queue.task_done()

    async def close(self):
        self.station_manager.stop_all()
        await self.api_client.close()
        await Tortoise.close_connections()
        self.logger.info("sys.lifecycle.process.shutdown")

    async def setup(self):
        self.logger.info("sys.lifecycle.process.startup", server_url=self.api_client.client.base_url)
        await Tortoise.init(
            db_url="sqlite://db.sqlite3",
            modules={"models": ["models"]},
        )
        await Tortoise.generate_schemas()
        self.logger.debug("sys.db.schema.ready")
    
    async def wipe_local_auth(self):
        self.logger.warning("sys.auth.local_db.wipe")
        await Gateway.all().delete()
        self.access_token = None
        self.gateway_id = None
    
    async def bootstrap(self):
        self.logger.debug("sys.boot.state.evaluating")

        if not self.access_token:
            gateway = await Gateway.get_or_none(mac_address=self.mac_address)
            if gateway:
                self.access_token = gateway.access_token
                self.gateway_id = gateway.id
                self.logger.info("sys.boot.local_cache.loaded", gateway_id=self.gateway_id)
            else:
                self.logger.info("sys.boot.auth.missing", action="require_provisioning")
                return
        
        self.logger.info("sys.boot.remote_api.syncing")
        try:
            retrieved_gateway = await self.api_client.retrieve_gateway_self(self.access_token)

            await Gateway.filter(id__not=retrieved_gateway["id"]).delete()

            gateway, _ = await Gateway.update_or_create(
                id=retrieved_gateway["id"],
                defaults={
                    "mac_address": retrieved_gateway["mac_address"],
                    "hostname": retrieved_gateway["hostname"],
                    "ip_address": retrieved_gateway["ip_address"],
                    "name": retrieved_gateway["name"],
                    "description": retrieved_gateway["description"],
                    "access_token": retrieved_gateway["access_token"],
                    "last_heartbeat": retrieved_gateway["last_heartbeat"],
                    "created_at": retrieved_gateway["created_at"],
                    "updated_at": retrieved_gateway["updated_at"],
                }
            )
            self.gateway_id = gateway.id
            self.logger.info("sys.boot.remote_api.success", gateway_id=self.gateway_id)
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                self.logger.warning("sys.boot.auth.rejected", status=e.response.status_code)
                raise AuthDegradedError("Bootstrap auth failed")
            else:
                self.logger.exception("sys.boot.remote_api.error", status=e.response.status_code)
        except httpx.RequestError:
            self.logger.warning("sys.boot.network.offline", action="fallback_to_local_cache")
    
    async def sync_weighing_stations(self):
        self.logger.info("sys.sync.weighing_stations.started")
        try:
            retrieved_stations = await self.api_client.list_gateway_stations(self.access_token)

            station_ids = []
            for station in retrieved_stations:
                station_ids.append(station["id"])
                await WeighingStation.update_or_create(
                    id=station["id"],
                    defaults={
                        "gateway_id": station["gateway"],
                        "name": station["name"],
                        "description": station["description"],
                        "serial_port": station["serial_port"],
                        "serial_description": station["serial_description"],
                        "serial_location": station["serial_location"],
                        "serial_number": station["serial_number"],
                        "serial_manufacturer": station["serial_manufacturer"],
                    },
                )

            deleted_count = await WeighingStation.filter(id__not_in=station_ids).delete()
            
            current_stations = await WeighingStation.all()
            self.station_manager.sync(current_stations)

            self.logger.info(
                "sys.sync.weighing_stations.completed",
                synced_count=len(retrieved_stations),
                deleted_count=deleted_count,
            )

        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                self.logger.error("net.api.sync_stations.auth_rejected", status=e.response.status_code)
                raise AuthDegradedError("Sync stations auth failed")
            self.logger.error("net.api.sync_stations.server_error", status=e.response.status_code)
        except httpx.RequestError:
            self.logger.warning("net.api.sync_stations.network_error")
        except Exception:
            self.logger.exception("sys.sync.weighing_stations.fatal_error")
    
    async def sync_market_data(self):
        self.logger.info("sys.sync.market_data.started")
        try:
            species_data = await self.api_client.fetch_species(self.access_token)
            producers_data = await self.api_client.fetch_producers(self.access_token)
            rfid_cards_data = await self.api_client.fetch_rfid_cards(self.access_token)

            async with in_transaction():
                await RFIDCard.all().delete()
                await Producer.all().delete()
                await Species.all().delete()

                await Species.bulk_create([Species(**s) for s in species_data])
                await Producer.bulk_create([Producer(**p) for p in producers_data])
                
                rfid_cards = []
                for data in rfid_cards_data:
                    rfid_cards.append(RFIDCard(
                        id=data["id"],
                        uuid=data["uuid"],
                        uid=data["uid"],
                        producer_id=data["producer"],
                        species_id=data["species"],
                        is_active=data["is_active"],
                        issued_at=data["issued_at"],
                        last_used_at=data.get("last_used_at")
                    ))
                await RFIDCard.bulk_create(rfid_cards)

            self.logger.info(
                "sys.sync.market_data.completed", 
                species=len(species_data), 
                producers=len(producers_data), 
                rfids=len(rfid_cards_data)
            )

        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                self.logger.error("net.api.sync_market.auth_rejected", status=e.response.status_code)
                raise AuthDegradedError("Sync market data auth failed")
            self.logger.error("net.api.sync_market.server_error", status=e.response.status_code)
        except httpx.RequestError:
            self.logger.warning("net.api.sync_market.network_error")
        except Exception:
            self.logger.exception("sys.sync.market_data.fatal_error")

    async def refresh_market_cache(self):
        self.logger.info("sys.cache.refresh.started")
        try:
            rfid_cache_data = {}
            
            active_cards = await RFIDCard.all().select_related("producer", "species")
            
            for card in active_cards:
                rfid_cache_data[card.uid] = RFIDInfo(
                    is_active=card.is_active,
                    producer_name=card.producer.name,
                    species_name=card.species.name
                )
            
            self.market_cache.update_rfid_data(rfid_cache_data)

            gateway = await Gateway.get(id=self.gateway_id)
            self.market_cache.gateway_name = gateway.name

            self.logger.info("sys.cache.refresh.completed", cached_rfid_count=len(rfid_cache_data))
            
        except Exception:
            self.logger.exception("sys.cache.refresh.failed")

    async def run(self):
        setup_logging()

        self.main_loop = asyncio.get_running_loop()
        await self.setup()

        is_running = True
        while is_running:
            try:
                await self.bootstrap()

                if not self.access_token:
                    await self.run_provisioning_loop()
                else:
                    await self.run_active_loop()

            except* AuthDegradedError:
                self.logger.warning("sys.loop.auth_degraded", action="wipe_and_retry")
                await self.wipe_local_auth()

            except* (ConnectionClosed, OSError):
                self.logger.exception("net.ws.connection_lost", retry_in=self.retry_interval)
                await asyncio.sleep(self.retry_interval)

            except* asyncio.CancelledError:
                self.logger.info("sys.loop.cancelled")
                is_running = False

            except* Exception:
                self.logger.exception("sys.loop.unexpected_crashed", retry_in=self.retry_interval)
                await asyncio.sleep(self.retry_interval)
    
    async def run_provisioning_loop(self):
        self.logger.info("net.ws.provisioning.connecting", url=self.provisioning_url)
        async with websockets.connect(self.provisioning_url, **self.ws_kwargs) as ws:
            self.logger.info("net.ws.provisioning.connected")
            async for message in ws:
                await self.dispatch_provisioning(ws, message)

                if self.access_token:
                    self.logger.info("biz.provisioning.handover_ready")
                    break
    
    async def dispatch_provisioning(self, websocket, message: str):
        try:
            data = json.loads(message)
            message_type = data["type"]
            match message_type:
                case "identify":
                    self.logger.info("biz.provisioning.identify.received")
                    await websocket.send(json.dumps({
                        "type": "identity",
                        "payload": {
                            "mac_address": self.mac_address,
                            "hostname": self.hostname,
                            "ip_address": self.ip_address,
                        },
                    }))
                case "gateway.registered":
                    self.logger.info("biz.provisioning.registered.received")
                    new_token = data["payload"]["access_token"]
                    if new_token:
                        self.access_token = new_token
                case _:
                    self.logger.warning("net.ws.message.ignored", type=message_type)
        except json.JSONDecodeError:
            self.logger.error("net.ws.message.invalid_json", message=message)
    
    async def run_active_loop(self):
        target_ws_url = f"{self.ws_url}/ws/devices/gateways/{self.gateway_id}/"
        self.logger.info("net.ws.active.connecting", url=target_ws_url)

        try:
            unsynced_records = await Record.all().values_list("uuid", flat=True)
            for record_uuid in unsynced_records:
                self.upload_queue.put_nowait(str(record_uuid))
            
            if unsynced_records:
                self.logger.info("sys.recovery.records_enqueued", count=len(unsynced_records))
            
            await self.sync_market_data()

            await self.refresh_market_cache()
            
            await self.sync_weighing_stations()

            async with websockets.connect(target_ws_url, **self.ws_kwargs) as ws:
                self.logger.info("net.ws.active.connected")

                heartbeat_worker = HeartbeatWorker(
                    api_client=self.api_client,
                    access_token=self.access_token,
                )
                upload_worker = RecordUploadWorker(
                    api_client=self.api_client,
                    upload_queue=self.upload_queue,
                    access_token=self.access_token,
                )
                
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.event_consumer_worker())
                    tg.create_task(self.listen_active_ws(ws))
                    tg.create_task(heartbeat_worker.run())
                    tg.create_task(upload_worker.run())
        finally:
            self.station_manager.stop_all()
    
    async def listen_active_ws(self, ws):
        async for message in ws:
            try:
                data = json.loads(message)
                message_type = data["type"]

                match message_type:
                    case "scan.peripherals":
                        self.logger.info("biz.active.scan_peripherals.executing")
                        peripherals = scan_peripherals()
                        await ws.send(json.dumps({
                            "type": "peripherals.scanned",
                            "payload": peripherals,
                        }))
                        self.logger.info("biz.active.scan_peripherals.completed", count=len(peripherals))

                    case "sync.weighing_stations":
                        self.logger.info("biz.active.sync_stations.executing")
                        await self.sync_weighing_stations()

                    case _:
                        self.logger.debug("net.ws.message.ignored", type=message_type)

            except json.JSONDecodeError:
                self.logger.error("net.ws.message.invalid_json")


async def main():
    client = HeadlessClient(base_url="https://stg.scaleledger.intedges.com")
    try:
        await client.run()
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
