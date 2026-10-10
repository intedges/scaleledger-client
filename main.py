import asyncio
from contextlib import suppress
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
        self._gateway_data: dict | None = None

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
    
    async def restore_local_state(self):
        unsynced_records = await Record.all().values_list("uuid", flat=True)
        for record_uuid in unsynced_records:
            self.upload_queue.put_nowait(str(record_uuid))
        self.logger.info("sys.recovery.records_enqueued", count=len(unsynced_records))

        gateway = await Gateway.get_or_none(mac_address=self.mac_address)
        if gateway is None:
            self.logger.info("sys.boot.auth.missing", action="require_provisioning")
            return

        self.access_token = gateway.access_token or None
        self.gateway_id = gateway.id
        await self.refresh_market_cache()
        stations = await WeighingStation.filter(gateway_id=gateway.id)
        self.station_manager.sync(stations)
        self.logger.info("sys.boot.local_cache.loaded", gateway_id=gateway.id)

    async def wipe_local_auth(self):
        self.logger.warning("sys.auth.credentials.rejected")
        # Preserve the Gateway row: deleting it also deletes cached Stations.
        await Gateway.filter(mac_address=self.mac_address).update(access_token="")
        self.access_token = None
        self._gateway_data = None

    async def bootstrap(self) -> bool:
        self.logger.info("sys.boot.remote_api.syncing")
        try:
            retrieved_gateway = await self.api_client.retrieve_gateway_self(self.access_token)
            # Validated credentials must survive a later settings sync failure.
            # Keep the cached Gateway ID and Stations until a complete snapshot arrives.
            saved = await Gateway.filter(mac_address=self.mac_address).update(
                access_token=self.access_token,
            )
            if not saved:
                await Gateway.create(
                    id=retrieved_gateway["id"],
                    mac_address=retrieved_gateway["mac_address"],
                    hostname=retrieved_gateway["hostname"],
                    ip_address=retrieved_gateway["ip_address"],
                    name=retrieved_gateway["name"],
                    description=retrieved_gateway["description"],
                    access_token=self.access_token,
                    last_heartbeat=retrieved_gateway["last_heartbeat"],
                    created_at=retrieved_gateway["created_at"],
                    updated_at=retrieved_gateway["updated_at"],
                )
            # Commit a replacement Gateway together with its Station snapshot.
            # Deleting the previous Gateway earlier would discard offline settings.
            self._gateway_data = retrieved_gateway
            self.gateway_id = retrieved_gateway["id"]
            self.market_cache.gateway_name = retrieved_gateway["name"]
            self.logger.info("sys.boot.remote_api.success", gateway_id=self.gateway_id)
            return True
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                self.logger.warning("sys.boot.auth.rejected", status=e.response.status_code)
                raise AuthDegradedError("Bootstrap auth failed")
            else:
                self.logger.exception("sys.boot.remote_api.error", status=e.response.status_code)
        except httpx.RequestError:
            self.logger.warning("sys.boot.network.offline", action="fallback_to_local_cache")
        return False
    
    async def sync_weighing_stations(self) -> bool:
        self.logger.info("sys.sync.weighing_stations.started")
        try:
            retrieved_stations = await self.api_client.list_gateway_stations(self.access_token)

            async with in_transaction():
                if self._gateway_data is not None:
                    data = self._gateway_data
                    await Gateway.filter(id__not=data["id"]).delete()
                    await Gateway.update_or_create(
                        id=data["id"],
                        defaults={
                            "mac_address": data["mac_address"],
                            "hostname": data["hostname"],
                            "ip_address": data["ip_address"],
                            "name": data["name"],
                            "description": data["description"],
                            "access_token": data["access_token"],
                            "last_heartbeat": data["last_heartbeat"],
                            "created_at": data["created_at"],
                            "updated_at": data["updated_at"],
                        },
                    )
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
            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                self.logger.error("net.api.sync_stations.auth_rejected", status=e.response.status_code)
                raise AuthDegradedError("Sync stations auth failed")
            self.logger.error("net.api.sync_stations.server_error", status=e.response.status_code)
        except httpx.RequestError:
            self.logger.warning("net.api.sync_stations.network_error")
        except Exception:
            self.logger.exception("sys.sync.weighing_stations.fatal_error")
        return False
    
    async def sync_market_data(self) -> bool:
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
            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                self.logger.error("net.api.sync_market.auth_rejected", status=e.response.status_code)
                raise AuthDegradedError("Sync market data auth failed")
            self.logger.error("net.api.sync_market.server_error", status=e.response.status_code)
        except httpx.RequestError:
            self.logger.warning("net.api.sync_market.network_error")
        except Exception:
            self.logger.exception("sys.sync.market_data.fatal_error")
        return False

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

            if self._gateway_data is None:
                gateway = await Gateway.get(id=self.gateway_id)
                self.market_cache.gateway_name = gateway.name

            self.logger.info("sys.cache.refresh.completed", cached_rfid_count=len(rfid_cache_data))
            
        except Exception:
            self.logger.exception("sys.cache.refresh.failed")

    async def run(self):
        setup_logging()

        self.main_loop = asyncio.get_running_loop()
        await self.setup()

        # Network restarts must never cancel a local write in progress.
        consumer = asyncio.create_task(self.event_consumer_worker())
        try:
            await self.restore_local_state()
            await self.run_network_loop()
        finally:
            await asyncio.to_thread(self.station_manager.stop_all)
            # Deliver callbacks already submitted by the stopped hardware threads.
            await asyncio.sleep(0)
            await self.event_queue.join()
            consumer.cancel()
            with suppress(asyncio.CancelledError):
                await consumer

    async def run_network_loop(self):
        while True:
            try:
                if not self.access_token:
                    await self.run_provisioning_loop()

                await self.bootstrap()
                if self.gateway_id is None:
                    await asyncio.sleep(self.retry_interval)
                else:
                    await self.run_active_loop()

            except* AuthDegradedError:
                self.logger.warning("sys.loop.auth_degraded", action="require_provisioning")
                await self.wipe_local_auth()

            except* Exception:
                self.logger.exception("net.worker.failed", retry_in=self.retry_interval)
                await asyncio.sleep(self.retry_interval)
    
    async def run_provisioning_loop(self):
        self.logger.info("net.ws.provisioning.connecting", url=self.provisioning_url)
        async for ws in websockets.connect(self.provisioning_url, **self.ws_kwargs):
            self.logger.info("net.ws.provisioning.connected")
            try:
                async for message in ws:
                    await self.dispatch_provisioning(ws, message)

                    if self.access_token:
                        self.logger.info("biz.provisioning.handover_ready")
                        await ws.close()
                        return
            except ConnectionClosed:
                self.logger.warning("net.ws.provisioning.disconnected")
            await asyncio.sleep(self.retry_interval)
    
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
                        # Resolve this registration before opening its active socket.
                        self.gateway_id = None
                        self._gateway_data = None
                case _:
                    self.logger.warning("net.ws.message.ignored", type=message_type)
        except json.JSONDecodeError:
            self.logger.error("net.ws.message.invalid_json", message=message)
    
    async def run_active_loop(self):
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
            tg.create_task(self.run_active_ws_loop())
            tg.create_task(heartbeat_worker.run())
            tg.create_task(upload_worker.run())

    async def run_active_ws_loop(self):
        # A cached ID may belong to settings saved before re-registration.
        # HTTP workers can already use the token while its current ID is resolved.
        while self._gateway_data is None:
            if await self.bootstrap():
                break
            await asyncio.sleep(self.retry_interval)

        target_ws_url = f"{self.ws_url}/ws/devices/gateways/{self.gateway_id}/"
        self.logger.info("net.ws.active.connecting", url=target_ws_url)

        async for ws in websockets.connect(target_ws_url, **self.ws_kwargs):
            self.logger.info("net.ws.active.connected")
            try:
                if await self.bootstrap() and await self.sync_market_data():
                    await self.refresh_market_cache()
                    if await self.sync_weighing_stations():
                        await self.listen_active_ws(ws)
            except ConnectionClosed:
                self.logger.warning("net.ws.active.disconnected")
            # Also reconnect on normal closes and retry an incomplete sync.
            await asyncio.sleep(self.retry_interval)
    
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
                        if not await self.sync_weighing_stations():
                            return

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
