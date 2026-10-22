import argparse
import asyncio
from concurrent.futures import Future
from contextlib import suppress
import json
import logging
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit

import httpx2
from structlog.stdlib import get_logger
from tortoise import Tortoise
from tortoise.exceptions import IntegrityError
from tortoise.transactions import in_transaction
from websockets.exceptions import ConnectionClosed, InvalidStatus
import websockets

from api import APIClient, AuthDegradedError
from cache import MarketDataCache, RFIDInfo
from config import ConfigError, load_settings, websocket_url
from credential import CredentialError, CredentialStore
from paths import AppPaths, program_dir
from events import BaseEvent, WeighingCompletedEvent
from logconfig import RepeatedFailureLog, setup_logging
from managers import WeighingStationManager
from models import Gateway, Producer, Record, RFIDCard, Species, WeighingStation
from settings_sync import SettingsSnapshot
from telemetry import LatestWeightBuffer, LiveWeightPublisher
from utils import acquire_instance_lock, get_hostname, get_ip_address, scan_peripherals
from workers import RecordUploadWorker
from windows_runtime import WindowsStopEvent, request_stop


class HeadlessClient:
    def __init__(
        self, base_url: str | None, db_path: Path | None = None, credential_path: Path | None = None,
    ):
        # Without a server address (unusable config.toml) only stations and local storage run.
        self.base_url = base_url.rstrip("/") if base_url else None
        paths = AppPaths(program_dir())
        self.db_path = db_path or paths.db_file
        self.credentials = CredentialStore(credential_path or paths.credential_file)

        self.api_client: APIClient | None = None
        self.ws_url = self.server_host = None
        self.ws_kwargs = {}
        if self.base_url:
            self.api_client = APIClient(base_url=self.base_url)
            self.ws_url = websocket_url(self.base_url)
            self.server_host = urlsplit(self.base_url).hostname
            if self.ws_url.startswith("wss://"):
                # Trust the same certificates as the HTTP client (the OS trust store).
                self.ws_kwargs["ssl"] = httpx2.create_ssl_context()

        self.hostname = get_hostname()

        self.logger = get_logger()
        # The server is retried every few seconds while it is down or rejecting requests.
        self.network_failures = RepeatedFailureLog(self.logger, "net.server", settle_time=0)

        self.retry_interval = 5

        # This PC's credential; the server accepts it once an administrator approves it.
        self.access_token: str | None = None
        self.gateway_id: int | None = None
        self._gateway_data: dict | None = None
        self._station_config_ready = asyncio.Event()
        self._settings_sync_requested = asyncio.Event()
        self._settings_sync_pending: asyncio.Future[bool] | None = None
        self.settings_sync_timeout = 60
        self._live_weight_stop_code: int | None = None

        self.market_cache = MarketDataCache()
        self.upload_queue: asyncio.Queue[str] = asyncio.Queue()
        self.event_queue: asyncio.Queue[tuple[BaseEvent, Future[None]]] = asyncio.Queue()
        self.live_weight_updates = LatestWeightBuffer()
        self.station_manager = WeighingStationManager(
            on_event=self.handle_hardware_event,
            market_cache = self.market_cache,
            on_live_weight=self.live_weight_updates.update,
            on_station_removed=self.live_weight_updates.remove,
        )
        self.main_loop = None
        self._cleanup_started = False

    def log_step(self, event: str, **kw):
        # Every retry repeats these steps while the server fails; its failure summary covers them.
        log = self.logger.debug if self.network_failures.failing else self.logger.info
        log(event, **kw)

    def handle_hardware_event(self, event: BaseEvent) -> Future[None]:
        result: Future[None] = Future()
        # A station stopping must not cancel a write already handed to the queue.
        result.set_running_or_notify_cancel()
        self.main_loop.call_soon_threadsafe(self.event_queue.put_nowait, (event, result))
        return result
    
    async def event_consumer_worker(self):
        self.logger.info("sys.worker.event_consumer.started")
        while True:
            event, result = await self.event_queue.get()
            try:
                if isinstance(event, WeighingCompletedEvent):
                    await self.save_record(event)
            except Exception as exc:
                result.set_exception(exc)
                self.logger.exception(
                    "biz.record.local_save_failed", event_id=event.uuid,
                    action="check_local_storage_before_retrying_same_measurement",
                )
            else:
                # No await between commit, upload handoff and acknowledgement.
                # Upload may subsequently purge the row; the acknowledgement stays valid.
                if isinstance(event, WeighingCompletedEvent):
                    self.upload_queue.put_nowait(event.uuid)
                result.set_result(None)
                if isinstance(event, WeighingCompletedEvent):
                    self.logger.info("biz.record.saved", uuid=event.uuid, weight=event.weight)
            finally:
                self.event_queue.task_done()

    async def save_record(self, event: WeighingCompletedEvent):
        try:
            await Record.create(
                uuid=event.uuid,
                rfid_card_uid=event.rfid_card_uid,
                weight=event.weight,
                measured_at=event.timestamp,
            )
        except IntegrityError:
            # A previous attempt may have committed before reporting an error.
            # Reuse only that exact original, never overwrite a UUID collision.
            record = await Record.get_or_none(uuid=event.uuid)
            if record is None or (
                record.rfid_card_uid != event.rfid_card_uid
                or record.weight != event.weight
                or record.measured_at != event.timestamp
            ):
                raise

    async def close(self):
        if self.api_client is not None:
            await self.api_client.close()
        await Tortoise.close_connections()
        self.logger.info("sys.lifecycle.process.shutdown")

    async def setup(self):
        self.logger.info(
            "sys.lifecycle.process.startup",
            server_url=self.base_url,
            db_path=str(self.db_path),
        )
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        await Tortoise.init(config={
            "connections": {
                # A file path needs no escaping, unlike a Windows path inside a database URL.
                "default": {
                    "engine": "tortoise.backends.sqlite",
                    "credentials": {"file_path": str(self.db_path)},
                },
            },
            "apps": {"models": {"models": ["models"], "default_connection": "default"}},
        })
        await Tortoise.generate_schemas()
        self.logger.debug("sys.db.schema.ready")
    
    async def restore_local_state(self):
        unsynced_records = await Record.all().values_list("uuid", flat=True)
        for record_uuid in unsynced_records:
            self.upload_queue.put_nowait(str(record_uuid))
        self.logger.info("sys.recovery.records_enqueued", count=len(unsynced_records))

        # A complete settings snapshot stores exactly one Gateway.
        gateway = await Gateway.first()
        if gateway is None:
            self.logger.info("sys.boot.local_cache.missing", action="wait_for_approval_and_settings")
            return

        self.gateway_id = gateway.id
        await self.refresh_market_cache()
        stations = await WeighingStation.filter(gateway_id=gateway.id)
        self.station_manager.sync(stations)
        self.logger.info("sys.boot.local_cache.loaded", gateway_id=gateway.id)

    def forget_server_session(self):
        self.logger.warning("sys.auth.credentials.rejected")
        # Keep the credential; only an explicit revocation replaces it. Cached
        # settings keep the stations running until a complete snapshot arrives.
        self._gateway_data = None
        self._station_config_ready.clear()
        # Approval may follow, so telemetry stopped by a rejection may publish again.
        self._live_weight_stop_code = None

    async def bootstrap(self) -> bool:
        self.log_step("sys.boot.remote_api.syncing")
        try:
            retrieved_gateway = await self.api_client.retrieve_gateway_self(self.access_token)
            # Resolve the authenticated route without publishing partial settings.
            # Keep the cached Gateway ID and Stations until a complete snapshot arrives.
            SettingsSnapshot.from_responses(retrieved_gateway, [], [], [], [])
            self._gateway_data = retrieved_gateway
            self.gateway_id = retrieved_gateway["id"]
            self.logger.info("sys.boot.remote_api.success", gateway_id=self.gateway_id)
            return True
            
        except httpx2.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                self.logger.warning("sys.boot.auth.rejected", status=e.response.status_code)
                raise AuthDegradedError("Bootstrap auth failed")
            else:
                self.network_failures.failure(
                    "sys.boot.remote_api.error", e, traceback=False, status=e.response.status_code,
                )
        except httpx2.RequestError as e:
            self.network_failures.failure(
                "sys.boot.network.offline", e, level=logging.WARNING, traceback=False,
                action="fallback_to_local_cache",
            )
        return False
    
    def request_settings_sync(self) -> asyncio.Future[bool]:
        # A request during a running fetch belongs to one subsequent snapshot.
        if self._settings_sync_pending is None:
            self._settings_sync_pending = asyncio.get_running_loop().create_future()
            self._settings_sync_requested.set()
        return self._settings_sync_pending

    async def sync_weighing_stations(self) -> bool:
        # Telemetry ownership refresh uses the same queue as control notifications.
        return await asyncio.shield(self.request_settings_sync())

    async def run_settings_sync_worker(self):
        current = None
        try:
            while True:
                await self._settings_sync_requested.wait()
                self._settings_sync_requested.clear()
                current = self._settings_sync_pending
                self._settings_sync_pending = None
                success = await self.sync_settings()
                if not current.done():
                    current.set_result(success)
                current = None
                if success:
                    self.network_failures.success()
                else:
                    # Retry only an unsuccessful request, never poll healthy settings.
                    self.request_settings_sync()
                    await asyncio.sleep(self.retry_interval)
        finally:
            for pending in (current, self._settings_sync_pending):
                if pending is not None and not pending.done():
                    pending.cancel()
            self._settings_sync_pending = None
            self._settings_sync_requested.clear()

    async def sync_settings(self) -> bool:
        self.log_step("sys.sync.settings.started")
        try:
            # Keep network waits outside the SQLite transaction and bound the
            # complete fetch, including a server that keeps streaming slowly.
            async with asyncio.timeout(self.settings_sync_timeout):
                gateway = await self.api_client.retrieve_gateway_self(self.access_token)
                species = await self.api_client.fetch_species(self.access_token)
                producers = await self.api_client.fetch_producers(self.access_token)
                cards = await self.api_client.fetch_rfid_cards(self.access_token)
                stations = await self.api_client.list_gateway_stations(self.access_token)
            snapshot = SettingsSnapshot.from_responses(gateway, stations, species, producers, cards)
            if snapshot.gateway_id != self.gateway_id:
                raise ValueError("Gateway identity changed; registration must be resolved first")
            await snapshot.persist()
        except httpx2.HTTPStatusError as exc:
            if exc.response.status_code in (401, 403):
                raise AuthDegradedError("Settings sync auth failed") from exc
            self.network_failures.failure(
                "net.api.sync_settings.server_error", exc,
                traceback=False, status=exc.response.status_code,
            )
        except (httpx2.RequestError, TimeoutError) as exc:
            self.network_failures.failure(
                "net.api.sync_settings.network_error", exc,
                level=logging.WARNING, traceback=False,
            )
        except Exception as exc:
            self.network_failures.failure("sys.sync.settings.failed", exc)
        else:
            self.market_cache.replace(snapshot.rfid_map, snapshot.gateway_name)
            # join() can wait for serial I/O. Never block WS, heartbeat or writes.
            # A cancelled to_thread continues running, so finish it before any
            # replacement task or process cleanup touches the station manager.
            applying = asyncio.create_task(asyncio.to_thread(self.station_manager.sync, snapshot.stations))
            cancelled = None
            while True:
                try:
                    await asyncio.shield(applying)
                    break
                except asyncio.CancelledError as exc:
                    if applying.cancelled():
                        raise
                    cancelled = exc
            if cancelled is not None:
                raise cancelled
            self._station_config_ready.set()
            self.logger.info(
                "sys.sync.settings.completed", stations=len(snapshot.stations),
                rfids=len(snapshot.rfid_map),
            )
            return True
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
            
            gateway = await Gateway.get(id=self.gateway_id)
            self.market_cache.replace(rfid_cache_data, gateway.name)

            self.logger.info("sys.cache.refresh.completed", cached_rfid_count=len(rfid_cache_data))
            
        except Exception:
            self.logger.exception("sys.cache.refresh.failed")

    async def run(self):
        self.main_loop = asyncio.get_running_loop()
        await self.setup()

        # Network restarts must never cancel a local write in progress.
        consumer = asyncio.create_task(self.event_consumer_worker())
        try:
            # A previous server's settings and records must be gone before restoring them.
            ready = self.api_client is not None and await self.prepare_credential()
            await self.restore_local_state()
            if ready:
                await self.run_network_loop()
            else:
                if self.api_client is None:
                    self.logger.warning("sys.network.disabled", reason="no_server_url")
                # Stations and local storage keep running without the server.
                await asyncio.Future()
        finally:
            self._cleanup_started = True
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
                await self.wait_for_approval()
                await self.bootstrap()
                if self.gateway_id is None:
                    await asyncio.sleep(self.retry_interval)
                else:
                    await self.run_active_loop()

            except* AuthDegradedError:
                self.logger.warning("sys.loop.auth_degraded", action="check_enrollment")
                self.forget_server_session()
                await asyncio.sleep(self.retry_interval)

            except* Exception as group:
                self.network_failures.failure("net.worker.failed", group, retry_in=self.retry_interval)
                await asyncio.sleep(self.retry_interval)

    async def prepare_credential(self) -> bool:
        try:
            try:
                stored = self.credentials.load()
            except CredentialError as exc:
                # A damaged file only costs a new approval; the cached settings stay.
                self.logger.warning("sys.auth.credential.invalid", error=str(exc))
                stored = None
            if stored is not None and stored.server_url == self.base_url:
                self.access_token = stored.credential
                return True
            if stored is not None:
                # A Gateway belongs to one server. Moving starts over with a new approval,
                # without sending the previous server's settings or records to the new one.
                discarded = await self.discard_server_data()
                self.logger.warning(
                    "sys.auth.server_changed", previous=stored.server_url, current=self.base_url,
                    discarded_records=discarded,
                )
            self.access_token = self.credentials.create(self.base_url)
            self.logger.info("sys.auth.credential.created")
        except OSError as exc:
            # Keep weighing; an unreadable file may still hold the approved credential.
            self.logger.error(
                "sys.auth.credential.unusable", error=str(exc), action="check_data_folder_then_restart",
            )
            return False
        return True

    async def discard_server_data(self) -> int:
        async with in_transaction() as connection:
            # Children first, as in a settings snapshot.
            for model in (RFIDCard, Producer, Species, WeighingStation, Gateway):
                await model.all().using_db(connection).delete()
            return await Record.all().using_db(connection).delete()

    async def wait_for_approval(self):
        announced = False
        while True:
            try:
                status = await self.api_client.request_enrollment(
                    self.access_token, hostname=self.hostname,
                    # Resolved per request in a thread: a slow DNS lookup must not
                    # delay the stations at startup or stall the event loop.
                    ip_address=await asyncio.to_thread(get_ip_address, self.server_host),
                )
            except (httpx2.HTTPStatusError, ValueError) as exc:
                self.network_failures.failure("net.api.enrollment.server_error", exc, traceback=False)
            except httpx2.RequestError as exc:
                self.network_failures.failure(
                    "net.api.enrollment.network_error", exc, level=logging.WARNING, traceback=False,
                )
            else:
                # Recovery is logged once the settings sync succeeds, not on this answer alone.
                match status:
                    case "approved":
                        self.log_step("biz.enrollment.approved")
                        return
                    case "revoked":
                        # The server will never accept this credential again; request a new approval.
                        self.access_token = self.credentials.create(self.base_url)
                        self.logger.warning("biz.enrollment.revoked", action="approve_this_pc_again_on_server")
                        continue
                    case "pending" if not announced:
                        self.logger.info(
                            "biz.enrollment.pending", hostname=self.hostname, action="approve_this_pc_on_server",
                        )
                        announced = True
            await asyncio.sleep(self.retry_interval)

    async def run_active_loop(self):
        upload_worker = RecordUploadWorker(
            api_client=self.api_client,
            upload_queue=self.upload_queue,
            access_token=self.access_token,
        )
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.run_settings_sync_worker())
            tg.create_task(self.run_active_ws_loop())
            tg.create_task(upload_worker.run())
            tg.create_task(self.run_live_weights_loop())

    async def run_live_weights_loop(self):
        if self._live_weight_stop_code is not None:
            return
        # A successful server snapshot establishes station ownership after registration.
        await self._station_config_ready.wait()
        publisher = LiveWeightPublisher(
            url=f"{self.ws_url}/ws/devices/gateways/{self.gateway_id}/weights/",
            access_token=self.access_token,
            updates=self.live_weight_updates,
            ws_kwargs=self.ws_kwargs,
            refresh_stations=self.sync_weighing_stations,
        )
        # Preserve a terminal protocol/ownership rejection across control socket
        # restarts. Only restarting this client or a rejected credential's renewed
        # approval enables publication again.
        self._live_weight_stop_code = await publisher.run()

    async def run_active_ws_loop(self):
        # A cached ID may belong to settings saved before re-registration.
        # HTTP workers can already use the token while its current ID is resolved.
        while self._gateway_data is None:
            if await self.bootstrap():
                break
            await asyncio.sleep(self.retry_interval)

        # REST can be reachable even when the control socket is unavailable.
        self.request_settings_sync()
        target_ws_url = f"{self.ws_url}/ws/devices/gateways/{self.gateway_id}/"
        self.log_step("net.ws.active.connecting", url=target_ws_url)

        try:
            async for ws in websockets.connect(
                target_ws_url, additional_headers={"Authorization": f"Gateway {self.access_token}"},
                **self.ws_kwargs,
            ):
                self.log_step("net.ws.active.connected")
                try:
                    self.request_settings_sync()
                    await self.listen_active_ws(ws)
                except* ConnectionClosed:
                    self.logger.warning("net.ws.active.disconnected")
                # Reconnect on normal closes as well; settings retries stay independent.
                await asyncio.sleep(self.retry_interval)
        except InvalidStatus as exc:
            # websockets retries only server errors; a rejected credential needs enrollment.
            if exc.response.status_code in (401, 403):
                raise AuthDegradedError("Control socket rejected the credential") from exc
            raise
    
    async def listen_active_ws(self, ws):
        # Heartbeat calls belong to this connection and must not survive reconnects.
        async with asyncio.TaskGroup() as tasks:
            heartbeat_task = None
            try:
                async for message in ws:
                    try:
                        data = json.loads(message)
                        message_type = data["type"]

                        match message_type:
                            case "heartbeat.request":
                                if heartbeat_task is None or heartbeat_task.done():
                                    heartbeat_task = tasks.create_task(self.send_heartbeat())

                            case "scan.peripherals":
                                self.logger.info("biz.active.scan_peripherals.executing")
                                peripherals = await asyncio.to_thread(scan_peripherals)
                                await ws.send(json.dumps({
                                    "type": "peripherals.scanned",
                                    "payload": peripherals,
                                }))
                                self.logger.info("biz.active.scan_peripherals.completed", count=len(peripherals))

                            case "sync.market_data" | "sync.gateway" | "sync.weighing_stations":
                                self.logger.info("biz.active.settings_sync.requested", type=message_type)
                                self.request_settings_sync()

                            case _:
                                self.logger.debug("net.ws.message.ignored", type=message_type)

                    except json.JSONDecodeError:
                        self.logger.error("net.ws.message.invalid_json")
            finally:
                if heartbeat_task is not None:
                    heartbeat_task.cancel()

    async def send_heartbeat(self):
        try:
            self.logger.debug("net.api.heartbeat.sending")
            await self.api_client.send_heartbeat(self.access_token)
            self.logger.debug("net.api.heartbeat.success")
        except httpx2.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                self.logger.error("net.api.heartbeat.auth_rejected", status=e.response.status_code)
                raise AuthDegradedError("Token expired during heartbeat") from e
            self.logger.error("net.api.heartbeat.server_error", status=e.response.status_code)
        except httpx2.RequestError as e:
            self.logger.warning("net.api.heartbeat.network_error", error=repr(e))


async def run_client(
    server_url: str | None, db_path: Path, stop_event: WindowsStopEvent | None = None,
):
    client: HeadlessClient | None = None

    async def run_with_cleanup():
        nonlocal client
        client = HeadlessClient(base_url=server_url, db_path=db_path)
        try:
            await client.run()
        finally:
            client._cleanup_started = True
            # Tortoise stores its ORM context in a ContextVar. Close in the
            # same task that initialized it so the SQLite worker also exits.
            await client.close()

    if stop_event is None:
        await run_with_cleanup()
        return

    async def wait_for_stop():
        while not stop_event.is_set():
            await asyncio.sleep(0.2)

    client_task = asyncio.create_task(run_with_cleanup())
    stop_task = asyncio.create_task(wait_for_stop())
    try:
        await asyncio.wait((client_task, stop_task), return_when=asyncio.FIRST_COMPLETED)
        if stop_task.done():
            # Propagate event failures instead of reporting a successful shutdown.
            await stop_task
            get_logger().info("sys.process.stop_requested")
        if client_task.done():
            await client_task
    finally:
        stop_task.cancel()
        if client is None or not client._cleanup_started:
            # Enter HeadlessClient.run's drain/close path, but do not interrupt
            # cleanup that has already started after completion or failure.
            client_task.cancel()
        try:
            with suppress(asyncio.CancelledError):
                await stop_task
        finally:
            with suppress(asyncio.CancelledError):
                await client_task


def stop_client(paths: AppPaths, timeout: float = 30.0) -> int:
    if sys.platform != "win32":
        if sys.stderr is not None:
            print("--stop은 Windows 전용입니다. 콘솔 실행에서는 Ctrl+C로 종료하세요.", file=sys.stderr)
        return 2
    try:
        signaled = request_stop(paths.root)
        deadline = time.monotonic() + timeout
        while paths.lock_file.exists():
            lock = acquire_instance_lock(paths.lock_file)
            if lock is not None:
                lock.close()
                break
            if not signaled:
                raise RuntimeError("프로그램이 시작 중입니다. 잠시 후 종료를 다시 요청하세요.")
            if time.monotonic() >= deadline:
                raise RuntimeError("종료가 아직 완료되지 않았습니다. 로그와 작업 상태를 확인하세요.")
            time.sleep(0.2)
    except (OSError, RuntimeError) as exc:
        if sys.stderr is not None:
            print(f"종료 실패: {exc}", file=sys.stderr)
        return 1
    if sys.stdout is not None:
        print("프로그램이 종료되었습니다." if signaled else "실행 중인 프로그램이 없습니다.")
    return 0


def main(argv: list[str] | None = None) -> int:
    paths = AppPaths(program_dir())
    parser = argparse.ArgumentParser(description="ScaleLedger headless client")
    parser.add_argument(
        "--config", type=Path, default=paths.config_file,
        help="settings file for development (default: config.toml in the program folder)",
    )
    commands = parser.add_mutually_exclusive_group()
    commands.add_argument(
        "--check-config", action="store_true",
        help="validate settings and exit without opening the database or connecting to devices",
    )
    commands.add_argument(
        "--stop", action="store_true",
        help="request graceful shutdown on Windows and wait up to 30 seconds",
    )
    args = parser.parse_args(argv)
    if args.stop:
        return stop_client(paths)

    settings = None
    config_error = None
    try:
        settings = load_settings(args.config)
    except ConfigError as exc:
        config_error = str(exc)

    if args.check_config:
        if config_error is not None:
            if sys.stderr is not None:
                print(config_error, file=sys.stderr)
            return 2
        if sys.stdout is not None:
            print(f"Configuration valid: {args.config}")
        return 0

    # Without a window it is easy to start twice; both would compete for the DB and COM ports.
    instance_lock = acquire_instance_lock(paths.lock_file)
    if instance_lock is None:
        # Opening the running process's log can prevent its rotation on Windows.
        if sys.stderr is not None:
            print("프로그램이 이미 실행 중입니다.", file=sys.stderr)
        return 1

    stop_event = None
    try:
        setup_logging(
            paths.log_dir,
            settings=settings.logging if settings else None,
        )
        logger = get_logger()
        server_url = settings.server_url if settings else None
        if config_error is not None:
            # Keep weighing with saved settings even when the configuration is invalid.
            logger.error("sys.config.invalid", error=config_error, action="disable_network")
        else:
            logger.info("sys.config.loaded", path=str(args.config), server_url=server_url)
        if sys.platform == "win32":
            stop_event = WindowsStopEvent(paths.root)
        asyncio.run(run_client(server_url, paths.db_file, stop_event))
    except KeyboardInterrupt:
        pass
    finally:
        if stop_event is not None:
            stop_event.close()
        instance_lock.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
