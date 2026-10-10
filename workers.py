import asyncio
import httpx
from structlog.stdlib import get_logger

from api import APIClient, AuthDegradedError, RecordCreateDTO
from models import Record


class RecordUploadWorker:
    def __init__(self, api_client: APIClient, upload_queue: asyncio.Queue[str], access_token: str):
        self.api_client = api_client
        self.upload_queue = upload_queue
        self.access_token = access_token
        self.logger = get_logger()
        self.retry_delay = 5.0

    async def run(self):
        self.logger.info("sys.worker.record_upload.started")

        while True:
            record_uuid = await self.upload_queue.get()
            unfinished = True

            try:
                record = await Record.get_or_none(uuid=record_uuid)

                if not record:
                    unfinished = False
                    self.logger.debug("biz.record.already_purged_or_missing", uuid=record_uuid)
                    continue

                await self.api_client.create_record(
                    access_token=self.access_token,
                    record=RecordCreateDTO(
                        uuid=record.uuid,
                        rfid_card_uid=record.rfid_card_uid,
                        weight=record.weight,
                        measured_at=record.measured_at,
                    ),
                )

                await record.delete()
                unfinished = False
                self.logger.info("biz.record.upload_success_and_purged", uuid=record_uuid)

            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                match status_code:
                    case 401 | 403:
                        self.logger.error("net.api.record_upload.auth_rejected", status=status_code)
                        raise AuthDegradedError("Token expired during upload")
                    case 400 | 422:
                        unfinished = False
                        self.logger.critical(
                            "biz.record.upload_permanently_rejected",
                            uuid=record_uuid,
                            status=status_code,
                            response=e.response.text,
                        )
                    case _ if status_code >= 500:
                        self.logger.error("net.api.record_upload.server_down", status=status_code)
                        await self._wait_before_retry(record_uuid)
                    case _:
                        self.logger.warning("net.api.record_upload.unhandlected_status", status=status_code)
                        await self._wait_before_retry(record_uuid)

            except httpx.RequestError:
                self.logger.warning("net.api.record_upload.network_offline")
                await self._wait_before_retry(record_uuid)

            finally:
                if unfinished:
                    self.upload_queue.put_nowait(record_uuid)
                self.upload_queue.task_done()

    async def _wait_before_retry(self, record_uuid: str):
        self.logger.info("sys.worker.record_upload.requeue", uuid=record_uuid, delay=self.retry_delay)
        await asyncio.sleep(self.retry_delay)


class HeartbeatWorker:
    def __init__(self, api_client: APIClient, access_token: str, interval: float = 30.0):
        self.api_client = api_client
        self.access_token = access_token
        self.logger = get_logger()
        self.interval = interval

    async def run(self):
        self.logger.info("sys.worker.heartbeat.started", interval=self.interval)

        while True:
            try:
                self.logger.debug("net.api.heartbeat.sending")
                await self.api_client.send_heartbeat(self.access_token)
                self.logger.debug("net.api.heartbeat.success")

            except httpx.HTTPStatusError as e:
                if e.response.status_code in (401, 403):
                    self.logger.error("net.api.heartbeat.auth_rejected", status=e.response.status_code)
                    raise AuthDegradedError("Token expired during heartbeat")
                self.logger.error("net.api.heartbeat.server_error", status=e.response.status_code)

            except httpx.RequestError:
                self.logger.warning("net.api.heartbeat.network_error")

            await asyncio.sleep(self.interval)
