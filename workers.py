import asyncio

import httpx2
from structlog.stdlib import get_logger
from tortoise.exceptions import OperationalError

from api import APIClient, AuthDegradedError, InvalidRecordResponse, MismatchedRecordResponse, RecordCreateDTO
from models import Record, RecordUploadResult


def _is_record_validation_error(response: httpx2.Response) -> bool:
    if response.status_code not in (400, 422):
        return False
    try:
        errors = response.json()
    except (ValueError, RecursionError):
        return False
    # A proxy can return HTTP 400 before the request reaches the record endpoint.
    if not isinstance(errors, dict):
        return False
    for field in ("uuid", "station_id", "rfid_card_uid", "weight", "measured_at", "non_field_errors"):
        message = errors.get(field)
        if isinstance(message, str) and message.strip():
            return True
        if isinstance(message, list) and any(isinstance(item, str) and item.strip() for item in message):
            return True
    return False


class RecordUploadWorker:
    def __init__(self, api_client: APIClient, upload_queue: asyncio.Queue[str], access_token: str):
        self.api_client = api_client
        self.upload_queue = upload_queue
        self.access_token = access_token
        self.logger = get_logger()
        self.retry_delay = 5.0
        self.upload_timeout = 30.0

    async def run(self):
        self.logger.info("sys.worker.record_upload.started")

        while True:
            record_uuid = await self.upload_queue.get()
            unfinished = True

            try:
                await self._upload_pending(record_uuid)
            except httpx2.HTTPStatusError as e:
                status_code = e.response.status_code
                match status_code:
                    case 401 | 403:
                        self.logger.error("net.api.record_upload.auth_rejected", status=status_code)
                        raise AuthDegradedError("Token expired during upload")
                    case _ if status_code >= 500:
                        self.logger.error("net.api.record_upload.server_down", status=status_code)
                        await self._wait_before_retry(record_uuid)
                    case _:
                        self.logger.warning("net.api.record_upload.unexpected_status", status=status_code)
                        await self._wait_before_retry(record_uuid)

            except TimeoutError:
                self.logger.warning("net.api.record_upload.timed_out", uuid=record_uuid)
                await self._wait_before_retry(record_uuid)
            except httpx2.RequestError as e:
                self.logger.warning("net.api.record_upload.network_offline", error=repr(e))
                await self._wait_before_retry(record_uuid)
            except InvalidRecordResponse as e:
                self.logger.warning("net.api.record_upload.invalid_response", uuid=record_uuid, error=str(e))
                await self._wait_before_retry(record_uuid)
            except OperationalError:
                self.logger.exception("biz.record.upload_storage_failed", uuid=record_uuid)
                await self._wait_before_retry(record_uuid)
            else:
                unfinished = False
            finally:
                if unfinished:
                    self.upload_queue.put_nowait(record_uuid)
                self.upload_queue.task_done()

    async def _upload_pending(self, record_uuid: str):
        record = await Record.pending_uploads().get_or_none(uuid=record_uuid)
        if record is None:
            return

        request = RecordCreateDTO(
            uuid=record.uuid, rfid_card_uid=record.rfid_card_uid, station_id=record.station_id,
            weight=record.weight, measured_at=record.measured_at,
        )
        try:
            # A response may keep streaming within the HTTP client's per-read timeout.
            async with asyncio.timeout(self.upload_timeout):
                receipt = await self.api_client.create_record(access_token=self.access_token, record=request)
        except httpx2.HTTPStatusError as exc:
            if not _is_record_validation_error(exc.response):
                raise
            error = f"HTTP {exc.response.status_code}: {exc.response.text}"
        except MismatchedRecordResponse as exc:
            error = str(exc)
        else:
            await RecordUploadResult.create(
                record_id=record.uuid,
                weight=receipt.weight,
                measured_at=receipt.measured_at,
                cancelled_at=receipt.cancelled_at,
                daily_number=receipt.daily_number,
                number_date=receipt.number_date,
            )
            self.logger.info(
                "biz.record.upload_confirmed", uuid=record_uuid,
                daily_number=receipt.daily_number, number_date=receipt.number_date,
                cancelled_at=receipt.cancelled_at,
            )
            return

        await RecordUploadResult.create(record_id=record.uuid, error=error)
        self.logger.error("biz.record.upload_needs_review", uuid=record_uuid, error=error)

    async def _wait_before_retry(self, record_uuid: str):
        self.logger.info("sys.worker.record_upload.requeue", uuid=record_uuid, delay=self.retry_delay)
        await asyncio.sleep(self.retry_delay)
