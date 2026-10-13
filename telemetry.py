import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import json
import logging
import random
import threading
import time

from structlog.stdlib import get_logger
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from logconfig import RepeatedFailureLog
from suwol1000 import LiveWeightSnapshot, WeightStatus


@dataclass
class _StationUpdate:
    connection_status: str
    sample: LiveWeightSnapshot | None = None
    last_sample_seq: int = 0
    status_pending: bool = True
    sample_pending: bool = False


class LatestWeightBuffer:
    """Bridge device threads to one sender, retaining only each station's latest state."""

    def __init__(self):
        self._lock = threading.Lock()
        self._latest: dict[int, _StationUpdate] = {}
        # One queue position per station, independent of the number of responses.
        self._pending: dict[int, None] = {}
        self._waiter: asyncio.Future | None = None

    def update(self, station_id: int, snapshot: LiveWeightSnapshot):
        with self._lock:
            state = self._latest.get(station_id)
            if state is None:
                state = _StationUpdate(snapshot.connection_status)
                self._latest[station_id] = state
            if snapshot.sample_valid:
                # A status event never resets the station's observed sample counter.
                if snapshot.sample_seq <= state.last_sample_seq:
                    return
                if state.connection_status != snapshot.connection_status:
                    state.connection_status = snapshot.connection_status
                    state.status_pending = True
                state.sample = snapshot
                state.last_sample_seq = snapshot.sample_seq
                state.sample_pending = True
            else:
                # Status changes invalidate samples taken before that transition,
                # including a connected notification after a serial reconnect.
                if state.connection_status != snapshot.connection_status or state.sample is not None:
                    state.status_pending = True
                state.connection_status = snapshot.connection_status
                state.sample = None
                state.sample_pending = False
                state.last_sample_seq = max(state.last_sample_seq, snapshot.sample_seq)
            if state.status_pending or state.sample_pending:
                self._pending[station_id] = None
                waiter, self._waiter = self._waiter, None
                if waiter is not None:
                    waiter.get_loop().call_soon_threadsafe(self._wake, waiter)

    @staticmethod
    def _wake(waiter: asyncio.Future):
        if not waiter.done():
            waiter.set_result(None)

    def remove(self, station_id: int):
        with self._lock:
            self._latest.pop(station_id, None)
            self._pending.pop(station_id, None)

    def replay_latest(self):
        # A new WebSocket has no station state: status first, then a still-valid
        # sample. get() re-reads current state between those sends, so a disconnect
        # during the status send cannot replay an obsolete sample afterwards.
        with self._lock:
            self._pending = dict.fromkeys(self._latest)
            for state in self._latest.values():
                state.status_pending = True
                state.sample_pending = state.sample is not None

    async def get(self) -> dict:
        loop = asyncio.get_running_loop()
        while True:
            with self._lock:
                if self._pending:
                    station_id = next(iter(self._pending))
                    del self._pending[station_id]
                    state = self._latest[station_id]
                    if state.status_pending:
                        state.status_pending = False
                        if state.sample_pending:
                            self._pending[station_id] = None
                        return {
                            "type": "station.status",
                            "payload": {"station_id": station_id, "connection_status": state.connection_status},
                        }
                    snapshot = state.sample
                    state.sample_pending = False
                    if snapshot is None:
                        continue
                else:
                    snapshot = None
                    waiter = loop.create_future()
                    self._waiter = waiter
            if snapshot is not None:
                return {"type": "weight.updated", "payload": weight_payload(station_id, snapshot)}
            try:
                await waiter
            finally:
                with self._lock:
                    if self._waiter is waiter:
                        self._waiter = None


def weight_payload(station_id: int, snapshot: LiveWeightSnapshot) -> dict:
    # Compute age at send time, including time spent offline or behind a slow send.
    if (
        not snapshot.sample_valid or snapshot.connection_status != "connected"
        or snapshot.sampled_at is None or snapshot.sampled_monotonic is None
        or snapshot.sample_seq < 1
        or (snapshot.weight is None and snapshot.weight_status not in (WeightStatus.OVERLOAD, WeightStatus.NONE))
    ):
        raise ValueError("A weight message requires a valid sample from the current serial connection")
    age = max(0, int((time.monotonic() - snapshot.sampled_monotonic) * 1000))
    return {
        "station_id": station_id,
        "weight": format(snapshot.weight, "f") if snapshot.weight is not None else None,
        "unit": snapshot.unit,
        "weight_status": snapshot.weight_status.value,
        "weight_type": snapshot.weight_type.value,
        "sampled_at": snapshot.sampled_at.isoformat(),
        "sample_age_ms": age,
        "sample_seq": snapshot.sample_seq,
        "connection_status": snapshot.connection_status,
    }


class LiveWeightPublisher:
    """Send each station update through one Gateway socket without a backlog."""

    def __init__(
        self, url: str, access_token: str, updates: LatestWeightBuffer,
        *, ws_kwargs: dict | None = None,
        refresh_stations: Callable[[], Awaitable[bool]] | None = None,
    ):
        self.url = url
        self.access_token = access_token
        self.updates = updates
        self.ws_kwargs = ws_kwargs or {}
        self.refresh_stations = refresh_stations
        self.heartbeat_interval = 1.0
        self.send_timeout = 2.0
        self.retry_interval = 1.0
        self.max_retry_interval = 30.0
        self.logger = get_logger()
        self.failures = RepeatedFailureLog(self.logger, "net.weights")

    async def run(self):
        retry_delay = self.retry_interval
        loop = asyncio.get_running_loop()
        needs_station_sync = False
        while True:
            connected_at = None
            try:
                if needs_station_sync:
                    if self.refresh_stations is None:
                        self.logger.error("net.weights.stopped", code=4403, action="refresh_station_configuration")
                        return 4403
                    if not await self.refresh_stations():
                        raise ConnectionError("Station configuration refresh failed")
                    needs_station_sync = False
                async with connect(
                    self.url,
                    additional_headers={"Authorization": f"Gateway {self.access_token}"},
                    open_timeout=5,
                    close_timeout=2,
                    max_queue=1,
                    max_size=4096,
                    **self.ws_kwargs,
                ) as ws:
                    connected_at = loop.time()
                    await self.run_connection(ws)
                    raise ConnectionError("Live weight connection closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Do not let a superseded publisher reclaim ownership indefinitely.
                # Configuration/protocol failures also need correction before retry.
                code = None
                if isinstance(exc, ConnectionClosed) and exc.rcvd is not None:
                    code = exc.rcvd.code
                elif isinstance(exc, InvalidStatus):
                    code = exc.response.status_code
                if code in (1009, 4400, 4401, 4409, 4429, 401, 403):
                    self.logger.error("net.weights.stopped", code=code, action="check_configuration_and_restart", error=str(exc))
                    return code
                if code == 4403:
                    needs_station_sync = True
                # Optional telemetry must never restart weighing, HTTP uploads or control.
                self.failures.failure(
                    "net.weights.unavailable", exc, level=logging.WARNING, traceback=False,
                )

            if connected_at is not None and loop.time() - connected_at >= 10:
                retry_delay = self.retry_interval
            await asyncio.sleep(random.uniform(retry_delay * 0.8, retry_delay * 1.2))
            retry_delay = min(self.max_retry_interval, retry_delay * 2)

    async def run_connection(self, ws):
        self.updates.replay_latest()
        sender = asyncio.create_task(self.send_weights(ws))
        receiver = asyncio.create_task(self.receive_messages(ws))
        try:
            await asyncio.wait((sender, receiver), return_when=asyncio.FIRST_COMPLETED)
        finally:
            sender.cancel()
            receiver.cancel()
            results = await asyncio.gather(sender, receiver, return_exceptions=True)
        # A send timeout may race with a received close. Preserve the server's
        # reason so ownership loss or a required station refresh cannot be lost.
        for result in results:
            if isinstance(result, ConnectionClosed):
                raise result
        for result in results:
            if isinstance(result, Exception):
                raise result

    @staticmethod
    async def receive_messages(ws):
        # Consume frames so unexpected server messages cannot block close/ping handling.
        async for _ in ws:
            pass

    async def send_weights(self, ws):
        sequence = 0
        while True:
            try:
                async with asyncio.timeout(self.heartbeat_interval):
                    packet = await self.updates.get()
            except TimeoutError:
                # Keep the Gateway lease alive without refreshing any sample age.
                packet = {"type": "gateway.heartbeat"}
            sequence += 1
            packet["sequence"] = sequence
            message = json.dumps(packet)
            async with asyncio.timeout(self.send_timeout):
                await ws.send(message)
            self.failures.success()
            # send() can complete synchronously; allow control/record tasks to run
            # even when device updates arrive continuously.
            await asyncio.sleep(0)
