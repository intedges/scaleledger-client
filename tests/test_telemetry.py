import asyncio
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from http import HTTPStatus
import json
import threading
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed, ConnectionClosedError
from websockets.frames import Close

from main import HeadlessClient
from suwol1000 import (
    DisplayRequestPacket, StationTelemetrySnapshot, PrinterStatus, ResponsePacket, WeighingStationWorker,
    VoiceCode, WeightStatus, WeightType,
)
from telemetry import LatestTelemetryBuffer, StationTelemetryPublisher, telemetry_payload
from tests.test_station_protocol import frame


def sample(weight="1", *, sequence=1, sampled_monotonic=None):
    return StationTelemetrySnapshot(
        weight=Decimal(weight) if weight is not None else None,
        weight_status=WeightStatus.STABLE, weight_type=WeightType.NET, unit="kg", weight_valid=True,
        sampled_at=datetime(2026, 10, 13, tzinfo=UTC),
        sampled_monotonic=time.monotonic() if sampled_monotonic is None else sampled_monotonic,
        sample_seq=sequence, connection_status="connected", sample_valid=True,
    )


def status_packet(station_id, status="connected", **extra):
    return {
        "type": "station.status",
        "payload": {
            "station_id": station_id, "connection_status": status,
        },
        **extra,
    }


class LatestTelemetryBufferTests(unittest.IsolatedAsyncioTestCase):
    async def assert_empty(self, updates):
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(updates.get(), 0.01)

    async def test_initial_and_connected_without_a_sample_send_only_status(self):
        updates = LatestTelemetryBuffer()
        updates.update(3, StationTelemetrySnapshot())
        self.assertEqual(await updates.get(), status_packet(3, "waiting"))
        await self.assert_empty(updates)
        updates.update(3, StationTelemetrySnapshot(connection_status="connected"))
        self.assertEqual(await updates.get(), status_packet(3))
        await self.assert_empty(updates)

    async def test_overwrites_pending_values_per_station_without_starving_other_stations(self):
        updates = LatestTelemetryBuffer()
        updates.update(3, sample("1"))
        updates.update(9, sample("2"))
        for value in range(100):
            updates.update(3, sample(str(value), sequence=value + 2))
        self.assertEqual(await updates.get(), status_packet(3))
        self.assertEqual(await updates.get(), status_packet(9))
        first, second = await updates.get(), await updates.get()
        self.assertEqual((first["payload"]["station_id"], first["payload"]["weight"]), (3, "99"))
        self.assertEqual((second["payload"]["station_id"], second["payload"]["weight"]), (9, "2"))
        await self.assert_empty(updates)

    async def test_thread_burst_schedules_one_wakeup_and_keeps_only_the_latest_sample(self):
        updates = LatestTelemetryBuffer()
        waiter = asyncio.create_task(updates.get())
        await asyncio.sleep(0)
        loop = asyncio.get_running_loop()

        def receive():
            for value in range(10000):
                updates.update(3, sample(str(value), sequence=value + 1))

        with patch.object(loop, "call_soon_threadsafe", wraps=loop.call_soon_threadsafe) as schedule:
            thread = threading.Thread(target=receive)
            thread.start()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(schedule.call_count, 1)
        self.assertEqual(await asyncio.wait_for(waiter, 1), status_packet(3))
        packet = await updates.get()
        self.assertEqual(packet["payload"]["sample_seq"], 10000)
        self.assertEqual(packet["payload"]["weight"], "9999")
        await self.assert_empty(updates)

    async def test_cancelled_wait_does_not_lose_the_next_update(self):
        updates = LatestTelemetryBuffer()
        await self.assert_empty(updates)
        updates.update(3, sample())
        self.assertEqual(await asyncio.wait_for(updates.get(), 1), status_packet(3))
        self.assertEqual((await updates.get())["type"], "station.telemetry")

    async def test_removed_stations_are_not_sent_or_replayed(self):
        updates = LatestTelemetryBuffer()
        updates.update(3, sample())
        updates.update(9, sample("2"))
        updates.remove(3)
        for _ in range(2):
            self.assertEqual(await updates.get(), status_packet(9))
            self.assertEqual((await updates.get())["payload"]["station_id"], 9)
            await self.assert_empty(updates)
            updates.replay_latest()

    async def test_update_then_removal_while_waking_does_not_break_the_next_wait(self):
        updates = LatestTelemetryBuffer()
        waiter = asyncio.create_task(updates.get())
        await asyncio.sleep(0)
        updates.update(3, sample())
        updates.remove(3)
        await asyncio.sleep(0)
        updates.update(9, sample())
        self.assertEqual(await asyncio.wait_for(waiter, 1), status_packet(9))

    async def test_duplicate_or_reversed_sample_sequence_cannot_resend_or_replace_latest(self):
        updates = LatestTelemetryBuffer()
        updates.update(3, sample("12", sequence=5))
        await updates.get()
        first = await updates.get()
        updates.update(3, sample("999", sequence=5))
        updates.update(3, sample("888", sequence=4))
        await self.assert_empty(updates)
        updates.replay_latest()
        self.assertEqual(await updates.get(), status_packet(3))
        replay = await updates.get()
        self.assertEqual(replay["payload"]["weight"], first["payload"]["weight"])
        self.assertEqual(replay["payload"]["sample_seq"], 5)

    async def test_status_invalidates_pending_sample_and_reconnect_requires_a_fresh_sample(self):
        updates = LatestTelemetryBuffer()
        old_sample = sample("42", sequence=7)
        updates.update(3, old_sample)
        self.assertEqual(await updates.get(), status_packet(3))
        updates.update(3, replace(old_sample, connection_status="disconnected", sample_valid=False))
        self.assertEqual(await updates.get(), status_packet(3, "disconnected"))
        await self.assert_empty(updates)
        updates.update(3, replace(old_sample, sample_valid=False))
        self.assertEqual(await updates.get(), status_packet(3))
        updates.replay_latest()
        self.assertEqual(await updates.get(), status_packet(3))
        await self.assert_empty(updates)
        updates.update(3, old_sample)
        await self.assert_empty(updates)
        updates.update(3, sample("43", sequence=8))
        packet = await updates.get()
        self.assertEqual(packet["type"], "station.telemetry")
        self.assertEqual(packet["payload"]["sample_seq"], 8)

    def test_payload_age_includes_time_waiting_and_preserves_decimal_precision(self):
        with patch("telemetry.time.monotonic", return_value=22.125):
            payload = telemetry_payload(3, sample("1E-7", sampled_monotonic=20))
        self.assertEqual(payload["weight"], "0.0000001")
        self.assertEqual(payload["sample_age_ms"], 2125)
        self.assertEqual(payload["sampled_at"], "2026-10-13T00:00:00+00:00")

    def test_only_observed_current_responses_can_be_serialized(self):
        valid = sample()
        invalid_samples = (
            StationTelemetrySnapshot(), replace(valid, sample_valid=False),
            replace(valid, connection_status="disconnected"), replace(valid, sampled_at=None),
            replace(valid, sampled_monotonic=None), replace(valid, sample_seq=0),
            replace(valid, weight=None), replace(valid, unit=None),
            replace(valid, weight_status=None), replace(valid, weight_type=None),
        )
        for snapshot in invalid_samples:
            with self.subTest(snapshot=snapshot), self.assertRaises(ValueError):
                telemetry_payload(3, snapshot)
        payload = telemetry_payload(3, replace(valid, weight_valid=False))
        self.assertIsNone(payload["weight"])
        self.assertIsNone(payload["unit"])
        self.assertFalse(payload["weight_valid"])

    async def test_peripheral_changes_share_the_frame_without_invalidating_weight(self):
        updates = LatestTelemetryBuffer()
        for sequence, status in enumerate((
            PrinterStatus.NORMAL, PrinterStatus.NO_PAPER, PrinterStatus.TRANSMITTING,
            PrinterStatus.NORMAL, None,
        ), 1):
            observed = replace(
                sample("42", sequence=sequence), printer_status=status,
                voice_code=VoiceCode.TAG_CARD, green_blink=True, red_blink=False,
                fan_on=True, heater_on=False,
            )
            updates.update(3, observed)
            if sequence == 1:
                self.assertEqual(await updates.get(), status_packet(3))
            packet = await updates.get()
            self.assertEqual(packet["type"], "station.telemetry")
            self.assertEqual(packet["payload"]["weight"], "42")
            self.assertTrue(packet["payload"]["weight_valid"])
            self.assertEqual(packet["payload"]["sample_seq"], sequence)
            self.assertEqual(packet["payload"]["printer_status"], status.name.lower() if status is not None else "unknown")
            self.assertEqual(packet["payload"]["voice_code"], 4)
            self.assertTrue(packet["payload"]["green_blink"])
            self.assertFalse(packet["payload"]["red_blink"])
            self.assertTrue(packet["payload"]["fan_on"])
            self.assertFalse(packet["payload"]["heater_on"])
            self.assertNotIn("printer_sampled_at", packet["payload"])
            self.assertNotIn("connection_status", packet["payload"])
            await self.assert_empty(updates)

    async def test_invalid_weight_still_publishes_every_new_response(self):
        updates = LatestTelemetryBuffer()
        original = sample("42", sequence=7)
        updates.update(3, original)
        await updates.get()
        await updates.get()
        for sequence, status in ((8, PrinterStatus.NO_PAPER), (9, PrinterStatus.NO_PAPER), (10, PrinterStatus.NORMAL)):
            observed = replace(
                original, sample_seq=sequence, weight_valid=False, weight=None, unit=None,
                printer_status=status, sampled_at=datetime(2026, 10, 26, tzinfo=UTC),
                sampled_monotonic=sequence,
            )
            updates.update(3, observed)
            with patch("telemetry.time.monotonic", return_value=sequence + 0.25):
                packet = await updates.get()
            self.assertEqual(packet["type"], "station.telemetry")
            self.assertFalse(packet["payload"]["weight_valid"])
            self.assertIsNone(packet["payload"]["weight"])
            self.assertEqual(packet["payload"]["printer_status"], status.name.lower())
            self.assertEqual(packet["payload"]["sample_age_ms"], 250)
            await self.assert_empty(updates)
        updates.replay_latest()
        self.assertEqual(await updates.get(), status_packet(3))
        replay = await updates.get()
        self.assertFalse(replay["payload"]["weight_valid"])
        self.assertEqual(replay["payload"]["sample_seq"], 10)
        await self.assert_empty(updates)

    async def test_old_responses_cannot_restore_previous_peripheral_values(self):
        updates = LatestTelemetryBuffer()
        newest = replace(
            sample(sequence=5, sampled_monotonic=20), printer_status=PrinterStatus.NO_PAPER,
            weight_valid=False, weight=None, temperature=28, fan_trigger_temp=30, heater_trigger_temp=5,
        )
        updates.update(3, newest)
        await updates.get()
        await updates.get()
        for sequence in (5, 4):
            updates.update(3, replace(
                newest, sample_seq=sequence, printer_status=PrinterStatus.NORMAL,
                temperature=29, fan_trigger_temp=40, heater_trigger_temp=0,
            ))
        await self.assert_empty(updates)
        updates.replay_latest()
        self.assertEqual(await updates.get(), status_packet(3))
        with patch("telemetry.time.monotonic", return_value=23):
            packet = await updates.get()
        self.assertEqual(packet["payload"]["printer_status"], "no_paper")
        self.assertEqual(packet["payload"]["temperature"], 28)
        self.assertEqual(packet["payload"]["fan_trigger_temp"], 30)
        self.assertEqual(packet["payload"]["heater_trigger_temp"], 5)
        self.assertEqual(packet["payload"]["sampled_at"], "2026-10-13T00:00:00+00:00")
        self.assertEqual(packet["payload"]["sample_age_ms"], 3000)

    async def test_disconnect_discards_all_pending_peripheral_observations(self):
        updates = LatestTelemetryBuffer()
        observed = replace(sample(sequence=2), printer_status=PrinterStatus.NO_PAPER)
        updates.update(3, observed)
        await updates.get()
        disconnected = StationTelemetrySnapshot(sample_seq=2, connection_status="disconnected")
        updates.update(3, disconnected)
        self.assertEqual(await updates.get(), status_packet(3, "disconnected"))
        await self.assert_empty(updates)
        updates.update(3, replace(disconnected, connection_status="connected"))
        updates.replay_latest()
        self.assertEqual(await updates.get(), status_packet(3))
        await self.assert_empty(updates)

    def test_unknown_false_and_zero_values_remain_distinct(self):
        observed = replace(
            sample(sampled_monotonic=20), printer_status=None, voice_code=VoiceCode.NONE,
            green_blink=False, red_blink=None, fan_on=False, heater_on=None, temperature=0,
            fan_trigger_temp=None, heater_trigger_temp=0,
        )
        with patch("telemetry.time.monotonic", return_value=22.125):
            payload = telemetry_payload(3, observed)
        self.assertEqual(payload["printer_status"], "unknown")
        self.assertEqual(payload["voice_code"], 0)
        self.assertIs(payload["green_blink"], False)
        self.assertIsNone(payload["red_blink"])
        self.assertIs(payload["fan_on"], False)
        self.assertIsNone(payload["heater_on"])
        self.assertEqual(payload["temperature"], 0)
        self.assertIsNone(payload["fan_trigger_temp"])
        self.assertEqual(payload["heater_trigger_temp"], 0)
        self.assertEqual(payload["sample_age_ms"], 2125)
        self.assertNotIn("printer_sample_age_ms", payload)


class WaitingSocket:
    def __init__(self):
        self.sending = asyncio.Event()
        self.receiving = asyncio.Event()
        self.sender_cancelled = asyncio.Event()
        self.receiver_cancelled = asyncio.Event()

    async def send(self, message):
        self.sending.set()
        try:
            await asyncio.Future()
        finally:
            self.sender_cancelled.set()

    async def __aiter__(self):
        self.receiving.set()
        try:
            await asyncio.Future()
            yield "unreachable"
        finally:
            self.receiver_cancelled.set()


class ClosingWhileSendingSocket(WaitingSocket):
    def __init__(self, code):
        super().__init__()
        self.close_error = ConnectionClosedError(Close(code, "Server rejected publication"), None)
        self.receiver_closed = asyncio.Event()

    async def __aiter__(self):
        self.receiving.set()
        try:
            await self.sending.wait()
            raise self.close_error
            yield "unreachable"
        finally:
            self.receiver_closed.set()


class PublisherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tasks = []
        self.updates = LatestTelemetryBuffer()
        self.frames = asyncio.Queue()

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        for task in self.tasks:
            with suppress(asyncio.CancelledError):
                await task

    def start(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    def publisher(self, *, seed=True, refresh_stations=None):
        if seed:
            self.updates.update(3, sample())
        publisher = StationTelemetryPublisher(
            "ws://127.0.0.1:8000/weights/", "token", self.updates,
            refresh_stations=refresh_stations,
        )
        publisher.failures = Mock()
        publisher.logger = Mock()
        return publisher

    async def collect(self, message):
        self.frames.put_nowait(json.loads(message))

    async def frame(self):
        return await asyncio.wait_for(self.frames.get(), 1)

    async def assert_no_frame(self):
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.frames.get(), 0.02)

    async def test_initial_station_without_measurement_sends_only_status(self):
        publisher = self.publisher(seed=False)
        self.updates.update(3, StationTelemetrySnapshot())
        self.start(publisher.send_telemetry(Mock(send=AsyncMock(side_effect=self.collect))))
        self.assertEqual(await self.frame(), status_packet(3, "waiting", sequence=1))
        await self.assert_no_frame()

    async def test_each_received_sample_is_sent_even_when_weight_is_unchanged(self):
        publisher = self.publisher()
        self.start(publisher.send_telemetry(Mock(send=AsyncMock(side_effect=self.collect))))
        self.assertEqual(await self.frame(), status_packet(3, sequence=1))
        async with asyncio.timeout(1):
            for sequence in range(1, 21):
                if sequence > 1:
                    self.updates.update(3, sample(sequence=sequence))
                frame = await self.frames.get()
                self.assertEqual(frame["type"], "station.telemetry")
                self.assertEqual(frame["sequence"], sequence + 1)
                self.assertEqual(frame["payload"]["sample_seq"], sequence)
                self.assertEqual(frame["payload"]["weight"], "1")

    async def test_stations_send_individually_on_one_socket_without_waiting_for_others(self):
        publisher = self.publisher(seed=False)
        ws = Mock(send=AsyncMock(side_effect=self.collect))
        self.start(publisher.send_telemetry(ws))
        sequences = {}
        packets = []
        for station_id in (3, 9, 3, 15):
            sequences[station_id] = sequences.get(station_id, 0) + 1
            self.updates.update(station_id, sample(str(station_id), sequence=sequences[station_id]))
            if sequences[station_id] == 1:
                status = await self.frame()
                self.assertEqual(status["type"], "station.status")
                packets.append(status)
            frame = await self.frame()
            self.assertEqual(frame["type"], "station.telemetry")
            self.assertEqual(frame["payload"]["station_id"], station_id)
            packets.append(frame)
        self.assertEqual([packet["sequence"] for packet in packets], list(range(1, 8)))
        self.assertEqual(ws.send.await_count, 7)

    async def test_idle_heartbeat_shares_sequence_and_does_not_replay_samples(self):
        publisher = self.publisher()
        self.assertEqual(publisher.heartbeat_interval, 1.0)
        publisher.heartbeat_interval = 0.02
        self.start(publisher.send_telemetry(Mock(send=AsyncMock(side_effect=self.collect))))
        self.assertEqual((await self.frame())["type"], "station.status")
        self.assertEqual((await self.frame())["type"], "station.telemetry")
        self.assertEqual(await self.frame(), {"type": "gateway.heartbeat", "sequence": 3})
        self.updates.update(3, sample(sequence=2))
        frame = await self.frame()
        self.assertEqual(frame["type"], "station.telemetry")
        self.assertEqual(frame["sequence"], 4)
        self.assertEqual(frame["payload"]["sample_seq"], 2)

    async def test_status_change_sends_no_sample_fields_or_duplicate_weight(self):
        publisher = self.publisher(seed=False)
        first = sample()
        self.updates.update(3, first)
        self.start(publisher.send_telemetry(Mock(send=AsyncMock(side_effect=self.collect))))
        await self.frame()
        await self.frame()
        self.updates.update(3, replace(first, connection_status="disconnected", sample_valid=False))
        self.assertEqual(await self.frame(), status_packet(3, "disconnected", sequence=3))
        self.updates.update(3, replace(first, sample_valid=False))
        self.assertEqual(await self.frame(), status_packet(3, sequence=4))
        await self.assert_no_frame()
        self.updates.update(3, sample("2", sequence=2))
        self.assertEqual((await self.frame())["payload"]["sample_seq"], 2)

    async def test_slow_send_keeps_latest_per_station_and_device_thread_keeps_receiving(self):
        publisher = self.publisher()
        release = asyncio.Event()

        async def send(message):
            await self.collect(message)
            if json.loads(message)["sequence"] == 2:
                await release.wait()

        self.start(publisher.send_telemetry(Mock(send=AsyncMock(side_effect=send))))
        await self.frame()
        self.assertEqual((await self.frame())["payload"]["weight"], "1")
        devices = []
        for station_id in (3, 9):
            serial = Mock(port=f"COM{station_id}")
            serial.send_and_receive.return_value = ResponsePacket(weight_value=Decimal(station_id))
            devices.append(WeighingStationWorker(
                serial, station_id=station_id,
                on_telemetry=lambda snapshot, i=station_id: self.updates.update(i, snapshot),
            ))

        def receive():
            for _ in range(100):
                for device in devices:
                    device.exchange(DisplayRequestPacket())

        # All 200 hardware responses finish while the only network sender is blocked.
        await asyncio.wait_for(asyncio.to_thread(receive), 1)
        self.assertTrue(self.frames.empty())
        for device in devices:
            self.assertEqual(device.get_telemetry().sample_seq, 100)
        release.set()
        packets = [await self.frame() for _ in range(3)]
        latest = [packet for packet in packets if packet["type"] == "station.telemetry"]
        self.assertEqual([frame["payload"]["station_id"] for frame in latest], [3, 9])
        self.assertEqual([frame["payload"]["sample_seq"] for frame in latest], [100, 100])
        status_index = next(index for index, packet in enumerate(packets) if packet["type"] == "station.status")
        weight_index = next(index for index, packet in enumerate(packets)
                            if packet["type"] == "station.telemetry" and packet["payload"]["station_id"] == 9)
        status = packets[status_index]["payload"]
        self.assertEqual((status["station_id"], status["connection_status"]), (9, "connected"))
        self.assertEqual(set(status), {"station_id", "connection_status"})
        self.assertEqual(packets[weight_index]["payload"]["printer_status"], "normal")
        self.assertLess(status_index, weight_index)
        await self.assert_no_frame()

    async def test_status_during_blocked_send_discards_pending_old_sample(self):
        publisher = self.publisher()
        release = asyncio.Event()

        async def send(message):
            await self.collect(message)
            if json.loads(message)["sequence"] == 2:
                await release.wait()

        self.start(publisher.send_telemetry(Mock(send=AsyncMock(side_effect=send))))
        await self.frame()
        await self.frame()
        pending = sample("99", sequence=2)
        self.updates.update(3, pending)
        self.updates.update(3, replace(pending, connection_status="disconnected", sample_valid=False))
        release.set()
        self.assertEqual(await self.frame(), status_packet(3, "disconnected", sequence=3))
        await self.assert_no_frame()

    async def test_new_sample_arriving_during_status_send_is_sent_after_that_status(self):
        publisher = self.publisher(seed=False)
        self.updates.update(3, StationTelemetrySnapshot(connection_status="connected"))
        release = asyncio.Event()

        async def send(message):
            await self.collect(message)
            if json.loads(message)["sequence"] == 1:
                await release.wait()

        self.start(publisher.send_telemetry(Mock(send=AsyncMock(side_effect=send))))
        self.assertEqual(await self.frame(), status_packet(3, sequence=1))
        self.updates.update(3, sample("7"))
        self.updates.update(3, sample("8", sequence=2))
        release.set()
        frame = await self.frame()
        self.assertEqual((frame["type"], frame["sequence"], frame["payload"]["weight"]), ("station.telemetry", 2, "8"))
        await self.assert_no_frame()

    async def test_disconnect_during_replay_status_send_prevents_old_sample_replay(self):
        publisher = self.publisher()
        release = asyncio.Event()

        async def send(message):
            await self.collect(message)
            if json.loads(message)["sequence"] == 1:
                await release.wait()

        self.updates.replay_latest()
        self.start(publisher.send_telemetry(Mock(send=AsyncMock(side_effect=send))))
        await self.frame()
        self.updates.update(3, StationTelemetrySnapshot(connection_status="disconnected", sample_seq=1))
        release.set()
        self.assertEqual(await self.frame(), status_packet(3, "disconnected", sequence=2))
        await self.assert_no_frame()

    async def test_pending_age_is_calculated_after_a_slow_send_finishes(self):
        publisher = self.publisher()
        release = asyncio.Event()

        async def send(message):
            await self.collect(message)
            if json.loads(message)["sequence"] == 1:
                await release.wait()

        self.start(publisher.send_telemetry(Mock(send=AsyncMock(side_effect=send))))
        await self.frame()
        self.updates.update(3, replace(
            sample(sequence=2, sampled_monotonic=time.monotonic() - 2),
            printer_status=PrinterStatus.NO_PAPER,
        ))
        release.set()
        payload = (await self.frame())["payload"]
        self.assertGreaterEqual(payload["sample_age_ms"], 2000)
        self.assertNotIn("printer_sample_age_ms", payload)
        self.assertEqual(payload["printer_status"], "no_paper")

    async def test_send_timeout_cancels_receiver(self):
        publisher = self.publisher()
        publisher.send_timeout = 0.02
        ws = WaitingSocket()
        with self.assertRaises(TimeoutError):
            await publisher.run_connection(ws)
        self.assertTrue(ws.sender_cancelled.is_set())
        self.assertTrue(ws.receiver_cancelled.is_set())

    async def assert_close_survives_simultaneous_send_timeout(self, code):
        publisher = self.publisher()
        publisher.send_timeout = 0.01
        ws = ClosingWhileSendingSocket(code)
        completed_tasks = []

        async def wait_for_both(tasks, *, return_when):
            self.assertEqual(return_when, asyncio.FIRST_COMPLETED)
            completed_tasks.extend(tasks)
            await asyncio.gather(*tasks, return_exceptions=True)
            # asyncio.wait returns an unordered set. Force the valid sender-first
            # order after both real tasks fail, so this race fails deterministically.
            return tuple(tasks), set()

        with patch("telemetry.asyncio.wait", side_effect=wait_for_both):
            with self.assertRaises(ConnectionClosed) as raised:
                await publisher.run_connection(ws)

        self.assertIs(raised.exception, ws.close_error)
        self.assertEqual(raised.exception.rcvd.code, code)
        self.assertTrue(ws.sender_cancelled.is_set())
        self.assertTrue(ws.receiver_closed.is_set())
        self.assertTrue(all(task.done() for task in completed_tasks))
        self.assertIsInstance(completed_tasks[0].exception(), TimeoutError)
        self.assertIs(completed_tasks[1].exception(), ws.close_error)

    async def test_simultaneous_send_timeout_preserves_terminal_server_close(self):
        await self.assert_close_survives_simultaneous_send_timeout(4409)

    async def test_simultaneous_send_timeout_preserves_station_refresh_server_close(self):
        await self.assert_close_survives_simultaneous_send_timeout(4403)

    async def test_shutdown_cancels_sender_and_receiver(self):
        publisher = self.publisher()
        ws = WaitingSocket()
        task = self.start(publisher.run_connection(ws))
        await asyncio.wait_for(ws.sending.wait(), 1)
        await asyncio.wait_for(ws.receiving.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(ws.sender_cancelled.is_set())
        self.assertTrue(ws.receiver_cancelled.is_set())

    async def test_reconnect_replays_status_then_latest_with_original_sample_time(self):
        connections = []
        recovered = asyncio.Event()
        publisher = self.publisher(seed=False)
        sampled_monotonic = time.monotonic() - 2
        for station_id in (3, 9):
            self.updates.update(station_id, sample(str(station_id), sampled_monotonic=sampled_monotonic))

        async def handler(ws):
            packets = [json.loads(await ws.recv()) for _ in range(4)]
            connections.append((ws.request.path, ws.request.headers["Authorization"], packets))
            if len(connections) < 3:
                for sequence in range(2, 102):
                    for station_id in (3, 9):
                        self.updates.update(station_id, sample(
                            str(station_id * sequence), sequence=sequence + (len(connections) - 1) * 100,
                            sampled_monotonic=sampled_monotonic,
                        ))
                await ws.close(code=1012 if len(connections) == 1 else 1013)
            else:
                await ws.send("ignored server notification")
                recovered.set()
                await ws.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            publisher.url = f"ws://127.0.0.1:{port}/ws/devices/gateways/7/weights/"
            publisher.retry_interval = 0.005
            task = self.start(publisher.run())
            await asyncio.wait_for(recovered.wait(), 2)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        self.assertEqual(len(connections), 3)
        for index, (path, auth, packets) in enumerate(connections):
            self.assertEqual(path, "/ws/devices/gateways/7/weights/")
            self.assertEqual(auth, "Gateway token")
            self.assertEqual([packet["sequence"] for packet in packets], [1, 2, 3, 4])
            self.assertEqual([packet["type"] for packet in packets], [
                "station.status", "station.status", "station.telemetry", "station.telemetry",
            ])
            self.assertEqual([packet["payload"]["station_id"] for packet in packets], [3, 9, 3, 9])
            for packet in packets[2:]:
                payload = packet["payload"]
                self.assertEqual(payload["sample_seq"], 1 + index * 100)
                self.assertEqual(payload["weight"], str(payload["station_id"] * (1 if index == 0 else 101)))
                self.assertEqual(payload["sampled_at"], "2026-10-13T00:00:00+00:00")
                self.assertGreaterEqual(payload["sample_age_ms"], 2000)

    async def test_shared_and_sample_sequences_stay_strict_on_a_real_socket(self):
        packets = []
        received = asyncio.Queue()
        publisher = self.publisher(seed=False)
        publisher.heartbeat_interval = 0.04

        async def handler(ws):
            # Cancelling the publisher closes its context with a 1011 frame.
            with suppress(ConnectionClosed):
                async for message in ws:
                    packet = json.loads(message)
                    packets.append(packet)
                    received.put_nowait(packet)

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            publisher.url = f"ws://127.0.0.1:{port}/weights/"
            task = self.start(publisher.run())
            for station_id in (3, 9):
                self.updates.update(station_id, StationTelemetrySnapshot(connection_status="connected"))
                self.assertEqual((await asyncio.wait_for(received.get(), 1))["type"], "station.status")
            for sample_seq, station_id in ((1, 3), (1, 9), (2, 3), (2, 9)):
                self.updates.update(station_id, replace(
                    sample(sequence=sample_seq),
                    printer_status=PrinterStatus.NO_PAPER if sample_seq == 1 else PrinterStatus.NORMAL,
                ))
                self.assertEqual((await asyncio.wait_for(received.get(), 1))["type"], "station.telemetry")
            self.updates.update(3, sample("999", sequence=2))
            self.updates.update(9, sample("888", sequence=1))
            heartbeat = await asyncio.wait_for(received.get(), 1)
            self.assertEqual(heartbeat["type"], "gateway.heartbeat")
            self.assertNotIn("payload", heartbeat)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        self.assertEqual([packet["sequence"] for packet in packets], list(range(1, len(packets) + 1)))
        for station_id in (3, 9):
            weights = [packet["payload"] for packet in packets
                       if packet["type"] == "station.telemetry" and packet["payload"]["station_id"] == station_id]
            self.assertEqual([payload["sample_seq"] for payload in weights], [1, 2])
            self.assertEqual([payload["weight"] for payload in weights], ["1", "1"])
            self.assertEqual([payload["printer_status"] for payload in weights], ["no_paper", "normal"])
            self.assertTrue(all(payload["sample_age_ms"] >= 0 for payload in weights))

    async def test_raw_responses_report_partial_failures_and_clear_on_frame_loss_over_websocket(self):
        received = asyncio.Queue()
        publisher = self.publisher(seed=False)
        serial = Mock(port="test-port")
        worker = WeighingStationWorker(
            serial, station_id=3, on_telemetry=lambda observation: self.updates.update(3, observation),
        )
        worker.logger = Mock()

        async def handler(ws):
            with suppress(ConnectionClosed):
                async for message in ws:
                    received.put_nowait(json.loads(message))

        async def next_packet():
            return await asyncio.wait_for(received.get(), 1)

        async with serve(handler, "127.0.0.1", 0) as server:
            publisher.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/weights/"
            task = self.start(publisher.run())
            serial.send_and_receive.return_value = ResponsePacket.from_bytes(frame(
                printer=b"1", voice=b"04", relay=b"19", temperature=b"+28", fan=b"30", heater=b"05",
            ))
            await asyncio.to_thread(worker.exchange, DisplayRequestPacket())
            self.assertEqual((await next_packet())["type"], "station.status")
            first = await next_packet()
            self.assertEqual(first["type"], "station.telemetry")
            payload = first["payload"]
            self.assertTrue(payload["weight_valid"])
            self.assertEqual(payload["printer_status"], "no_paper")
            self.assertEqual(payload["voice_code"], 4)
            self.assertTrue(payload["green_blink"])
            self.assertFalse(payload["red_blink"])
            self.assertTrue(payload["fan_on"])
            self.assertTrue(payload["heater_on"])
            self.assertEqual(payload["temperature"], 28)
            self.assertEqual(payload["fan_trigger_temp"], 30)
            self.assertEqual(payload["heater_trigger_temp"], 5)
            self.assertEqual(payload["sample_seq"], 1)

            serial.send_and_receive.return_value = ResponsePacket.from_bytes(frame(
                weight=b"ST,NT," + b"\x00" * 8 + b"kg", printer=b"0", voice=b"00", relay=b"02",
                temperature=b"+29", fan=b"40", heater=b"00",
            ))
            await asyncio.to_thread(worker.exchange, DisplayRequestPacket())
            second = await next_packet()
            self.assertEqual(second["type"], "station.telemetry")
            payload = second["payload"]
            self.assertIsNone(payload["weight"])
            self.assertFalse(payload["weight_valid"])
            self.assertEqual(payload["printer_status"], "normal")
            self.assertEqual(payload["voice_code"], 0)
            self.assertFalse(payload["green_blink"])
            self.assertTrue(payload["red_blink"])
            self.assertFalse(payload["fan_on"])
            self.assertFalse(payload["heater_on"])
            self.assertEqual(payload["temperature"], 29)
            self.assertEqual(payload["fan_trigger_temp"], 40)
            self.assertEqual(payload["heater_trigger_temp"], 0)
            self.assertEqual(payload["sample_seq"], 2)
            self.assertNotEqual(payload["sampled_at"], first["payload"]["sampled_at"])

            serial.send_and_receive.return_value = ResponsePacket.from_bytes(frame(
                printer=b"\x00", voice=b"\x00\x00", relay=b"\x00\x00", temperature=b"\x00\x00\x00",
                fan=b"\x00\x00", heater=b"\x00\x00",
            ))
            await asyncio.to_thread(worker.exchange, DisplayRequestPacket())
            third = await next_packet()
            self.assertEqual(third["type"], "station.telemetry")
            payload = third["payload"]
            self.assertTrue(payload["weight_valid"])
            self.assertEqual(payload["printer_status"], "unknown")
            self.assertIsNone(payload["voice_code"])
            self.assertIsNone(payload["green_blink"])
            self.assertIsNone(payload["red_blink"])
            self.assertIsNone(payload["fan_on"])
            self.assertIsNone(payload["heater_on"])
            self.assertIsNone(payload["temperature"])
            self.assertIsNone(payload["fan_trigger_temp"])
            self.assertIsNone(payload["heater_trigger_temp"])
            self.assertEqual(payload["sample_seq"], 3)

            serial.send_and_receive.side_effect = ValueError("invalid frame")
            with self.assertRaises(ValueError):
                await asyncio.to_thread(worker.exchange, DisplayRequestPacket())
            self.assertEqual(await next_packet(), status_packet(3, "error", sequence=5))
            self.assertFalse(worker.get_telemetry().sample_valid)
            self.assertIsNone(worker.get_telemetry().voice_code)
            self.assertIsNone(worker.get_telemetry().temperature)
            self.assertIsNone(worker.get_telemetry().fan_trigger_temp)
            self.assertIsNone(worker.get_telemetry().heater_trigger_temp)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def test_terminal_websocket_close_codes_stop_without_retry(self):
        for code in (1009, 4400, 4401, 4409, 4429):
            with self.subTest(code=code):
                attempts = 0

                async def handler(ws):
                    nonlocal attempts
                    attempts += 1
                    await ws.close(code=code)

                async with serve(handler, "127.0.0.1", 0) as server:
                    port = server.sockets[0].getsockname()[1]
                    publisher = self.publisher()
                    publisher.url = f"ws://127.0.0.1:{port}/weights/"
                    publisher.retry_interval = 0.005
                    self.assertEqual(await asyncio.wait_for(publisher.run(), 1), code)
                    await asyncio.sleep(0.02)
                self.assertEqual(attempts, 1)

    async def test_superseded_publisher_stops_without_cancelling_control_sibling(self):
        telemetry_stopped = asyncio.Event()
        control_started = asyncio.Event()
        control_stopped = asyncio.Event()

        async def handler(ws):
            await ws.close(code=4409)

        async def control():
            control_started.set()
            try:
                await asyncio.Future()
            finally:
                control_stopped.set()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            publisher = self.publisher()
            publisher.url = f"ws://127.0.0.1:{port}/weights/"

            async def telemetry():
                self.assertEqual(await publisher.run(), 4409)
                telemetry_stopped.set()

            async def run_group():
                async with asyncio.TaskGroup() as group:
                    group.create_task(control())
                    group.create_task(telemetry())

            task = self.start(run_group())
            await asyncio.wait_for(control_started.wait(), 1)
            await asyncio.wait_for(telemetry_stopped.wait(), 1)
            self.assertFalse(task.done())
            self.assertFalse(control_stopped.is_set())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_unauthorized_handshakes_stop_without_retry(self):
        for status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            with self.subTest(status=status):
                attempts = 0

                def reject(connection, request):
                    nonlocal attempts
                    attempts += 1
                    return connection.respond(status, "Unavailable")

                async with serve(AsyncMock(), "127.0.0.1", 0, process_request=reject) as server:
                    port = server.sockets[0].getsockname()[1]
                    publisher = self.publisher()
                    publisher.url = f"ws://127.0.0.1:{port}/weights/"
                    self.assertEqual(await asyncio.wait_for(publisher.run(), 1), status)
                self.assertEqual(attempts, 1)

    async def test_unsupported_endpoint_retries_without_escaping_active_tasks(self):
        rejected = asyncio.Event()
        attempts = 0

        def reject(connection, request):
            nonlocal attempts
            attempts += 1
            if attempts >= 2:
                rejected.set()
            return connection.respond(HTTPStatus.NOT_FOUND, "Unavailable")

        async with serve(AsyncMock(), "127.0.0.1", 0, process_request=reject) as server:
            port = server.sockets[0].getsockname()[1]
            publisher = self.publisher()
            publisher.url = f"ws://127.0.0.1:{port}/weights/"
            publisher.retry_interval = 0.005
            task = self.start(publisher.run())
            await asyncio.wait_for(rejected.wait(), 1)
            self.assertFalse(task.done())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_station_ownership_rejection_waits_for_successful_refresh_and_purges_removed_station(self):
        attempts = 0
        refresh_attempts = 0
        recovered = asyncio.Event()
        packets = []

        async def refresh():
            nonlocal refresh_attempts
            refresh_attempts += 1
            self.assertEqual(attempts, 1)
            if refresh_attempts == 1:
                return False
            self.updates.remove(3)
            return True

        publisher = self.publisher(refresh_stations=refresh)
        self.updates.update(9, sample("9"))

        async def handler(ws):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                await ws.close(code=4403)
                return
            packets.extend([json.loads(await ws.recv()), json.loads(await ws.recv())])
            recovered.set()
            await ws.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            publisher.url = f"ws://127.0.0.1:{port}/weights/"
            publisher.retry_interval = 0.005
            task = self.start(publisher.run())
            await asyncio.wait_for(recovered.wait(), 2)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        self.assertEqual(attempts, 2)
        self.assertEqual(refresh_attempts, 2)
        self.assertEqual([packet["type"] for packet in packets], ["station.status", "station.telemetry"])
        self.assertEqual([packet["sequence"] for packet in packets], [1, 2])
        self.assertEqual([packet["payload"]["station_id"] for packet in packets], [9, 9])


class PublisherStartupTests(unittest.IsolatedAsyncioTestCase):
    def client(self):
        client = object.__new__(HeadlessClient)
        client._telemetry_stop_code = None
        client._station_config_ready = asyncio.Event()
        client.ws_url = "wss://scale.example"
        client.gateway_id = 8
        client.access_token = "new-token"
        client.ws_kwargs = {"ssl": object()}
        client.telemetry_updates = LatestTelemetryBuffer()
        client.sync_weighing_stations = AsyncMock(return_value=True)
        return client

    async def test_waits_for_station_sync_and_passes_current_identity_tls_and_refresh(self):
        client = self.client()
        publisher = Mock(run=AsyncMock(return_value=4409))
        with patch("main.StationTelemetryPublisher", return_value=publisher) as constructor:
            task = asyncio.create_task(client.run_telemetry_loop())
            try:
                await asyncio.sleep(0)
                constructor.assert_not_called()
                client._station_config_ready.set()
                await asyncio.wait_for(task, 1)
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            constructor.assert_called_once_with(
                url="wss://scale.example/ws/devices/gateways/8/weights/",
                access_token="new-token", updates=client.telemetry_updates,
                ws_kwargs=client.ws_kwargs, refresh_stations=client.sync_weighing_stations,
            )
            publisher.run.assert_awaited_once()
            self.assertEqual(client._telemetry_stop_code, 4409)

    async def test_terminal_stop_is_preserved_across_control_loop_restarts(self):
        client = self.client()
        client._station_config_ready.set()
        publisher = Mock(run=AsyncMock(return_value=4409))
        with patch("main.StationTelemetryPublisher", return_value=publisher) as constructor:
            await client.run_telemetry_loop()
            # A new active TaskGroup must not let a superseded publisher reclaim ownership.
            await client.run_telemetry_loop()
            constructor.assert_called_once()
            publisher.run.assert_awaited_once()
        self.assertEqual(client._telemetry_stop_code, 4409)

    async def test_rejected_credential_lets_a_renewed_approval_publish_again(self):
        client = self.client()
        client.logger = Mock()
        client._station_config_ready.set()
        publisher = Mock(run=AsyncMock(return_value=401))
        with patch("main.StationTelemetryPublisher", return_value=publisher) as constructor:
            await client.run_telemetry_loop()
            client.forget_server_session()
            # The next approval completes a station sync before publishing again.
            client._station_config_ready.set()
            await client.run_telemetry_loop()
        self.assertEqual(constructor.call_count, 2)
        self.assertEqual(client._telemetry_stop_code, 401)

    async def test_control_and_ownership_refresh_share_serialized_snapshots(self):
        client = self.client()
        del client.sync_weighing_stations
        client._settings_sync_requested = asyncio.Event()
        client._settings_sync_pending = None
        client.network_failures = Mock()
        client.retry_interval = 0.001
        entered = asyncio.Queue()
        release = asyncio.Queue()

        async def synchronize():
            entered.put_nowait(None)
            await release.get()
            return True

        client.sync_settings = AsyncMock(side_effect=synchronize)
        first = asyncio.create_task(client.sync_weighing_stations())
        first_control = client.request_settings_sync()
        await asyncio.sleep(0)
        worker = asyncio.create_task(client.run_settings_sync_worker())
        second = None
        try:
            await asyncio.wait_for(entered.get(), 1)
            second = asyncio.create_task(client.sync_weighing_stations())
            second_control = client.request_settings_sync()
            await asyncio.sleep(0)
            self.assertTrue(entered.empty())
            self.assertEqual(client.sync_settings.await_count, 1)
            self.assertFalse(first.done())
            self.assertFalse(second.done())
            release.put_nowait(None)
            self.assertEqual(await asyncio.wait_for(asyncio.gather(first, first_control), 1), [True, True])
            await asyncio.wait_for(entered.get(), 1)
            self.assertFalse(second.done())
            self.assertFalse(second_control.done())
            release.put_nowait(None)
            self.assertEqual(await asyncio.wait_for(asyncio.gather(second, second_control), 1), [True, True])
            self.assertEqual(client.sync_settings.await_count, 2)
        finally:
            worker.cancel()
            first.cancel()
            if second is not None:
                second.cancel()
            await asyncio.gather(worker, first, *([second] if second is not None else []), return_exceptions=True)
