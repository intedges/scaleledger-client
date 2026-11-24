from concurrent.futures import Future
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
import json
import unittest
from unittest.mock import Mock, patch
import uuid

from cache import MarketDataCache, RFIDInfo
from device_configuration import DeviceConfiguration
from managers import WeighingStationManager
from models import WeighingStation
from printer import EscPosBuilder, Receipt, ReceiptTemplate
from suwol1000 import PrinterRequestPacket, WorkerState
from tests.helpers import prepare_vehicle, receipt_text_and_qrs


class ConfigurationReceiptTests(unittest.TestCase):
    def receipt(self, **values):
        return Receipt(
            record_uuid=uuid.uuid4(), gateway_name="현장 PC", station_name="저울 1",
            rfid_card_uid="12345678", producer_name="생산자", species_name="",
            weight=Decimal(123), measured_at=datetime(2026, 11, 15, tzinfo=UTC), **values,
        )

    def test_auction_header_is_optional_and_preserves_gateway_and_station(self):
        base = self.receipt()
        original = ReceiptTemplate.render(base).decode("cp949")
        named = ReceiptTemplate.render(replace(base, auction_name="동해수산물경매장")).decode("cp949")
        self.assertNotIn("동해수산물경매장", original)
        self.assertIn("동해수산물경매장", named)
        self.assertIn("현장 PC", named)
        self.assertIn("저울 1", named)
        self.assertLess(named.index("동해수산물경매장"), named.index("계량번호"))

    def test_receipts_lead_with_number_and_weight_and_print_uuid_and_qr(self):
        receipt = self.receipt(source_station_id=11)
        for number, day, label in (
            (None, None, "계량번호 미확인"),
            (18, date(2026, 11, 14), "계량번호 18번"),
        ):
            with self.subTest(number=number):
                raw = ReceiptTemplate.render(replace(receipt, daily_number=number, number_date=day))
                text, qrs = receipt_text_and_qrs(raw)
                qr, = qrs
                self.assertEqual(qr, {
                    "uuid": str(receipt.record_uuid), "station_id": 11, "rfid_card_uid": "12345678",
                    "weight": 123, "measured_at": "2026-11-15T00:00:00+00:00",
                })
                self.assertIs(type(qr["weight"]), int)
                self.assertIn(json.dumps(qr, separators=(",", ":")).encode(), raw)
                weight_at = text.index("123 kg")
                uuid_at = text.index(f"\n{receipt.record_uuid}\n")
                self.assertLess(text.index(label), weight_at)
                for detail in (receipt.producer_name, receipt.rfid_card_uid, receipt.gateway_name,
                               receipt.station_name, "계량일시"):
                    detail_at = text.index(detail)
                    self.assertLess(weight_at, detail_at)
                    self.assertLess(detail_at, uuid_at)
                if day:
                    self.assertIn("기준일 2026-11-14", text)
                    self.assertNotIn("미확인", text)
                else:
                    self.assertNotIn("기준일", text)

    def test_long_korean_header_wraps_without_splitting_characters(self):
        name = ("가나다라마바ABC " * 10)[:100]
        rendered = EscPosBuilder().add_wrapped_text(name).build()[3:]
        lines = rendered.splitlines()
        self.assertEqual(b"".join(lines).decode("cp949"), name)
        self.assertTrue(all(len(line) <= 42 for line in lines))
        self.assertTrue(all(line.decode("cp949") for line in lines))

    def test_new_workers_share_configuration_and_updates_do_not_restart_ports(self):
        manager = WeighingStationManager(Mock(), MarketDataCache())
        initial = DeviceConfiguration(print_copies=2, serial_read_timeout=1.5, auction_name="경매장")
        updated = replace(initial, print_copies=9)
        stations = [WeighingStation(id=i, gateway_id=1, name=f"저울 {i}", serial_port=f"COM{i}")
                    for i in (1, 2)]
        with patch("managers.threading.Thread") as thread:
            thread.return_value.is_alive.return_value = False
            manager.sync(stations, configuration=initial)
            workers = [runtime.worker for runtime in manager.workers.values()]
            for worker in workers:
                self.assertEqual(worker.configuration, initial)
                self.assertEqual(worker.client.timeout, 1.5)
            manager.sync(stations, configuration=updated)
            self.assertEqual([runtime.worker for runtime in manager.workers.values()], workers)
            self.assertEqual(thread.call_count, 2)
            # A changed port starts a new owner with the latest configuration.
            stations[0].serial_port = "COM3"
            manager.sync(stations, configuration=updated)
            self.assertIsNot(manager.workers[1].worker, workers[0])
            self.assertEqual(manager.workers[1].worker.configuration, updated)

    def test_receipt_and_copies_are_frozen_before_a_delayed_save(self):
        result = Future()
        cache = MarketDataCache()
        cache.replace({"12345678": RFIDInfo(True, "기존 생산자", "기존 품목")}, "기존 PC")
        manager = WeighingStationManager(Mock(return_value=result), cache)
        initial = DeviceConfiguration(auction_name="기존 경매장", print_copies=2)
        station = WeighingStation(id=1, gateway_id=1, name="기존 저울", serial_port="COM1")
        with patch("managers.threading.Thread"), patch("suwol1000.time.monotonic", return_value=10):
            manager.sync([station], configuration=initial)
            worker = manager.workers[1].worker
            worker.last_plate = "12345678"
            worker.last_weight = Decimal(123)
            prepare_vehicle(worker)
            self.assertEqual(worker.measure(), WorkerState.SAVE)
            cache.replace({"12345678": RFIDInfo(True, "새 생산자", "새 품목")}, "새 PC")
            station.name = "새 저울"
            manager.sync([station], configuration=replace(initial, auction_name="새 경매장", print_copies=9))
            result.set_result(None)
            self.assertEqual(worker.save(), WorkerState.PRINT)
            worker.exchange = Mock()
            worker.print()
        request = worker.exchange.call_args.args[0]
        self.assertIsInstance(request, PrinterRequestPacket)
        self.assertEqual(request.copies, 2)
        text, _ = receipt_text_and_qrs(request.document_bytes)
        for original in ("기존 경매장", "기존 PC", "기존 저울", "기존 생산자", "기존 품목"):
            self.assertIn(original, text)
        for changed in ("새 경매장", "새 PC", "새 저울", "새 생산자", "새 품목"):
            self.assertNotIn(changed, text)
        manager.on_event.assert_called_once()
        worker.exchange.assert_called_once()
