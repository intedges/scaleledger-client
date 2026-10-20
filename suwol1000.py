from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
import dataclasses
from datetime import UTC, datetime
from decimal import Decimal
from enum import auto, Enum, IntEnum, StrEnum, IntFlag
import logging
import math
from typing import ClassVar, Literal, Self

import threading
import time

import serial
import structlog
from structlog.stdlib import get_logger

from events import BaseEvent, RFIDTaggedEvent, WeighingCompletedEvent
from logconfig import RepeatedFailureLog
from weighing_cycle import CycleState, WeighingCycle


STX = 2
ETX = 3


class CommandCode(StrEnum):
    DISPLAY = "D"      # 기본 표시 및 제어
    PRINTER = "P"      # 프린터 출력
    TEMPERATURE = "T"  # 온도 설정


class VoiceCode(IntEnum):
    NONE = 0               # (출력 없음)
    WEIGHT_COMPLETE = 1    # 계량이 끝났습니다.
    STAND_BY = 2           # 잠시 대기하십시오.
    PLEASE_WAIT = 3        # 잠시만 기다려주십시오.
    TAG_CARD = 4           # 카드를 대어주십시오.
    INDICATOR_ERROR = 5    # 인디케이터 이상입니다.
    OVERLOAD = 6           # 과적입니다.
    CHECK_ADMIN = 7        # 관리자에게 확인하십시오.
    UNREGISTERED_CARD = 8  # 등록되지 않은 카드입니다.
    WARNING_BEEP = 9       # 삐삐삐삐삐 (단순 경고음)
    SYSTEM_ERROR = 10      # 시스템 이상입니다. 잠시만 기다려주십시오.
    THANK_YOU = 11         # 이용해주셔서 감사합니다.
    ALERT_SOUND = 12       # 찌잉 (경고음)


class InputCode(StrEnum):
    NONE = "0"           # (입력 없음)
    VEHICLE_NO = "N"     # 차량번호
    CUSTOMER_CODE = "C"  # 거래처
    PRODUCT_CODE = "M"   # 제품
    REPRINT = "P"        # 전표 재발행


class RelayCode(IntFlag):
    OFF    = 0
    GREEN  = 1 << 0  # Relay 1: 녹색등 점멸
    RED    = 1 << 1  # Relay 2: 적색등 점멸
    RELAY3 = 1 << 2  # Relay 3: 모름
    FAN    = 1 << 3  # Relay 4: 팬 작동
    HEATER = 1 << 4  # Relay 5: 히터 작동
    RELAY6 = 1 << 5  # Relay 6: 모름
    RELAY7 = 1 << 6  # Relay 7: 모름
    RELAY8 = 1 << 7  # Relay 8: 모름


class PrinterStatus(IntEnum):
    NORMAL = 0        # 정상
    NO_PAPER = 1      # 용지없음
    TRANSMITTING = 2  # 전송중


class WeightStatus(StrEnum):
    NONE = "00"      # 신호 없음
    STABLE = "ST"    # 안정 (Stable)
    UNSTABLE = "US"  # 불안정 (Unstable)
    OVERLOAD = "OL"  # 과적 (Overload)


class WeightType(StrEnum):
    NONE = "00"   # 신호 없음
    NET = "NT"    # 순중량 (Net)
    GROSS = "GS"  # 총중량 (Gross)
    TARE = "TR"   # 용기중량 (Tare)


@dataclass(frozen=True)
class RequestPacket(ABC):
    device_id: Literal[0, 1, 2, 3, 4, 5, 6, 7, 8, 9] = 0
    command_code: ClassVar[CommandCode]

    @abstractmethod
    def to_bytes(self) -> bytes:
        pass


@dataclass(frozen=True)
class DisplayRequestPacket(RequestPacket):
    command_code: ClassVar[CommandCode] = CommandCode.DISPLAY
    display_weight: Decimal = Decimal("0")
    display_plate: str = ""
    green_blink: bool = False
    red_blink: bool = False
    voice_code: VoiceCode = VoiceCode.NONE

    def to_bytes(self) -> bytes:
        device_id = str(self.device_id)

        command_code = self.command_code

        # sign = "-" if self.display_weight.is_signed() else "+"
        # abs_weight = str(abs(self.display_weight))
        # display_weight_bytes = f"{sign}{abs_weight:>7.7}".encode()  # 8 bytes

        # 아래 유효숫자 기반 표현형이 깔끔하지만 절대값이 999999보다 커지면 exponent 포함되며 8바이트 초과되니 주의
        display_weight_bytes = f"{self.display_weight:=+8.6}".encode()  # 8 bytes
        display_plate_bytes = f"{self.display_plate[-6:]:>6}".encode()  # 6 bytes

        reserved1_bytes = b"000000"  # 6 bytes

        relay_value = (
            RelayCode.GREEN * self.green_blink |
            RelayCode.RED * self.red_blink |
            RelayCode.RELAY3 * False |
            RelayCode.FAN * False |
            RelayCode.HEATER * False |
            RelayCode.RELAY6 * False |
            RelayCode.RELAY7 * False |
            RelayCode.RELAY8 * False
        )

        # *주의* hex 표현 아님: 0, 1, ..., 9, A, B, C, D, E, F
        # ascii 코드 순서 형태: 0, 1, ..., 9, :, ;, <, =, >, ?
        high_nibble = (relay_value & 0b11110000) >> 4
        low_nibble  = (relay_value & 0b00001111)
        relay_bytes = bytes([high_nibble + ord("0"), low_nibble + ord("0")])  # 2 bytes

        voice_index_bytes = f"{self.voice_code:02d}".encode()  # 2 bytes

        reserved2_bytes = b"0000"  # 4 bytes

        return bytes([
            STX,                    # 1 byte
            ord(device_id),         # 1 byte
            ord(command_code),      # 1 byte
            *display_weight_bytes,  # 8 bytes
            *display_plate_bytes,   # 6 bytes
            *reserved1_bytes,       # 6 bytes
            *relay_bytes,           # 2 bytes
            *voice_index_bytes,     # 2 bytes
            *reserved2_bytes,       # 4 bytes
            ETX,                    # 1 byte
        ])


@dataclass(frozen=True)
class PrinterRequestPacket(RequestPacket):
    command_code: ClassVar[CommandCode] = CommandCode.PRINTER
    copies: Literal[1, 2, 3, 4, 5, 6, 7, 8, 9] = 1
    document_bytes: bytes = b""

    def to_bytes(self) -> bytes:
        if len(self.document_bytes) > 9999:
            raise ValueError(f"Document exceeds 9999 bytes: {len(self.document_bytes)}")
        
        device_id = str(self.device_id)
        command_code = self.command_code
        length_bytes = f"{len(self.document_bytes):04d}".encode()
        copies = str(self.copies)
        document_bytes = self.document_bytes

        return bytes([
            STX,                # 1 byte
            ord(device_id),     # 1 byte
            ord(command_code),  # 1 byte
            *length_bytes,      # 4 bytes
            ord(copies),        # 1 byte
            *document_bytes,    # len(document_bytes) bytes
            ETX,                # 1 byte
        ])


# TODO(donghyeon): Define communication packets for fan/heater settings


@dataclass(frozen=True)
class ResponsePacket:
    device_id: int = 0
    command_code: CommandCode = CommandCode.DISPLAY
    rfid_card_uid: str = "00000000"
    user_command_code: InputCode = InputCode.NONE
    user_input: str = "000000"
    green_blink: bool = False
    red_blink: bool = False
    fan_on: bool = False
    heater_on: bool = False
    unknown_input: str = "00"
    voice_code: VoiceCode = VoiceCode.NONE
    inner_temperature: int = 0
    fan_trigger_temp: int = 30
    heater_trigger_temp: int = 5
    printer_status: PrinterStatus = PrinterStatus.NORMAL
    reserved: str = "0000"
    weight_status: WeightStatus = WeightStatus.STABLE
    weight_type: WeightType = WeightType.NET
    weight_value: Decimal = Decimal("0")
    weight_unit: str = "kg"

    @classmethod
    def from_bytes(cls, raw: bytes) -> Self:
        if len(raw) != 53:
            raise ValueError(f"Invalid response packet length: {len(raw)} bytes")
        if raw[0] != STX or raw[-1] != ETX:
            raise ValueError("Invalid STX/ETX")

        data = raw.decode(errors="replace")

        device_id = int(data[1])

        command_code = CommandCode(data[2])

        rfid_card_uid = data[3:11]

        user_command_code = InputCode(data[11])
        user_input = data[12:18]

        # *주의* hex 표현 아님: 0, 1, ..., 9, A, B, C, D, E, F
        # ascii 코드 순서 형태: 0, 1, ..., 9, :, ;, <, =, >, ?
        relay_str = data[18:20]
        high_nibble = ord(relay_str[0]) - ord("0")
        low_nibble  = ord(relay_str[1]) - ord("0")
        relay_value = (high_nibble << 4) | low_nibble
        relay_code = RelayCode(relay_value)

        green_blink = RelayCode.GREEN in relay_code
        red_blink = RelayCode.RED in relay_code
        fan_on = RelayCode.FAN in relay_code
        heater_on = RelayCode.HEATER in relay_code

        unknown_input = data[20:22]

        voice_code = VoiceCode(int(data[22:24]))

        inner_temperature = int(data[24:27])
        fan_trigger_temp = int(data[27:29])
        heater_trigger_temp = int(data[29:31])

        printer_status = PrinterStatus(int(data[31]))

        reserved = data[32:36]

        weight_status = WeightStatus(data[36:38])
        # assert data[38] == ","
        weight_type = WeightType(data[39:41])
        # assert data[41] == ","
        sign = data[42]
        abs_weight = data[43:50].strip()
        weight_value = Decimal(f"{sign}{abs_weight}")
        weight_unit = data[50:52]

        return cls(
            device_id=device_id,
            command_code=command_code,
            rfid_card_uid=rfid_card_uid,
            user_command_code=user_command_code,
            user_input=user_input,
            green_blink=green_blink,
            red_blink=red_blink,
            fan_on=fan_on,
            heater_on=heater_on,
            unknown_input=unknown_input,
            voice_code=voice_code,
            inner_temperature=inner_temperature,
            fan_trigger_temp=fan_trigger_temp,
            heater_trigger_temp=heater_trigger_temp,
            printer_status=printer_status,
            weight_status=weight_status,
            weight_type=weight_type,
            weight_value=weight_value,
            weight_unit=weight_unit,
        )


class SerialClient:
    def __init__(self, port: str, timeout: float = 1.0, write_timeout: float = 1.0):
        self.port = port
        self.timeout = timeout
        self.write_timeout = write_timeout
        self.serial: serial.Serial | None = None

    def connect(self):
        self.serial = serial.Serial(
            self.port,
            timeout=self.timeout,
            write_timeout=self.write_timeout,
        )
        self.serial.reset_input_buffer()

    def disconnect(self):
        if self.serial and self.serial.is_open:
            self.serial.close()

    def interrupt(self):
        if self.serial and self.serial.is_open:
            try:
                self.serial.cancel_read()
                self.serial.cancel_write()
            except Exception:
                pass

    def send_and_receive(self, request: RequestPacket) -> ResponsePacket:
        if self.serial is None or not self.serial.is_open:
            raise serial.SerialException("Serial port is not connected")
        
        self.serial.write(request.to_bytes())
        response = self.serial.read_until(expected=bytes([ETX]))
        return ResponsePacket.from_bytes(response)


class VoiceTimeoutError(TimeoutError):
    """The device did not acknowledge and finish a requested announcement."""


class WorkerState(Enum):
    INITIALIZE = auto()
    CONNECT = auto()
    IDLE = auto()
    VERIFY = auto()
    MEASURE = auto()
    SAVE = auto()
    COMPLETE = auto()
    PRINT = auto()
    RECOVER = auto()


@dataclass(frozen=True)
class LiveWeightSnapshot:
    weight: Decimal | None = None
    unit: str = "kg"
    weight_status: WeightStatus = WeightStatus.NONE
    weight_type: WeightType = WeightType.NONE
    sampled_at: datetime | None = None
    sampled_monotonic: float | None = None
    sample_seq: int = 0
    connection_status: str = "waiting"
    sample_valid: bool = False


class WeighingStationWorker:
    def __init__(
        self,
        serial_client: SerialClient,
        on_event: Callable[[BaseEvent], Future[None] | None] | None = None,
        rfid_validator: Callable[[RFIDTaggedEvent], bool] | None = None,
        receipt_builder: Callable[[WeighingCompletedEvent], bytes | None] | None = None,
        polling_interval: float = 0.1,
        retry_interval: float = 1.0,
        initial_sample_seq: int = 0,
        on_live_weight: Callable[[LiveWeightSnapshot], None] | None = None,
        vehicle_threshold: Decimal = Decimal("50"),
        vehicle_confirm_seconds: float = 3.0,
        voice_timeout: float = 15.0,
    ):
        self.client = serial_client
        self.on_event = on_event or print
        self.rfid_validator = rfid_validator or (lambda _: True)
        self.receipt_builder = receipt_builder
        self.polling_interval = polling_interval
        self.retry_interval = retry_interval
        self.on_live_weight = on_live_weight
        self.cycle = WeighingCycle(
            vehicle_threshold, vehicle_confirm_seconds, vehicle_confirm_seconds,
        )
        if (
            isinstance(voice_timeout, bool) or not isinstance(voice_timeout, (int, float))
            or not math.isfinite(voice_timeout) or voice_timeout <= 0
        ):
            raise ValueError("voice_timeout must be finite and positive")
        self.voice_timeout = voice_timeout
        self._processing = False
        self._indicator_fault = False
        self._save_fault = False
        self._ready_announced = 0
        self._next_rejected_announcement = 0.0
        self._next_save_announcement = 0.0
        self._visit_saved = False

        self.state = WorkerState.INITIALIZE
        self.last_weight = Decimal("0")
        self.last_plate = ""
        self.last_event: BaseEvent | None = None
        self._pending_save: WeighingCompletedEvent | None = None
        self._save_result: Future[None] | None = None
        self._saved_event_uuid: str | None = None
        self.save_retry_interval = 5.0
        self.save_warning_interval = 5.0
        self._save_retry_at = 0.0
        self._save_started_at = 0.0
        self._save_delay_reported = False
        self.stop_event = threading.Event()
        self._snapshot_lock = threading.Lock()
        self._live_weight = LiveWeightSnapshot()
        self._sample_seq = initial_sample_seq

        self.logger = get_logger().bind(port=serial_client.port)
        # A powered-off scale fails about once a second; summarize the repeats.
        self.failures = RepeatedFailureLog(self.logger, "hw.station")
        self.live_weight_failures = RepeatedFailureLog(self.logger, "hw.station.live_weight")

    def stop(self):
        self.logger.info("sys.worker.stop_requested")
        self.stop_event.set()
        self._set_connection_status("disconnected")
        self.client.interrupt()

    def get_live_weight(self) -> LiveWeightSnapshot:
        # Keep weight, device status and sample time from the same serial response.
        with self._snapshot_lock:
            return self._live_weight

    def _set_connection_status(self, status: str):
        with self._snapshot_lock:
            if self.stop_event.is_set():
                status = "disconnected"
            if self._live_weight.connection_status == status and not self._live_weight.sample_valid:
                return
            self._live_weight = dataclasses.replace(
                self._live_weight, connection_status=status, sample_valid=False,
            )
            self._notify_live_weight()

    def _notify_live_weight(self):
        # Called under _snapshot_lock so stop cannot publish before an older sample.
        # The callback must only enqueue/update state; it must not wait for I/O or
        # re-enter this worker. The separate sender performs all network work.
        if self.on_live_weight is None:
            return
        try:
            self.on_live_weight(self._live_weight)
        except Exception as exc:
            self.live_weight_failures.failure("hw.station.live_weight.callback_failed", exc)
        else:
            self.live_weight_failures.success()

    def run(self):
        self.logger.info("sys.worker.started")

        while not self.stop_event.is_set():
            structlog.contextvars.bind_contextvars(state=self.state.name)
            try:
                match self.state:
                    case WorkerState.INITIALIZE:
                        self.state = self.initialize()
                    case WorkerState.CONNECT:
                        self.state = self.connect()
                    case WorkerState.IDLE:
                        self.state = self.idle()
                    case WorkerState.VERIFY:
                        self.state = self.verify()
                    case WorkerState.MEASURE:
                        self.state = self.measure()
                    case WorkerState.SAVE:
                        self.state = self.save()
                    case WorkerState.COMPLETE:
                        self.state = self.complete()
                    case WorkerState.PRINT:
                        self.state = self.print()
                    case WorkerState.RECOVER:
                        self.state = self.recover()
            
            except VoiceTimeoutError as e:
                self.record_failure("hw.speaker.timeout", e)
                self.cycle.invalidate()
                self._indicator_fault = True
                self.state = self._resume_state()

            except ValueError as e:
                self.cycle.invalidate()
                self._indicator_fault = True
                self.record_failure("hw.protocol.parse_error", e)
                self.stop_event.wait(self.polling_interval)
                self.state = self._resume_state()

            except serial.SerialTimeoutException as e:
                self.cycle.invalidate()
                self._indicator_fault = True
                self.record_failure("hw.serial.timeout", e)
                self.state = WorkerState.RECOVER

            except serial.SerialException as e:
                self.cycle.invalidate()
                self._indicator_fault = True
                self.record_failure("hw.serial.connection_lost", e)
                self.state = WorkerState.RECOVER

            except Exception as e:
                self.cycle.invalidate()
                self._indicator_fault = True
                self.record_failure("sys.worker.unexpected_error", e)
                self.state = WorkerState.RECOVER

        if self._pending_save is not None:
            self.logger.warning(
                "biz.weighing.stopped_before_save_confirmation",
                event_id=self._pending_save.uuid,
                action="check_local_save_result_before_reweighing",
            )
        self.client.disconnect()
        self._set_connection_status("disconnected")
        self.logger.info("sys.worker.terminated")

    def record_failure(self, event: str, exc: Exception):
        # stop() cancels a pending read or write; that is not a device failure.
        if self.stop_event.is_set():
            return
        self._set_connection_status("disconnected" if isinstance(exc, serial.SerialException) else "error")
        if self.state in (
            WorkerState.VERIFY, WorkerState.MEASURE, WorkerState.SAVE,
            WorkerState.COMPLETE, WorkerState.PRINT,
        ):
            # An interrupted weighing must stay traceable to its card and receipt.
            context = {"rfid_card_uid": self.last_plate}
            if isinstance(self.last_event, WeighingCompletedEvent):
                context["event_id"] = self.last_event.uuid
            self.failures.failure(event, exc, always_log=True, **context)
        else:
            self.failures.failure(event, exc)

    def exchange(self, request: RequestPacket) -> ResponsePacket:
        try:
            response = self.client.send_and_receive(request)
        except Exception as exc:
            self.cycle.invalidate()
            self._indicator_fault = True
            # Voice polling handles parse errors locally, so capture failures here too.
            if not self.stop_event.is_set():
                self._set_connection_status("disconnected" if isinstance(exc, serial.SerialException) else "error")
            raise
        if not self.stop_event.is_set():
            if self._valid_weighing_response(response):
                previous_state = self.cycle.state
                self.last_weight = response.weight_value
                self.cycle.observe(response.weight_value, now=time.monotonic())
                self._indicator_fault = False
                if previous_state == CycleState.LOCKED and self.cycle.state == CycleState.WAIT_ENTRY:
                    self._visit_saved = False
                if previous_state != self.cycle.state:
                    self.logger.info(
                        "biz.weighing.vehicle_state", previous_state=previous_state.name,
                        vehicle_state=self.cycle.state.name, weight=str(response.weight_value),
                    )
            else:
                self.cycle.invalidate()
                self._indicator_fault = True
        with self._snapshot_lock:
            if not self.stop_event.is_set():
                if self._valid_live_response(response):
                    self._sample_seq += 1
                    self._live_weight = LiveWeightSnapshot(
                        weight=response.weight_value,
                        unit=response.weight_unit,
                        weight_status=response.weight_status,
                        weight_type=response.weight_type,
                        sampled_at=datetime.now(UTC),
                        sampled_monotonic=time.monotonic(),
                        sample_seq=self._sample_seq,
                        connection_status="connected",
                        sample_valid=True,
                    )
                    self._notify_live_weight()
                elif self._live_weight.connection_status != "error":
                    # Leave the business parser contract intact; invalid numbers aren't live samples.
                    self._live_weight = dataclasses.replace(
                        self._live_weight, connection_status="error", sample_valid=False,
                    )
                    self._notify_live_weight()
        self.failures.success()
        return response

    @staticmethod
    def _valid_live_response(response: ResponsePacket) -> bool:
        # Keep the business parser unchanged while obeying the live API bounds.
        # Check the exponent before formatting so malformed tiny values cannot
        # allocate arbitrarily long fixed-point strings.
        weight = response.weight_value
        return (
            weight.is_finite()
            and weight.copy_abs() <= Decimal("1000000000")
            and weight.as_tuple().exponent >= -30
            and len(format(weight, "f")) <= 32
            and bool(response.weight_unit.strip())
            and len(response.weight_unit) <= 8
        )
    
    @classmethod
    def _valid_weighing_response(cls, response: ResponsePacket) -> bool:
        return (
            cls._valid_live_response(response)
            and response.weight_unit.strip().lower() == "kg"
            and response.weight_status in (WeightStatus.STABLE, WeightStatus.UNSTABLE)
            and response.weight_type in (WeightType.NET, WeightType.GROSS)
        )

    def _display_request(
        self, *, voice_code: VoiceCode = VoiceCode.NONE, error: bool = False,
    ) -> DisplayRequestPacket:
        # Green alone always means a card may be accepted. Both lamps mean busy.
        fault = error or self._indicator_fault or self._save_fault
        busy = self._processing or self._pending_save is not None
        return DisplayRequestPacket(
            display_weight=self.last_weight, display_plate=self.last_plate,
            green_blink=not fault and (busy or self.cycle.state == CycleState.READY),
            red_blink=fault or busy,
            voice_code=voice_code,
        )

    def _accept_card(self, response: ResponsePacket) -> bool:
        if (
            response.rfid_card_uid == "00000000" or self._processing
            or self._pending_save is not None or self.stop_event.is_set()
            or not self.cycle.reserve(now=time.monotonic())
        ):
            return False
        self._processing = True
        self.last_plate = response.rfid_card_uid
        self.last_event = RFIDTaggedEvent(rfid_card_uid=response.rfid_card_uid)
        self._saved_event_uuid = None
        self.logger.info("hw.rfid.detected", rfid_card_uid=self.last_plate, next_state="VERIFY")
        self.on_event(self.last_event)
        return True

    def _resume_state(self) -> WorkerState:
        if self._pending_save is not None:
            self._processing = True
            return WorkerState.SAVE
        self._processing = False
        # Never resume a card interrupted by an I/O fault, or recreate a saved record.
        if self.cycle.state == CycleState.RESERVED:
            self.cycle.invalidate()
        if not self._has_saved_measurement():
            self.last_plate = ""
            self.last_event = None
        return WorkerState.IDLE

    def poll_until_voice_ends(
        self, base_request: DisplayRequestPacket, *, accept_card: bool = False,
    ) -> ResponsePacket | None:
        response = None
        voice = base_request.voice_code
        if voice != VoiceCode.NONE:
            self.logger.info("hw.speaker.on", voice_code=voice.name)
        deadline = time.monotonic() + self.voice_timeout
        sent = False
        acknowledged = voice == VoiceCode.NONE
        while not self.stop_event.is_set():
            if time.monotonic() >= deadline:
                raise VoiceTimeoutError(f"Voice {voice.name} did not finish within {self.voice_timeout}s")
            # Send the command only once, even when its acknowledgement is lost.
            # Recompute lamps so a card accepted during TAG_CARD immediately means busy.
            request = self._display_request(
                voice_code=VoiceCode.NONE if sent else voice,
                error=base_request.red_blink and not base_request.green_blink,
            )
            if not accept_card and request.green_blink and not request.red_blink:
                # A new vehicle can arrive during a rejection announcement.
                # Only advertise readiness when this loop actually accepts cards.
                request = dataclasses.replace(request, green_blink=False)
            sent = True
            try:
                response = self.exchange(request)
            except ValueError as exc:
                if not self.stop_event.is_set():
                    self.failures.failure(
                        "hw.protocol.parse_error", exc, level=logging.WARNING, action="ignore_and_continue",
                    )
            else:
                if accept_card:
                    self._accept_card(response)
                acknowledged |= response.voice_code == voice
                if acknowledged and response.voice_code == VoiceCode.NONE:
                    return response
            self.stop_event.wait(self.polling_interval)
        return response

    def initialize(self) -> WorkerState:
        self.logger.info("sys.worker.startup", next_state="CONNECT")
        return WorkerState.CONNECT

    def connect(self) -> WorkerState:
        self.logger.debug("hw.serial.connecting")
        self.client.connect()
        self._set_connection_status("connected")
        # Reconnects repeat with every retry while failing; the failure summary covers them.
        log = self.logger.debug if self.failures.failing else self.logger.info
        log("hw.serial.connected", next_state="IDLE")
        return self._resume_state()

    def idle(self) -> WorkerState:
        response = self.exchange(self._display_request())
        if self._accept_card(response):
            return WorkerState.VERIFY

        if self.cycle.state == CycleState.READY and self._ready_announced != self.cycle.ready_sequence:
            self._ready_announced = self.cycle.ready_sequence
            self.poll_until_voice_ends(
                self._display_request(voice_code=VoiceCode.TAG_CARD), accept_card=True,
            )
            if self._processing and self.cycle.state == CycleState.RESERVED:
                return WorkerState.VERIFY
            if self._processing:
                self._processing = False
                self.last_plate = ""
                self.last_event = None
        elif response.rfid_card_uid != "00000000" and time.monotonic() >= self._next_rejected_announcement:
            self._next_rejected_announcement = time.monotonic() + 10.0
            voice = VoiceCode.WEIGHT_COMPLETE if self._visit_saved else VoiceCode.STAND_BY
            self.poll_until_voice_ends(self._display_request(voice_code=voice))

        self.stop_event.wait(self.polling_interval)
        return WorkerState.IDLE

    def verify(self) -> WorkerState:
        self.logger.info("biz.rfid_card.verifying", rfid_card_uid=self.last_plate)
        request = self._display_request(voice_code=VoiceCode.PLEASE_WAIT)
        response = self.poll_until_voice_ends(request)
        if self.stop_event.is_set():
            return WorkerState.IDLE
        
        if not isinstance(self.last_event, RFIDTaggedEvent) or self.cycle.state != CycleState.RESERVED:
            self.logger.warning("biz.weighing.card_cancelled", reason="vehicle_no_longer_ready")
            return self._resume_state()

        is_valid = self.rfid_validator(self.last_event)
        if not is_valid:
            self.logger.info("biz.rfid_card.unregistered", next_state="IDLE")
            request = self._display_request(voice_code=VoiceCode.UNREGISTERED_CARD, error=True)
            self.poll_until_voice_ends(request)
            self.cycle.release()
            self._processing = False
            self.last_plate = ""
            self.last_event = None
            return WorkerState.IDLE

        self.logger.info("biz.rfid_card.verified", next_state="MEASURE")
        return WorkerState.MEASURE
    
    def measure(self) -> WorkerState:
        if self.stop_event.is_set():
            return WorkerState.IDLE
        self.logger.info("hw.weighing.started")
        if self._pending_save is None:
            if not self.cycle.consume(now=time.monotonic()):
                self.logger.warning("biz.weighing.card_cancelled", reason="vehicle_no_longer_ready")
                return self._resume_state()
            self._processing = True
            self._visit_saved = False
            self._pending_save = WeighingCompletedEvent(
                rfid_card_uid=self.last_plate, weight=int(self.last_weight),
            )
            self.last_event = self._pending_save
            self._saved_event_uuid = None
            self._submit_save()
        return WorkerState.SAVE

    def _submit_save(self):
        self._save_started_at = time.monotonic()
        self._save_delay_reported = False
        try:
            result = self.on_event(self._pending_save)
            if not isinstance(result, Future):
                raise RuntimeError("Local storage did not return a save acknowledgement")
            self._save_result = result
        except Exception as exc:
            self._save_failed(exc)

    def _save_failed(self, exc: Exception):
        self._save_fault = True
        self._save_result = None
        self._save_retry_at = time.monotonic() + self.save_retry_interval
        self.logger.critical(
            "biz.weighing.local_save_failed", event_id=self._pending_save.uuid,
            rfid_card_uid=self._pending_save.rfid_card_uid, error=repr(exc),
            retry_in=self.save_retry_interval,
            action="check_local_storage_do_not_reweigh",
        )

    def save(self) -> WorkerState:
        if self.stop_event.is_set():
            return WorkerState.SAVE
        if self._save_result is None and time.monotonic() >= self._save_retry_at:
            self._submit_save()
        if self._save_result is not None:
            if self._save_result.done():
                try:
                    self._save_result.result()
                except Exception as exc:
                    self._save_failed(exc)
                else:
                    self._saved_event_uuid = self._pending_save.uuid
                    self._save_fault = False
                    self._visit_saved = self.cycle.state == CycleState.LOCKED
                    self._pending_save = None
                    self._save_result = None
                    return WorkerState.COMPLETE
            elif not self._save_delay_reported and (
                time.monotonic() - self._save_started_at >= self.save_warning_interval
            ):
                self._save_delay_reported = True
                self.logger.warning(
                    "biz.weighing.local_save_pending", event_id=self._pending_save.uuid,
                    action="check_local_storage_do_not_reweigh",
                )

        # Keep receiving live weight, but ignore new cards while this original is pending.
        # A delay is not a failed write: never cancel or resubmit its in-flight Future.
        if self._save_fault and time.monotonic() >= self._next_save_announcement:
            self._next_save_announcement = time.monotonic() + 30.0
            self.poll_until_voice_ends(self._display_request(voice_code=VoiceCode.CHECK_ADMIN, error=True))
        else:
            self.exchange(self._display_request())
        self.stop_event.wait(self.polling_interval)
        return WorkerState.SAVE

    def _has_saved_measurement(self) -> bool:
        return (
            isinstance(self.last_event, WeighingCompletedEvent)
            and self._saved_event_uuid == self.last_event.uuid
        )

    def complete(self) -> WorkerState:
        if self.stop_event.is_set() or not self._has_saved_measurement():
            return WorkerState.IDLE
        self.logger.info("hw.weighing.completed", next_state="PRINT")

        request = self._display_request(voice_code=VoiceCode.WEIGHT_COMPLETE)
        response = self.poll_until_voice_ends(request)
        return WorkerState.PRINT
    
    def print(self) -> WorkerState:
        if self.stop_event.is_set() or not self._has_saved_measurement():
            return WorkerState.IDLE
        receipt_bytes = None
        if self.receipt_builder is not None and isinstance(self.last_event, WeighingCompletedEvent):
            receipt_bytes = self.receipt_builder(self.last_event)
        
        if receipt_bytes:
            self.logger.info("hw.printer.command.sending", payload_length = len(receipt_bytes))
            request = PrinterRequestPacket(document_bytes=receipt_bytes)
            self.exchange(request)

        request = self._display_request(voice_code=VoiceCode.THANK_YOU)
        response = self.poll_until_voice_ends(request)

        self.last_plate = ""
        self.last_event = None
        self._saved_event_uuid = None
        self._processing = False
        # A subsequent vehicle may already be ready after departure was observed
        # during output. If we show green, accept its card in this response too.
        response = self.exchange(self._display_request())
        return WorkerState.VERIFY if self._accept_card(response) else WorkerState.IDLE

    def recover(self) -> WorkerState:
        self.logger.debug("sys.worker.recovery_scheduled", retry_in=self.retry_interval, next_state="CONNECT")
        self.client.disconnect()

        self.stop_event.wait(self.retry_interval)
        return WorkerState.CONNECT


def main():
    logger = get_logger()

    worker = WeighingStationWorker(
        serial_client=SerialClient(port="COM3"),
    )
    worker_thread = threading.Thread(target=worker.run)
    worker_thread.start()

    try:
        worker_thread.join()
    except KeyboardInterrupt:
        logger.warning("sys.main.keyboard_interrupt_detected")
        worker.stop()
        worker_thread.join()
        logger.info("sys.main.shutdown_complete")


if __name__ == "__main__":
    main()
