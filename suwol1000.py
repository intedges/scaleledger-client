from abc import ABC, abstractmethod
from dataclasses import dataclass
import dataclasses
from decimal import Decimal
from enum import auto, Enum, IntEnum, StrEnum, IntFlag
from typing import Callable, ClassVar, Literal

import threading

import serial
import structlog
from structlog.stdlib import get_logger

from events import BaseEvent, RFIDTaggedEvent, WeighingCompletedEvent


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
    document_bytes: bytes = bytes()

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
    def from_bytes(cls, raw: bytes) -> "ResponsePacket":
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


class WorkerState(Enum):
    INITIALIZE = auto()
    CONNECT = auto()
    IDLE = auto()
    VERIFY = auto()
    MEASURE = auto()
    PRINT = auto()
    RECOVER = auto()


class WeighingStationWorker:
    def __init__(
        self,
        serial_client: SerialClient,
        on_event: Callable[[BaseEvent], None] | None = None,
        rfid_validator: Callable[[RFIDTaggedEvent], bool] | None = None,
        receipt_builder: Callable[[WeighingCompletedEvent], bytes | None] | None = None,
        polling_interval: float = 0.1,
        retry_interval: float = 1.0,
    ):
        self.client = serial_client
        self.on_event = on_event or print
        self.rfid_validator = rfid_validator or (lambda _: True)
        self.receipt_builder = receipt_builder
        self.polling_interval = polling_interval
        self.retry_interval = retry_interval

        self.state = WorkerState.INITIALIZE
        self.last_weight = Decimal("0")
        self.last_plate = ""
        self.last_event: BaseEvent | None = None
        self.stop_event = threading.Event()

        self.logger = get_logger().bind(port=serial_client.port)

    def stop(self):
        self.logger.info("sys.worker.stop_requested")
        self.stop_event.set()
        self.client.interrupt()

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
                    case WorkerState.PRINT:
                        self.state = self.print()
                    case WorkerState.RECOVER:
                        self.state = self.recover()
            
            except ValueError:
                self.logger.exception("hw.protocol.parse_error")
                self.stop_event.wait(self.polling_interval)
                self.state = WorkerState.IDLE

            except serial.SerialTimeoutException:
                self.logger.exception("hw.serial.timeout")
                self.state = WorkerState.RECOVER

            except serial.SerialException:
                self.logger.exception("hw.serial.connection_lost")
                self.state = WorkerState.RECOVER

            except Exception:
                self.logger.exception("sys.worker.unexpected_error")
                self.state = WorkerState.RECOVER

        self.client.disconnect()
        self.logger.info("sys.worker.terminated")
    
    def poll_until_voice_ends(self, base_request: DisplayRequestPacket) -> ResponsePacket | None:
        response = None

        if base_request.voice_code != VoiceCode.NONE:
            self.logger.info("hw.speaker.on", voice_code=base_request.voice_code.name)

        voice_sent = False
        is_speaker_busy = True
        while (is_speaker_busy or not voice_sent) and not self.stop_event.is_set():
            request = dataclasses.replace(
                base_request,
                display_weight=self.last_weight,
                voice_code=VoiceCode.NONE if voice_sent else base_request.voice_code,
            )
            try:
                response = self.client.send_and_receive(request)
                self.last_weight = response.weight_value
                is_speaker_busy = response.voice_code != VoiceCode.NONE
                voice_sent |= response.voice_code == base_request.voice_code
            except ValueError:
                self.logger.warning("hw.protocol.parse_error", action="ignore_and_continue")
            self.stop_event.wait(self.polling_interval)
        return response

    def initialize(self) -> WorkerState:
        self.logger.info("sys.worker.startup", next_state="CONNECT")
        return WorkerState.CONNECT

    def connect(self) -> WorkerState:
        self.logger.debug("hw.serial.connecting")
        self.client.connect()
        self.logger.info("hw.serial.connected", next_state="IDLE")
        return WorkerState.IDLE

    def idle(self) -> WorkerState:
        request = DisplayRequestPacket(display_weight=self.last_weight)
        response = self.client.send_and_receive(request)
        self.last_weight = response.weight_value

        if response.rfid_card_uid != "00000000":
            self.last_plate = response.rfid_card_uid
            self.logger.info(
                "hw.rfid.detected",
                rfid_card_uid=response.rfid_card_uid,
                next_state="MEASURE",
            )
            self.last_event = RFIDTaggedEvent(rfid_card_uid=response.rfid_card_uid)
            self.on_event(self.last_event)
            return WorkerState.VERIFY

        self.stop_event.wait(self.polling_interval)
        return WorkerState.IDLE
    
    def verify(self) -> WorkerState:
        self.logger.info("biz.rfid_card.verifying", rfid_card_uid=self.last_plate)
        request = DisplayRequestPacket(
            display_weight=self.last_weight,
            display_plate=self.last_plate,
            voice_code=VoiceCode.PLEASE_WAIT,
        )
        response = self.poll_until_voice_ends(request)
        if self.stop_event.is_set():
            return WorkerState.IDLE
        
        if not isinstance(self.last_event, RFIDTaggedEvent):
            self.logger.error("sys.worker.invalid_event_type")
            return WorkerState.IDLE

        is_valid = self.rfid_validator(self.last_event)
        if not is_valid:
            self.logger.info("biz.rfid_card.unregistered", next_state="IDLE")
            request = DisplayRequestPacket(
                display_weight=self.last_weight,
                display_plate=self.last_plate,
                red_blink=True,
                voice_code=VoiceCode.UNREGISTERED_CARD,
            )
            response = self.poll_until_voice_ends(request)
            return WorkerState.IDLE

        self.logger.info("biz.rfid_card.verified", next_state="MEASURE")
        return WorkerState.MEASURE
    
    def measure(self) -> WorkerState:
        self.logger.info("hw.weighing.started")
        self.last_event = WeighingCompletedEvent(rfid_card_uid=self.last_plate, weight=int(self.last_weight))
        self.on_event(self.last_event)
        self.logger.info("hw.weighing.completed", next_state="PRINT")

        request = DisplayRequestPacket(
            display_weight=self.last_weight,
            display_plate=self.last_plate,
            green_blink=True,
            voice_code=VoiceCode.WEIGHT_COMPLETE,
        )
        response = self.poll_until_voice_ends(request)
        return WorkerState.PRINT
    
    def print(self) -> WorkerState:
        receipt_bytes = None
        if self.receipt_builder is not None and isinstance(self.last_event, WeighingCompletedEvent):
            receipt_bytes = self.receipt_builder(self.last_event)
        
        if receipt_bytes:
            self.logger.info("hw.printer.command.sending", payload_length = len(receipt_bytes))
            request = PrinterRequestPacket(document_bytes=receipt_bytes)
            response = self.client.send_and_receive(request)
            self.last_weight = response.weight_value


        request = DisplayRequestPacket(
            display_weight=self.last_weight,
            display_plate=self.last_plate,
            green_blink=True,
            voice_code=VoiceCode.THANK_YOU,
        )
        response = self.poll_until_voice_ends(request)

        self.last_plate = ""
        self.last_event = None
        return WorkerState.IDLE

    def recover(self) -> WorkerState:
        self.logger.info("sys.worker.recovery_scheduled", retry_in=self.retry_interval, next_state="CONNECT")
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
