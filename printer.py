from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Self
import uuid


ESC = 0x1B  # 27
FS  = 0x1C  # 28
GS  = 0x1D  # 29
LF  = 0x0A  # 10
FF  = 0x0C  # 12
CAN = 0x18  # 24


class EscPosCommand:
    INITIALIZE = bytes([ESC, ord("@")])
    LINE_FEED = bytes([LF])
    PRINT_AND_FEED = bytes([ESC, ord("d")])

    PAGE_MODE = bytes([ESC, ord("L")])
    PRINT_PAGE = bytes([ESC, FF])
    CANCEL_PAGE = bytes([CAN])
    
    KOREAN_ON = bytes([FS, ord("&")])
    KOREAN_OFF = bytes([FS, ord(".")])

    ALIGN_LEFT = bytes([ESC, ord("a"), 0])
    ALIGN_CENTER = bytes([ESC, ord("a"), 1])
    ALIGN_RIGHT = bytes([ESC, ord("a"), 2])

    BOLD_ON = bytes([ESC, ord("E"), 1])
    BOLD_OFF = bytes([ESC, ord("E"), 0])

    UNDERLINE_ON = bytes([ESC, ord("-"), 1])  #+ bytes([FS, ord("-"), 1])
    UNDERLINE_OFF = bytes([ESC, ord("-"), 0])  #+ bytes([FS, ord("-"), 0])
    
    TEXT_NORMAL = bytes([GS, ord("!"), 0x00])  #+ bytes([FS, ord("W"), 0])
    TEXT_DOUBLE = bytes([GS, ord("!"), 0x11])  #+ bytes([FS, ord("W"), 1])

    FULL_CUT = bytes([GS, ord("V"), 0])
    PARTIAL_CUT = bytes([GS, ord("V"), 1])

    BARCODE_2D = bytes([GS, ord("("), ord("k")])


class QrCodeCommand(StrEnum):
    MODEL = "A"
    SIZE = "C"
    ECL = "E"
    STORE = "P"
    PRINT = "Q"
    
    def encode(self, params: bytes):
        total_bytes = 2 + len(params)  # [cn, fn, *params] (total count of bytes following pL and pH)
        cn = ord("1")
        fn = ord(self.value)
        pL = total_bytes % 256
        pH = total_bytes // 256
        return bytes([*EscPosCommand.BARCODE_2D, pL, pH, cn, fn, *params])


class Alignment(StrEnum):
    LEFT = "left"
    CENTER = "center"
    RIGHT = "right"


class ECL(StrEnum):
    L = "0"
    M = "1"
    Q = "2"
    H = "3"


class EscPosBuilder:
    def __init__(self):
        self._buffer = bytearray()
        self._buffer.extend(EscPosCommand.CANCEL_PAGE)
        self._buffer.extend(EscPosCommand.INITIALIZE)
    
    def set_kanji_mode(self, on: bool = True) -> Self:
        cmd = EscPosCommand.KOREAN_ON if on else EscPosCommand.KOREAN_OFF
        self._buffer.extend(cmd)
        return self
    
    def set_align(self, align: Alignment = Alignment.LEFT) -> Self:
        match align:
            case Alignment.CENTER:
                self._buffer.extend(EscPosCommand.ALIGN_CENTER)
            case Alignment.RIGHT:
                self._buffer.extend(EscPosCommand.ALIGN_RIGHT)
            case _:
                self._buffer.extend(EscPosCommand.ALIGN_LEFT)
        return self
    
    def set_bold(self, on: bool = True) -> Self:
        cmd = EscPosCommand.BOLD_ON if on else EscPosCommand.BOLD_OFF
        self._buffer.extend(cmd)
        return self

    def set_underline(self, on: bool = True) -> Self:
        cmd = EscPosCommand.UNDERLINE_ON if on else EscPosCommand.UNDERLINE_OFF
        self._buffer.extend(cmd)
        return self
    
    def set_quadruple(self, on: bool = True) -> Self:
        cmd = EscPosCommand.TEXT_DOUBLE if on else EscPosCommand.TEXT_NORMAL
        self._buffer.extend(cmd)
        return self
    
    def add_text(self, text: str) -> Self:
        self._buffer.extend(text.encode("cp949", errors="replace"))
        return self
    
    def add_kv(self, key: str, value: str, max_col: int = 42) -> Self:
        k_bytes = len(key.encode("cp949", errors="replace"))
        v_bytes = len(value.encode("cp949", errors="replace"))
        spaces = max(1, max_col - k_bytes - v_bytes)
        
        line = f"{key}{' ' * spaces}{value}"
        self.add_text(line).feed_lines(1)
        return self

    def feed_lines(self, lines: int = 1, now: bool = False) -> Self:
        if now:
            self._buffer.extend(EscPosCommand.PRINT_AND_FEED + bytes([lines]))
        else:
            self._buffer.extend(EscPosCommand.LINE_FEED * lines)
        return self
    
    def add_separator(self, char: str = "-", columns: int = 42) -> Self:
        separator_line = char * columns
        self.add_text(separator_line).feed_lines(1)
        return self
    
    def add_qr_code(self, data: str, size: int = 3, ecl: ECL = ECL.M) -> Self:
        self._buffer.extend(QrCodeCommand.MODEL.encode(bytes([ord("2"), 0])))
        self._buffer.extend(QrCodeCommand.SIZE.encode(bytes([size])))
        self._buffer.extend(QrCodeCommand.ECL.encode(bytes([ord(ecl)])))
        self._buffer.extend(QrCodeCommand.STORE.encode(bytes([ord("0"), *data.encode()])))
        self._buffer.extend(QrCodeCommand.PRINT.encode(bytes([ord("0")])))
        return self

    def cut(self, partial: bool = False, pre_feed_lines: int = 4) -> Self:
        self.feed_lines(pre_feed_lines, now=True)
        cmd = EscPosCommand.PARTIAL_CUT if partial else EscPosCommand.FULL_CUT
        self._buffer.extend(cmd)
        return self
    
    def print(self) -> Self:
        cmd = EscPosCommand.PRINT_PAGE
        self._buffer.extend(cmd)
        return self

    def build(self) -> bytes:
        return bytes(self._buffer)


@dataclass(frozen=True)
class Receipt:
    record_uuid: uuid.UUID
    gateway_name: str
    station_name: str
    rfid_card_uid: str
    producer_name: str
    species_name: str
    weight: Decimal
    measured_at: datetime


class ReceiptTemplate:
    @staticmethod
    def render(receipt: Receipt) -> bytes:
        short_uuid = str(receipt.record_uuid).split("-")[0].upper()

        builder = (
            EscPosBuilder()

            # Header
            .set_align(Alignment.CENTER)
            .set_quadruple(True)
            .set_bold(True)
            .add_text("계 량 전 표")
            .set_quadruple(False)
            .set_bold(False)
            .feed_lines(2)
            
            # Meta Data
            .set_align(Alignment.LEFT)
            .add_separator("=", 42)
            .add_kv("발행일시", receipt.measured_at.strftime("%Y-%m-%d %H:%M:%S"))
            .add_kv("전표번호", short_uuid)
            .add_kv("계 량 기", receipt.gateway_name)
            .add_kv("계 근 대", receipt.station_name)
            .add_separator("-", 42)
            
            # Body
            .add_kv("생산자명", receipt.producer_name)
            .add_kv("품 목 명", receipt.species_name)
            .add_kv("카드번호", receipt.rfid_card_uid)
            
            # Weight
            .set_align(Alignment.RIGHT)
            .set_quadruple(True)
            .set_bold(True)
            .add_text(f"{receipt.weight:,} kg")
            .feed_lines(1)
            .set_quadruple(False)
            .set_bold(False)
            
            # Footer
            .set_align(Alignment.CENTER)
            .add_separator("=", 42)
            .add_text("SUWOL ScaleLedger System")

            .cut()
        )
        
        return builder.build()
