import socket
from typing import Any
import uuid

import serial.tools.list_ports


def get_mac_address() -> str:
    return uuid.getnode().to_bytes(6).hex(":")


def get_ip_address() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def get_hostname() -> str:
    return socket.gethostname()


def scan_peripherals() -> list[dict[str, Any]]:
    ports = serial.tools.list_ports.comports()
    return [
        {
            "device": port.device,
            "name": port.device,
            "description": port.description,
            "hwid": port.hwid,
            "vid": port.vid,
            "pid": port.pid,
            "serial_number": port.serial_number,
            "location": port.location,
            "manufacturer": port.manufacturer,
            "product": port.product,
            "interface": port.interface,
        }
        for port in ports
    ]
