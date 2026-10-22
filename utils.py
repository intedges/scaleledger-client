from pathlib import Path
import socket
import sys
from typing import Any, BinaryIO

import serial.tools.list_ports

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl


def get_ip_address(server_host: str) -> str:
    # The address of the interface that routes to the server; sites have no internet route.
    try:
        addresses = socket.getaddrinfo(server_host, 80, type=socket.SOCK_DGRAM)
    except OSError:
        return "127.0.0.1"
    for family, socktype, protocol, _, address in addresses:
        try:
            with socket.socket(family, socktype, protocol) as s:
                s.connect(address)
                return s.getsockname()[0]
        except OSError:
            continue
    return "127.0.0.1"


def acquire_instance_lock(path: Path) -> BinaryIO | None:
    """Lock *path* for the life of the process, or return None if another process holds it.

    The operating system releases the lock when the process exits, even after a crash.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = path.open("a+b")
    try:
        if sys.platform == "win32":
            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_file.close()
        return None
    return lock_file


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
