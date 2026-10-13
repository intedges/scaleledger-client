"""Cooperative shutdown across the scheduled task and the operator's session."""

import ctypes
from ctypes import wintypes
from functools import cache
import hashlib
import os
from pathlib import Path


def _event_name(root: Path) -> str:
    identity = os.path.normcase(str(root.resolve())).encode("utf-8")
    return "Global\\ScaleLedgerClient-" + hashlib.sha256(identity).hexdigest()[:32]


@cache
def _kernel32():
    # Load only when used on Windows; importing this module remains portable.
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    signatures = {
        "CreateEventW": ([ctypes.c_void_p, wintypes.BOOL, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
        "OpenEventW": ([wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
        "SetEvent": ([wintypes.HANDLE], wintypes.BOOL),
        "ResetEvent": ([wintypes.HANDLE], wintypes.BOOL),
        "WaitForSingleObject": ([wintypes.HANDLE, wintypes.DWORD], wintypes.DWORD),
        "CloseHandle": ([wintypes.HANDLE], wintypes.BOOL),
    }
    for name, (arguments, result) in signatures.items():
        function = getattr(kernel, name)
        function.argtypes = arguments
        function.restype = result
    return kernel


class WindowsStopEvent:
    def __init__(self, root: Path):
        kernel = _kernel32()
        # Use the creator's default DACL; desktop commands use the same account.
        # Global is needed because boot tasks run outside the desktop session.
        self.handle = kernel.CreateEventW(None, True, False, _event_name(root))
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        if not kernel.ResetEvent(self.handle):
            error = ctypes.WinError(ctypes.get_last_error())
            self.close()
            raise error

    def is_set(self) -> bool:
        result = _kernel32().WaitForSingleObject(self.handle, 0)
        if result == 0:  # WAIT_OBJECT_0
            return True
        if result == 258:  # WAIT_TIMEOUT
            return False
        raise ctypes.WinError(ctypes.get_last_error())

    def close(self) -> None:
        if self.handle:
            _kernel32().CloseHandle(self.handle)
            self.handle = None


def request_stop(root: Path) -> bool:
    kernel = _kernel32()
    handle = kernel.OpenEventW(0x0002, False, _event_name(root))  # EVENT_MODIFY_STATE
    if not handle:
        error = ctypes.get_last_error()
        if error == 2:  # ERROR_FILE_NOT_FOUND: no running instance has created the event.
            return False
        raise ctypes.WinError(error)
    try:
        if not kernel.SetEvent(handle):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        kernel.CloseHandle(handle)
    return True
