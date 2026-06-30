from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Mapping


def now_ms() -> int:
    return int(time.time() * 1000)


def worker_heartbeat_ms(status: Mapping[str, Any]) -> int | None:
    value = status.get("worker_heartbeat_ms")
    if value is None:
        value = status.get("heartbeat_ms")
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def process_id_exists(pid: object) -> bool | None:
    try:
        value = int(pid)
    except (TypeError, ValueError):
        return False
    if value <= 0:
        return False
    if value == os.getpid():
        return True
    if os.name == "posix":
        try:
            os.kill(value, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True
    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = [
                ctypes.c_uint32,
                ctypes.c_int,
                ctypes.c_uint32,
            ]
            kernel32.OpenProcess.restype = ctypes.c_void_p
            kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
            kernel32.CloseHandle.restype = ctypes.c_int
            handle = kernel32.OpenProcess(0x1000, False, value)
            if handle:
                kernel32.CloseHandle(handle)
                return True
            error = ctypes.get_last_error()
            if error == 5:
                return True
            if error == 87:
                return False
        except (AttributeError, OSError):
            return None
    return None


def worker_status_is_running(
    status: Mapping[str, Any],
    *,
    stale_after_seconds: int = 180,
    now_ms_value: int | None = None,
) -> bool:
    if not status.get("running"):
        return False
    process_exists = process_id_exists(status.get("pid"))
    if process_exists is False:
        return False
    heartbeat = worker_heartbeat_ms(status)
    if heartbeat is None:
        return False
    current = now_ms() if now_ms_value is None else int(now_ms_value)
    return current - heartbeat <= max(0, int(stale_after_seconds)) * 1000


class RangeBackfillStatusStore:
    def __init__(self, path: str | Path = "data/state/range_backfill_status.json") -> None:
        self.path = Path(path)

    def read(self) -> dict[str, Any] | None:
        try:
            if not self.path.exists():
                return None
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return data if isinstance(data, dict) else None

    def write(self, values: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": 1, **dict(values), "updated_at_ms": now_ms()}
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.path)

    def patch(self, **updates: Any) -> None:
        current = self.read() or {}
        current.update(updates)
        self.write(current)
