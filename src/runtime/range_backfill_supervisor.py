from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class RangeBackfillSupervisor:
    project_root: str | Path = "."
    script_path: str | Path = "tools/range_backfill_worker.py"
    pid_file: str | Path = "data/run/range_backfill_worker.pid"
    status_json: str | Path = "data/reports/range_backfill/status.json"
    log_file: str | Path = "logs/range_backfill_worker.out"
    python_bin: str | None = None

    def __post_init__(self) -> None:
        self.project_root = Path(self.project_root).resolve()
        self.script_path = _resolve(self.project_root, self.script_path)
        self.pid_file = _resolve(self.project_root, self.pid_file)
        self.status_json = _resolve(self.project_root, self.status_json)
        self.log_file = _resolve(self.project_root, self.log_file)
        self.python_bin = self.python_bin or sys.executable

    def start(self, *, args: list[str] | None = None) -> int | None:
        existing = self.running_pid()
        if existing is not None:
            return existing
        self.pid_file.parent.mkdir(parents=True, exist_ok=True)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        command = [
            str(self.python_bin),
            "-u",
            str(self.script_path),
            "--mode",
            "daemon",
            "--pid-file",
            str(self.pid_file),
            "--json-status",
            str(self.status_json),
            *(args or []),
        ]
        log_handle = self.log_file.open("a", buffering=1, encoding="utf-8")
        proc = subprocess.Popen(
            command,
            cwd=str(self.project_root),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=(os.name != "nt"),
            env=os.environ.copy(),
            text=True,
        )
        self.pid_file.write_text(str(proc.pid), encoding="utf-8")
        return proc.pid

    def running_pid(self) -> int | None:
        if not self.pid_file.exists():
            return None
        try:
            pid = int(self.pid_file.read_text(encoding="utf-8").strip())
        except ValueError:
            self.pid_file.unlink(missing_ok=True)
            return None
        if _pid_alive(pid):
            return pid
        self.pid_file.unlink(missing_ok=True)
        return None

    def stop_if_configured(self) -> bool:
        if str(os.getenv("AETHER_STOP_BACKFILL_WITH_LIVE", "false")).strip().lower() not in {"1", "true", "yes", "on"}:
            return False
        pid = self.running_pid()
        if pid is None:
            return False
        try:
            os.kill(pid, 15)
        except OSError:
            return False
        return True

    def read_status(self) -> dict[str, Any] | None:
        if not self.status_json.exists():
            return None
        try:
            return json.loads(self.status_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None


def _resolve(root: Path, path: str | Path) -> Path:
    item = Path(path)
    return item if item.is_absolute() else root / item


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _windows_pid_alive(pid)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _windows_pid_alive(pid: int) -> bool:
    import ctypes

    process_query_limited_information = 0x1000
    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
    if not handle:
        return False
    try:
        return True
    finally:
        kernel32.CloseHandle(handle)
