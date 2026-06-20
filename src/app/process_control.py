from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


@dataclass(frozen=True)
class ProcessControlResult:
    ok: bool
    status: str
    pid: int | None = None
    message: str = ""


class PidFileProcessController:
    """Tiny PID-file process controller for server scripts.

    It is intentionally small and local-only. It does not replace systemd or a
    process supervisor; it just gives scripts convenient start/stop/restart
    controls without adding external dependencies.
    """

    def __init__(self, *, pid_file: str | Path, log_file: str | Path, cwd: str | Path | None = None) -> None:
        self.pid_file = Path(pid_file)
        self.log_file = Path(log_file)
        self.cwd = Path(cwd) if cwd is not None else None

    def status(self) -> ProcessControlResult:
        pid = self._read_pid()
        if pid is None:
            return ProcessControlResult(ok=False, status="stopped", message="pid file does not exist")
        if _is_process_running(pid):
            return ProcessControlResult(ok=True, status="running", pid=pid, message=f"process {pid} is running")
        self._remove_pid_file()
        return ProcessControlResult(ok=False, status="stale", pid=pid, message=f"removed stale pid file for {pid}")

    def start(self, command: Sequence[str]) -> ProcessControlResult:
        current = self.status()
        if current.ok and current.pid is not None:
            return ProcessControlResult(ok=False, status="already_running", pid=current.pid, message=current.message)

        self.pid_file.parent.mkdir(parents=True, exist_ok=True)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        log_handle = self.log_file.open("ab")
        process = subprocess.Popen(
            tuple(command),
            cwd=str(self.cwd) if self.cwd is not None else None,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        self.pid_file.write_text(str(process.pid), encoding="utf-8")
        return ProcessControlResult(ok=True, status="started", pid=process.pid, message=f"started process {process.pid}")

    def stop(self, *, timeout_seconds: float = 10.0) -> ProcessControlResult:
        pid = self._read_pid()
        if pid is None:
            return ProcessControlResult(ok=True, status="stopped", message="pid file does not exist")
        if not _is_process_running(pid):
            self._remove_pid_file()
            return ProcessControlResult(ok=True, status="stopped", pid=pid, message="process was not running")

        _terminate_process(pid)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if not _is_process_running(pid):
                self._remove_pid_file()
                return ProcessControlResult(ok=True, status="stopped", pid=pid, message=f"stopped process {pid}")
            time.sleep(0.1)

        _kill_process(pid)
        self._remove_pid_file()
        return ProcessControlResult(ok=True, status="killed", pid=pid, message=f"killed process {pid}")

    def _read_pid(self) -> int | None:
        if not self.pid_file.exists():
            return None
        try:
            return int(self.pid_file.read_text(encoding="utf-8").strip())
        except ValueError:
            self._remove_pid_file()
            return None

    def _remove_pid_file(self) -> None:
        try:
            self.pid_file.unlink()
        except FileNotFoundError:
            pass


def _is_process_running(pid: int) -> bool:
    if pid <= 0:
        return False
    proc_stat = Path(f"/proc/{pid}/stat")
    if proc_stat.exists():
        try:
            state = proc_stat.read_text(encoding="utf-8", errors="ignore").split()[2]
            if state == "Z":
                return False
        except Exception:
            pass
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _terminate_process(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except PermissionError:
        raise
    except OSError:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def _kill_process(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError:
        raise
    except OSError:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
