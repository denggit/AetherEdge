from __future__ import annotations

import ctypes
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
    """Cross-platform pid-file process controller used by app scripts.

    Linux deployments use POSIX signals. Windows development uses the Popen
    handle when available and falls back to ``taskkill`` for existing pid files.
    This module is generic process control only; it does not know about market
    data, strategies, orders, or exchange adapters.
    """

    def __init__(self, *, pid_file: str | Path, log_file: str | Path, cwd: str | Path | None = None) -> None:
        self.pid_file = Path(pid_file)
        self.log_file = Path(log_file)
        self.cwd = Path(cwd) if cwd is not None else None
        self._child: subprocess.Popen[str] | None = None

    def start(self, command: Sequence[str]) -> ProcessControlResult:
        existing = self.status()
        if existing.status == "running":
            return ProcessControlResult(ok=True, status="already_running", pid=existing.pid)
        self.pid_file.parent.mkdir(parents=True, exist_ok=True)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        log_handle = open(self.log_file, "a", buffering=1, encoding="utf-8")
        child = subprocess.Popen(
            tuple(command),
            cwd=str(self.cwd) if self.cwd is not None else None,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=(os.name != "nt"),
            text=True,
        )
        self._child = child
        self.pid_file.write_text(str(child.pid), encoding="utf-8")
        return ProcessControlResult(ok=True, status="started", pid=child.pid)

    def status(self) -> ProcessControlResult:
        if not self.pid_file.exists():
            return ProcessControlResult(ok=False, status="not_running")
        raw = self.pid_file.read_text(encoding="utf-8").strip()
        if not raw:
            self.pid_file.unlink(missing_ok=True)
            return ProcessControlResult(ok=False, status="stale", message="empty pid file")
        try:
            pid = int(raw)
        except ValueError:
            self.pid_file.unlink(missing_ok=True)
            return ProcessControlResult(ok=False, status="stale", message="invalid pid file")
        if self._child is not None and self._child.pid == pid:
            if self._child.poll() is None:
                return ProcessControlResult(ok=True, status="running", pid=pid)
            self.pid_file.unlink(missing_ok=True)
            return ProcessControlResult(ok=False, status="stale", pid=pid)
        if _is_process_running(pid):
            return ProcessControlResult(ok=True, status="running", pid=pid)
        self.pid_file.unlink(missing_ok=True)
        return ProcessControlResult(ok=False, status="stale", pid=pid)

    def stop(self, *, timeout_seconds: float = 20.0) -> ProcessControlResult:
        status = self.status()
        if status.status in {"not_running", "stale"}:
            self.pid_file.unlink(missing_ok=True)
            return ProcessControlResult(ok=True, status=status.status, pid=status.pid)
        pid = status.pid
        if pid is None:
            self.pid_file.unlink(missing_ok=True)
            return ProcessControlResult(ok=True, status="not_running")

        child = self._child if self._child is not None and self._child.pid == pid else None
        if child is not None:
            result = self._stop_child_handle(child, timeout_seconds=timeout_seconds)
        else:
            result = _stop_process_by_pid(pid, timeout_seconds=timeout_seconds)
        self.pid_file.unlink(missing_ok=True)
        self._child = None
        return ProcessControlResult(ok=True, status=result, pid=pid)

    def _stop_child_handle(self, child: subprocess.Popen[str], *, timeout_seconds: float) -> str:
        if child.poll() is not None:
            return "stopped"
        child.terminate()
        try:
            child.wait(timeout=timeout_seconds)
            return "stopped"
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=max(1.0, min(timeout_seconds, 10.0)))
            return "killed"


def _is_process_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _is_windows_process_running(pid)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _stop_process_by_pid(pid: int, *, timeout_seconds: float) -> str:
    if os.name == "nt":
        return _stop_windows_process(pid)
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return "stopped"
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not _is_process_running(pid):
            return "stopped"
        time.sleep(0.05)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return "stopped"
    return "killed"


def _is_windows_process_running(pid: int) -> bool:
    # PROCESS_QUERY_LIMITED_INFORMATION = 0x1000. This is less privileged than
    # os.kill(pid, 0) on Windows and avoids WinError 5 for normal user-owned
    # processes. Invalid/non-existing pids simply return a null handle.
    process_query_limited_information = 0x1000
    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
    if not handle:
        return False
    try:
        return True
    finally:
        kernel32.CloseHandle(handle)


def _stop_windows_process(pid: int) -> str:
    completed = subprocess.run(
        ("taskkill", "/PID", str(pid), "/T", "/F"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return "killed" if completed.returncode == 0 else "stopped"
