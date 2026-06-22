#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Simple watchdog for AetherEdge live runner.

This process starts scripts/run_live.py as a child process. If the live runner
exits unexpectedly, the watchdog restarts it after a short delay.

Stop behavior:
- stop the watchdog process with SIGTERM / Ctrl+C
- watchdog will terminate the live child before exiting

This is intentionally simple. Process control lives in the shell script;
Python only supervises the live child.
"""
from __future__ import annotations

import asyncio
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_LIVE_SCRIPT = PROJECT_ROOT / "scripts" / "run_live.py"
DEFAULT_LIVE_LOG = PROJECT_ROOT / "logs" / "aether_live.out"
DEFAULT_CHILD_PID_FILE = PROJECT_ROOT / "data" / "run" / "aether_live.pid"

_running = True
_child: Optional[subprocess.Popen] = None

from src.utils.log import get_logger

logger = get_logger(__name__)


def log(message: str) -> None:
    logger.info("WATCHDOG | %s", message)


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _resolve_path(raw: str | None, default: Path) -> Path:
    path = Path(raw).expanduser() if raw else default
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _parse_fatal_exit_codes(raw: str | None) -> frozenset[int]:
    if not raw:
        return frozenset()
    codes: set[int] = set()
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            codes.add(int(part))
        except ValueError:
            log(f"Ignoring non-integer fatal exit code value: {part!r}")
    return frozenset(codes)


def _send_watchdog_alert(subject: str, content: str) -> None:
    if not _truthy(os.getenv("AETHER_ENABLE_EMAIL_ALERT")):
        return
    try:
        from src.utils.email_sender import send_email

        asyncio.run(send_email(subject=subject, content=content, content_type="plain"))
    except Exception as exc:  # noqa: BLE001 - watchdog must never crash because alert failed
        log(f"Failed to send watchdog email alert: {exc}")


def _send_quick_fail_alert(
    *,
    quick_failure_count: int,
    quick_fail_seconds: float,
    last_returncode: int,
    last_uptime_seconds: float,
    live_script: Path,
    log_file: Path,
) -> None:
    subject = "AetherEdge watchdog quick-fail circuit opened"
    content = (
        f"project_root={PROJECT_ROOT}\n"
        f"live_script={live_script}\n"
        f"quick_failure_count={quick_failure_count}\n"
        f"quick_fail_seconds={quick_fail_seconds}\n"
        f"last_returncode={last_returncode}\n"
        f"last_uptime_seconds={last_uptime_seconds:.2f}\n"
        f"log_file={log_file}\n"
    )
    _send_watchdog_alert(subject, content)


def _send_fatal_exit_alert(
    *,
    returncode: int,
    live_script: Path,
    log_file: Path,
) -> None:
    subject = "AetherEdge watchdog received fatal exit code"
    content = (
        f"project_root={PROJECT_ROOT}\n"
        f"live_script={live_script}\n"
        f"returncode={returncode}\n"
        f"log_file={log_file}\n"
    )
    _send_watchdog_alert(subject, content)


def terminate_child(reason: str) -> None:
    global _child
    child = _child
    if child is None or child.poll() is not None:
        return

    log(f"Stopping live child pid={child.pid} reason={reason}")
    try:
        child.terminate()
        child.wait(timeout=float(os.getenv("AETHER_WATCHDOG_CHILD_STOP_TIMEOUT_SECONDS", "20")))
        log(f"Live child stopped pid={child.pid} returncode={child.returncode}")
    except subprocess.TimeoutExpired:
        log(f"Live child did not stop in time, killing pid={child.pid}")
        child.kill()
        child.wait(timeout=10)
    finally:
        _child = None
        try:
            _resolve_path(os.getenv("LIVE_PID_FILE"), DEFAULT_CHILD_PID_FILE).unlink(missing_ok=True)
        except Exception:
            pass


def handle_stop_signal(signum: int, _frame) -> None:  # type: ignore[no-untyped-def]
    global _running
    _running = False
    log(f"Received signal={signum}. Watchdog is shutting down.")
    terminate_child(f"watchdog_signal_{signum}")


def build_command() -> list[str]:
    python_bin = os.getenv("LIVE_PYTHON_BIN", sys.executable)
    live_script = _resolve_path(os.getenv("LIVE_SCRIPT"), DEFAULT_LIVE_SCRIPT)
    extra_args = shlex.split(os.getenv("LIVE_ARGS", ""))
    return [python_bin, "-u", str(live_script), *extra_args]


def start_child() -> subprocess.Popen:
    command = build_command()
    live_log_path = _resolve_path(os.getenv("LIVE_LOG_FILE"), DEFAULT_LIVE_LOG)
    live_pid_file = _resolve_path(os.getenv("LIVE_PID_FILE"), DEFAULT_CHILD_PID_FILE)
    live_log_path.parent.mkdir(parents=True, exist_ok=True)
    live_pid_file.parent.mkdir(parents=True, exist_ok=True)

    log(f"Starting live child: {' '.join(command)}")
    log(f"Live child log file: {live_log_path}")
    log_file = open(live_log_path, "a", buffering=1, encoding="utf-8")
    child = subprocess.Popen(
        command,
        cwd=str(PROJECT_ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        env=os.environ.copy(),
        text=True,
    )
    live_pid_file.write_text(str(child.pid), encoding="utf-8")
    log(f"Live child started pid={child.pid}")
    return child


def main() -> int:
    global _child
    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    restart_seconds = float(os.getenv("AETHER_WATCHDOG_RESTART_DELAY_SECONDS", "5"))
    max_restarts = int(os.getenv("AETHER_WATCHDOG_MAX_RESTARTS", "0"))  # 0 means unlimited
    quick_fail_seconds = float(os.getenv("WATCHDOG_QUICK_FAIL_SECONDS", "60"))
    max_quick_failures = int(os.getenv("WATCHDOG_MAX_QUICK_FAILURES", "3"))
    fatal_exit_codes = _parse_fatal_exit_codes(os.getenv("WATCHDOG_FATAL_EXIT_CODES", "78"))

    restart_count = 0
    quick_failure_count = 0
    _quick_fail_alert_sent = False
    live_script = _resolve_path(os.getenv("LIVE_SCRIPT"), DEFAULT_LIVE_SCRIPT)
    live_log_path = _resolve_path(os.getenv("LIVE_LOG_FILE"), DEFAULT_LIVE_LOG)

    log("Watchdog started")
    log(f"Project root: {PROJECT_ROOT}")
    log(f"Restart delay: {restart_seconds}s, max_restarts={max_restarts or 'unlimited'}")
    log(f"Quick-fail: {quick_fail_seconds}s, max_quick_failures={max_quick_failures}")
    log(f"Fatal exit codes: {sorted(fatal_exit_codes) if fatal_exit_codes else 'none'}")

    while _running:
        start_monotonic = time.monotonic()
        _child = start_child()
        returncode = _child.wait()
        uptime = time.monotonic() - start_monotonic
        try:
            _resolve_path(os.getenv("LIVE_PID_FILE"), DEFAULT_CHILD_PID_FILE).unlink(missing_ok=True)
        except Exception:
            pass

        if not _running:
            break

        # ── Child exited successfully ──
        if returncode == 0:
            quick_failure_count = 0
            log("Live child exited normally returncode=0. Watchdog stops.")
            return 0

        # ── Fatal exit code: stop immediately, no restart ──
        if returncode in fatal_exit_codes:
            log(f"Fatal exit code received returncode={returncode}. Watchdog stops.")
            if not _quick_fail_alert_sent:
                _send_fatal_exit_alert(
                    returncode=returncode,
                    live_script=live_script,
                    log_file=live_log_path,
                )
            return returncode

        # ── Quick-fail detection ──
        if uptime < quick_fail_seconds:
            quick_failure_count += 1
            log(
                f"Quick failure detected | returncode={returncode} uptime={uptime:.2f}s "
                f"quick_failure_count={quick_failure_count}/{max_quick_failures}"
            )
        else:
            # Ran long enough — reset quick-failure counter.
            if quick_failure_count > 0:
                log(
                    f"Quick-failure counter reset after long-running child | "
                    f"uptime={uptime:.2f}s previous_quick_failures={quick_failure_count} "
                    f"returncode={returncode}"
                )
            quick_failure_count = 0

        # ── Quick-fail circuit breaker ──
        if quick_failure_count >= max_quick_failures:
            log("Quick-fail circuit breaker opened. Watchdog stops.")
            if not _quick_fail_alert_sent:
                _quick_fail_alert_sent = True
                _send_quick_fail_alert(
                    quick_failure_count=quick_failure_count,
                    quick_fail_seconds=quick_fail_seconds,
                    last_returncode=returncode,
                    last_uptime_seconds=uptime,
                    live_script=live_script,
                    log_file=live_log_path,
                )
            return returncode or 1

        # ── Normal restart path ──
        restart_count += 1
        message = (
            f"Live child exited unexpectedly returncode={returncode}. "
            f"restart_count={restart_count} quick_failure_count={quick_failure_count}"
        )
        log(message)
        _send_watchdog_alert("AetherEdge live runner exited", message)
        if max_restarts > 0 and restart_count >= max_restarts:
            log("Max restart count reached. Watchdog exits.")
            return returncode if returncode != 0 else 1

        time.sleep(restart_seconds)

    log("Watchdog stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
