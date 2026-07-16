from __future__ import annotations

import asyncio
import os
import shlex
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol, Sequence

from src.app.alerts import AppAlert, AlertSink
from src.utils.log import get_logger

logger = get_logger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LIVE_SCRIPT = PROJECT_ROOT / "scripts" / "run_live.py"
DEFAULT_LIVE_LOG = PROJECT_ROOT / "logs" / "aether_live.out"
DEFAULT_CHILD_PID_FILE = PROJECT_ROOT / "data" / "run" / "aether_live.pid"


@dataclass(frozen=True)
class WatchdogConfig:
    command: tuple[str, ...]
    restart_delay_seconds: float = 5.0
    max_restarts: int = 0
    max_failures: int = 0
    quick_fail_seconds: float = 0.0
    max_quick_failures: int = 0
    fatal_exit_codes: frozenset[int] = frozenset()
    cwd: Path | None = None
    stdout_path: Path | None = None
    child_pid_file: Path | None = None
    child_stop_timeout_seconds: float = 20.0

    def __post_init__(self) -> None:
        if not self.command:
            raise ValueError("watchdog command must not be empty")
        for name in (
            "restart_delay_seconds",
            "quick_fail_seconds",
            "child_stop_timeout_seconds",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        if (
            self.max_restarts < 0
            or self.max_failures < 0
            or self.max_quick_failures < 0
        ):
            raise ValueError("watchdog limits must be non-negative")


@dataclass
class WatchdogStats:
    starts: int = 0
    restarts: int = 0
    alerts_sent: int = 0
    quick_failures: int = 0
    last_return_code: int | None = None


class _AlertSinkLike(Protocol):
    async def send(self, alert: AppAlert) -> None:
        ...


class ProcessWatchdog:
    """Cross-platform child supervisor used by both app and live CLI paths."""

    def __init__(
        self,
        config: WatchdogConfig,
        *,
        alert_sink: AlertSink | _AlertSinkLike | None = None,
    ) -> None:
        self.config = config
        self.alert_sink = alert_sink
        self.stats = WatchdogStats()
        self._stop = False
        self._process: asyncio.subprocess.Process | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def stop(self) -> None:
        self._stop = True
        process = self._process
        if process is None or process.returncode is not None:
            return
        logger.info("WATCHDOG | Stopping live child pid=%s", process.pid)
        try:
            process.terminate()
        except ProcessLookupError:
            return
        loop = self._loop
        if loop is not None:
            loop.call_later(
                self.config.child_stop_timeout_seconds,
                self._kill_child_if_running,
                process,
            )

    async def run(self) -> WatchdogStats:
        self._loop = asyncio.get_running_loop()
        while not self._stop:
            started_at = time.monotonic()
            process, output = await self._start_child()
            return_code = await process.wait()
            uptime = time.monotonic() - started_at
            self.stats.last_return_code = return_code
            self._process = None
            self._remove_child_pid_file()
            if output is not None:
                output.close()

            if return_code == 0:
                self.stats.quick_failures = 0
                logger.info("WATCHDOG | Live child exited normally returncode=0")
                return self.stats
            if self._stop:
                logger.info(
                    "WATCHDOG | Live child stopped pid=%s returncode=%s",
                    process.pid,
                    return_code,
                )
                return self.stats
            if return_code in self.config.fatal_exit_codes:
                logger.error(
                    "WATCHDOG | Fatal exit code received returncode=%s; stopping",
                    return_code,
                )
                await self._send_alert(
                    subject="AetherEdge watchdog received fatal exit code",
                    content=f"returncode={return_code}",
                )
                return self.stats

            if self.config.quick_fail_seconds and uptime < self.config.quick_fail_seconds:
                self.stats.quick_failures += 1
                logger.warning(
                    "WATCHDOG | Quick failure returncode=%s uptime=%.2fs count=%s/%s",
                    return_code,
                    uptime,
                    self.stats.quick_failures,
                    self.config.max_quick_failures,
                )
            else:
                if self.stats.quick_failures:
                    logger.info(
                        "WATCHDOG | Quick-failure counter reset uptime=%.2fs previous=%s",
                        uptime,
                        self.stats.quick_failures,
                    )
                self.stats.quick_failures = 0
            if (
                self.config.max_quick_failures
                and self.stats.quick_failures >= self.config.max_quick_failures
            ):
                logger.error("WATCHDOG | Quick-fail circuit opened; stopping")
                await self._send_alert(
                    subject="AetherEdge watchdog quick-fail circuit opened",
                    content=(
                        f"quick_failure_count={self.stats.quick_failures}\n"
                        f"quick_fail_seconds={self.config.quick_fail_seconds}\n"
                        f"last_returncode={return_code}\n"
                        f"last_uptime_seconds={uptime:.2f}"
                    ),
                )
                return self.stats

            message = (
                f"returncode={return_code} restart_count={self.stats.restarts + 1} "
                f"quick_failure_count={self.stats.quick_failures}"
            )
            logger.warning("WATCHDOG | Live child exited unexpectedly %s", message)
            await self._send_alert(
                subject="AetherEdge live runner exited",
                content=message,
            )
            if self.config.max_failures and self.stats.starts >= self.config.max_failures:
                logger.error("WATCHDOG | Max restart count reached; stopping")
                return self.stats
            if (
                self.config.max_restarts
                and self.stats.restarts >= self.config.max_restarts
            ):
                logger.error("WATCHDOG | Max restart count reached; stopping")
                return self.stats
            self.stats.restarts += 1
            if self.config.restart_delay_seconds:
                await asyncio.sleep(self.config.restart_delay_seconds)
        return self.stats

    @property
    def stop_requested(self) -> bool:
        return self._stop

    async def _start_child(
        self,
    ) -> tuple[asyncio.subprocess.Process, object | None]:
        output = None
        if self.config.stdout_path is not None:
            self.config.stdout_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info("WATCHDOG | Live child log file: %s", self.config.stdout_path)
            output = self.config.stdout_path.open(
                "a", buffering=1, encoding="utf-8"
            )
        logger.info("WATCHDOG | Starting live child: %s", " ".join(self.config.command))
        try:
            process = await asyncio.create_subprocess_exec(
                *self.config.command,
                cwd=None if self.config.cwd is None else str(self.config.cwd),
                stdout=output,
                stderr=asyncio.subprocess.STDOUT if output is not None else None,
                stdin=asyncio.subprocess.DEVNULL,
                start_new_session=os.name != "nt",
            )
        except BaseException:
            if output is not None:
                output.close()
            raise
        self._process = process
        self.stats.starts += 1
        if self.config.child_pid_file is not None:
            self.config.child_pid_file.parent.mkdir(parents=True, exist_ok=True)
            self.config.child_pid_file.write_text(str(process.pid), encoding="utf-8")
        logger.info("WATCHDOG | Live child started pid=%s", process.pid)
        return process, output

    def _kill_child_if_running(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is None:
            logger.warning("WATCHDOG | Live child stop timed out; killing pid=%s", process.pid)
            try:
                process.kill()
            except ProcessLookupError:
                pass

    def _remove_child_pid_file(self) -> None:
        if self.config.child_pid_file is None:
            return
        try:
            self.config.child_pid_file.unlink(missing_ok=True)
        except OSError:
            logger.exception("WATCHDOG | Failed to remove child pid file")

    async def _send_alert(self, *, subject: str, content: str) -> None:
        if self.alert_sink is None:
            return
        await self.alert_sink.send(
            AppAlert(subject=subject, content=content, severity="error")
        )
        self.stats.alerts_sent += 1


class EmailWatchdogAlertSink:
    async def send(self, alert: AppAlert) -> None:
        from src.utils.email_sender import send_email

        try:
            await send_email(
                subject=alert.subject,
                content=alert.content,
                content_type="plain",
            )
        except Exception as exc:
            logger.warning("WATCHDOG | Failed to send email alert: %s", exc)


def build_live_runner_command(
    *, project_root: str | Path, child_args: Sequence[str] = ()
) -> tuple[str, ...]:
    root = Path(project_root)
    return (
        sys.executable,
        str(root / "scripts" / "run_live.py"),
        *tuple(child_args),
    )


def build_command(
    *,
    project_root: Path = PROJECT_ROOT,
    environ: Mapping[str, str] | None = None,
) -> list[str]:
    env = os.environ if environ is None else environ
    python_bin = env.get("LIVE_PYTHON_BIN", sys.executable)
    live_script = _resolve_path(
        env.get("LIVE_SCRIPT"),
        project_root / "scripts" / "run_live.py",
        project_root,
    )
    return [python_bin, "-u", str(live_script), *shlex.split(env.get("LIVE_ARGS", ""))]


def build_live_watchdog_from_env(
    *,
    project_root: Path = PROJECT_ROOT,
    environ: Mapping[str, str] | None = None,
) -> ProcessWatchdog:
    env = os.environ if environ is None else environ
    config = WatchdogConfig(
        command=tuple(build_command(project_root=project_root, environ=env)),
        restart_delay_seconds=float(
            env.get("AETHER_WATCHDOG_RESTART_DELAY_SECONDS", "5")
        ),
        max_failures=int(env.get("AETHER_WATCHDOG_MAX_RESTARTS", "0")),
        quick_fail_seconds=float(env.get("WATCHDOG_QUICK_FAIL_SECONDS", "60")),
        max_quick_failures=int(env.get("WATCHDOG_MAX_QUICK_FAILURES", "3")),
        fatal_exit_codes=_parse_fatal_exit_codes(
            env.get("WATCHDOG_FATAL_EXIT_CODES", "78")
        ),
        cwd=project_root,
        stdout_path=_resolve_path(
            env.get("LIVE_LOG_FILE"),
            project_root / "logs" / "aether_live.out",
            project_root,
        ),
        child_pid_file=_resolve_path(
            env.get("LIVE_PID_FILE"),
            project_root / "data" / "run" / "aether_live.pid",
            project_root,
        ),
        child_stop_timeout_seconds=float(
            env.get("AETHER_WATCHDOG_CHILD_STOP_TIMEOUT_SECONDS", "20")
        ),
    )
    sink = (
        EmailWatchdogAlertSink()
        if _truthy(env.get("AETHER_ENABLE_EMAIL_ALERT"))
        else None
    )
    return ProcessWatchdog(config, alert_sink=sink)


def run_live_watchdog(*, project_root: Path = PROJECT_ROOT) -> int:
    watchdog = build_live_watchdog_from_env(project_root=project_root)

    logger.info("WATCHDOG | Watchdog started")
    logger.info("WATCHDOG | Project root: %s", project_root)
    logger.info(
        "WATCHDOG | Restart delay: %ss, max_restarts=%s",
        watchdog.config.restart_delay_seconds,
        watchdog.config.max_failures or "unlimited",
    )
    logger.info(
        "WATCHDOG | Quick-fail: %ss, max_quick_failures=%s",
        watchdog.config.quick_fail_seconds,
        watchdog.config.max_quick_failures,
    )
    logger.info(
        "WATCHDOG | Fatal exit codes: %s",
        sorted(watchdog.config.fatal_exit_codes) or "none",
    )

    def handle_signal(signum: int, _frame: object) -> None:
        logger.info("WATCHDOG | Received signal=%s; shutting down", signum)
        watchdog.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    stats = asyncio.run(watchdog.run())
    return 0 if watchdog.stop_requested else (stats.last_return_code or 0)


def _parse_fatal_exit_codes(raw: str | None) -> frozenset[int]:
    codes: set[int] = set()
    for part in str(raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            codes.add(int(part))
        except ValueError:
            logger.info("WATCHDOG | Ignoring non-integer fatal exit code: %r", part)
    return frozenset(codes)


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _resolve_path(raw: str | None, default: Path, project_root: Path) -> Path:
    path = Path(raw).expanduser() if raw else default
    return path if path.is_absolute() else project_root / path


__all__ = [
    "DEFAULT_CHILD_PID_FILE",
    "DEFAULT_LIVE_LOG",
    "DEFAULT_LIVE_SCRIPT",
    "EmailWatchdogAlertSink",
    "ProcessWatchdog",
    "WatchdogConfig",
    "WatchdogStats",
    "build_command",
    "build_live_runner_command",
    "build_live_watchdog_from_env",
    "run_live_watchdog",
]
