from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from src.app.alerts import AlertSink, AppAlert, NoopAlertSink


@dataclass(frozen=True)
class WatchdogConfig:
    """Process watchdog config for the live runner.

    The watchdog is intentionally process-level: it supervises the trading
    process from outside instead of running inside the strategy/app loop. If
    the child process crashes, this parent can still alert and restart it.
    """

    command: tuple[str, ...]
    cwd: str | None = None
    restart_delay_seconds: float = 5.0
    max_restarts: int | None = None
    stop_on_success: bool = True


@dataclass
class WatchdogStats:
    starts: int = 0
    restarts: int = 0
    alerts_sent: int = 0
    last_return_code: int | None = None


class ProcessWatchdog:
    """Small async supervisor for a child trading process.

    This class does not know about strategies, orders, exchanges, or platform
    adapters. It only starts a child command, watches its exit code, emits an
    alert on abnormal exit, and restarts within the configured limit.
    """

    def __init__(self, config: WatchdogConfig, *, alert_sink: AlertSink | None = None) -> None:
        if not config.command:
            raise ValueError("watchdog command must not be empty")
        self.config = config
        self.alert_sink = alert_sink or NoopAlertSink()
        self.stats = WatchdogStats()

    async def run(self) -> WatchdogStats:
        while True:
            self.stats.starts += 1
            process = await asyncio.create_subprocess_exec(*self.config.command, cwd=self.config.cwd)
            return_code = await process.wait()
            self.stats.last_return_code = return_code

            if return_code == 0 and self.config.stop_on_success:
                return self.stats

            await self._alert_child_exit(return_code)

            if self.config.max_restarts is not None and self.stats.restarts >= self.config.max_restarts:
                return self.stats

            self.stats.restarts += 1
            if self.config.restart_delay_seconds > 0:
                await asyncio.sleep(self.config.restart_delay_seconds)

    async def _alert_child_exit(self, return_code: int) -> None:
        subject = "AetherEdge live runner exited"
        content = (
            f"AetherEdge child process exited with code {return_code}.\n"
            f"command: {_format_command(self.config.command)}\n"
            f"cwd: {self.config.cwd or os.getcwd()}\n"
            f"starts: {self.stats.starts}\n"
            f"restarts_so_far: {self.stats.restarts}\n"
        )
        await self.alert_sink.send(AppAlert(subject=subject, content=content, severity="error"))
        self.stats.alerts_sent += 1


def build_live_runner_command(*, project_root: str | Path, child_args: Sequence[str] = ()) -> tuple[str, ...]:
    """Build the default command supervised by the watchdog."""

    import sys

    root = Path(project_root)
    return (sys.executable, str(root / "scripts" / "run_live.py"), *tuple(child_args))


def _format_command(command: Sequence[str]) -> str:
    return " ".join(str(part) for part in command)
