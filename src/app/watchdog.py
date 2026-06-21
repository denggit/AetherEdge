from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence

from src.app.alerts import AppAlert, AlertSink


@dataclass(frozen=True)
class WatchdogConfig:
    command: tuple[str, ...]
    restart_delay_seconds: float = 5.0
    max_restarts: int = 0  # 0 means unlimited.

    def __post_init__(self) -> None:
        if not self.command:
            raise ValueError("watchdog command must not be empty")
        if self.restart_delay_seconds < 0:
            raise ValueError("restart_delay_seconds must be non-negative")
        if self.max_restarts < 0:
            raise ValueError("max_restarts must be non-negative")


@dataclass
class WatchdogStats:
    starts: int = 0
    restarts: int = 0
    alerts_sent: int = 0
    last_return_code: int | None = None


class _AlertSinkLike(Protocol):
    async def send(self, alert: AppAlert) -> None:
        ...


class ProcessWatchdog:
    """Generic app-level child-process watchdog.

    It supervises a command and optionally restarts it. The watchdog is generic
    process control and intentionally does not know about trading strategies,
    orders, market data, or exchange adapters.
    """

    def __init__(self, config: WatchdogConfig, *, alert_sink: AlertSink | _AlertSinkLike | None = None) -> None:
        self.config = config
        self.alert_sink = alert_sink
        self.stats = WatchdogStats()
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    async def run(self) -> WatchdogStats:
        while True:
            self.stats.starts += 1
            proc = await asyncio.create_subprocess_exec(*self.config.command)
            return_code = await proc.wait()
            self.stats.last_return_code = return_code
            if return_code == 0 or self._stop:
                return self.stats
            await self._send_alert(return_code)
            if self.config.max_restarts and self.stats.restarts >= self.config.max_restarts:
                return self.stats
            self.stats.restarts += 1
            if self.config.restart_delay_seconds:
                await asyncio.sleep(self.config.restart_delay_seconds)

    async def _send_alert(self, return_code: int) -> None:
        if self.alert_sink is None:
            return
        alert = AppAlert(
            subject="AetherEdge live child exited",
            content=f"Live child exited with return_code={return_code}",
            severity="error",
        )
        await self.alert_sink.send(alert)
        self.stats.alerts_sent += 1


def build_live_runner_command(*, project_root: str | Path, child_args: Sequence[str] = ()) -> tuple[str, ...]:
    root = Path(project_root)
    return (sys.executable, str(root / "scripts" / "run_live.py"), *tuple(child_args))
