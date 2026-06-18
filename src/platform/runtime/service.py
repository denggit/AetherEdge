from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from src.platform.account.events import AccountEvent
from src.platform.runtime.config import RuntimeConfig
from src.platform.runtime.context import RuntimeContext
from src.platform.runtime.handlers import NoopRuntimeEventHandler, RuntimeEventHandler
from src.platform.snapshot import PlatformSnapshot, fetch_platform_snapshot


@dataclass
class RuntimeStats:
    snapshots_saved: int = 0
    account_events_saved: int = 0
    handler_errors: int = 0
    last_event: AccountEvent | None = None


@dataclass
class RuntimeRunResult:
    stats: RuntimeStats
    stopped: bool


class PlatformRuntime:
    """Minimal lifecycle service for wiring platform interfaces.

    It collects a startup snapshot and records private account events. It does
    not run strategy logic and does not place/cancel/amend orders.
    """

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        context: RuntimeContext,
        handlers: list[RuntimeEventHandler] | None = None,
    ) -> None:
        self._config = config
        self._context = context
        self._handlers = handlers or [NoopRuntimeEventHandler()]
        self._stop_event = asyncio.Event()
        self.stats = RuntimeStats()

    @property
    def context(self) -> RuntimeContext:
        return self._context

    def stop(self) -> None:
        self._stop_event.set()

    async def collect_startup_snapshot(self) -> PlatformSnapshot:
        snapshot = await fetch_platform_snapshot(account=self._context.account, execution=self._context.execution, asset=self._config.asset)
        if self._config.save_startup_snapshot:
            self._context.state_store.save_snapshot(snapshot)
            self.stats.snapshots_saved += 1
        await self._notify_snapshot(snapshot)
        return snapshot

    async def run(self, *, max_account_events: int | None = None) -> RuntimeRunResult:
        await self.collect_startup_snapshot()
        if not self._config.enable_private_event_stream or self._context.account_event_stream is None:
            return RuntimeRunResult(stats=self.stats, stopped=self._stop_event.is_set())
        await self.consume_account_events(max_events=max_account_events)
        return RuntimeRunResult(stats=self.stats, stopped=self._stop_event.is_set())

    async def consume_account_events(self, *, max_events: int | None = None) -> None:
        if self._context.account_event_stream is None:
            return
        count = 0
        async for event in self._context.account_event_stream.stream_events():
            if self._stop_event.is_set():
                break
            self._context.state_store.save_account_event(event)
            self.stats.account_events_saved += 1
            self.stats.last_event = event
            await self._notify_account_event(event)
            count += 1
            if max_events is not None and count >= max_events:
                break

    async def _notify_snapshot(self, snapshot: PlatformSnapshot) -> None:
        for handler in self._handlers:
            try:
                await handler.on_snapshot(snapshot)
            except Exception:
                self.stats.handler_errors += 1

    async def _notify_account_event(self, event: AccountEvent) -> None:
        for handler in self._handlers:
            try:
                await handler.on_account_event(event)
            except Exception:
                self.stats.handler_errors += 1
