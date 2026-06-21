from __future__ import annotations

from src.app import AppConfig, AppContext, AppRunner
from src.app.runner import AppRunnerStats
from src.runtime.config import LiveRuntimeConfig, live_runtime_config_from_app
from src.runtime.context import LiveRuntimeContext
from src.runtime.models import RuntimeHealth, RuntimePhase


class LiveRuntimeRunner:
    """Live runtime wrapper around the existing lightweight AppRunner.

    This phase keeps the proven app runner path intact while introducing a
    runtime-domain lifecycle surface for warmup, schedulers and recovery to plug
    into in later phases.
    """

    def __init__(self, *, app_config: AppConfig, app_context: AppContext, runtime_config: LiveRuntimeConfig | None = None) -> None:
        self.app_config = app_config
        self.runtime_config = runtime_config or live_runtime_config_from_app(app_config)
        self.context = LiveRuntimeContext(config=self.runtime_config, app_context=app_context)
        self._runner = AppRunner(config=app_config, context=app_context)
        self._health = RuntimeHealth(
            phase=RuntimePhase.CREATED,
            warmup_complete=not self.runtime_config.warmup_enabled,
            caught_up=not self.runtime_config.warmup_enabled,
            metadata={"runtime_mode": self.runtime_config.mode.value},
        )

    async def run(self, *, max_market_events: int | None = None) -> AppRunnerStats:
        self._health = RuntimeHealth(phase=RuntimePhase.RUNNING, warmup_complete=True, caught_up=True, metadata=self._health.metadata)
        try:
            return await self._runner.run_streams(max_market_events=max_market_events)
        finally:
            self._health = RuntimeHealth(phase=RuntimePhase.STOPPED, warmup_complete=True, caught_up=True, metadata=self._health.metadata)

    async def start(self) -> RuntimeHealth:
        self._health = RuntimeHealth(phase=RuntimePhase.RUNNING, warmup_complete=True, caught_up=True, metadata=self._health.metadata)
        return self._health

    async def stop(self) -> RuntimeHealth:
        self._runner.stop()
        self._health = RuntimeHealth(phase=RuntimePhase.STOPPED, warmup_complete=True, caught_up=True, metadata=self._health.metadata)
        return self._health

    async def health(self) -> RuntimeHealth:
        return self._health
