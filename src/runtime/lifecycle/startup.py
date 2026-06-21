from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from src.app import AppConfig, build_app_context
from src.runtime.config import LiveRuntimeConfig, live_runtime_config_from_app
from src.runtime.context import LiveRuntimeContext
from src.runtime.models import RuntimeHealth, RuntimePhase


@dataclass(frozen=True)
class StartupResult:
    context: LiveRuntimeContext
    health: RuntimeHealth


class LiveStartupService:
    """Build live runtime context without embedding business logic in app/platform."""

    def __init__(self, *, app_config: AppConfig, defaults_path: str = "config/aether_defaults.json") -> None:
        self.app_config = app_config
        self.defaults_path = defaults_path

    def build_context(self, *, services: Mapping[str, Any] | None = None) -> StartupResult:
        live_config: LiveRuntimeConfig = live_runtime_config_from_app(self.app_config, defaults_path=self.defaults_path)
        app_context = build_app_context(self.app_config)
        context = LiveRuntimeContext(config=live_config, app_context=app_context, services=services or {})
        return StartupResult(
            context=context,
            health=RuntimeHealth(
                phase=RuntimePhase.CREATED,
                warmup_complete=not live_config.warmup_enabled,
                caught_up=not live_config.warmup_enabled,
                metadata={"runtime_mode": live_config.mode.value, "strategy": self.app_config.strategy},
            ),
        )
