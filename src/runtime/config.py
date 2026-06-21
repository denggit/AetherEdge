from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from src.app import AppConfig
from src.platform.config import load_env_config
from src.runtime.models import RuntimeMode


@dataclass(frozen=True)
class LiveRuntimeConfig:
    """Runtime-domain config layered on top of the existing AppConfig."""

    app: AppConfig
    mode: RuntimeMode = RuntimeMode.LEGACY_APP
    warmup_enabled: bool = True
    background_queue_maxsize: int = 1000
    scheduler_poll_seconds: float = 1.0
    closed_bar_interval: str = "4h"
    closed_bar_buffer_ms: int = 60_000
    range_pct: Decimal = Decimal("0.002")
    producer_stale_timeout_ms: int = 60_000

    @property
    def symbol(self) -> str:
        return self.app.symbol


def runtime_mode_from_env(
    *,
    defaults_path: str | Path = "config/aether_defaults.json",
    env_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> RuntimeMode:
    defaults = _load_defaults(defaults_path)
    env = load_env_config(env_file, environ=environ)
    value = env.get("AETHER_RUNTIME_MODE", str(defaults.get("runtime_mode", RuntimeMode.LEGACY_APP.value)))
    return RuntimeMode(str(value).strip().lower())


def live_runtime_config_from_app(
    app_config: AppConfig,
    *,
    defaults_path: str | Path = "config/aether_defaults.json",
    env_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> LiveRuntimeConfig:
    defaults = _load_defaults(defaults_path)
    env = load_env_config(env_file, environ=environ)
    return LiveRuntimeConfig(
        app=app_config,
        mode=RuntimeMode(str(env.get("AETHER_RUNTIME_MODE", defaults.get("runtime_mode", RuntimeMode.LEGACY_APP.value))).strip().lower()),
        warmup_enabled=_bool(env.get("AETHER_WARMUP_ENABLED", defaults.get("warmup_enabled", True))),
        background_queue_maxsize=int(env.get("AETHER_BACKGROUND_QUEUE_MAXSIZE", defaults.get("background_queue_maxsize", 1000))),
        scheduler_poll_seconds=float(env.get("AETHER_SCHEDULER_POLL_SECONDS", defaults.get("scheduler_poll_seconds", 1.0))),
        closed_bar_interval=str(env.get("AETHER_CLOSED_BAR_INTERVAL", defaults.get("closed_bar_interval", "4h"))),
        closed_bar_buffer_ms=int(env.get("AETHER_CLOSED_BAR_BUFFER_MS", defaults.get("closed_bar_buffer_ms", 60_000))),
        range_pct=Decimal(str(env.get("AETHER_RANGE_PCT", defaults.get("range_pct", "0.002")))),
        producer_stale_timeout_ms=int(env.get("AETHER_PRODUCER_STALE_TIMEOUT_MS", defaults.get("producer_stale_timeout_ms", 60_000))),
    )


def _load_defaults(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def _bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
