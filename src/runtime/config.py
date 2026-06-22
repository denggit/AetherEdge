from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from src.app import AppConfig
from src.order_management import MasterFollowerPolicyConfig
from src.platform.config import load_env_config
from src.runtime.models import RuntimeMode
from src.runtime.startup_catchup import StartupCatchupConfig


MASTER_FOLLOWER_ENV_KEYS = frozenset(
    {
        "AETHER_MASTER_EXCHANGE",
        "AETHER_FOLLOWER_EXCHANGES",
        "AETHER_ENTRY_DEVIATION_ALERT_PCT",
        "AETHER_FOLLOWER_ENTRY_MAX_ATTEMPTS",
        "AETHER_FOLLOWER_ENTRY_RETRY_DELAY_SECONDS",
        "AETHER_MASTER_ENTRY_MAX_ATTEMPTS",
        "AETHER_MASTER_ENTRY_RETRY_DELAY_SECONDS",
        "AETHER_MASTER_FAIL_MANUAL_GRACE_SECONDS",
        "AETHER_CLOSE_ORPHAN_FOLLOWER_AFTER_GRACE",
        "AETHER_DO_NOT_REJOIN_MID_POSITION_AFTER_FOLLOWER_DESYNC",
    }
)


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
    master_follower_policy: MasterFollowerPolicyConfig | None = None
    startup_catchup: StartupCatchupConfig = StartupCatchupConfig()

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
    env = _load_runtime_env(env_file=env_file, environ=environ)
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
    env = _load_runtime_env(env_file=env_file, environ=environ)
    master_follower_env = _master_follower_env(env, env_file=env_file, environ=environ)
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
        master_follower_policy=MasterFollowerPolicyConfig.from_env(
            app_exchanges=app_config.exchanges,
            data_exchange=app_config.data_exchange,
            env=master_follower_env,
        ),
        startup_catchup=StartupCatchupConfig.from_mapping(defaults.get("startup_catchup")),
    )


def _load_runtime_env(*, env_file: str | Path | None, environ: Mapping[str, str] | None) -> dict[str, str]:
    values = dict(load_env_config(env_file, environ=environ))
    if environ is not None and env_file is None:
        # Synthetic environ mappings used by tests should be hermetic: do not
        # inherit the developer's project .env just because load_env_config can
        # read it by default. Production calls pass environ=None and still load
        # the real project environment.
        allowed = {str(key) for key in environ.keys()}
        values = {key: value for key, value in values.items() if key in allowed}
    return values


def _load_defaults(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def _bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _master_follower_env(
    env: Mapping[str, str],
    *,
    env_file: str | Path | None,
    environ: Mapping[str, str] | None,
) -> dict[str, str]:
    """Keep injected runtime-config tests from inheriting project role config.

    ``load_env_config`` intentionally reads the project ``.env`` and overlays
    the provided mapping. That is correct for production. For callers passing a
    synthetic ``environ`` without an explicit ``env_file`` (mostly tests), the
    app config object is already authoritative for exchanges, so stale project
    master/follower variables should not leak into the derived runtime config.
    """

    values = dict(env)
    if environ is None or env_file is not None:
        return values

    for key in MASTER_FOLLOWER_ENV_KEYS:
        values.pop(key, None)
    values.update({str(key): str(value) for key, value in environ.items() if str(key) in MASTER_FOLLOWER_ENV_KEYS})
    return values
