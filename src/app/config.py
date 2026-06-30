from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.platform.config import get_project_env_config, load_env_config
from src.platform.exchanges.models import ExchangeName


@dataclass(frozen=True)
class AppConfig:
    symbol: str
    exchanges: tuple[ExchangeName, ...]
    data_exchange: ExchangeName
    strategy: str
    data_streams: tuple[str, ...]
    state_db_path: str
    market_queue_maxsize: int
    signal_queue_maxsize: int
    alert_queue_maxsize: int
    dry_run: bool
    enable_email_alerts: bool

    @classmethod
    def from_env(
        cls,
        *,
        defaults_path: str | Path = "config/aether_defaults.json",
        env_file: str | Path | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> "AppConfig":
        defaults = _load_defaults(defaults_path)
        env = _load_app_env(env_file=env_file, environ=environ)

        symbol = env.get("AETHER_MARKET", str(defaults.get("symbol", "ETH-USDT-PERP")))
        exchanges = _exchange_tuple(env.get("AETHER_EXCHANGES"), defaults.get("exchanges", ["okx"]))
        data_exchange = ExchangeName(str(env.get("AETHER_DATA_EXCHANGE", defaults.get("data_exchange", exchanges[0].value))).strip().lower())
        strategy = env.get("AETHER_STRATEGY", str(defaults.get("strategy", "strategies.empty_strategy:Strategy")))
        data_streams = _str_tuple(env.get("AETHER_DATA_STREAMS"), defaults.get("data_streams", ["trades", "order_book"]))
        return cls(
            symbol=symbol,
            exchanges=exchanges,
            data_exchange=data_exchange,
            strategy=strategy,
            data_streams=data_streams,
            state_db_path=env.get("AETHER_STATE_DB", str(defaults.get("state_db_path", "data/state/aether_state.sqlite3"))),
            market_queue_maxsize=int(env.get("AETHER_MARKET_QUEUE_MAXSIZE", defaults.get("market_queue_maxsize", 50000))),
            signal_queue_maxsize=int(env.get("AETHER_SIGNAL_QUEUE_MAXSIZE", defaults.get("signal_queue_maxsize", 200))),
            alert_queue_maxsize=int(env.get("AETHER_ALERT_QUEUE_MAXSIZE", defaults.get("alert_queue_maxsize", 100))),
            dry_run=_bool(env.get("AETHER_DRY_RUN", defaults.get("dry_run", True))),
            enable_email_alerts=_bool(env.get("AETHER_ENABLE_EMAIL_ALERT", defaults.get("enable_email_alerts", False))),
        )


def _load_defaults(path: str | Path) -> dict[str, Any]:
    defaults_path = Path(path)
    if not defaults_path.exists():
        return {}
    return json.loads(defaults_path.read_text(encoding="utf-8"))


def _load_app_env(*, env_file: str | Path | None, environ: Mapping[str, str] | None) -> dict[str, str]:
    if environ is None and env_file is None:
        return dict(get_project_env_config().values)
    if environ is not None and env_file is None:
        return {str(key): str(value) for key, value in environ.items()}
    return dict(load_env_config(env_file, environ=environ))


def _exchange_tuple(raw: str | None, default: Any) -> tuple[ExchangeName, ...]:
    values = _str_tuple(raw, default)
    if not values:
        raise ValueError("at least one exchange is required")
    return tuple(ExchangeName(value.strip().lower()) for value in values)


def _str_tuple(raw: str | None, default: Any) -> tuple[str, ...]:
    if raw is None:
        if isinstance(default, str):
            raw_values = [default]
        else:
            raw_values = list(default or [])
    else:
        raw_values = [part.strip() for part in raw.split(",")]
    return tuple(value for value in raw_values if value)


def _bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
