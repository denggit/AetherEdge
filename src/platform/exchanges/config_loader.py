from __future__ import annotations

from collections.abc import Mapping

from src.platform.config import (
    get_project_env_config,
    has_project_env_config,
    load_env_config,
)
from src.platform.exchanges.models import ExchangeConfig, MarginMode
from src.platform.exchanges.names import ExchangeName


def load_exchange_config(
    exchange: ExchangeName | str,
    env: Mapping[str, str] | None = None,
) -> ExchangeConfig:
    """Load adapter configuration at the application composition boundary."""

    if env is not None:
        values = {str(key): str(value) for key, value in env.items()}
    elif has_project_env_config():
        values = dict(get_project_env_config().values)
    else:
        values = load_env_config()

    exchange_name = (
        exchange
        if isinstance(exchange, ExchangeName)
        else ExchangeName(str(exchange).strip().lower())
    )
    base = ExchangeConfig(
        sandbox=_bool_env(
            values.get(
                f"{exchange_name.value.upper()}_SANDBOX",
                values.get("SANDBOX", "false"),
            )
        ),
        timeout_seconds=float(
            values.get("API_TIMEOUT_SECONDS", "10.0") or 10.0
        ),
        recv_window_ms=int(
            values.get("BINANCE_RECV_WINDOW_MS", "5000") or 5000
        ),
        live_trading_enabled=_bool_env(
            values.get("AETHER_LIVE_TRADING", "false")
        ),
        default_margin_mode=MarginMode(
            str(values.get("MARGIN_MODE", "cross")).strip().lower()
        ),
    )
    if exchange_name == ExchangeName.OKX:
        from src.platform.exchanges.okx.credentials import (
            resolve_okx_credentials,
        )

        api_key, api_secret, passphrase = resolve_okx_credentials(base, values)
        return ExchangeConfig(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            sandbox=base.sandbox,
            timeout_seconds=base.timeout_seconds,
            recv_window_ms=base.recv_window_ms,
            live_trading_enabled=base.live_trading_enabled,
            default_margin_mode=base.default_margin_mode,
        )
    if exchange_name == ExchangeName.BINANCE:
        from src.platform.exchanges.binance.credentials import (
            resolve_binance_credentials,
        )

        api_key, api_secret = resolve_binance_credentials(base, values)
        return ExchangeConfig(
            api_key=api_key,
            api_secret=api_secret,
            sandbox=base.sandbox,
            timeout_seconds=base.timeout_seconds,
            recv_window_ms=base.recv_window_ms,
            live_trading_enabled=base.live_trading_enabled,
            default_margin_mode=base.default_margin_mode,
        )
    return base


def _bool_env(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


__all__ = ["load_exchange_config"]
