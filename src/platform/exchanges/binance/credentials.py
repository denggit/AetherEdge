from __future__ import annotations

from collections.abc import Mapping

from src.platform.exchanges.models import ExchangeConfig


def resolve_binance_credentials(config: ExchangeConfig, env: Mapping[str, str]) -> tuple[str, str]:
    """Resolve Binance credentials using only the maintained key names."""

    api_key = config.api_key or env.get("BINANCE_API_KEY", "")
    api_secret = config.api_secret or env.get("BINANCE_SECRET_KEY", "")
    return api_key, api_secret


__all__ = ["resolve_binance_credentials"]
