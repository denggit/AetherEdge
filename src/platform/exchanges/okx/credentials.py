from __future__ import annotations

from collections.abc import Mapping

from src.platform.exchanges.models import ExchangeConfig


def resolve_okx_credentials(config: ExchangeConfig, env: Mapping[str, str]) -> tuple[str, str, str]:
    """Resolve OKX credentials using only the maintained key names."""

    api_key = config.api_key or env.get("OKX_API_KEY", "")
    api_secret = config.api_secret or env.get("OKX_SECRET_KEY", "")
    passphrase = config.passphrase or env.get("OKX_PASSPHRASE", "")
    return api_key, api_secret, passphrase


__all__ = ["resolve_okx_credentials"]
