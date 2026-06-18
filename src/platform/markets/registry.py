from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Mapping

from src.platform.exchanges.models import ExchangeName
from src.platform.markets.models import MarketProfile

DEFAULT_MARKET_SYMBOL = "ETH-USDT-PERP"
_PROFILE_DIR = Path(__file__).resolve().parent / "profiles"
_PROFILES: dict[str, MarketProfile] | None = None


def get_market_profile(symbol: str | None = None) -> MarketProfile:
    profiles = _load_profiles()
    selected = symbol or os.environ.get("AETHER_MARKET") or os.environ.get("MARKET_SYMBOL") or _default_symbol(profiles)
    try:
        return profiles[selected]
    except KeyError as exc:
        raise ValueError(f"Unsupported market profile: {selected!r}") from exc


def list_market_profiles() -> list[MarketProfile]:
    return sorted(_load_profiles().values(), key=lambda profile: profile.symbol)


def register_market_profile(profile: MarketProfile) -> None:
    profiles = _load_profiles()
    profiles[profile.symbol] = profile


def to_exchange_symbol(exchange: ExchangeName, canonical_symbol: str) -> str:
    return get_market_profile(canonical_symbol).raw_symbol(exchange)


def to_canonical_symbol(exchange: ExchangeName, raw_symbol: str) -> str:
    exchange_name = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())
    for profile in _load_profiles().values():
        if profile.exchange_symbols.get(exchange_name) == raw_symbol:
            return profile.symbol
    raise ValueError(f"Unsupported raw symbol mapping: exchange={exchange_name.value}, raw_symbol={raw_symbol!r}")


def _load_profiles() -> dict[str, MarketProfile]:
    global _PROFILES
    if _PROFILES is None:
        _PROFILES = {}
        for path in sorted(_PROFILE_DIR.glob("*.json")):
            profile = _profile_from_json(path)
            _PROFILES[profile.symbol] = profile
    return _PROFILES


def _profile_from_json(path: Path) -> MarketProfile:
    data = json.loads(path.read_text(encoding="utf-8"))
    return MarketProfile(
        symbol=str(data["symbol"]),
        base_asset=str(data["base_asset"]),
        quote_asset=str(data["quote_asset"]),
        contract_type=str(data.get("contract_type", "perp")),
        default=bool(data.get("default", False)),
        exchange_symbols=_exchange_mapping(data.get("exchange_symbols", {})),
        contract_value_by_exchange=_decimal_exchange_mapping(data.get("contract_value_by_exchange", {})),
        min_quantity_by_exchange=_decimal_exchange_mapping(data.get("min_quantity_by_exchange", {})),
        quantity_unit_by_exchange={ExchangeName(str(k)): str(v) for k, v in dict(data.get("quantity_unit_by_exchange", {})).items()},
        raw=data,
    )


def _exchange_mapping(values: Mapping[str, str]) -> dict[ExchangeName, str]:
    return {ExchangeName(str(key)): str(value) for key, value in values.items()}


def _decimal_exchange_mapping(values: Mapping[str, str]) -> dict[ExchangeName, Decimal]:
    return {ExchangeName(str(key)): Decimal(str(value)) for key, value in values.items()}


def _default_symbol(profiles: Mapping[str, MarketProfile]) -> str:
    for profile in profiles.values():
        if profile.default:
            return profile.symbol
    return DEFAULT_MARKET_SYMBOL
