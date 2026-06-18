from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping

from src.platform.exchanges.models import ExchangeName


@dataclass(frozen=True)
class MarketProfile:
    """Local market metadata for one canonical trading instrument."""

    symbol: str
    base_asset: str
    quote_asset: str
    contract_type: str = "perp"
    default: bool = False
    exchange_symbols: Mapping[ExchangeName, str] = field(default_factory=dict)
    contract_value_by_exchange: Mapping[ExchangeName, Decimal] = field(default_factory=dict)
    min_quantity_by_exchange: Mapping[ExchangeName, Decimal] = field(default_factory=dict)
    quantity_unit_by_exchange: Mapping[ExchangeName, str] = field(default_factory=dict)
    raw: Mapping[str, Any] = field(default_factory=dict)

    def raw_symbol(self, exchange: ExchangeName | str) -> str:
        exchange_name = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())
        try:
            return self.exchange_symbols[exchange_name]
        except KeyError as exc:
            raise ValueError(f"No raw symbol configured for {exchange_name.value}:{self.symbol}") from exc

    def contract_value(self, exchange: ExchangeName | str) -> Decimal | None:
        exchange_name = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())
        return self.contract_value_by_exchange.get(exchange_name)

    def min_quantity(self, exchange: ExchangeName | str) -> Decimal | None:
        exchange_name = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())
        return self.min_quantity_by_exchange.get(exchange_name)
