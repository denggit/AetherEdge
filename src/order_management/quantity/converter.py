from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

from src.platform.exchanges.models import ExchangeName, OrderRequest, StopMarketOrderRequest
from src.platform.markets import MarketProfile


@dataclass(frozen=True)
class NativeQuantityConversion:
    """Record how a strategy base-asset quantity maps to one exchange.

    AetherEdge strategy signals express ``quantity`` in base asset units, e.g.
    ETH for ``ETH-USDT-PERP``. Some venues, notably OKX swaps, place orders in
    contracts instead. This model keeps that conversion explicit and auditable.
    """

    exchange: ExchangeName
    symbol: str
    base_quantity: Decimal
    native_quantity: Decimal
    quantity_unit: str
    contract_value: Decimal | None = None

    def metadata(self) -> dict[str, str | None]:
        return {
            "exchange": self.exchange.value,
            "symbol": self.symbol,
            "base_quantity": _decimal_text(self.base_quantity),
            "native_quantity": _decimal_text(self.native_quantity),
            "quantity_unit": self.quantity_unit,
            "contract_value": None if self.contract_value is None else _decimal_text(self.contract_value),
        }


class NativeQuantityConverter:
    """Convert canonical base-asset signal quantities into venue-native size."""

    BASE_ASSET_UNITS = {"base", "base_asset", "asset", "coin"}
    CONTRACT_UNITS = {"contract", "contracts"}

    def convert_quantity(
        self,
        *,
        exchange: ExchangeName | str,
        symbol: str,
        base_quantity: Decimal,
        market_profile: MarketProfile,
    ) -> NativeQuantityConversion:
        exchange_name = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())
        if base_quantity <= 0:
            raise ValueError("base_quantity must be positive")

        unit = str(market_profile.quantity_unit_by_exchange.get(exchange_name, "base_asset")).strip().lower()
        contract_value = market_profile.contract_value(exchange_name)
        if unit in self.CONTRACT_UNITS:
            if contract_value is None or contract_value <= 0:
                raise ValueError(f"contract_value is required for contract-sized exchange {exchange_name.value}:{symbol}")
            native_quantity = base_quantity / contract_value
        elif unit in self.BASE_ASSET_UNITS:
            native_quantity = base_quantity
        elif contract_value is not None and contract_value != Decimal("1"):
            native_quantity = base_quantity / contract_value
        else:
            native_quantity = base_quantity

        return NativeQuantityConversion(
            exchange=exchange_name,
            symbol=symbol,
            base_quantity=base_quantity,
            native_quantity=native_quantity,
            quantity_unit=unit,
            contract_value=contract_value,
        )

    def native_to_base_quantity(
        self,
        *,
        exchange: ExchangeName | str,
        symbol: str,
        native_quantity: Decimal,
        market_profile: MarketProfile,
    ) -> Decimal:
        exchange_name = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())
        if native_quantity < 0:
            raise ValueError("native_quantity must be non-negative")

        unit = str(market_profile.quantity_unit_by_exchange.get(exchange_name, "base_asset")).strip().lower()
        contract_value = market_profile.contract_value(exchange_name)
        if unit in self.CONTRACT_UNITS:
            if contract_value is None or contract_value <= 0:
                raise ValueError(f"contract_value is required for contract-sized exchange {exchange_name.value}:{symbol}")
            return native_quantity * contract_value
        if unit in self.BASE_ASSET_UNITS:
            return native_quantity
        if contract_value is not None and contract_value != Decimal("1"):
            return native_quantity * contract_value
        return native_quantity

    def convert_order_request(self, request: OrderRequest, *, exchange: ExchangeName | str, market_profile: MarketProfile) -> tuple[OrderRequest, NativeQuantityConversion]:
        conversion = self.convert_quantity(
            exchange=exchange,
            symbol=request.symbol,
            base_quantity=request.quantity,
            market_profile=market_profile,
        )
        return replace(request, quantity=conversion.native_quantity), conversion

    def convert_stop_market_request(
        self,
        request: StopMarketOrderRequest,
        *,
        exchange: ExchangeName | str,
        market_profile: MarketProfile,
    ) -> tuple[StopMarketOrderRequest, NativeQuantityConversion | None]:
        if request.quantity is None:
            return request, None
        conversion = self.convert_quantity(
            exchange=exchange,
            symbol=request.symbol,
            base_quantity=request.quantity,
            market_profile=market_profile,
        )
        return replace(request, quantity=conversion.native_quantity), conversion


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def conversion_metadata(value: NativeQuantityConversion | None) -> dict[str, Any]:
    return {} if value is None else value.metadata()
