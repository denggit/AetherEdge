from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping

from src.platform.exchanges.names import ExchangeName


from enum import Enum


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(str, Enum):
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"
    BOTH = "both"


class PositionMode(str, Enum):
    ONE_WAY = "one_way"
    HEDGE = "hedge"


class TimeInForce(str, Enum):
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    POST_ONLY = "post_only"


class MarginMode(str, Enum):
    CROSS = "cross"
    ISOLATED = "isolated"


class TriggerPriceType(str, Enum):
    LAST = "last"
    MARK = "mark"
    INDEX = "index"


@dataclass(frozen=True)
class ExchangeConfig:
    """Pure runtime values passed into exchange adapters."""

    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    passphrase: str = field(default="", repr=False)  # OKX only.
    sandbox: bool = False
    timeout_seconds: float = 10.0
    recv_window_ms: int = 5000  # Binance signed request window.
    live_trading_enabled: bool = False
    default_margin_mode: MarginMode = MarginMode.CROSS
    extra_headers: Mapping[str, str] = field(default_factory=dict, repr=False)

    @classmethod
    def from_env(
        cls,
        exchange: ExchangeName | str,
        env: Mapping[str, str] | None = None,
    ) -> "ExchangeConfig":
        """Deprecated strategy-tool compatibility; use load_exchange_config."""

        from src.platform.exchanges.config_loader import load_exchange_config

        return load_exchange_config(exchange, env)

@dataclass(frozen=True)
class Balance:
    exchange: ExchangeName
    asset: str
    total: Decimal
    available: Decimal
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Position:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    side: PositionSide
    quantity: Decimal
    entry_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    leverage: Decimal | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class InstrumentRule:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    price_tick: Decimal | None = None
    quantity_step: Decimal | None = None
    min_quantity: Decimal | None = None
    min_notional: Decimal | None = None
    max_quantity: Decimal | None = None
    contract_value: Decimal | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderRequest:
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    price: Decimal | None = None
    client_order_id: str | None = None
    reduce_only: bool = False
    position_side: PositionSide | None = None
    margin_mode: MarginMode | None = None
    time_in_force: TimeInForce | None = None


@dataclass(frozen=True)
class StopMarketOrderRequest:
    symbol: str
    side: OrderSide
    trigger_price: Decimal
    quantity: Decimal | None = None
    client_order_id: str | None = None
    reduce_only: bool = True
    position_side: PositionSide | None = None
    margin_mode: MarginMode | None = None
    trigger_price_type: TriggerPriceType = TriggerPriceType.LAST
    close_position: bool = False

    def __post_init__(self) -> None:
        if self.trigger_price <= 0:
            raise ValueError("trigger_price must be positive")
        if not self.close_position:
            if self.quantity is None:
                raise ValueError("quantity is required unless close_position=True")
            if self.quantity <= 0:
                raise ValueError("quantity must be positive")


@dataclass(frozen=True)
class CancelOrderRequest:
    symbol: str
    order_id: str | None = None
    client_order_id: str | None = None

    def __post_init__(self) -> None:
        if not self.order_id and not self.client_order_id:
            raise ValueError("order_id or client_order_id is required")


@dataclass(frozen=True)
class AmendOrderRequest:
    symbol: str
    order_id: str | None = None
    client_order_id: str | None = None
    new_quantity: Decimal | None = None
    new_price: Decimal | None = None

    def __post_init__(self) -> None:
        if not self.order_id and not self.client_order_id:
            raise ValueError("order_id or client_order_id is required")
        if self.new_quantity is None and self.new_price is None:
            raise ValueError("new_quantity or new_price is required")


@dataclass(frozen=True)
class OrderQuery:
    symbol: str
    order_id: str | None = None
    client_order_id: str | None = None

    def __post_init__(self) -> None:
        if not self.order_id and not self.client_order_id:
            raise ValueError("order_id or client_order_id is required")




@dataclass(frozen=True)
class StopOrderQuery:
    symbol: str
    stop_order_id: str | None = None
    client_order_id: str | None = None

    def __post_init__(self) -> None:
        if not self.stop_order_id and not self.client_order_id:
            raise ValueError("stop_order_id or client_order_id is required")


@dataclass(frozen=True)
class CancelStopOrderRequest:
    symbol: str
    stop_order_id: str | None = None
    client_order_id: str | None = None
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if not any(
            value is not None and bool(str(value).strip())
            for value in (self.stop_order_id, self.client_order_id)
        ):
            raise ValueError("stop_order_id or client_order_id is required")


@dataclass(frozen=True)
class LeverageRequest:
    symbol: str
    leverage: Decimal
    margin_mode: MarginMode = MarginMode.CROSS
    position_side: PositionSide | None = None

    def __post_init__(self) -> None:
        if self.leverage <= 0:
            raise ValueError("leverage must be positive")


@dataclass(frozen=True)
class LeverageInfo:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    leverage: Decimal | None
    margin_mode: MarginMode | None = None
    position_side: PositionSide | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class Order:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    order_id: str | None
    client_order_id: str | None
    status: OrderStatus
    side: OrderSide | None = None
    order_type: OrderType | None = None
    price: Decimal | None = None
    quantity: Decimal | None = None
    filled_quantity: Decimal | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)
def __getattr__(name: str):
    """Resolve import-only aliases without coupling model initialization."""

    if name not in {"Kline", "Ticker", "Trade"}:
        raise AttributeError(name)
    from src.platform.data.models import (
        MarketKline,
        MarketTicker,
        MarketTrade,
    )

    return {
        "Kline": MarketKline,
        "Ticker": MarketTicker,
        "Trade": MarketTrade,
    }[name]
