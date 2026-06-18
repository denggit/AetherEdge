from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


class ExchangeName(str, Enum):
    OKX = "okx"
    BINANCE = "binance"


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


class TimeInForce(str, Enum):
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    POST_ONLY = "post_only"


class MarginMode(str, Enum):
    CROSS = "cross"
    ISOLATED = "isolated"


@dataclass(frozen=True)
class ExchangeConfig:
    """Runtime config passed into exchange adapters.

    Keep secrets here instead of letting business modules read env variables.
    The adapter decides how to sign and map requests.
    """

    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""  # OKX only.
    sandbox: bool = False
    timeout_seconds: float = 10.0
    recv_window_ms: int = 5000  # Binance signed request window.
    default_margin_mode: MarginMode = MarginMode.CROSS
    extra_headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Kline:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Ticker:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    price: Decimal
    time_ms: int | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)


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
class CancelOrderRequest:
    symbol: str
    order_id: str | None = None
    client_order_id: str | None = None

    def __post_init__(self) -> None:
        if not self.order_id and not self.client_order_id:
            raise ValueError("order_id or client_order_id is required")


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
