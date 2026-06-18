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
    """Runtime config passed into exchange adapters.

    ``from_env()`` loads ``.env`` first and overlays process environment.
    Credential names are intentionally strict to avoid preserving old typo-based
    fallbacks:

    - OKX: ``OKX_API_KEY``, ``OKX_SECRET_KEY``, ``OKX_PASSPHRASE``
    - Binance USD-M: ``BINANCE_API_KEY``, ``BINANCE_SECRET_KEY``
    """

    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""  # OKX only.
    sandbox: bool = False
    timeout_seconds: float = 10.0
    recv_window_ms: int = 5000  # Binance signed request window.
    live_trading_enabled: bool = False
    default_margin_mode: MarginMode = MarginMode.CROSS
    extra_headers: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(
        cls,
        exchange: ExchangeName | str,
        env: Mapping[str, str] | None = None,
    ) -> "ExchangeConfig":
        from src.platform.config import load_env_config

        values = load_env_config(environ=env) if env is not None else load_env_config()
        exchange_name = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())
        base = cls(
            sandbox=_bool_env(values.get(f"{exchange_name.value.upper()}_SANDBOX", values.get("SANDBOX", "false"))),
            timeout_seconds=float(values.get("API_TIMEOUT_SECONDS", "10.0") or 10.0),
            recv_window_ms=int(values.get("BINANCE_RECV_WINDOW_MS", "5000") or 5000),
            live_trading_enabled=_bool_env(values.get("AETHER_LIVE_TRADING", "false")),
            default_margin_mode=MarginMode(str(values.get("MARGIN_MODE", "cross")).strip().lower()),
        )
        if exchange_name == ExchangeName.OKX:
            from src.platform.exchanges.okx.credentials import resolve_okx_credentials

            api_key, api_secret, passphrase = resolve_okx_credentials(base, values)
            return cls(
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
            from src.platform.exchanges.binance.credentials import resolve_binance_credentials

            api_key, api_secret = resolve_binance_credentials(base, values)
            return cls(
                api_key=api_key,
                api_secret=api_secret,
                passphrase="",
                sandbox=base.sandbox,
                timeout_seconds=base.timeout_seconds,
                recv_window_ms=base.recv_window_ms,
                live_trading_enabled=base.live_trading_enabled,
                default_margin_mode=base.default_margin_mode,
            )
        return base


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
    is_closed: bool = True
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

    def __post_init__(self) -> None:
        if not self.stop_order_id and not self.client_order_id:
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


def _bool_env(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
