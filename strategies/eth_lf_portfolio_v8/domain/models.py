from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


class Side(int, Enum):
    SHORT = -1
    FLAT = 0
    LONG = 1


class V8DecisionType(str, Enum):
    NONE = "none"
    OPEN = "open"
    ADD = "add"
    CLOSE = "close"
    PLACE_STOP = "place_stop"


@dataclass(frozen=True)
class ClosedKlineContext:
    symbol: str
    exchange: str
    timeframe: str
    open_time_ms: int
    close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal | None = None


@dataclass(frozen=True)
class RangeAggregateContext:
    symbol: str
    exchange: str
    timeframe: str
    bucket_start_ms: int
    bucket_end_ms: int
    range_pct: Decimal
    bar_count: int
    first_open: Decimal
    last_close: Decimal
    high: Decimal
    low: Decimal
    buy_notional_sum: Decimal
    sell_notional_sum: Decimal
    delta_notional_sum: Decimal
    notional_sum: Decimal
    micro_return_pct: Decimal
    imbalance: Decimal
    taker_buy_ratio: Decimal
    close_pos: Decimal


@dataclass(frozen=True)
class MicroDecision:
    signal_side: Side
    context_available: bool
    aligned: bool
    contra: bool
    entry_risk_scale: Decimal
    action: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EngineSignal:
    """One LF engine's directional vote before portfolio routing."""

    side: Side
    engine: str
    priority: int
    risk_mult: Decimal = Decimal("1")
    quality_mult: Decimal = Decimal("1")
    reason: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RoutedSignal:
    side: Side
    engine: str
    priority: int
    risk_mult: Decimal = Decimal("1")
    quality_mult: Decimal = Decimal("1")
    reason: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def flat(cls) -> "RoutedSignal":
        return cls(side=Side.FLAT, engine="none", priority=0)


@dataclass(frozen=True)
class BarReadyContext:
    kline: ClosedKlineContext
    range_aggregate: RangeAggregateContext | None
    micro: MicroDecision
    global_risk_scale: Decimal
    routed_signal: RoutedSignal = field(default_factory=RoutedSignal.flat)

    @property
    def final_entry_risk_scale(self) -> Decimal:
        return self.micro.entry_risk_scale * self.global_risk_scale


@dataclass(frozen=True)
class V8TradeDecision:
    """Strategy-internal decision before mapping to AetherEdge TradeSignal.

    Quantity is always base-asset quantity. Exchange-specific native quantity
    conversion remains in ``src/order_management``.
    """

    decision_type: V8DecisionType
    side: Side
    symbol: str
    quantity: Decimal | None = None
    stop_price: Decimal | None = None
    engine: str = "none"
    reason: str = ""
    bar_close_time_ms: int | None = None
    entry_risk_scale: Decimal = Decimal("1")
    risk_mult: Decimal = Decimal("1")
    quality_mult: Decimal = Decimal("1")
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def none(cls, *, symbol: str, reason: str = "") -> "V8TradeDecision":
        return cls(decision_type=V8DecisionType.NONE, side=Side.FLAT, symbol=symbol, reason=reason)
