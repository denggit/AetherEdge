from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_EVEN
from typing import Mapping

from src.market_data.models import RangeFootprintFeature, TradeFeatureQuality
from src.platform.data.models import MarketTrade, TradeSide

_BAR_ID_MULT = 1_000_000


@dataclass
class _PriceBucket:
    buy_notional: Decimal = Decimal("0")
    sell_notional: Decimal = Decimal("0")


@dataclass
class _ActiveRangeFootprint:
    exchange: str
    symbol: str
    range_pct: Decimal
    price_step: Decimal
    range_bar_id: int
    range_start_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    total_notional: Decimal = Decimal("0")
    buy_notional: Decimal = Decimal("0")
    sell_notional: Decimal = Decimal("0")
    trade_count: int = 0
    last_time_ms: int | None = None
    price_buckets: dict[Decimal, _PriceBucket] = field(default_factory=dict)


@dataclass
class _BuilderStats:
    features_closed: int = 0
    invalid_trades: int = 0
    out_of_order_trades: int = 0


class RangeFootprintBuilder:
    """Stream closed range-bar footprint context from normalized trades.

    The closing trade belongs to the range bar. Price buckets use
    ``floor(price / price_step) * price_step``. Only threshold-closed range
    bars produce a feature; an active bar is never promoted by a time
    watermark.
    """

    def __init__(
        self,
        *,
        range_pct: Decimal | str | float = Decimal("0.002"),
        price_step: Decimal | str | float = Decimal("1"),
        contract_value: Decimal | str | float = Decimal("1"),
    ) -> None:
        self.range_pct = Decimal(str(range_pct))
        self.price_step = Decimal(str(price_step))
        self.contract_value = Decimal(str(contract_value))
        if self.range_pct <= 0:
            raise ValueError("range_pct must be positive")
        if self.price_step <= 0:
            raise ValueError("price_step must be positive")
        if self.contract_value <= 0:
            raise ValueError("contract_value must be positive")
        self._active: _ActiveRangeFootprint | None = None
        self._day_seq: dict[str, int] = {}
        self._stats = _BuilderStats()

    def on_trade(self, trade: MarketTrade) -> tuple[RangeFootprintFeature, ...]:
        time_ms = _trade_time_ms(trade)
        if time_ms is None or time_ms < 0 or trade.price <= 0 or trade.quantity <= 0:
            self._stats.invalid_trades += 1
            return ()
        if self._active is not None and self._active.last_time_ms is not None:
            if time_ms < self._active.last_time_ms:
                self._stats.out_of_order_trades += 1
                return ()
        if self._active is None:
            self._active = self._new_active(trade=trade, time_ms=time_ms)

        self._add_trade(self._active, trade=trade, time_ms=time_ms)
        if not self._should_close(self._active, trade.price):
            return ()

        feature = self._close(self._active)
        self._active = None
        self._stats.features_closed += 1
        return (feature,)

    def discard_active(self) -> None:
        """Drop an incomplete range without emitting a COMPLETE feature."""
        self._active = None

    @property
    def has_active_range(self) -> bool:
        return self._active is not None

    @property
    def stats(self) -> Mapping[str, int]:
        return {
            "features_closed": self._stats.features_closed,
            "invalid_trades": self._stats.invalid_trades,
            "out_of_order_trades": self._stats.out_of_order_trades,
        }

    def _new_active(
        self, *, trade: MarketTrade, time_ms: int
    ) -> _ActiveRangeFootprint:
        day = datetime.fromtimestamp(time_ms / 1000, tz=UTC).strftime("%Y%m%d")
        seq = self._day_seq.get(day, 0) + 1
        self._day_seq[day] = seq
        exchange = (
            trade.exchange.value
            if hasattr(trade.exchange, "value")
            else str(trade.exchange)
        )
        return _ActiveRangeFootprint(
            exchange=exchange,
            symbol=trade.symbol,
            range_pct=self.range_pct,
            price_step=self.price_step,
            range_bar_id=int(day) * _BAR_ID_MULT + seq,
            range_start_ms=time_ms,
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
        )

    def _add_trade(
        self,
        active: _ActiveRangeFootprint,
        *,
        trade: MarketTrade,
        time_ms: int,
    ) -> None:
        price = trade.price
        notional = price * trade.quantity * self.contract_value
        active.high = max(active.high, price)
        active.low = min(active.low, price)
        active.close = price
        active.total_notional += notional
        active.trade_count += 1
        active.last_time_ms = time_ms

        bucket_price = _price_bucket(price, self.price_step)
        bucket = active.price_buckets.setdefault(bucket_price, _PriceBucket())
        if trade.side is TradeSide.BUY:
            active.buy_notional += notional
            bucket.buy_notional += notional
        elif trade.side is TradeSide.SELL:
            active.sell_notional += notional
            bucket.sell_notional += notional

    def _should_close(
        self, active: _ActiveRangeFootprint, price: Decimal
    ) -> bool:
        upper = active.open * (Decimal("1") + self.range_pct)
        lower = active.open * (Decimal("1") - self.range_pct)
        return price >= upper or price <= lower

    def _close(self, active: _ActiveRangeFootprint) -> RangeFootprintFeature:
        if active.last_time_ms is None:
            raise RuntimeError("cannot close a range footprint without trades")

        ordered = sorted(active.price_buckets.items())
        pressures = [
            pressure
            for _, bucket in ordered
            if (
                pressure := _bucket_delta_pressure(bucket)
            )
            is not None
        ]
        # CoinBacktest's pandas groupby first/last aggregation skips null
        # bucket pressures, so use the first/last computable sorted bucket.
        low_pressure = pressures[0] if pressures else None
        high_pressure = pressures[-1] if pressures else None
        total_delta = active.buy_notional - active.sell_notional
        total_pressure = (
            total_delta / active.total_notional
            if active.total_notional > 0
            else None
        )
        context_available = bool(
            pressures
            and low_pressure is not None
            and high_pressure is not None
            and total_pressure is not None
        )
        quality = (
            TradeFeatureQuality.COMPLETE.value
            if context_available
            else TradeFeatureQuality.MISSING_FOOTPRINT_CONTEXT.value
        )
        return RangeFootprintFeature(
            exchange=active.exchange,
            symbol=active.symbol,
            range_pct=active.range_pct,
            price_step=active.price_step,
            range_bar_id=active.range_bar_id,
            range_start_ms=active.range_start_ms,
            range_end_ms=active.last_time_ms,
            available_time_ms=active.last_time_ms,
            fp_max_bucket_abs_delta_pressure=(
                max(abs(value) for value in pressures)
                if pressures
                else Decimal("0")
            ),
            fp_low_bucket_delta_pressure=low_pressure or Decimal("0"),
            fp_high_bucket_delta_pressure=high_pressure or Decimal("0"),
            fp_delta_pressure=total_pressure or Decimal("0"),
            bucket_count=len(active.price_buckets),
            trade_count=active.trade_count,
            context_available=context_available,
            quality=quality,
        )


def _bucket_delta_pressure(bucket: _PriceBucket) -> Decimal | None:
    denominator = bucket.buy_notional + bucket.sell_notional
    if denominator <= 0:
        return None
    return (bucket.buy_notional - bucket.sell_notional) / denominator


def _price_bucket(price: Decimal, price_step: Decimal) -> Decimal:
    index = (price / price_step).to_integral_value(rounding=ROUND_FLOOR)
    return (index * price_step).quantize(
        Decimal("0.00000001"),
        rounding=ROUND_HALF_EVEN,
    )


def _trade_time_ms(trade: MarketTrade) -> int | None:
    if trade.trade_time_ms is not None:
        return trade.trade_time_ms
    return trade.event_time_ms
