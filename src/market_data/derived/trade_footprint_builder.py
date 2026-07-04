from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping

from src.market_data.models import TradeFootprintFeature, TradeFeatureQuality
from src.platform.data.models import MarketTrade, TradeSide


@dataclass
class _FootprintAccum:
    symbol: str = ""
    exchange: str = ""
    open_time_ms: int = 0
    close_time_ms: int = 0
    open: Decimal = Decimal("0")
    high: Decimal = Decimal("0")
    low: Decimal = Decimal("0")
    close: Decimal = Decimal("0")
    buy_notional: Decimal = Decimal("0")
    sell_notional: Decimal = Decimal("0")
    trade_count: int = 0
    available_time_ms: int = 0
    context_available: bool = True


class TradeFootprintBuilder:
    """Streaming trade footprint feature builder.

    Consumes normalized MarketTrade flows and produces TradeFootprintFeature
    for closed 1m buckets. Footprint features are derived from the same trade
    stream as FixedTimeTradeBar — never from OHLCV.

    For now ``fp_max_bucket_abs_delta_pressure`` is not computed (requires
    sub-bucket context). When unavailable the field stays 0 and quality is
    marked MISSING_FOOTPRINT_CONTEXT.
    """

    _MAX_PENDING_CLOSED = 8

    def __init__(self) -> None:
        self._active: _FootprintAccum | None = None
        self._pending_closed: list[TradeFootprintFeature] = []
        self._stats = _FootprintStats()

    # ------------------------------------------------------------------
    # Core feed
    # ------------------------------------------------------------------

    def on_trade(
        self,
        trade: MarketTrade,
        *,
        trade_bar_open: Decimal = Decimal("0"),
        trade_bar_high: Decimal = Decimal("0"),
        trade_bar_low: Decimal = Decimal("0"),
        trade_bar_close: Decimal = Decimal("0"),
    ) -> tuple[TradeFootprintFeature, ...]:
        """Feed a normalized trade with its parent 1m bar context."""
        time_ms = _trade_time_ms(trade)
        if time_ms is None or time_ms <= 0:
            self._stats.invalid += 1
            return ()
        if trade.price <= 0 or trade.quantity <= 0:
            self._stats.invalid += 1
            return ()

        bucket_start = _bucket_start_ms(time_ms)
        exchange = trade.exchange.value if hasattr(trade.exchange, "value") else str(trade.exchange)

        if self._active is not None and bucket_start > self._active.open_time_ms:
            self._active.available_time_ms = max(self._active.available_time_ms, time_ms)
            closed = self._close()
            self._flush(closed)
            result = tuple(self._drain())
            self._active = self._new_accum(
                symbol=trade.symbol,
                exchange=exchange,
                bucket_start=bucket_start,
                trade=trade,
            )
            self._add_trade(self._active, trade, time_ms)
            return result

        if self._active is None:
            self._active = self._new_accum(
                symbol=trade.symbol,
                exchange=exchange,
                bucket_start=bucket_start,
                trade=trade,
            )
            self._add_trade(self._active, trade, time_ms)
            return ()

        if bucket_start < self._active.open_time_ms:
            self._stats.out_of_order += 1
            return ()

        # Update running OHLCV from trade-bar context
        if trade_bar_open > 0:
            if self._active.trade_count == 0:
                self._active.open = trade_bar_open
            self._active.high = trade_bar_high if trade_bar_high > 0 else self._active.high
            self._active.low = trade_bar_low if trade_bar_low > 0 else self._active.low
            self._active.close = trade_bar_close if trade_bar_close > 0 else self._active.close

        self._add_trade(self._active, trade, time_ms)
        return ()

    def drain(self) -> tuple[TradeFootprintFeature, ...]:
        result: list[TradeFootprintFeature] = list(self._drain())
        if self._active is not None and self._active.trade_count > 0:
            self._active.available_time_ms = max(
                self._active.available_time_ms, self._active.close_time_ms
            )
            closed = self._close()
            self._flush(closed)
            result.extend(self._drain())
        return tuple(result)

    # ------------------------------------------------------------------
    # Snapshot / restore
    # ------------------------------------------------------------------

    def snapshot_state(self) -> Mapping[str, Any]:
        active = None
        if self._active is not None:
            active = {
                "symbol": self._active.symbol,
                "exchange": self._active.exchange,
                "open_time_ms": self._active.open_time_ms,
                "close_time_ms": self._active.close_time_ms,
                "open": str(self._active.open),
                "high": str(self._active.high),
                "low": str(self._active.low),
                "close": str(self._active.close),
                "buy_notional": str(self._active.buy_notional),
                "sell_notional": str(self._active.sell_notional),
                "trade_count": self._active.trade_count,
                "available_time_ms": self._active.available_time_ms,
                "context_available": self._active.context_available,
            }
        return {"version": 1, "active": active}

    @classmethod
    def restore_state(cls, state: Mapping[str, Any]) -> "TradeFootprintBuilder":
        if int(state.get("version", 0)) != 1:
            raise ValueError("unsupported footprint builder checkpoint version")
        builder = cls()
        raw = state.get("active")
        if raw is not None:
            if not isinstance(raw, Mapping):
                raise ValueError("active footprint state must be a mapping")
            builder._active = _FootprintAccum(
                symbol=str(raw["symbol"]),
                exchange=str(raw["exchange"]),
                open_time_ms=int(raw["open_time_ms"]),
                close_time_ms=int(raw["close_time_ms"]),
                open=Decimal(str(raw["open"])),
                high=Decimal(str(raw["high"])),
                low=Decimal(str(raw["low"])),
                close=Decimal(str(raw["close"])),
                buy_notional=Decimal(str(raw["buy_notional"])),
                sell_notional=Decimal(str(raw["sell_notional"])),
                trade_count=int(raw["trade_count"]),
                available_time_ms=int(raw["available_time_ms"]),
                context_available=bool(raw.get("context_available", True)),
            )
        return builder

    @property
    def stats(self) -> Mapping[str, int]:
        return {
            "features_closed": self._stats.features_closed,
            "invalid_trades": self._stats.invalid,
            "out_of_order_trades": self._stats.out_of_order,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _new_accum(self, *, symbol: str, exchange: str, bucket_start: int, trade: MarketTrade) -> _FootprintAccum:
        return _FootprintAccum(
            symbol=symbol,
            exchange=exchange,
            open_time_ms=bucket_start,
            close_time_ms=bucket_start + 60_000 - 1,
        )

    def _add_trade(self, accum: _FootprintAccum, trade: MarketTrade, time_ms: int) -> None:
        notional = trade.price * trade.quantity
        if trade.side is TradeSide.BUY:
            accum.buy_notional += notional
        elif trade.side is TradeSide.SELL:
            accum.sell_notional += notional
        accum.trade_count += 1
        accum.available_time_ms = max(accum.available_time_ms, time_ms)

    def _close(self) -> TradeFootprintFeature:
        return self._feature_from_accum(self._active)

    def _feature_from_accum(self, accum: _FootprintAccum) -> TradeFootprintFeature:
        delta = accum.buy_notional - accum.sell_notional
        total = accum.buy_notional + accum.sell_notional
        taker_buy_ratio = (accum.buy_notional / total) if total > 0 else Decimal("0")

        span = accum.high - accum.low
        close_pos = Decimal("0.5")
        if span > 0:
            close_pos = (accum.close - accum.low) / span

        range_pct = Decimal("0")
        if accum.open > 0:
            range_pct = (accum.high - accum.low) / accum.open

        return_pct = Decimal("0")
        if accum.open > 0:
            return_pct = accum.close / accum.open - Decimal("1")

        # fp_max_bucket_abs_delta_pressure is not computed in R007
        quality = TradeFeatureQuality.COMPLETE.value
        context_available = True
        if not accum.context_available:
            quality = TradeFeatureQuality.MISSING_FOOTPRINT_CONTEXT.value
            context_available = False

        return TradeFootprintFeature(
            exchange=accum.exchange,
            symbol=accum.symbol,
            timeframe="1m",
            open_time_ms=accum.open_time_ms,
            close_time_ms=accum.close_time_ms,
            available_time_ms=accum.available_time_ms,
            delta_notional=delta,
            abs_delta_notional=abs(delta),
            taker_buy_ratio=taker_buy_ratio,
            close_pos=close_pos,
            range_pct=range_pct,
            return_pct=return_pct,
            fp_max_bucket_abs_delta_pressure=Decimal("0"),
            context_available=context_available,
            quality=quality,
            source="trade_derived",
        )

    def _flush(self, feature: TradeFootprintFeature) -> None:
        self._pending_closed.append(feature)
        while len(self._pending_closed) > self._MAX_PENDING_CLOSED:
            self._pending_closed.pop(0)

    def _drain(self) -> list[TradeFootprintFeature]:
        result = list(self._pending_closed)
        self._pending_closed.clear()
        return result


@dataclass
class _FootprintStats:
    features_closed: int = 0
    invalid: int = 0
    out_of_order: int = 0


def _trade_time_ms(trade: MarketTrade) -> int | None:
    if trade.trade_time_ms is not None:
        return trade.trade_time_ms
    return trade.event_time_ms


def _bucket_start_ms(time_ms: int) -> int:
    return (time_ms // 60_000) * 60_000
