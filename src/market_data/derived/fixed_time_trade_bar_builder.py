from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.market_data.models import FixedTimeTradeBar, TradeFeatureQuality
from src.platform.data.models import MarketTrade, TradeSide

_ONE_MINUTE_MS = 60_000


@dataclass
class _ActiveTradeBar:
    symbol: str
    exchange: str
    open_time_ms: int
    close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("0")
    buy_volume: Decimal = Decimal("0")
    sell_volume: Decimal = Decimal("0")
    buy_notional: Decimal = Decimal("0")
    sell_notional: Decimal = Decimal("0")
    trade_count: int = 0
    large_buy_notional: Decimal = Decimal("0")
    large_sell_notional: Decimal = Decimal("0")
    large_trade_count: int = 0
    last_trade_time_ms: int = 0
    first_trade_time_ms: int = 0
    available_time_ms: int = 0
    invalid_trade_count: int = 0


class FixedTimeTradeBarBuilder:
    """Streaming 1m fixed-time trade bar builder.

    Aggregates normalized MarketTrade flows into 1-minute fixed bucket bars.
    Bars close when a trade from the NEXT bucket arrives. Out-of-order trades
    arriving after a bar is closed are dropped (not backfilled).

    The builder only holds the current active bucket and a small pending buffer
    of recently closed bars. It never accumulates long histories in memory.
    """

    _MAX_PENDING_CLOSED = 8

    def __init__(
        self,
        *,
        contract_value: Decimal | str | float = Decimal("1"),
        large_trade_threshold_notional: Decimal | str | float = Decimal("10000"),
    ) -> None:
        self.contract_value = Decimal(str(contract_value))
        self.large_threshold = Decimal(str(large_trade_threshold_notional))
        if self.contract_value <= 0:
            raise ValueError("contract_value must be positive")
        if self.large_threshold <= 0:
            raise ValueError("large_trade_threshold_notional must be positive")

        self._active: _ActiveTradeBar | None = None
        self._pending_closed: list[FixedTimeTradeBar] = []
        self._stats = _BuilderStats()

    # ------------------------------------------------------------------
    # Core feed method
    # ------------------------------------------------------------------

    def on_trade(self, trade: MarketTrade) -> tuple[FixedTimeTradeBar, ...]:
        """Feed a normalized trade; returns newly-closed bars, if any."""
        time_ms = _trade_time_ms(trade)
        if time_ms is None or time_ms <= 0:
            self._stats.invalid += 1
            return ()

        if not _trade_valid(trade):
            self._stats.invalid += 1
            return ()

        bucket_start = _bucket_start_ms(time_ms)

        if self._active is not None and bucket_start > self._active.open_time_ms:
            # The trade that triggers the close provides available_time_ms
            # for the bar being closed.
            self._active.available_time_ms = max(self._active.available_time_ms, time_ms)
            closed = self._close_active_bar()
            self._flush_closed(closed)
            result = tuple(self._drain_pending())
            self._active = self._new_bar(
                symbol=trade.symbol,
                exchange=trade.exchange.value if hasattr(trade.exchange, "value") else str(trade.exchange),
                bucket_start=bucket_start,
            )
            self._add_trade(self._active, trade, time_ms)
            return result

        if self._active is None:
            self._active = self._new_bar(
                symbol=trade.symbol,
                exchange=trade.exchange.value if hasattr(trade.exchange, "value") else str(trade.exchange),
                bucket_start=bucket_start,
            )

        if bucket_start < self._active.open_time_ms:
            self._stats.out_of_order += 1
            return ()

        self._add_trade(self._active, trade, time_ms)
        return ()

    def drain(self) -> tuple[FixedTimeTradeBar, ...]:
        """Force-close active bar and return any pending closed bars."""
        result: list[FixedTimeTradeBar] = list(self._drain_pending())
        if self._active is not None and self._active.trade_count > 0:
            # Force available_time_ms to at least close_time_ms
            self._active.available_time_ms = max(
                self._active.available_time_ms, self._active.close_time_ms
            )
            closed = self._close_active_bar()
            self._flush_closed(closed)
            result.extend(self._drain_pending())
        return tuple(result)

    def snapshot_active(self) -> FixedTimeTradeBar | None:
        """Snapshot the current active bar without closing it."""
        if self._active is None or self._active.trade_count == 0:
            return None
        return self._bar_from_active(self._active)

    # ------------------------------------------------------------------
    # State snapshot / restore
    # ------------------------------------------------------------------

    def snapshot_state(self) -> Mapping[str, Any]:
        """Return a JSON-safe snapshot of builder state."""
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
                "volume": str(self._active.volume),
                "buy_volume": str(self._active.buy_volume),
                "sell_volume": str(self._active.sell_volume),
                "buy_notional": str(self._active.buy_notional),
                "sell_notional": str(self._active.sell_notional),
                "trade_count": self._active.trade_count,
                "large_buy_notional": str(self._active.large_buy_notional),
                "large_sell_notional": str(self._active.large_sell_notional),
                "large_trade_count": self._active.large_trade_count,
                "last_trade_time_ms": self._active.last_trade_time_ms,
                "first_trade_time_ms": self._active.first_trade_time_ms,
                "available_time_ms": self._active.available_time_ms,
                "invalid_trade_count": self._active.invalid_trade_count,
            }
        return {
            "version": 1,
            "contract_value": str(self.contract_value),
            "large_threshold": str(self.large_threshold),
            "active": active,
        }

    @classmethod
    def restore_state(cls, state: Mapping[str, Any]) -> "FixedTimeTradeBarBuilder":
        if int(state.get("version", 0)) != 1:
            raise ValueError("unsupported trade bar builder checkpoint version")
        builder = cls(
            contract_value=Decimal(str(state["contract_value"])),
            large_trade_threshold_notional=Decimal(str(state["large_threshold"])),
        )
        raw_active = state.get("active")
        if raw_active is not None:
            if not isinstance(raw_active, Mapping):
                raise ValueError("active state must be a mapping")
            builder._active = _ActiveTradeBar(
                symbol=str(raw_active["symbol"]),
                exchange=str(raw_active["exchange"]),
                open_time_ms=int(raw_active["open_time_ms"]),
                close_time_ms=int(raw_active["close_time_ms"]),
                open=Decimal(str(raw_active["open"])),
                high=Decimal(str(raw_active["high"])),
                low=Decimal(str(raw_active["low"])),
                close=Decimal(str(raw_active["close"])),
                volume=Decimal(str(raw_active["volume"])),
                buy_volume=Decimal(str(raw_active["buy_volume"])),
                sell_volume=Decimal(str(raw_active["sell_volume"])),
                buy_notional=Decimal(str(raw_active["buy_notional"])),
                sell_notional=Decimal(str(raw_active["sell_notional"])),
                trade_count=int(raw_active["trade_count"]),
                large_buy_notional=Decimal(str(raw_active["large_buy_notional"])),
                large_sell_notional=Decimal(str(raw_active["large_sell_notional"])),
                large_trade_count=int(raw_active["large_trade_count"]),
                last_trade_time_ms=int(raw_active["last_trade_time_ms"]),
                first_trade_time_ms=int(raw_active["first_trade_time_ms"]),
                available_time_ms=int(raw_active["available_time_ms"]),
                invalid_trade_count=int(raw_active["invalid_trade_count"]),
            )
        return builder

    @property
    def stats(self) -> Mapping[str, int]:
        return {
            "bars_closed": self._stats.bars_closed,
            "invalid_trades": self._stats.invalid,
            "out_of_order_trades": self._stats.out_of_order,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _new_bar(self, *, symbol: str, exchange: str, bucket_start: int) -> _ActiveTradeBar:
        return _ActiveTradeBar(
            symbol=symbol,
            exchange=exchange,
            open_time_ms=bucket_start,
            close_time_ms=bucket_start + _ONE_MINUTE_MS - 1,
            open=Decimal("0"),
            high=Decimal("0"),
            low=Decimal("0"),
            close=Decimal("0"),
        )

    def _add_trade(self, bar: _ActiveTradeBar, trade: MarketTrade, time_ms: int) -> None:
        price = trade.price
        quantity = trade.quantity
        notional = price * quantity * self.contract_value
        is_large = notional >= self.large_threshold

        if bar.trade_count == 0:
            bar.open = price
            bar.high = price
            bar.low = price
            bar.close = price
            bar.first_trade_time_ms = time_ms
        else:
            bar.high = max(bar.high, price)
            bar.low = min(bar.low, price)
            bar.close = price

        bar.volume += quantity
        bar.trade_count += 1
        bar.last_trade_time_ms = time_ms
        bar.available_time_ms = max(bar.available_time_ms, time_ms)

        if trade.side is TradeSide.BUY:
            bar.buy_volume += quantity
            bar.buy_notional += notional
            if is_large:
                bar.large_buy_notional += notional
                bar.large_trade_count += 1
        elif trade.side is TradeSide.SELL:
            bar.sell_volume += quantity
            bar.sell_notional += notional
            if is_large:
                bar.large_sell_notional += notional
                bar.large_trade_count += 1

    def _close_active_bar(self) -> FixedTimeTradeBar:
        bar = self._bar_from_active(self._active)
        self._active = None
        self._stats.bars_closed += 1
        return bar

    def _bar_from_active(self, active: _ActiveTradeBar) -> FixedTimeTradeBar:
        delta_volume = active.buy_volume - active.sell_volume
        delta_notional = active.buy_notional - active.sell_notional
        quality = TradeFeatureQuality.COMPLETE.value
        if active.trade_count < 2:
            quality = TradeFeatureQuality.DEGRADED_LOW_TRADE_COUNT.value

        total_notional = active.buy_notional + active.sell_notional
        large_total = active.large_buy_notional + active.large_sell_notional
        large_share = (large_total / total_notional) if total_notional > 0 else Decimal("0")

        return FixedTimeTradeBar(
            exchange=active.exchange,
            symbol=active.symbol,
            timeframe="1m",
            open_time_ms=active.open_time_ms,
            close_time_ms=active.close_time_ms,
            available_time_ms=active.available_time_ms,
            open=active.open,
            high=active.high,
            low=active.low,
            close=active.close,
            volume=active.volume,
            buy_volume=active.buy_volume,
            sell_volume=active.sell_volume,
            buy_notional=active.buy_notional,
            sell_notional=active.sell_notional,
            delta_volume=delta_volume,
            delta_notional=delta_notional,
            abs_delta_notional=abs(delta_notional),
            trade_count=active.trade_count,
            large_buy_notional=active.large_buy_notional,
            large_sell_notional=active.large_sell_notional,
            large_trade_count=active.large_trade_count,
            large_trade_share=large_share,
            quality=quality,
            source="trade_derived",
        )

    def _flush_closed(self, bar: FixedTimeTradeBar) -> None:
        self._pending_closed.append(bar)
        while len(self._pending_closed) > self._MAX_PENDING_CLOSED:
            self._pending_closed.pop(0)

    def _drain_pending(self) -> list[FixedTimeTradeBar]:
        result = list(self._pending_closed)
        self._pending_closed.clear()
        return result


@dataclass
class _BuilderStats:
    bars_closed: int = 0
    invalid: int = 0
    out_of_order: int = 0


def _trade_time_ms(trade: MarketTrade) -> int | None:
    if trade.trade_time_ms is not None:
        return trade.trade_time_ms
    return trade.event_time_ms


def _bucket_start_ms(time_ms: int) -> int:
    return (time_ms // _ONE_MINUTE_MS) * _ONE_MINUTE_MS


def _trade_valid(trade: MarketTrade) -> bool:
    return trade.price > 0 and trade.quantity > 0
