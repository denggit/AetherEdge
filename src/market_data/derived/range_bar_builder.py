from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from src.market_data.models import RangeBar
from src.platform.data.models import MarketTrade, TradeSide

_BAR_ID_MULT = 1_000_000


@dataclass
class _ActiveRangeBar:
    symbol: str
    range_pct: Decimal
    bar_id: int
    start_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("0")
    buy_notional: Decimal = Decimal("0")
    sell_notional: Decimal = Decimal("0")
    trade_count: int = 0
    last_time_ms: int | None = None


class RangeBarBuilder:
    """Streaming range-bar builder aligned with CoinBacktest's close rule.

    A bar starts on the first trade price and closes after the trade is added
    when price moves at least ``range_pct`` away from the bar open. The closing
    trade belongs to the closed bar, matching the CoinBacktest range-bar
    prebuild logic.
    """

    def __init__(self, *, range_pct: Decimal | str | float, contract_value: Decimal | str | float) -> None:
        self.range_pct = Decimal(str(range_pct))
        self.contract_value = Decimal(str(contract_value))
        if self.range_pct <= 0:
            raise ValueError("range_pct must be positive")
        if self.contract_value <= 0:
            raise ValueError("contract_value must be positive")
        self.active: _ActiveRangeBar | None = None
        self._day_seq: dict[str, int] = {}

    def on_trade(self, trade: MarketTrade) -> tuple[RangeBar, ...]:
        time_ms = _trade_time_ms(trade)
        if time_ms is None or trade.price <= 0 or trade.quantity <= 0:
            return ()
        if self.active is None:
            self.active = self._new_bar(symbol=trade.symbol, time_ms=time_ms, price=trade.price)
        self._add_trade(self.active, trade=trade, time_ms=time_ms)
        if self._should_close(self.active, trade.price):
            closed = self._close_bar(self.active, end_time_ms=time_ms)
            self.active = None
            return (closed,)
        return ()

    def snapshot_open_bar(self) -> RangeBar | None:
        if self.active is None or self.active.last_time_ms is None:
            return None
        return self._close_bar(self.active, end_time_ms=self.active.last_time_ms)

    def seed_from_bars(self, bars) -> None:
        """Seed per-day sequence counters from persisted bars after restart."""
        for bar in bars:
            day = str(int(bar.bar_id) // _BAR_ID_MULT)
            seq = int(bar.bar_id) % _BAR_ID_MULT
            self._day_seq[day] = max(self._day_seq.get(day, 0), seq)

    def _new_bar(self, *, symbol: str, time_ms: int, price: Decimal) -> _ActiveRangeBar:
        day = datetime.fromtimestamp(time_ms / 1000, tz=UTC).strftime("%Y%m%d")
        seq = self._day_seq.get(day, 0) + 1
        self._day_seq[day] = seq
        return _ActiveRangeBar(
            symbol=symbol,
            range_pct=self.range_pct,
            bar_id=int(day) * _BAR_ID_MULT + seq,
            start_time_ms=time_ms,
            open=price,
            high=price,
            low=price,
            close=price,
        )

    def _add_trade(self, bar: _ActiveRangeBar, *, trade: MarketTrade, time_ms: int) -> None:
        price = trade.price
        quantity = trade.quantity
        notional = price * quantity * self.contract_value
        bar.high = max(bar.high, price)
        bar.low = min(bar.low, price)
        bar.close = price
        bar.volume += quantity
        bar.trade_count += 1
        bar.last_time_ms = time_ms
        if trade.side is TradeSide.BUY:
            bar.buy_notional += notional
        elif trade.side is TradeSide.SELL:
            bar.sell_notional += notional

    def _should_close(self, bar: _ActiveRangeBar, price: Decimal) -> bool:
        up = bar.open * (Decimal("1") + self.range_pct)
        down = bar.open * (Decimal("1") - self.range_pct)
        return price >= up or price <= down

    def _close_bar(self, bar: _ActiveRangeBar, *, end_time_ms: int) -> RangeBar:
        return RangeBar(
            symbol=bar.symbol,
            range_pct=bar.range_pct,
            bar_id=bar.bar_id,
            start_time_ms=bar.start_time_ms,
            end_time_ms=end_time_ms,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            buy_notional=bar.buy_notional,
            sell_notional=bar.sell_notional,
            trade_count=bar.trade_count,
        )


def _trade_time_ms(trade: MarketTrade) -> int | None:
    if trade.trade_time_ms is not None:
        return trade.trade_time_ms
    return trade.event_time_ms
