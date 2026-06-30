from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.models import TimeRange
from src.market_data.storage import SqliteRangeBarStore, SqliteTradeStore
from src.market_data.warmup.current_rangebar import CurrentRangeBarWarmupService
from src.platform import ExchangeName
from src.platform.data.models import MarketTrade, TradeSide


class FakeHistoricalTradeFeed:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = 0

    async def fetch_trades(self, *, symbol: str, start_time_ms: int, end_time_ms: int, limit: int = 1000, oldest_first: bool = True):
        self.calls += 1
        rows = [row for row in self.rows if row.symbol == symbol and start_time_ms <= (row.trade_time_ms or 0) <= end_time_ms]
        return rows[:limit]


def _trade(price: str, time_ms: int, *, side: TradeSide = TradeSide.BUY) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal(price),
        quantity=Decimal("1"),
        side=side,
        trade_id=str(time_ms),
        trade_time_ms=time_ms,
        event_time_ms=time_ms,
    )


@pytest.mark.asyncio
async def test_current_rangebar_warmup_downloads_persists_and_reuses_coverage(tmp_path) -> None:
    trades = [_trade("100", 1_000), _trade("100.2", 2_000, side=TradeSide.SELL)]
    feed = FakeHistoricalTradeFeed(trades)
    trade_store = SqliteTradeStore(
        tmp_path / "market.sqlite3",
        save_raw_trades=True,
    )
    range_store = SqliteRangeBarStore(tmp_path / "market.sqlite3")
    service = CurrentRangeBarWarmupService(
        trade_repository=trade_store,
        trade_coverage_repository=trade_store,
        range_bar_repository=range_store,
        historical_trade_feed=feed,
        range_pct=Decimal("0.002"),
        contract_value=Decimal("0.1"),
        batch_limit=1000,
    )

    result = await service.warmup(symbol="ETH-USDT-PERP", time_range=TimeRange(1_000, 3_000))
    second = await service.warmup(symbol="ETH-USDT-PERP", time_range=TimeRange(1_000, 3_000))

    assert result.caught_up is True
    assert result.trades_loaded == 2
    assert result.trades_available == 2
    assert result.range_bars_saved == 1
    assert second.trades_loaded == 0
    assert second.trades_available == 2
    assert feed.calls == 1
    bars = range_store.load(symbol="ETH-USDT-PERP", range_pct="0.002", time_range=TimeRange(1_000, 3_000))
    assert len(bars) == 1
    assert bars[0].buy_notional == Decimal("10.0")
    assert bars[0].sell_notional == Decimal("10.02")


def _range_bar(bar_id: int, start_ms: int, end_ms: int) -> object:
    from src.market_data.models import RangeBar

    return RangeBar(
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        bar_id=bar_id,
        start_time_ms=start_ms,
        end_time_ms=end_ms,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        buy_notional=Decimal("100"),
        sell_notional=Decimal("0"),
        trade_count=1,
    )


def test_replace_range_removes_stale_rebuilt_bucket_bars(tmp_path) -> None:
    store = SqliteRangeBarStore(tmp_path / "market.sqlite3")
    time_range = TimeRange(1_000, 4_000)
    store.save([
        _range_bar(1, 1_000, 2_000),
        _range_bar(2, 2_001, 3_000),
        _range_bar(3, 3_001, 3_500),
    ])

    saved = store.replace_range(
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        time_range=time_range,
        rows=[_range_bar(1, 1_000, 2_500)],
    )

    assert saved == 1
    rows = store.load(symbol="ETH-USDT-PERP", range_pct="0.002", time_range=time_range)
    assert [row.bar_id for row in rows] == [1]
    assert rows[0].end_time_ms == 2_500
