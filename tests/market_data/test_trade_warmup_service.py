from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.models import MarketDataSet, TimeRange, WarmupRequest
from src.market_data.storage import SqliteTradeStore
from src.market_data.warmup import TradeWarmupService
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName


def _trade(time_ms: int) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("2000"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id=str(time_ms),
        event_time_ms=time_ms,
        trade_time_ms=time_ms,
    )


class FakeHistoricalTradeFeed:
    def __init__(self, rows: list[MarketTrade]) -> None:
        self.rows = rows
        self.calls: list[tuple[int, int, int]] = []

    async def fetch_trades(self, *, symbol: str, start_time_ms: int, end_time_ms: int, limit: int = 1000, oldest_first: bool = True) -> list[MarketTrade]:
        self.calls.append((start_time_ms, end_time_ms, limit))
        rows = [row for row in self.rows if row.symbol == symbol and row.trade_time_ms is not None and start_time_ms <= row.trade_time_ms <= end_time_ms]
        return rows[:limit]


@pytest.mark.asyncio
async def test_trade_warmup_service_fetches_missing_coverage_and_marks_it(tmp_path):
    store = SqliteTradeStore(tmp_path / "market.sqlite3")
    feed = FakeHistoricalTradeFeed([_trade(1000), _trade(2000), _trade(3000)])
    service = TradeWarmupService(data_feed=feed, repository=store, coverage_repository=store, batch_limit=2)

    request = WarmupRequest(symbol="ETH-USDT-PERP", dataset=MarketDataSet.TRADES, time_range=TimeRange(1000, 3000))
    result = await service.warmup(request)

    assert result.records_loaded == 3
    assert result.caught_up is True
    assert result.gaps_after == ()
    assert [row.trade_time_ms for row in store.load(symbol="ETH-USDT-PERP", time_range=TimeRange(1000, 3000))] == [1000, 2000, 3000]
    assert feed.calls == [(1000, 3000, 2), (2001, 3000, 2)]

    second = await service.warmup(request)
    assert second.records_loaded == 0
    assert len(feed.calls) == 2


@pytest.mark.asyncio
async def test_trade_warmup_service_rejects_non_trade_dataset(tmp_path):
    store = SqliteTradeStore(tmp_path / "market.sqlite3")
    service = TradeWarmupService(data_feed=FakeHistoricalTradeFeed([]), repository=store, coverage_repository=store)

    with pytest.raises(ValueError):
        await service.warmup(WarmupRequest(symbol="ETH-USDT-PERP", dataset=MarketDataSet.KLINES, time_range=TimeRange(0, 1), interval="4H"))
