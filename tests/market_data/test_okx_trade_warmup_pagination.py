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


class LateFullBatchFeed:
    async def fetch_trades(self, *, symbol: str, start_time_ms: int, end_time_ms: int, limit: int = 1000, oldest_first: bool = True) -> list[MarketTrade]:
        return [_trade(5000), _trade(6000)][:limit]


@pytest.mark.asyncio
async def test_okx_reverse_history_trades_does_not_mark_unfetched_early_gap_covered(tmp_path):
    store = SqliteTradeStore(
        tmp_path / "market.sqlite3",
        save_raw_trades=True,
    )
    service = TradeWarmupService(data_feed=LateFullBatchFeed(), repository=store, coverage_repository=store, batch_limit=2)

    request = WarmupRequest(symbol="ETH-USDT-PERP", dataset=MarketDataSet.TRADES, time_range=TimeRange(1000, 6000))
    result = await service.warmup(request)

    assert result.caught_up is False
    assert result.records_loaded == 2
    assert tuple((gap.time_range.start_time_ms, gap.time_range.end_time_ms) for gap in result.gaps_after) == ((1000, 4999),)
    assert [(row.start_time_ms, row.end_time_ms) for row in store.coverage_ranges(symbol="ETH-USDT-PERP", time_range=TimeRange(1000, 6000), source="historical")] == [(5000, 6000)]
