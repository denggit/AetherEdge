from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.models import MarketDataSet, TimeRange, WarmupRequest
from src.market_data.storage import SqliteKlineStore
from src.market_data.warmup import KlineWarmupService
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker
from src.platform.exchanges.models import ExchangeName
from src.platform.markets import MarketProfile

STEP = 4 * 60 * 60_000


def _kline(open_time_ms: int) -> MarketKline:
    return MarketKline(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        interval="4H",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + STEP - 1,
        open=Decimal("1000"),
        high=Decimal("1010"),
        low=Decimal("990"),
        close=Decimal("1005"),
        volume=Decimal("10"),
        is_closed=True,
    )


class FakeMarketDataFeed:
    def __init__(self, rows: list[MarketKline]) -> None:
        self.rows = rows
        self.calls: list[tuple[int | None, int | None, int]] = []

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.OKX

    @property
    def symbol(self) -> str:
        return "ETH-USDT-PERP"

    @property
    def market_profile(self) -> MarketProfile:  # pragma: no cover - not used by warmup
        raise NotImplementedError

    async def fetch_klines(self, *, interval: str, limit: int = 100, start_time_ms: int | None = None, end_time_ms: int | None = None, use_cache: bool = True, oldest_first: bool = False) -> list[MarketKline]:
        self.calls.append((start_time_ms, end_time_ms, limit))
        rows = [row for row in self.rows if row.interval == interval]
        if start_time_ms is not None:
            rows = [row for row in rows if row.open_time_ms >= start_time_ms]
        if end_time_ms is not None:
            rows = [row for row in rows if row.open_time_ms <= end_time_ms]
        rows = sorted(rows, key=lambda row: row.open_time_ms)
        return rows[:limit]

    async def fetch_ticker(self) -> MarketTicker:  # pragma: no cover - not used by warmup
        raise NotImplementedError

    def stream_trades(self):  # pragma: no cover - not used by warmup
        raise NotImplementedError

    def stream_order_book(self):  # pragma: no cover - not used by warmup
        raise NotImplementedError

    def stream_events(self):  # pragma: no cover - not used by warmup
        raise NotImplementedError


class LatestPageMarketDataFeed(FakeMarketDataFeed):
    async def fetch_klines(self, *, interval: str, limit: int = 100, start_time_ms: int | None = None, end_time_ms: int | None = None, use_cache: bool = True, oldest_first: bool = False) -> list[MarketKline]:
        self.calls.append((start_time_ms, end_time_ms, limit))
        rows = [row for row in self.rows if row.interval == interval]
        if start_time_ms is not None:
            rows = [row for row in rows if row.open_time_ms >= start_time_ms]
        if end_time_ms is not None:
            rows = [row for row in rows if row.open_time_ms <= end_time_ms]
        rows = sorted(rows, key=lambda row: row.open_time_ms, reverse=True)[:limit]
        if oldest_first:
            rows.reverse()
        return rows


@pytest.mark.asyncio
async def test_kline_warmup_service_backfills_only_missing_klines(tmp_path):
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    store.save([_kline(0), _kline(STEP * 2)])
    feed = FakeMarketDataFeed([_kline(STEP), _kline(STEP * 3)])
    service = KlineWarmupService(data_feed=feed, repository=store, batch_limit=10)

    result = await service.warmup(WarmupRequest(symbol="ETH-USDT-PERP", dataset=MarketDataSet.KLINES, time_range=TimeRange(0, STEP * 3), interval="4H"))

    assert result.records_loaded == 2
    assert result.caught_up is True
    assert result.gaps_after == ()
    rows = store.load(symbol="ETH-USDT-PERP", interval="4H", time_range=TimeRange(0, STEP * 3))
    assert [row.open_time_ms for row in rows] == [0, STEP, STEP * 2, STEP * 3]
    assert feed.calls == [(STEP, STEP, 10), (STEP * 3, STEP * 3, 10)]


@pytest.mark.asyncio
async def test_kline_warmup_service_keeps_backfilling_until_gap_is_closed(tmp_path):
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    rows = [_kline(STEP * index) for index in range(5)]
    feed = LatestPageMarketDataFeed(rows)
    service = KlineWarmupService(data_feed=feed, repository=store, batch_limit=2)

    result = await service.warmup(WarmupRequest(symbol="ETH-USDT-PERP", dataset=MarketDataSet.KLINES, time_range=TimeRange(0, STEP * 4), interval="4H"))

    assert result.records_loaded == 5
    assert result.caught_up is True
    assert result.gaps_after == ()
    saved = store.load(symbol="ETH-USDT-PERP", interval="4H", time_range=TimeRange(0, STEP * 4))
    assert [row.open_time_ms for row in saved] == [0, STEP, STEP * 2, STEP * 3, STEP * 4]
    assert feed.calls == [
        (0, STEP * 4, 2),
        (0, STEP * 2, 2),
        (0, 0, 2),
    ]


@pytest.mark.asyncio
async def test_kline_warmup_service_reports_remaining_gap_when_feed_has_no_data(tmp_path):
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    feed = FakeMarketDataFeed([])
    service = KlineWarmupService(data_feed=feed, repository=store)

    result = await service.warmup(WarmupRequest(symbol="ETH-USDT-PERP", dataset=MarketDataSet.KLINES, time_range=TimeRange(0, STEP), interval="4H"))

    assert result.records_loaded == 0
    assert result.caught_up is False
    assert len(result.gaps_after) == 1


@pytest.mark.asyncio
async def test_kline_warmup_service_rejects_non_kline_dataset(tmp_path):
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    feed = FakeMarketDataFeed([])
    service = KlineWarmupService(data_feed=feed, repository=store)

    with pytest.raises(ValueError):
        await service.warmup(WarmupRequest(symbol="ETH-USDT-PERP", dataset=MarketDataSet.TRADES, time_range=TimeRange(0, STEP)))
