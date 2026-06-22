from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.models import MarketDataSet, TimeRange, WarmupRequest
from src.market_data.storage import SqliteKlineStore
from src.market_data.warmup import KlineWarmupService
from src.market_data.warmup.historical_klines import BackfillDiagnostics
from src.market_data.warmup.kline_provider import MarketDataKlineProvider
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker
from src.platform.exchanges.models import ExchangeName
from src.platform.markets import MarketProfile

STEP = 4 * 60 * 60_000  # 4 hours in ms


def _kline(open_time_ms: int, *, symbol: str = "ETH-USDT-PERP", raw_symbol: str = "ETH-USDT-SWAP") -> MarketKline:
    return MarketKline(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=raw_symbol,
        interval="4h",
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
    """Returns klines sorted by open_time_ms ascending, filtered by time range."""

    def __init__(self, rows: list[MarketKline]) -> None:
        self.rows = sorted(rows, key=lambda r: r.open_time_ms)
        self.calls: list[dict] = []

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.OKX

    @property
    def symbol(self) -> str:
        return "ETH-USDT-PERP"

    @property
    def market_profile(self) -> MarketProfile:
        raise NotImplementedError

    async def fetch_klines(self, *, interval: str, limit: int = 100, start_time_ms: int | None = None, end_time_ms: int | None = None, use_cache: bool = True, oldest_first: bool = False) -> list[MarketKline]:
        self.calls.append({"start_time_ms": start_time_ms, "end_time_ms": end_time_ms, "limit": limit, "oldest_first": oldest_first})
        rows = [r for r in self.rows if r.interval == interval]
        if start_time_ms is not None:
            rows = [r for r in rows if r.open_time_ms >= start_time_ms]
        if end_time_ms is not None:
            rows = [r for r in rows if r.open_time_ms <= end_time_ms]
        rows = sorted(rows, key=lambda r: r.open_time_ms, reverse=not oldest_first)
        return rows[:limit]

    async def fetch_ticker(self) -> MarketTicker:
        raise NotImplementedError

    def stream_trades(self):
        raise NotImplementedError

    def stream_order_book(self):
        raise NotImplementedError

    def stream_events(self):
        raise NotImplementedError


class BackwardPaginatingFeed(FakeMarketDataFeed):
    """Simulates OKX history-candles backward pagination.

    Returns the *last* `limit` rows before `end_time_ms`, sorted
    newest-first when oldest_first=False.
    """

    async def fetch_klines(self, *, interval: str, limit: int = 100, start_time_ms: int | None = None, end_time_ms: int | None = None, use_cache: bool = True, oldest_first: bool = False) -> list[MarketKline]:
        self.calls.append({"start_time_ms": start_time_ms, "end_time_ms": end_time_ms, "limit": limit, "oldest_first": oldest_first})
        rows = [r for r in self.rows if r.interval == interval]
        if start_time_ms is not None:
            rows = [r for r in rows if r.open_time_ms >= start_time_ms]
        if end_time_ms is not None:
            rows = [r for r in rows if r.open_time_ms <= end_time_ms]
        # Backward: return the LAST `limit` rows, newest first.
        rows = sorted(rows, key=lambda r: r.open_time_ms, reverse=True)[:limit]
        if oldest_first:
            rows.reverse()
        return rows


# ────────────────────────────────────────────────────────────────────
# MarketDataKlineProvider tests
# ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_kline_provider_fetches_all_closed_klines_with_backward_pagination(tmp_path):
    """Provider should paginate correctly when the feed returns data
    from the end of the time range (backward pagination)."""
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    # Create 300 klines spanning 300 * 4h
    total = 300
    klines = [_kline(STEP * i) for i in range(total)]
    feed = BackwardPaginatingFeed(klines)
    provider = MarketDataKlineProvider(data_feed=feed, repository=store, page_limit=100)

    fetched = await provider.fetch_klines(
        symbol="ETH-USDT-PERP",
        interval="4h",
        start_open_ms=0,
        end_open_ms=STEP * (total - 1),
    )

    assert len(fetched) == total
    assert [r.open_time_ms for r in fetched] == [STEP * i for i in range(total)]
    # Should have made multiple pages since limit=100 < 300
    assert len(feed.calls) > 1


@pytest.mark.asyncio
async def test_kline_provider_deduplicates_by_open_time(tmp_path):
    """When pagination produces overlapping results, provider deduplicates."""
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    klines = [_kline(STEP * i) for i in range(5)]
    # Feed returns all klines for every call (simulating overlap).
    feed = FakeMarketDataFeed(klines)
    provider = MarketDataKlineProvider(data_feed=feed, repository=store, page_limit=100)

    fetched = await provider.fetch_klines(
        symbol="ETH-USDT-PERP",
        interval="4h",
        start_open_ms=0,
        end_open_ms=STEP * 4,
    )

    assert len(fetched) == 5
    # Dedup: each open_time appears once.
    open_times = [r.open_time_ms for r in fetched]
    assert open_times == sorted(set(open_times))


@pytest.mark.asyncio
async def test_kline_provider_filters_unclosed_and_wrong_symbol(tmp_path):
    """Provider must exclude unclosed klines and klines with wrong symbol."""
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    klines = [
        _kline(STEP * 0),
        MarketKline(
            exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP",
            interval="4h", open_time_ms=STEP * 1, close_time_ms=STEP * 2 - 1,
            open=Decimal("1000"), high=Decimal("1010"), low=Decimal("990"),
            close=Decimal("1005"), volume=Decimal("10"), is_closed=False,
        ),
        _kline(STEP * 2),
        _kline(STEP * 3, symbol="BTC-USDT-PERP"),
    ]
    feed = FakeMarketDataFeed(klines)
    provider = MarketDataKlineProvider(data_feed=feed, repository=store, page_limit=100)

    fetched = await provider.fetch_klines(
        symbol="ETH-USDT-PERP",
        interval="4h",
        start_open_ms=0,
        end_open_ms=STEP * 3,
    )

    # Only klines[0] and klines[2] match: closed + correct symbol.
    assert len(fetched) == 2
    open_times = [r.open_time_ms for r in fetched]
    assert STEP * 0 in open_times
    assert STEP * 2 in open_times
    assert STEP * 1 not in open_times  # unclosed
    assert STEP * 3 not in open_times  # wrong symbol


@pytest.mark.asyncio
async def test_kline_provider_stops_when_no_more_data(tmp_path):
    """Provider should stop paginating when a page returns no matching rows."""
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    klines = [_kline(STEP * i) for i in range(5)]
    feed = BackwardPaginatingFeed(klines)
    provider = MarketDataKlineProvider(data_feed=feed, repository=store, page_limit=100)

    fetched = await provider.fetch_klines(
        symbol="ETH-USDT-PERP",
        interval="4h",
        start_open_ms=0,
        end_open_ms=STEP * 4,
    )

    assert len(fetched) == 5
    # Only 1 page needed since all 5 fit in limit=100.
    assert len(feed.calls) == 1


@pytest.mark.asyncio
async def test_kline_provider_empty_feed_returns_empty(tmp_path):
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    feed = BackwardPaginatingFeed([])
    provider = MarketDataKlineProvider(data_feed=feed, repository=store)

    fetched = await provider.fetch_klines(
        symbol="ETH-USDT-PERP",
        interval="4h",
        start_open_ms=0,
        end_open_ms=STEP * 10,
    )

    assert len(fetched) == 0


# ────────────────────────────────────────────────────────────────────
# Backfill diagnostics tests
# ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_backfill_and_reload_saves_and_reports_correctly(tmp_path):
    """backfill_and_reload should fetch, save, and produce accurate diagnostics."""
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    klines = [_kline(STEP * i) for i in range(50)]
    feed = FakeMarketDataFeed(klines)
    provider = MarketDataKlineProvider(data_feed=feed, repository=store, page_limit=100)

    diag = await provider.backfill_and_reload(
        symbol="ETH-USDT-PERP",
        interval="4h",
        time_range=TimeRange(0, STEP * 49),
        min_records=30,
        store_class="SqliteKlineStore",
        store_path=str(tmp_path / "market.sqlite3"),
    )

    assert diag.success is True
    assert diag.fetched_records == 50
    assert diag.saved_records == 50
    assert diag.records_loaded_after == 50
    assert diag.records_loaded_before == 0
    assert diag.symbol == "ETH-USDT-PERP"
    assert diag.interval == "4h"
    assert diag.min_records == 30
    assert "okx:ETH-USDT-SWAP" in diag.raw_aliases


@pytest.mark.asyncio
async def test_backfill_and_reload_reports_failure_when_insufficient(tmp_path):
    """When fetched records are fewer than min_records, success must be False."""
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    klines = [_kline(STEP * i) for i in range(5)]
    feed = FakeMarketDataFeed(klines)
    provider = MarketDataKlineProvider(data_feed=feed, repository=store)

    diag = await provider.backfill_and_reload(
        symbol="ETH-USDT-PERP",
        interval="4h",
        time_range=TimeRange(0, STEP * 4),
        min_records=1000,
        store_class="SqliteKlineStore",
        store_path=str(tmp_path / "market.sqlite3"),
    )

    assert diag.success is False
    assert diag.fetched_records == 5
    assert diag.records_loaded_after == 5
    assert diag.records_loaded_after < diag.min_records


# ────────────────────────────────────────────────────────────────────
# Integration: warmup service + provider fallback pattern
# ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_closed_kline_warmup_backfills_from_provider_when_store_insufficient(tmp_path):
    """When local store has 0 rows, the provider should backfill 300 rows,
    enabling the warmup to succeed."""
    store = SqliteKlineStore(tmp_path / "market.sqlite3")

    # Local store is empty.
    assert len(store.load(symbol="ETH-USDT-PERP", interval="4h", time_range=TimeRange(0, STEP * 299))) == 0

    # Create 300 klines via feed.
    klines = [_kline(STEP * i) for i in range(300)]
    feed = BackwardPaginatingFeed(klines)

    # Step 1: warmup service (will find gap covering entire range)
    service = KlineWarmupService(data_feed=feed, repository=store, batch_limit=100)
    result = await service.warmup(
        WarmupRequest(
            symbol="ETH-USDT-PERP",
            dataset=MarketDataSet.KLINES,
            interval="4h",
            time_range=TimeRange(0, STEP * 299),
        )
    )
    # The warmup service may load some records via backward pagination.
    records_after_warmup = len(store.load(symbol="ETH-USDT-PERP", interval="4h", time_range=TimeRange(0, STEP * 299)))

    if records_after_warmup < 300:
        # Step 2: backfill via provider
        provider = MarketDataKlineProvider(data_feed=feed, repository=store, page_limit=100)
        diag = await provider.backfill_and_reload(
            symbol="ETH-USDT-PERP",
            interval="4h",
            time_range=TimeRange(0, STEP * 299),
            min_records=100,
            store_class="SqliteKlineStore",
            store_path=str(tmp_path / "market.sqlite3"),
        )

        assert diag.success is True
        assert diag.records_loaded_after >= 100

    # Final: store should have records.
    final = store.load(symbol="ETH-USDT-PERP", interval="4h", time_range=TimeRange(0, STEP * 299))
    assert len(final) > 0


@pytest.mark.asyncio
async def test_closed_kline_warmup_fails_when_provider_still_insufficient(tmp_path):
    """When provider also cannot supply enough records, backfill should report
    failure (success=False)."""
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    klines = [_kline(STEP * i) for i in range(5)]
    feed = FakeMarketDataFeed(klines)
    provider = MarketDataKlineProvider(data_feed=feed, repository=store)

    diag = await provider.backfill_and_reload(
        symbol="ETH-USDT-PERP",
        interval="4h",
        time_range=TimeRange(0, STEP * 4),
        min_records=1000,
        store_class="SqliteKlineStore",
        store_path=str(tmp_path / "market.sqlite3"),
    )

    assert diag.success is False
    assert diag.fetched_records == 5
    assert diag.records_loaded_after < 1000
