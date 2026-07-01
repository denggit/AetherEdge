from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.micro_repair import (
    RangeMicroRepairError,
    RangeMicroRepairRebuildService,
    RangeMicroRepairService,
)
from src.market_data.derived import RangeBarBuilder
from src.market_data.models import RangeBarAggregate, RangeCoverageStatus
from src.market_data.range_checkpoint import RangeMicroRepairJob, SqliteRangeCheckpointStore
from src.market_data.storage import SqliteRangeBarStore
from src.platform.data.models import (
    MarketDataSource,
    MarketTrade,
    TradeSide,
)
from src.platform.exchanges.models import ExchangeName


def _trade(
    ts: int,
    trade_id: str,
    *,
    price: str = "100",
    source: MarketDataSource = MarketDataSource.REST,
) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal(price),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id=trade_id,
        trade_time_ms=ts,
        source=source,
    )


class _PagedProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int, int]] = []

    async def fetch_trades(
        self,
        *,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 100,
    ):
        self.calls.append((start_time_ms, end_time_ms, limit))
        rows = [_trade(1001, "1"), _trade(1002, "2"), _trade(1003, "3")]
        return [row for row in rows if start_time_ms <= row.trade_time_ms <= end_time_ms][
            :limit
        ]


@pytest.mark.asyncio
async def test_micro_repair_fetches_forward_pages_and_deduplicates_boundary() -> None:
    provider = _PagedProvider()
    result = await RangeMicroRepairService(
        provider, page_limit=2, max_pages=5, max_seconds=1
    ).fetch(
        symbol="ETH-USDT-PERP",
        start_time_ms=1001,
        end_time_ms=1003,
    )

    assert [row.trade_time_ms for row in result.trades] == [1001, 1002, 1003]
    assert result.rest_pages == 3
    assert result.rest_raw_trades == 5
    assert result.rest_deduped_trades == 3
    assert result.fetch_mode == "time_range_fallback"
    assert result.fallback_reason == "missing_trade_ids"


class _StalledProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch_trades(self, **kwargs):
        self.calls += 1
        if self.calls == 1:
            return [_trade(1001, "1"), _trade(1002, "2")]
        return [_trade(1002, "2"), _trade(1002, "2")]


@pytest.mark.asyncio
async def test_micro_repair_fails_closed_when_time_pagination_stalls() -> None:
    with pytest.raises(RangeMicroRepairError, match="stalled"):
        await RangeMicroRepairService(
            _StalledProvider(), page_limit=2, max_pages=5, max_seconds=1
        ).fetch(
            symbol="ETH-USDT-PERP",
            start_time_ms=1001,
            end_time_ms=1003,
        )


class _BudgetAwareProvider:
    def __init__(self) -> None:
        self.last_historical_trade_pages = 0
        self.call = None

    async def fetch_trades(
        self,
        *,
        symbol,
        start_time_ms,
        end_time_ms,
        limit,
        max_pages=None,
    ):
        self.call = (limit, max_pages)
        self.last_historical_trade_pages = 3
        return [_trade(ts, str(ts)) for ts in range(1001, 1251)]


@pytest.mark.asyncio
async def test_micro_repair_passes_total_page_budget_to_platform_adapter() -> None:
    provider = _BudgetAwareProvider()
    result = await RangeMicroRepairService(
        provider, page_limit=100, max_pages=5, max_seconds=1
    ).fetch(
        symbol="ETH-USDT-PERP",
        start_time_ms=1001,
        end_time_ms=1300,
    )

    assert provider.call == (500, 5)
    assert result.rest_pages == 3
    assert len(result.trades) == 250


class _AnchoredProvider:
    def __init__(self) -> None:
        self.last_historical_trade_pages = 13
        self.anchor_call = None
        self.time_calls = 0

    async def fetch_trades_between_ids(self, **kwargs):
        self.anchor_call = kwargs
        return [
            _trade(1002, "1020"),
            _trade(1001, "1001"),
            _trade(1002, "1020"),
            _trade(1087, "1087"),
        ]

    async def fetch_trades(self, **kwargs):
        self.time_calls += 1
        raise AssertionError("time-range fallback must not be used")


@pytest.mark.asyncio
async def test_micro_repair_prefers_trade_id_anchor_and_deduplicates() -> None:
    provider = _AnchoredProvider()
    result = await RangeMicroRepairService(
        provider,
        page_limit=100,
        max_pages=20,
        max_seconds=1,
    ).fetch(
        symbol="ETH-USDT-PERP",
        start_time_ms=1001,
        end_time_ms=1087,
        newer_trade_id="2000",
        older_trade_id="1000",
    )

    assert provider.time_calls == 0
    assert provider.anchor_call == {
        "symbol": "ETH-USDT-PERP",
        "newer_trade_id": "2000",
        "older_trade_id": "1000",
        "start_time_ms": 1001,
        "end_time_ms": 1087,
        "limit": 100,
        "max_pages": 20,
        "oldest_first": True,
    }
    assert [row.trade_id for row in result.trades] == [
        "1001",
        "1020",
        "1087",
    ]
    assert result.rest_pages == 13
    assert result.rest_raw_trades == 4
    assert result.rest_deduped_trades == 3
    assert result.fetch_mode == "trade_id_anchor"
    assert result.fallback_reason is None


@pytest.mark.asyncio
async def test_micro_repair_falls_back_when_checkpoint_trade_id_is_missing() -> None:
    class _Provider:
        def __init__(self) -> None:
            self.anchor_calls = 0
            self.time_call = None

        async def fetch_trades_between_ids(self, **kwargs):
            self.anchor_calls += 1
            raise AssertionError("anchor fetch must not be called")

        async def fetch_trades(self, **kwargs):
            self.time_call = kwargs
            return [_trade(1001, "1")]

    provider = _Provider()
    result = await RangeMicroRepairService(
        provider,
        page_limit=100,
        max_pages=5,
        max_seconds=1,
    ).fetch(
        symbol="ETH-USDT-PERP",
        start_time_ms=1001,
        end_time_ms=1087,
        newer_trade_id="2000",
        older_trade_id=None,
    )

    assert provider.anchor_calls == 0
    assert provider.time_call["start_time_ms"] == 1001
    assert provider.time_call["end_time_ms"] == 1087
    assert result.fetch_mode == "time_range_fallback"
    assert result.fallback_reason == "missing_trade_ids"


@pytest.mark.asyncio
async def test_rebuild_service_uses_independent_checkpoint_builder_and_marks_complete(
    tmp_path,
    monkeypatch,
) -> None:
    bucket_start = 1_780_000_000_000
    bucket_end = bucket_start + 1_999
    checkpoint_ts = bucket_start + 1_000
    first_live_ts = bucket_start + 1_088
    builder = RangeBarBuilder(range_pct="0.001", contract_value="1")
    builder.on_trade(_trade(checkpoint_ts, "4048125172"))
    job = RangeMicroRepairJob(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=bucket_start,
        bucket_end_ms=bucket_end,
        checkpoint_last_trade_id="4048125172",
        checkpoint_last_trade_ts_ms=checkpoint_ts,
        builder_state=builder.snapshot_state(),
        coverage_status=RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
        missing_gap_ms=87_580,
        first_live_trade_ts_ms=first_live_ts,
        first_live_trade_id="4048126437",
        repair_gap_start_ms=checkpoint_ts + 1,
        repair_gap_end_ms=first_live_ts - 1,
        journal_start_ms=first_live_ts,
        journal_end_ms=bucket_end,
        journal_status="journal_finalized",
        created_at_ms=bucket_start,
        updated_at_ms=bucket_start,
    )
    class _Provider:
        def __init__(self) -> None:
            self.anchor_call = None
            self.time_calls = 0
            self.last_historical_trade_pages = 13

        async def fetch_trades_between_ids(self, **kwargs):
            self.anchor_call = kwargs
            return [
                _trade(checkpoint_ts + 1, "4048125173", price="100.2"),
                _trade(checkpoint_ts + 20, "4048125200", price="100.4"),
                _trade(first_live_ts - 1, "4048126436", price="100.6"),
            ]

        async def fetch_trades(self, **kwargs):
            self.time_calls += 1
            raise AssertionError("time-range fallback must not be used")

    provider = _Provider()
    replayed = []
    original_on_trade = RangeBarBuilder.on_trade

    def recording_on_trade(self, trade):
        replayed.append(trade.trade_time_ms)
        return original_on_trade(self, trade)

    monkeypatch.setattr(RangeBarBuilder, "on_trade", recording_on_trade)
    checkpoint_store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    checkpoint_store.save_completed_aggregate(
        exchange="okx",
        aggregate=RangeBarAggregate(
            symbol="ETH-USDT-PERP",
            range_pct=Decimal("0.001"),
            bucket_start_ms=bucket_start,
            bucket_end_ms=bucket_end,
            bar_count=1,
            first_open=Decimal("100"),
            last_close=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            buy_notional_sum=Decimal("1"),
            sell_notional_sum=Decimal("0"),
            delta_notional_sum=Decimal("1"),
            notional_sum=Decimal("1"),
        ),
        coverage_status=RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
        missing_gap_ms=500,
        completed_at_ms=bucket_end,
    )
    result = await RangeMicroRepairRebuildService(
        provider=provider,
        range_bar_store=SqliteRangeBarStore(tmp_path / "market.sqlite3"),
        checkpoint_store=checkpoint_store,
        contract_value="1",
        page_limit=100,
        max_pages=20,
        max_seconds=1,
    ).rebuild(
        job,
        journal_trades=[
            _trade(
                first_live_ts,
                "4048126437",
                price="100.8",
                source=MarketDataSource.WEBSOCKET,
            ),
            _trade(
                bucket_start + 1_100,
                "j2",
                price="101.0",
                source=MarketDataSource.WEBSOCKET,
            ),
            _trade(
                bucket_start + 1_200,
                "j3",
                price="101.2",
                source=MarketDataSource.WEBSOCKET,
            ),
            _trade(
                bucket_start + 1_999,
                "j4",
                price="101.4",
                source=MarketDataSource.WEBSOCKET,
            ),
        ],
        completed_at_ms=bucket_end + 1,
    )

    repaired = checkpoint_store.load_completed_aggregate(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_end_ms=bucket_end,
    )
    assert provider.time_calls == 0
    assert provider.anchor_call["newer_trade_id"] == "4048126437"
    assert provider.anchor_call["older_trade_id"] == "4048125172"
    assert provider.anchor_call["start_time_ms"] == checkpoint_ts + 1
    assert provider.anchor_call["end_time_ms"] == first_live_ts - 1
    assert provider.anchor_call["end_time_ms"] != bucket_end
    assert replayed == [
        checkpoint_ts + 1,
        checkpoint_ts + 20,
        first_live_ts - 1,
        first_live_ts,
        bucket_start + 1_100,
        bucket_start + 1_200,
        bucket_start + 1_999,
    ]
    assert result.repair_gap_ms == 87
    assert result.replayed_rest_trades == 3
    assert result.replayed_journal_trades == 4
    assert result.fetch_mode == "trade_id_anchor"
    assert result.range_bars_written > 0
    assert repaired is not None
    assert repaired.coverage_status == "COMPLETE"
    assert repaired.missing_gap_ms == 0
    complete_history = checkpoint_store.load_complete_history(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        before_bucket_end_ms=bucket_end + 1,
        limit=10,
    )
    assert [row.bucket_end_ms for row in complete_history] == [bucket_end]


@pytest.mark.asyncio
async def test_rebuild_skips_rest_when_first_live_is_next_trade_ms(
    tmp_path,
) -> None:
    bucket_start = 1_780_000_000_000
    bucket_end = bucket_start + 1_999
    checkpoint_ts = bucket_start + 1_000
    first_live_ts = checkpoint_ts + 1
    builder = RangeBarBuilder(range_pct="0.001", contract_value="1")
    builder.on_trade(_trade(checkpoint_ts, "cp"))
    job = RangeMicroRepairJob(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=bucket_start,
        bucket_end_ms=bucket_end,
        checkpoint_last_trade_id="cp",
        checkpoint_last_trade_ts_ms=checkpoint_ts,
        builder_state=builder.snapshot_state(),
        coverage_status=RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
        missing_gap_ms=1,
        first_live_trade_ts_ms=first_live_ts,
        first_live_trade_id="j1",
        repair_gap_start_ms=first_live_ts,
        repair_gap_end_ms=first_live_ts - 1,
        journal_start_ms=first_live_ts,
        journal_end_ms=bucket_end,
        journal_status="journal_finalized",
    )

    class _NoRestProvider:
        calls = 0

        async def fetch_trades(self, **kwargs):
            self.calls += 1
            raise AssertionError("REST must not be called for an empty gap")

    provider = _NoRestProvider()
    checkpoint_store = SqliteRangeCheckpointStore(
        tmp_path / "checkpoint.sqlite3"
    )
    result = await RangeMicroRepairRebuildService(
        provider=provider,
        range_bar_store=SqliteRangeBarStore(tmp_path / "market.sqlite3"),
        checkpoint_store=checkpoint_store,
        contract_value="1",
    ).rebuild(
        job,
        journal_trades=[
            _trade(
                first_live_ts,
                "j1",
                price="100.2",
                source=MarketDataSource.WEBSOCKET,
            )
        ],
        completed_at_ms=bucket_end + 1,
    )

    assert provider.calls == 0
    assert result.repair_gap_ms == 0
    assert result.replayed_rest_trades == 0
    assert result.replayed_journal_trades == 1
