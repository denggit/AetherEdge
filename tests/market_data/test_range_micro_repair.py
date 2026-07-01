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


def _trade(ts: int, trade_id: str) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id=trade_id,
        trade_time_ms=ts,
        source=MarketDataSource.REST,
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


@pytest.mark.asyncio
async def test_rebuild_service_uses_independent_checkpoint_builder_and_marks_complete(
    tmp_path,
) -> None:
    bucket_start = 1_780_000_000_000
    bucket_end = bucket_start + 9_999
    checkpoint_ts = bucket_start + 100
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
        missing_gap_ms=87_580,
        created_at_ms=bucket_start,
        updated_at_ms=bucket_start,
    )
    class _Provider:
        async def fetch_trades(self, **kwargs):
            return [
                MarketTrade(
                    exchange=ExchangeName.OKX,
                    symbol="ETH-USDT-PERP",
                    raw_symbol="ETH-USDT-SWAP",
                    price=Decimal("100.2"),
                    quantity=Decimal("1"),
                    side=TradeSide.BUY,
                    trade_id="r1",
                    trade_time_ms=checkpoint_ts + 1,
                    source=MarketDataSource.REST,
                )
            ]

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
        provider=_Provider(),
        range_bar_store=SqliteRangeBarStore(tmp_path / "market.sqlite3"),
        checkpoint_store=checkpoint_store,
        contract_value="1",
        page_limit=100,
        max_pages=5,
        max_seconds=1,
    ).rebuild(job, completed_at_ms=bucket_end + 1)

    repaired = checkpoint_store.load_completed_aggregate(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_end_ms=bucket_end,
    )
    assert result.range_bars_written == 1
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
