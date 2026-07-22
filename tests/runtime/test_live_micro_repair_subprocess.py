from __future__ import annotations

from decimal import Decimal

import pytest

from src.app import (
    AppConfig,
    AppContext,
    AsyncAlertDispatcher,
    NoopAlertSink,
)
from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.models import RangeBarAggregate, RangeCoverageStatus
from src.market_data.range_checkpoint import (
    MICRO_REPAIR_QUEUED,
    RangeBuilderCheckpoint,
    SqliteRangeCheckpointStore,
)
from src.market_data.range_repair import (
    SqliteRangeRepairJournalStore,
)
from src.market_data.storage import SqliteRangeBarStore
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode
from src.runtime.market_data.range_config import RangeRuntimeConfig
from src.runtime.requirements import StrategyRuntimeRequirements

H4 = 4 * 60 * 60_000
BUCKET_START = 1_780_000_000_000 - (1_780_000_000_000 % H4)
NOW_MS = BUCKET_START + 10_000


class _Data:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self) -> None:
        self.fetch_calls = 0
        self.stream_calls = 0

    async def fetch_trades(self, **kwargs):
        self.fetch_calls += 1
        raise AssertionError("live main process must not fetch repair trades")

    async def stream_trades(self):
        self.stream_calls += 1
        if False:
            yield None


class _Strategy:
    pass


class _MicroSupervisor:
    def __init__(self) -> None:
        self.launches = []

    def start_startup_recovery(self, **kwargs) -> bool:
        self.launches.append(dict(kwargs))
        return True


@pytest.mark.asyncio
async def test_live_main_launches_subprocess_without_touching_trade_flow(
    tmp_path, monkeypatch
) -> None:
    app = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="test",
        data_streams=("trades",),
        state_db_path=str(tmp_path / "state.sqlite3"),
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )
    data = _Data()
    context = AppContext(
        data=data,
        execution=object(),
        state_store=object(),
        strategy=_Strategy(),
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    checkpoint_store = SqliteRangeCheckpointStore(
        tmp_path / "checkpoint.sqlite3"
    )
    builder = RangeBarBuilder(range_pct="0.002", contract_value="0.01")
    builder.on_trade(
        MarketTrade(
            exchange=ExchangeName.OKX,
            symbol=app.symbol,
            raw_symbol="ETH-USDT-SWAP",
            price=Decimal("100"),
            quantity=Decimal("1"),
            side=TradeSide.BUY,
            trade_id="cp",
            trade_time_ms=NOW_MS - 100,
        )
    )
    checkpoint_store.save_checkpoint(
        RangeBuilderCheckpoint(
            exchange="okx",
            symbol=app.symbol,
            range_pct="0.002",
            bucket_start_ms=BUCKET_START,
            bucket_end_ms=BUCKET_START + H4 - 1,
            last_trade_id="cp",
            last_trade_ts_ms=NOW_MS - 100,
            last_ws_recv_ts_ms=NOW_MS - 100,
            range_bar_count=0,
            aggregate={},
            builder_state=builder.snapshot_state(),
            coverage_status="COMPLETE",
            missing_gap_ms=0,
            checkpoint_updated_at_ms=NOW_MS - 100,
        )
    )
    config = LiveRuntimeConfig(
        app=app,
        mode=RuntimeMode.LIVE_RUNTIME,
    )
    range_config = RangeRuntimeConfig(
        checkpoint_db_path=str(tmp_path / "checkpoint.sqlite3"),
        market_data_db_path=str(tmp_path / "market.sqlite3"),
        repair_journal_db=str(tmp_path / "journal.sqlite3"),
    )
    requirements = StrategyRuntimeRequirements.from_mapping(
        {
            "trades": {"enabled": True, "stream_enabled": True},
            "range_bars": {"enabled": True, "range_pct": "0.002"},
        }
    )
    micro_supervisor = _MicroSupervisor()
    runner = LiveRuntimeRunner(
        app_config=app,
        app_context=context,
        runtime_config=config,
        range_config=range_config,
        services={
            "runtime_requirements": requirements,
            "range_checkpoint_store": checkpoint_store,
            "range_bar_store": SqliteRangeBarStore(
                tmp_path / "market.sqlite3"
            ),
            "range_bar_aggregator": RangeBarAggregator(),
            "range_micro_repair_supervisor": micro_supervisor,
        },
    )
    monkeypatch.setattr(
        "src.runtime.components.catchup.time.time", lambda: NOW_MS / 1000
    )

    runner._initialize_rangebar_trust_window()
    await runner._process_trade(
        MarketTrade(
            exchange=ExchangeName.OKX,
            symbol=app.symbol,
            raw_symbol="ETH-USDT-SWAP",
            price=Decimal("100.2"),
            quantity=Decimal("1"),
            side=TradeSide.BUY,
            trade_id="first-live",
            trade_time_ms=NOW_MS + 88,
        )
    )
    runner._finalize_range_repair_journal(
        bucket_start_ms=BUCKET_START,
        finalized_at_ms=BUCKET_START + H4,
    )
    runner._get_range_checkpoint_writer().stop(flush=True)
    runner._get_range_repair_journal_writer().stop(flush=True)

    job = checkpoint_store.load_micro_repair_job(
        exchange="okx",
        symbol=app.symbol,
        range_pct="0.002",
        bucket_start_ms=BUCKET_START,
    )
    assert job is not None
    assert job.status == MICRO_REPAIR_QUEUED
    assert len(micro_supervisor.launches) == 1
    assert micro_supervisor.launches[0]["bucket_start_ms"] == BUCKET_START
    assert data.fetch_calls == 0
    assert data.stream_calls == 0
    assert runner._market_queue.empty()
    journal_state = SqliteRangeRepairJournalStore(
        tmp_path / "journal.sqlite3"
    ).load_state(
        exchange="okx",
        symbol=app.symbol,
        range_pct="0.002",
        bucket_start_ms=BUCKET_START,
    )
    assert journal_state is not None
    assert journal_state.first_live_trade_ts_ms == NOW_MS + 88
    assert journal_state.first_live_trade_id == "first-live"
    assert journal_state.finalized
    assert journal_state.status == "journal_finalized"
    assert journal_state.checkpoint_last_trade_ts_ms == NOW_MS - 100
    assert journal_state.first_live_trade_ts_ms - 1 == NOW_MS + 87

    checkpoint_store.save_completed_aggregate(
        exchange="okx",
        aggregate=RangeBarAggregate(
            symbol=app.symbol,
            range_pct=Decimal("0.002"),
            bucket_start_ms=BUCKET_START,
            bucket_end_ms=BUCKET_START + H4 - 1,
            bar_count=1,
            first_open=Decimal("100"),
            last_close=Decimal("101"),
            high=Decimal("101"),
            low=Decimal("100"),
            buy_notional_sum=Decimal("1"),
            sell_notional_sum=Decimal("1"),
            delta_notional_sum=Decimal("0"),
            notional_sum=Decimal("2"),
        ),
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        missing_gap_ms=0,
        completed_at_ms=NOW_MS,
    )
    runner._refresh_range_micro_repair_coverage(BUCKET_START)
    coverage = runner._range_coverage_for_bucket(BUCKET_START)
    assert coverage.coverage_status == RangeCoverageStatus.COMPLETE.value
    assert coverage.missing_gap_ms == 0
    assert runner._range_module.degraded_reason(BUCKET_START) is None
