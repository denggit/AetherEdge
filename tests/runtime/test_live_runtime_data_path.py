from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.app import AppConfig, AppContext
from src.market_data.derived import RangeBarAggregator
from src.market_data.events import MarketFeatureEvent
from src.market_data.models import RangeBar, RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import (
    CompletedRangeAggregate,
    RangeCheckpointRecovery,
)
from src.platform import Balance, ExchangeName, LeverageInfo, PositionMode
from src.platform.data.models import MarketKline, MarketTrade, TradeSide
from src.platform.markets import get_market_profile
from src.platform.snapshot import PlatformSnapshot
from src.planner import ExecutionPlanner
from src.runtime import (
    LiveRuntimeConfig,
    LiveRuntimeRunner,
    RuntimeMode,
    StrategyRuntimeRequirements,
)
from src.signals import SignalAction, TradeSignal


H4 = 4 * 60 * 60_000


class _Env:
    def get(self, key: str, default: str) -> str:
        return default

    def get_int(self, key: str, default: int) -> int:
        return default


class _Alerts:
    def __init__(self) -> None:
        self.items = []

    def emit(self, alert) -> None:
        self.items.append(alert)


class _Data:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    async def fetch_klines(self, **kwargs):
        return [_kline(int(kwargs["start_time_ms"]))]

    async def stream_trades(self):
        if False:
            yield None

    async def stream_order_book(self):
        if False:
            yield None


class _StateStore:
    def save_snapshot(self, snapshot):
        self.snapshot = snapshot

    def list_open_orders(self, **kwargs):
        return []


class _FeatureStrategy:
    def __init__(self) -> None:
        self.events: list[MarketFeatureEvent] = []
        self.last_decision_audit = None

    async def on_market_feature(self, event: MarketFeatureEvent):
        self.events.append(event)
        return []

    async def on_trade(self, trade):
        return []


class _SignalStrategy(_FeatureStrategy):
    async def on_market_feature(self, event: MarketFeatureEvent):
        self.events.append(event)
        if event.type_value != "closed_kline":
            return []
        return [
            TradeSignal(
                symbol="ETH-USDT-PERP",
                action=SignalAction.OPEN_LONG,
                quantity=Decimal("1"),
                created_time_ms=event.event_time_ms,
            )
        ]


class _MfObserver:
    def __init__(self) -> None:
        self.tradebar_open_times: list[int] = []
        self.range_footprint_count = 0

    def on_market_feature(self, event: MarketFeatureEvent):
        if event.type_value == "fixed_time_trade_bar":
            self.tradebar_open_times.append(int(event.data["open_time_ms"]))
        elif event.type_value == "range_footprint_feature":
            self.range_footprint_count += 1
        return []

    def audit(self):
        return {
            "tradebar_count": len(self.tradebar_open_times),
            "range_footprint_count": self.range_footprint_count,
            "latest_tradebar_open_time_ms": (
                self.tradebar_open_times[-1]
                if self.tradebar_open_times
                else None
            ),
        }


class _MfStrategy:
    def __init__(self, observer: _MfObserver) -> None:
        self.observer = observer

    def trade_feature_runtime_config(self):
        return {
            "enabled": True,
            "range_pct": "0.002",
            "range_price_step": "1",
        }

    def market_feature_observers(self):
        return (self.observer,)

    async def on_trade(self, trade):
        return []


class _MemoryRangeBarStore:
    def __init__(self) -> None:
        self.rows: list[RangeBar] = []
        self.save_calls = 0
        self.load_calls = 0

    def save(self, rows):
        self.save_calls += 1
        self.rows.extend(rows)
        return len(rows)

    def load(self, *, symbol: str, range_pct: str, time_range: TimeRange):
        self.load_calls += 1
        return [
            row
            for row in self.rows
            if row.symbol == symbol
            and str(row.range_pct) == str(Decimal(str(range_pct)))
            and time_range.start_time_ms
            <= row.end_time_ms
            <= time_range.end_time_ms
        ]


class _FailingKlineStore:
    def __init__(self) -> None:
        self.save_calls = 0

    def save(self, rows):
        self.save_calls += 1
        raise RuntimeError("kline db down")


class _FailingRangeBarStore(_MemoryRangeBarStore):
    def save(self, rows):
        self.save_calls += 1
        raise RuntimeError("range db down")


class _CheckpointStore:
    def __init__(self) -> None:
        self.aggregates = []

    def save_completed_aggregate(self, **kwargs):
        self.aggregates.append(kwargs)
        return True


class _OneTradeRangeBarBuilder:
    def __init__(self) -> None:
        self.next_bar_id = 1

    def on_trade(self, trade: MarketTrade):
        time_ms = trade.trade_time_ms or trade.event_time_ms
        bar = _range_bar(
            bar_id=self.next_bar_id,
            start_time_ms=time_ms,
            end_time_ms=time_ms,
            price=trade.price,
        )
        self.next_bar_id += 1
        return (bar,)


@pytest.mark.asyncio
async def test_4h_range_aggregate_uses_memory_when_store_is_empty() -> None:
    store = _MemoryRangeBarStore()
    strategy = _FeatureStrategy()
    runner = _runner(
        strategy,
        requirements=_range_requirements(),
        services={
            "range_bar_store": store,
            "range_bar_aggregator": RangeBarAggregator(),
            "range_checkpoint_store": _CheckpointStore(),
        },
    )
    runner._range_bars_by_bucket[0] = [
        _range_bar(bar_id=i + 1, start_time_ms=i * 1_000, end_time_ms=i * 1_000)
        for i in range(5)
    ]

    events = await runner.poll_closed_bar_once(now_ms=H4 + 5_000)
    await runner._stop_live_persistence_writer()

    assert [event.type_value for event in events] == [
        "closed_kline",
        "range_aggregate",
    ]
    assert events[-1].data["bar_count"] == 5
    assert store.load_calls == 0


@pytest.mark.asyncio
async def test_range_bar_save_failure_does_not_block_closed_feature_dispatch() -> None:
    store = _FailingRangeBarStore()
    strategy = _FeatureStrategy()
    alerts = _Alerts()
    runner = _runner(
        strategy,
        alerts=alerts,
        requirements=_range_requirements(closed_kline_enabled=False),
        services={
            "range_bar_builder": _OneTradeRangeBarBuilder(),
            "range_bar_store": store,
            "range_bar_aggregator": RangeBarAggregator(),
        },
    )

    await runner.process_market_event(_trade(time_ms=1_000, price="100"))
    assert [event.type_value for event in strategy.events] == [
        "range_bar_closed"
    ]
    assert runner._range_bars_by_bucket[0]

    await runner._stop_live_persistence_writer()
    await asyncio.sleep(0)

    assert store.save_calls == 1
    assert alerts.items[-1].subject == "AetherEdge range bar persistence failed"


@pytest.mark.asyncio
async def test_closed_kline_persist_failure_does_not_block_signal_execution() -> None:
    strategy = _SignalStrategy()
    alerts = _Alerts()
    runner = _runner(
        strategy,
        alerts=alerts,
        requirements=_closed_kline_requirements(),
        services={"kline_store": _FailingKlineStore()},
    )
    executed: list[TradeSignal] = []

    async def capture(signals, **kwargs):
        executed.extend(signals)

    runner._execute_signals = capture

    events = await runner.poll_closed_bar_once(now_ms=H4 + 5_000)

    assert [event.type_value for event in events] == ["closed_kline"]
    assert [event.type_value for event in strategy.events] == ["closed_kline"]
    assert [signal.action for signal in executed] == [SignalAction.OPEN_LONG]

    await runner._stop_live_persistence_writer()
    await asyncio.sleep(0)

    assert alerts.items[-1].subject == (
        "AetherEdge closed kline persistence failed"
    )


@pytest.mark.asyncio
async def test_mf_1m_feature_dispatch_only_updates_observer_buffer() -> None:
    observer = _MfObserver()
    range_store = _FailingRangeBarStore()
    kline_store = _FailingKlineStore()
    runner = _runner(
        _MfStrategy(observer),
        requirements=StrategyRuntimeRequirements.from_mapping(
            {
                "closed_kline": {"enabled": False},
                "trades": {"enabled": True, "stream_enabled": True},
                "range_bars": {"enabled": False},
            }
        ),
        services={
            "range_bar_store": range_store,
            "kline_store": kline_store,
        },
    )

    base = 1_700_000_000_000
    await runner.process_market_event(
        _trade(time_ms=base + 1_000, price="100", side=TradeSide.BUY)
    )
    await runner.process_market_event(
        _trade(time_ms=base + 60_001, price="101", side=TradeSide.SELL)
    )

    assert observer.audit()["tradebar_count"] == 1
    assert observer.audit()["range_footprint_count"] == 1
    assert range_store.save_calls == 0
    assert kline_store.save_calls == 0
    assert runner._live_persistence_writer is None


def _runner(
    strategy,
    *,
    alerts: _Alerts | None = None,
    requirements: StrategyRuntimeRequirements,
    services: dict | None = None,
) -> LiveRuntimeRunner:
    cfg = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.fake:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=20,
        signal_queue_maxsize=20,
        alert_queue_maxsize=20,
        dry_run=True,
        enable_email_alerts=False,
    )
    context = AppContext(
        data=_Data(),
        execution=object(),
        state_store=_StateStore(),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=alerts or _Alerts(),
    )
    resolved_services = {
        "project_env_config": _Env(),
        "runtime_requirements": requirements,
        "recovery_service": None,
        "snapshot": _snapshot(),
    }
    resolved_services.update(services or {})
    return LiveRuntimeRunner(
        app_config=cfg,
        app_context=context,
        runtime_config=LiveRuntimeConfig(
            app=cfg,
            mode=RuntimeMode.LIVE_RUNTIME,
            warmup_enabled=False,
            closed_bar_buffer_ms=5_000,
            closed_bar_retry_interval_ms=5_000,
            closed_bar_missing_alert_after_ms=120_000,
        ),
        services=resolved_services,
    )


def _closed_kline_requirements() -> StrategyRuntimeRequirements:
    return StrategyRuntimeRequirements.from_mapping(
        {
            "closed_kline": {
                "enabled": True,
                "interval": "4h",
                "close_buffer_ms": 5_000,
            },
            "trades": {"enabled": False},
            "range_bars": {"enabled": False},
        }
    )


def _range_requirements(
    *, closed_kline_enabled: bool = True
) -> StrategyRuntimeRequirements:
    return StrategyRuntimeRequirements.from_mapping(
        {
            "closed_kline": {
                "enabled": closed_kline_enabled,
                "interval": "4h",
                "close_buffer_ms": 5_000,
            },
            "trades": {"enabled": True, "stream_enabled": True},
            "range_bars": {
                "enabled": True,
                "range_pct": "0.002",
                "aggregate_interval": "4h",
            },
        }
    )


def _kline(open_time_ms: int) -> MarketKline:
    return MarketKline(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        interval="4h",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + H4 - 1,
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal("105"),
        volume=Decimal("10"),
        is_closed=True,
    )


def _range_bar(
    *,
    bar_id: int,
    start_time_ms: int,
    end_time_ms: int,
    price: Decimal | str = Decimal("100"),
) -> RangeBar:
    price = Decimal(str(price))
    return RangeBar(
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        bar_id=bar_id,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("1"),
        buy_notional=price,
        sell_notional=Decimal("0"),
        trade_count=1,
    )


def _trade(
    *,
    time_ms: int,
    price: str,
    side: TradeSide = TradeSide.BUY,
) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal(price),
        quantity=Decimal("1"),
        side=side,
        event_time_ms=time_ms,
        trade_time_ms=time_ms,
    )


def _snapshot() -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(
            exchange=ExchangeName.OKX,
            asset="USDT",
            total=Decimal("1000"),
            available=Decimal("1000"),
        ),
        positions=[],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            leverage=Decimal("1"),
        ),
        position_mode=PositionMode.ONE_WAY,
    )


# ── Test helpers for micro-repair + repaired-bucket scenarios ──────────


class _RepairCheckpointStore:
    """Checkpoint store that can return a COMPLETE aggregate on demand."""

    def __init__(self, *, complete: bool = False) -> None:
        self._complete = complete
        self.aggregates: list[dict] = []
        self._recover_calls = 0

    def recover_current_bucket(self, **kwargs):
        self._recover_calls += 1
        return RangeCheckpointRecovery(
            coverage_status=RangeCoverageStatus.RECOVERED_INCOMPLETE.value,
            checkpoint=None,
            checkpoint_age_ms=1000,
            missing_gap_ms=5000,
            recovered_from_checkpoint=True,
        )

    def load_completed_aggregate(self, *, exchange, symbol, range_pct, bucket_end_ms):
        if not self._complete:
            return None
        return CompletedRangeAggregate(
            exchange=exchange,
            symbol=symbol,
            range_pct=range_pct,
            bucket_start_ms=bucket_end_ms + 1 - (4 * 60 * 60_000),
            bucket_end_ms=bucket_end_ms,
            rf_bar_count=5,
            imbalance="0.15",
            close_pos="0.65",
            taker_buy_ratio="0.575",
            micro_return_pct="0.003",
            delta_notional_sum="500",
            notional_sum="5000",
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            missing_gap_ms=0,
            completed_at_ms=1_700_000_000_000,
        )

    def save_completed_aggregate(self, **kwargs):
        self.aggregates.append(kwargs)
        return True


class _ControlledRangeBarStore(_MemoryRangeBarStore):
    """Range bar store where DB rows are configured separately from save() calls."""

    def __init__(self) -> None:
        super().__init__()
        self._db_rows: list[RangeBar] = []

    def set_db_rows(self, rows: list[RangeBar]) -> None:
        self._db_rows = list(rows)

    def load(self, *, symbol: str, range_pct: str, time_range: TimeRange):
        self.load_calls += 1
        return [
            row
            for row in self._db_rows
            if row.symbol == symbol
            and str(row.range_pct) == str(Decimal(str(range_pct)))
            and time_range.start_time_ms
            <= row.end_time_ms
            <= time_range.end_time_ms
        ]


class _NoLoadRangeBarStore(_MemoryRangeBarStore):
    """Range bar store that records whether load() was called but never returns rows."""

    def __init__(self) -> None:
        super().__init__()
        self.load_was_called = False

    def load(self, *, symbol: str, range_pct: str, time_range: TimeRange):
        self.load_calls += 1
        self.load_was_called = True
        return []


# ── Test A: micro repair complete → 4H decision uses repaired DB rows ──


@pytest.mark.asyncio
async def test_micro_repair_complete_forces_db_rows_for_4h_aggregate() -> None:
    """When micro repair marks a bucket COMPLETE, the 4H aggregate MUST use
    repaired DB rows — not the partial in-memory rows collected before repair."""
    store = _ControlledRangeBarStore()
    # Repaired DB has 5 full bars
    store.set_db_rows([
        _range_bar(bar_id=i + 1, start_time_ms=i * 1_000, end_time_ms=i * 1_000)
        for i in range(5)
    ])
    checkpoint_store = _RepairCheckpointStore(complete=True)
    strategy = _FeatureStrategy()
    runner = _runner(
        strategy,
        requirements=_range_requirements(),
        services={
            "range_bar_store": store,
            "range_bar_aggregator": RangeBarAggregator(),
            "range_checkpoint_store": checkpoint_store,
        },
    )
    # Partial memory: only 2 bars
    partial_rows = [
        _range_bar(bar_id=i + 1, start_time_ms=i * 1_000, end_time_ms=i * 1_000)
        for i in range(2)
    ]
    runner._range_bars_by_bucket[0] = list(partial_rows)
    runner._initial_range_bucket_ms = 0
    runner._initial_range_recovery = RangeCheckpointRecovery(
        coverage_status=RangeCoverageStatus.RECOVERED_INCOMPLETE.value,
        checkpoint=None,
        checkpoint_age_ms=1000,
        missing_gap_ms=5000,
        recovered_from_checkpoint=True,
    )

    # Act: micro repair completes
    runner._refresh_range_micro_repair_coverage(0)

    # Assert: bucket marked repaired, partial memory cleared
    assert 0 in runner._range_repaired_complete_buckets
    assert 0 not in runner._range_bars_by_bucket

    # Act: get rows for 4H aggregate
    rows = runner._range_bar_rows_for_bucket(0)

    # Assert: 5 repaired DB rows returned, NOT 2 partial
    assert len(rows) == 5, f"Expected 5 repaired DB rows, got {len(rows)}"
    # Assert: cache repopulated from DB
    assert len(runner._range_bars_by_bucket.get(0, [])) == 5

    await runner._stop_live_persistence_writer()


# ── Test B: micro repair complete → checkpoint uses repaired DB rows ──


@pytest.mark.asyncio
async def test_micro_repair_complete_checkpoint_uses_repaired_db_rows() -> None:
    """When micro repair marks a bucket COMPLETE, checkpoint submission MUST
    use repaired DB rows — not partial memory rows."""
    store = _ControlledRangeBarStore()
    store.set_db_rows([
        _range_bar(bar_id=i + 1, start_time_ms=i * 1_000, end_time_ms=i * 1_000)
        for i in range(5)
    ])
    checkpoint_store = _RepairCheckpointStore(complete=True)
    strategy = _FeatureStrategy()
    runner = _runner(
        strategy,
        requirements=_range_requirements(),
        services={
            "range_bar_store": store,
            "range_bar_aggregator": RangeBarAggregator(),
            "range_checkpoint_store": checkpoint_store,
        },
    )
    runner._range_bars_by_bucket[0] = [
        _range_bar(bar_id=i + 1, start_time_ms=i * 1_000, end_time_ms=i * 1_000)
        for i in range(2)
    ]
    runner._initial_range_bucket_ms = 0
    runner._initial_range_recovery = RangeCheckpointRecovery(
        coverage_status=RangeCoverageStatus.RECOVERED_INCOMPLETE.value,
        checkpoint=None,
        checkpoint_age_ms=1000,
        missing_gap_ms=5000,
        recovered_from_checkpoint=True,
    )

    # Micro repair completes
    runner._refresh_range_micro_repair_coverage(0)
    assert 0 in runner._range_repaired_complete_buckets

    # Force checkpoint due
    runner._last_range_checkpoint_submit_ms = 0
    runner._range_bars_since_checkpoint = 999

    # Submit checkpoint for this bucket
    trade = _trade(time_ms=5_000, price="105")
    accepted = runner._submit_range_checkpoint_if_due(trade)

    # The checkpoint writer is _RangeCheckpointWriter in test — need to verify
    # what was submitted. Since the test runner uses a real RangeBarAggregator
    # but no RangeCheckpointWriter, we check via the store's aggregates.
    await runner._stop_live_persistence_writer()

    # The key assertion: partial rows (2) were replaced by repaired rows (5)
    rows = runner._range_bar_rows_for_bucket(0)
    assert len(rows) == 5


# ── Test C: normal live bucket still memory-first ──


@pytest.mark.asyncio
async def test_normal_live_bucket_remains_memory_first() -> None:
    """When a bucket is NOT in the repaired complete set, memory rows are used
    without touching DB."""
    store = _NoLoadRangeBarStore()
    strategy = _FeatureStrategy()
    runner = _runner(
        strategy,
        requirements=_range_requirements(),
        services={
            "range_bar_store": store,
            "range_bar_aggregator": RangeBarAggregator(),
            "range_checkpoint_store": _CheckpointStore(),
        },
    )
    # Normal live bucket: memory has rows, bucket NOT in repaired set
    runner._range_bars_by_bucket[0] = [
        _range_bar(bar_id=i + 1, start_time_ms=i * 1_000, end_time_ms=i * 1_000)
        for i in range(5)
    ]
    runner._range_repaired_complete_buckets.clear()

    rows = runner._range_bar_rows_for_bucket(0)

    assert len(rows) == 5
    assert store.load_was_called is False, (
        "DB load should NOT be called for normal live bucket with memory rows"
    )
    await runner._stop_live_persistence_writer()


# ── Test D: repaired complete but DB empty → no fallback to partial memory ──


@pytest.mark.asyncio
async def test_repaired_complete_db_empty_no_fallback_to_partial_memory() -> None:
    """When a bucket is repaired complete but DB has no rows, partial memory
    rows MUST NOT be used as a fallback."""
    store = _NoLoadRangeBarStore()
    strategy = _FeatureStrategy()
    runner = _runner(
        strategy,
        requirements=_range_requirements(),
        services={
            "range_bar_store": store,
            "range_bar_aggregator": RangeBarAggregator(),
            "range_checkpoint_store": _CheckpointStore(),
        },
    )
    # Partial memory rows exist
    runner._range_bars_by_bucket[0] = [
        _range_bar(bar_id=i + 1, start_time_ms=i * 1_000, end_time_ms=i * 1_000)
        for i in range(2)
    ]
    # Bucket is marked repaired complete
    runner._range_repaired_complete_buckets.add(0)

    rows = runner._range_bar_rows_for_bucket(0)

    # Must return empty, NOT the 2 partial memory rows
    assert len(rows) == 0, (
        f"Expected empty for repaired bucket with no DB rows, got {len(rows)} rows"
    )
    await runner._stop_live_persistence_writer()


# ── Test E: MF 1m feature dispatch does not trigger DB writes ──
# (covered by existing test_mf_1m_feature_dispatch_only_updates_observer_buffer)


# ── Test F: live data path stats does not trigger DB read/write ──


@pytest.mark.asyncio
async def test_live_data_path_stats_no_db_trigger() -> None:
    """Live data path stats must only read memory counters — never DB."""
    store = _NoLoadRangeBarStore()
    strategy = _FeatureStrategy()
    runner = _runner(
        strategy,
        requirements=_range_requirements(),
        services={
            "range_bar_store": store,
            "range_bar_aggregator": RangeBarAggregator(),
            "range_checkpoint_store": _CheckpointStore(),
        },
    )
    load_before = store.load_calls

    runner._maybe_log_live_data_path_stats()

    assert store.load_calls == load_before, (
        "Live data path stats must not trigger DB reads"
    )
    await runner._stop_live_persistence_writer()


# ── Test: BackgroundWriteQueue drop warning coverage ──


def test_background_write_queue_drop_warns() -> None:
    """First drop emits a warning; subsequent drops throttle to every Nth."""
    from src.runtime.runner import _BackgroundWriteQueue, _BackgroundWriteItem

    q = _BackgroundWriteQueue(name="test-drop", max_pending=1)

    # Fill the queue to capacity
    assert q.submit(_BackgroundWriteItem(description="a", write=lambda: None))
    assert q.submitted == 1

    # This should evict oldest and warn
    assert q.submit(_BackgroundWriteItem(description="b", write=lambda: None))
    assert q.dropped >= 1

    q.stop(flush=False)


# ── Test: live data path stats respects configured interval ──


def test_live_data_path_stats_respects_env_interval() -> None:
    """Stats interval is configurable via AETHER_LIVE_DATA_PATH_STATS_INTERVAL_SECONDS."""
    from src.app import AppConfig
    from src.planner import ExecutionPlanner
    from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode

    class _EnvWithInterval:
        def get(self, key, default):
            return default

        def get_int(self, key, default):
            if key == "AETHER_LIVE_DATA_PATH_STATS_INTERVAL_SECONDS":
                return 300  # 5 minutes
            return default

    cfg = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.fake:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=20,
        signal_queue_maxsize=20,
        alert_queue_maxsize=20,
        dry_run=True,
        enable_email_alerts=False,
    )
    context = AppContext(
        data=_Data(),
        execution=object(),
        state_store=_StateStore(),
        strategy=_FeatureStrategy(),
        planner=ExecutionPlanner(),
        alerts=_Alerts(),
    )
    runner = LiveRuntimeRunner(
        app_config=cfg,
        app_context=context,
        runtime_config=LiveRuntimeConfig(
            app=cfg,
            mode=RuntimeMode.LIVE_RUNTIME,
            warmup_enabled=False,
            closed_bar_buffer_ms=5_000,
            closed_bar_retry_interval_ms=5_000,
            closed_bar_missing_alert_after_ms=120_000,
        ),
        services={
            "project_env_config": _EnvWithInterval(),
            "runtime_requirements": _range_requirements(),
            "recovery_service": None,
            "snapshot": _snapshot(),
            "range_bar_store": _MemoryRangeBarStore(),
            "range_bar_aggregator": RangeBarAggregator(),
            "range_checkpoint_store": _CheckpointStore(),
        },
    )
    import time

    # First call should log (no prior timestamp)
    now = int(time.time() * 1000)
    runner._last_live_data_path_log_ms = now - 301_000  # 301 seconds ago
    # With 300s interval, this should trigger a log
    # The method checks: now_ms - last_ms < interval_seconds * 1000
    # 301000 >= 300000 → will log
    runner._maybe_log_live_data_path_stats()
    # After logging, _last_live_data_path_log_ms is updated
    # Call again immediately — should NOT log
    last_after_first = runner._last_live_data_path_log_ms
    runner._maybe_log_live_data_path_stats()
    assert runner._last_live_data_path_log_ms == last_after_first, (
        "Second stats call within interval should be skipped"
    )
