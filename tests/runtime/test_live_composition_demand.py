from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.app import AppConfig, AppContext
from src.platform import ExchangeName
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.markets import get_market_profile
from src.platform.state.sqlite_store import SqliteStateStore
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime.composition import compose_live_runtime
from src.runtime.market_data.range_config import RangeRuntimeConfig
from src.runtime.market_data.range_integrity import RangeBucketIntegrityStatus
from src.runtime.market_data.runtime import MarketDataRuntimeError
from src.runtime.requirements import (
    AccountStateRequirement,
    ClosedKlineRequirement,
    OrderBookRequirement,
    OrderStateRequirement,
    RangeBarRequirement,
    StrategyRuntimeRequirements,
    TradeStreamRequirement,
)
from src.runtime.services import RuntimeServices


class _IdleTradeStream:
    async def stream_trades(self):
        await asyncio.Event().wait()
        if False:
            yield None


class _IdleOrderBookStream:
    async def stream_order_book(self):
        await asyncio.Event().wait()
        if False:
            yield None


class _ControlledTradeStream:
    def __init__(self) -> None:
        self.queue = asyncio.Queue()

    async def stream_trades(self):
        while True:
            yield await self.queue.get()


def _trade(trade_id: str, event_ms: int) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id=trade_id,
        trade_time_ms=event_ms,
        event_time_ms=event_ms,
    )


class _Strategy:
    def __init__(self, *, trade_features=False) -> None:
        self.trade_features = trade_features
        self.config_reads = 0

    def trade_feature_runtime_config(self):
        self.config_reads += 1
        return (
            dict(self.trade_features)
            if isinstance(self.trade_features, dict)
            else {"enabled": self.trade_features}
        )


class _FeatureObserverStrategy(_Strategy):
    observer_id = "startup-feature-observer"
    enabled = True

    def __init__(self) -> None:
        super().__init__(
            trade_features={"fixed_time_trade_bars_enabled": True}
        )
        self.events = []

    def market_feature_observers(self):
        return (self,)

    async def on_market_feature(self, event):
        self.events.append(event)
        return ()


class _Alerts:
    def start(self) -> None:
        return None

    def emit(self, _alert) -> None:
        return None

    async def stop(self) -> None:
        return None


def _app_config(tmp_path) -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="tests.fake:Strategy",
        data_streams=(),
        state_db_path=str(tmp_path / "state.sqlite3"),
        market_queue_maxsize=8,
        signal_queue_maxsize=8,
        alert_queue_maxsize=8,
        dry_run=True,
        enable_email_alerts=False,
    )


def _context(strategy: _Strategy, state_store=None) -> AppContext:
    return AppContext(
        data=SimpleNamespace(
            market_profile=get_market_profile("ETH-USDT-PERP")
        ),
        execution=object(),
        state_store=state_store or object(),
        strategy=strategy,
        planner=object(),
        alerts=_Alerts(),
    )


def _requirements(
    *,
    trades: bool = False,
    order_book: bool = False,
    range_bars: bool = False,
    closed_kline: bool = False,
) -> StrategyRuntimeRequirements:
    return StrategyRuntimeRequirements(
        trades=TradeStreamRequirement(
            enabled=trades,
            stream_enabled=trades,
        ),
        order_book=OrderBookRequirement(
            enabled=order_book,
            stream_enabled=order_book,
        ),
        range_bars=RangeBarRequirement(enabled=range_bars),
        closed_kline=ClosedKlineRequirement(
            enabled=closed_kline,
            interval="4h",
        ),
        account_state=AccountStateRequirement(
            startup_snapshot_enabled=False,
            poll_enabled=False,
        ),
        order_state=OrderStateRequirement(
            post_submit_sync_enabled=False,
            poll_when_position_enabled=False,
        ),
    )


def _compose(
    tmp_path,
    requirements,
    strategy,
    *,
    state_store=None,
    range_config=None,
    services=None,
):
    app_config = _app_config(tmp_path)
    runtime_services = services or RuntimeServices()
    runtime_services.runtime_requirements = requirements
    return compose_live_runtime(
        app_config,
        app_context=_context(strategy, state_store),
        runtime_config=LiveRuntimeConfig(
            app=app_config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        range_config=range_config,
        services=runtime_services,
    )


@pytest.mark.parametrize(
    ("requirements", "trade_features", "uses_trade"),
    [
        (_requirements(closed_kline=True), False, False),
        (_requirements(trades=True, closed_kline=True), False, True),
        (
            _requirements(closed_kline=True),
            {
                "fixed_time_trade_bars_enabled": True,
                "trade_footprint_enabled": False,
                "range_footprint_enabled": False,
            },
            True,
        ),
        (
            _requirements(closed_kline=True),
            {
                "fixed_time_trade_bars_enabled": False,
                "trade_footprint_enabled": True,
                "range_footprint_enabled": False,
            },
            True,
        ),
        (
            _requirements(closed_kline=True),
            {
                "fixed_time_trade_bars_enabled": False,
                "trade_footprint_enabled": False,
                "range_footprint_enabled": True,
            },
            True,
        ),
        (_requirements(range_bars=True, closed_kline=True), False, True),
    ],
)
def test_closed_bar_integrity_follows_resolved_trade_pipeline(
    tmp_path, requirements, trade_features, uses_trade
) -> None:
    application = _compose(
        tmp_path, requirements, _Strategy(trade_features=trade_features)
    )

    assert (
        application.runner.runtime_services.trade_data_integrity_tracker
        is not None
    ) is uses_trade
    assert (
        application.runner.runtime_services.market_event_processor is not None
    ) is uses_trade


@pytest.mark.asyncio
async def test_feature_only_startup_partial_survives_repeated_restart(
    tmp_path, monkeypatch
) -> None:
    bucket_start = 1_800_000_000_000
    state_path = tmp_path / "state.sqlite3"
    monkeypatch.setattr(
        "src.runtime.composition.create_trade_stream",
        lambda *_args, **_kwargs: _IdleTradeStream(),
    )
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        lambda *_args, **_kwargs: _IdleOrderBookStream(),
    )
    requirements = _requirements(closed_kline=True)

    first = _compose(
        tmp_path,
        requirements,
        _Strategy(trade_features=True),
        state_store=SqliteStateStore(state_path),
    )
    monkeypatch.setattr(
        "src.runtime.components.closed_bar.time.time",
        lambda: (bucket_start + 60_000) / 1000,
    )
    await first.market_data.prepare(first.runner._market_data_capabilities)
    await first.market_data.stop()

    restarted = _compose(
        tmp_path,
        requirements,
        _Strategy(trade_features=True),
        state_store=SqliteStateStore(state_path),
    )
    monkeypatch.setattr(
        "src.runtime.components.closed_bar.time.time",
        lambda: (bucket_start + 120_000) / 1000,
    )
    await restarted.market_data.prepare(
        restarted.runner._market_data_capabilities
    )
    tracker = restarted.runner.runtime_services.trade_data_integrity_tracker

    assert tracker.dropped_count == 0
    assert "startup_partial_trade_window" in (
        tracker.invalid_reason(bucket_start, bucket_start + 14_400_000 - 1)
        or ""
    )
    await restarted.market_data.stop()


@pytest.mark.parametrize(
    ("requirements", "expected"),
    [
        (_requirements(), ()),
        (_requirements(trades=True), ("trade-stream",)),
        (_requirements(order_book=True), ("order-book-stream",)),
        (
            _requirements(range_bars=True),
            ("trade-stream", "range-bars"),
        ),
    ],
)
def test_formal_composition_resolves_exact_demand_without_opening_streams(
    tmp_path,
    monkeypatch,
    requirements,
    expected,
) -> None:
    created = {"trades": 0, "books": 0}

    def trade_factory(*_args, **_kwargs):
        created["trades"] += 1
        return _IdleTradeStream()

    def book_factory(*_args, **_kwargs):
        created["books"] += 1
        return _IdleOrderBookStream()

    monkeypatch.setattr("src.runtime.composition.create_trade_stream", trade_factory)
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        book_factory,
    )
    strategy = _Strategy()
    application = _compose(tmp_path, requirements, strategy)
    plan = application.market_data.plan(
        application.runner._market_data_capabilities
    )

    assert plan.module_ids == expected
    assert created == {"trades": 0, "books": 0}
    assert application.market_data.state().plan is None
    assert strategy.config_reads == 1


@pytest.mark.asyncio
async def test_formal_trade_feed_disables_transparent_reconnect(
    tmp_path, monkeypatch
) -> None:
    observed = {}

    def trade_factory(*_args, **kwargs):
        observed.update(kwargs)
        return _IdleTradeStream()

    monkeypatch.setattr("src.runtime.composition.create_trade_stream", trade_factory)
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        lambda *_args, **_kwargs: _IdleOrderBookStream(),
    )
    application = _compose(tmp_path, _requirements(trades=True), _Strategy())
    await application.market_data.start(application.runner._market_data_capabilities)
    assert observed["reconnect"] is False
    assert observed["max_reconnects"] == 0
    await application.market_data.stop()


@pytest.mark.asyncio
async def test_formal_shared_features_use_one_trade_stream_and_shutdown_cleanly(
    tmp_path,
    monkeypatch,
) -> None:
    created = {"trades": 0, "books": 0}

    def trade_factory(*_args, **_kwargs):
        created["trades"] += 1
        return _IdleTradeStream()

    def book_factory(*_args, **_kwargs):
        created["books"] += 1
        return _IdleOrderBookStream()

    monkeypatch.setattr("src.runtime.composition.create_trade_stream", trade_factory)
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        book_factory,
    )
    strategy = _Strategy(trade_features=True)
    application = _compose(tmp_path, _requirements(), strategy)
    current = asyncio.current_task()

    plan = await application.market_data.start(
        application.runner._market_data_capabilities
    )
    assert plan.module_ids == (
        "trade-stream",
        "fixed-time-trade-bars",
        "range-footprint",
        "trade-footprint",
    )
    assert created == {"trades": 1, "books": 0}
    assert len(
        [module_id for module_id in plan.module_ids if module_id == "trade-stream"]
    ) == 1

    await application.market_data.stop()

    assert application.market_data.state().started_module_ids == ()
    leaked = [
        task
        for task in asyncio.all_tasks()
        if task is not current and not task.done()
    ]
    assert leaked == []
    assert strategy.config_reads == 1


@pytest.mark.asyncio
async def test_empty_formal_composition_starts_no_market_resources(
    tmp_path,
    monkeypatch,
) -> None:
    trade_factory = SimpleNamespace(calls=0)
    book_factory = SimpleNamespace(calls=0)

    def create_trade(*_args, **_kwargs):
        trade_factory.calls += 1
        return _IdleTradeStream()

    def create_book(*_args, **_kwargs):
        book_factory.calls += 1
        return _IdleOrderBookStream()

    monkeypatch.setattr("src.runtime.composition.create_trade_stream", create_trade)
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        create_book,
    )
    application = _compose(tmp_path, _requirements(), _Strategy())

    plan = await application.market_data.start(
        application.runner._market_data_capabilities
    )
    assert plan.module_ids == ()
    assert application.market_data.state().health == ()
    assert trade_factory.calls == 0
    assert book_factory.calls == 0

    await application.market_data.stop()
    assert application.market_data.state().plan is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("feature_config", "expected_module"),
    [
        ({"fixed_time_trade_bars_enabled": True}, "fixed-time-trade-bars"),
        ({"trade_footprint_enabled": True}, "trade-footprint"),
        ({"range_footprint_enabled": True}, "range-footprint"),
    ],
)
async def test_formal_composition_instantiates_only_selected_trade_feature(
    tmp_path,
    monkeypatch,
    feature_config,
    expected_module,
) -> None:
    created = {"trades": 0, "books": 0}

    def create_trade(*_args, **_kwargs):
        created["trades"] += 1
        return _IdleTradeStream()

    def create_book(*_args, **_kwargs):
        created["books"] += 1
        return _IdleOrderBookStream()

    monkeypatch.setattr("src.runtime.composition.create_trade_stream", create_trade)
    monkeypatch.setattr("src.runtime.composition.create_order_book_stream", create_book)
    application = _compose(
        tmp_path,
        _requirements(),
        _Strategy(trade_features=feature_config),
    )
    plan = await application.market_data.start(
        application.runner._market_data_capabilities
    )

    assert plan.module_ids == ("trade-stream", expected_module)
    processor = application.market_data._event_processor
    assert processor is not None
    assert tuple(module.module_id for module in processor._modules) == (
        expected_module,
    )
    assert created == {"trades": 1, "books": 0}
    await application.market_data.stop()


@pytest.mark.asyncio
async def test_closed_kline_only_creates_no_trade_processor_or_source(
    tmp_path,
    monkeypatch,
) -> None:
    created = {"trades": 0, "books": 0}

    def create_trade(*_args, **_kwargs):
        created["trades"] += 1
        return _IdleTradeStream()

    def create_book(*_args, **_kwargs):
        created["books"] += 1
        return _IdleOrderBookStream()

    monkeypatch.setattr("src.runtime.composition.create_trade_stream", create_trade)
    monkeypatch.setattr("src.runtime.composition.create_order_book_stream", create_book)
    application = _compose(
        tmp_path,
        _requirements(closed_kline=True),
        _Strategy(),
    )
    plan = await application.market_data.start(
        application.runner._market_data_capabilities
    )

    assert plan.module_ids == ()
    assert application.market_data._event_processor is None
    assert created == {"trades": 0, "books": 0}
    await application.market_data.stop()


@pytest.mark.asyncio
async def test_delayed_first_trade_seals_startup_gap_and_recovers_features(
    tmp_path, monkeypatch
) -> None:
    bucket_start = 1_800_000_000_000
    stream = _ControlledTradeStream()
    monkeypatch.setattr(
        "src.runtime.composition.create_trade_stream",
        lambda *_args, **_kwargs: stream,
    )
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        lambda *_args, **_kwargs: _IdleOrderBookStream(),
    )
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    strategy = _FeatureObserverStrategy()
    application = _compose(
        tmp_path,
        _requirements(),
        strategy,
        state_store=store,
    )
    monkeypatch.setattr(
        "src.runtime.components.closed_bar.time.time",
        lambda: (bucket_start + 178_000) / 1000,
    )
    await application.market_data.prepare(
        application.runner._market_data_capabilities
    )
    tracker = application.runner.runtime_services.trade_data_integrity_tracker
    repair_token = tracker.revision

    startup_started = asyncio.get_running_loop().time()
    await application.market_data.start_prepared()
    startup_duration_ms = (
        asyncio.get_running_loop().time() - startup_started
    ) * 1000

    for index, event_ms in enumerate(
        (
            bucket_start + 185_000,
            bucket_start + 245_000,
            bucket_start + 305_000,
        ),
        start=1,
    ):
        await stream.queue.put(_trade(f"startup-{index}", event_ms))
        processor = application.market_data._event_processor
        await asyncio.wait_for(
            _wait_until(lambda: processor.stats.trades_processed >= index),
            timeout=1,
        )

    fixed_time = next(
        module
        for module in application.market_data._host.modules
        if module.module_id == "fixed-time-trade-bars"
    )
    durable = store.load_trade_integrity_windows(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
    )

    assert tracker.revision == repair_token == 1
    assert tracker.dropped_count == 0
    assert durable == [{
        "start_ms": bucket_start,
        "end_ms": bucket_start + 185_000,
        "last_issue_revision": 1,
        "repaired_through_revision": 0,
        "reason": "startup_partial_trade_window",
        "complete": False,
    }]
    assert fixed_time.features_suppressed == 1
    assert fixed_time.features_emitted == 1
    assert [event.data["open_time_ms"] for event in strategy.events] == [
        bucket_start + 240_000
    ]
    assert tracker.invalid_reason(
        bucket_start, bucket_start + 14_400_000 - 1
    ) is not None
    assert (
        application.runner._heartbeat_service.build().last_market_event_ms
        == bucket_start + 305_000
    )
    print(
        "startup_integrity_smoke "
        f"startup_duration_ms={startup_duration_ms:.6f} "
        "startup_partial_duration_ms=7000 "
        "range_processing_ms=0 "
        "repair_maintenance_ms=0 "
        f"feature_processing_ms="
        f"{processor.stats.module_timings['fixed-time-trade-bars']:.6f}"
    )
    await application.market_data.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundaries_crossed", [1, 2])
async def test_startup_gap_is_split_into_one_durable_row_per_4h_bucket(
    tmp_path, monkeypatch, boundaries_crossed
) -> None:
    bucket_start = 1_800_000_000_000
    interval_ms = 4 * 60 * 60_000
    stream = _ControlledTradeStream()
    monkeypatch.setattr(
        "src.runtime.composition.create_trade_stream",
        lambda *_args, **_kwargs: stream,
    )
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        lambda *_args, **_kwargs: _IdleOrderBookStream(),
    )
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    application = _compose(
        tmp_path,
        _requirements(),
        _FeatureObserverStrategy(),
        state_store=store,
    )
    monkeypatch.setattr(
        "src.runtime.components.closed_bar.time.time",
        lambda: (bucket_start + interval_ms - 10_000) / 1000,
    )
    await application.market_data.start(
        application.runner._market_data_capabilities
    )

    first_trade_ms = (
        bucket_start + boundaries_crossed * interval_ms + 10_000
    )
    await stream.queue.put(_trade("first-live", first_trade_ms))
    processor = application.market_data._event_processor
    await asyncio.wait_for(
        _wait_until(lambda: processor.stats.trades_processed == 1),
        timeout=1,
    )
    rows = store.load_trade_integrity_windows(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
    )

    assert rows == [
        {
            "start_ms": bucket_start + offset * interval_ms,
            "end_ms": (
                first_trade_ms
                if offset == boundaries_crossed
                else bucket_start + (offset + 1) * interval_ms - 1
            ),
            "last_issue_revision": offset + 1,
            "repaired_through_revision": 0,
            "reason": "startup_partial_trade_window",
            "complete": False,
        }
        for offset in range(boundaries_crossed + 1)
    ]
    assert processor.stats.trades_processed == 1
    await application.market_data.stop()


@pytest.mark.asyncio
async def test_cross_bucket_startup_keeps_old_range_repair_token_independent(
    tmp_path, monkeypatch
) -> None:
    bucket_start = 1_800_000_000_000
    interval_ms = 4 * 60 * 60_000
    stream = _ControlledTradeStream()
    monkeypatch.setattr(
        "src.runtime.composition.create_trade_stream",
        lambda *_args, **_kwargs: stream,
    )
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        lambda *_args, **_kwargs: _IdleOrderBookStream(),
    )
    now_seconds = (bucket_start + interval_ms - 10_000) / 1000
    monkeypatch.setattr(
        "src.runtime.components.closed_bar.time.time",
        lambda: now_seconds,
    )
    monkeypatch.setattr(
        "src.runtime.market_data.range_module.time.time",
        lambda: now_seconds,
    )
    range_config = RangeRuntimeConfig(
        checkpoint_db_path=str(tmp_path / "range.sqlite3"),
        micro_repair_enabled=False,
        repair_journal_enabled=False,
        repair_journal_db=str(tmp_path / "repair.sqlite3"),
        speed_refresh_enabled=False,
        backfill_enabled=False,
        market_data_db_path=str(tmp_path / "market.sqlite3"),
    )
    class RangeStore:
        def load(self, **_kwargs):
            return []

        def save(self, rows):
            return len(rows)

    services = RuntimeServices(range_bar_store=RangeStore())
    application = _compose(
        tmp_path,
        _requirements(range_bars=True),
        _Strategy(),
        state_store=SqliteStateStore(tmp_path / "state.sqlite3"),
        range_config=range_config,
        services=services,
    )
    await application.market_data.prepare(
        application.runner._market_data_capabilities
    )
    range_module = application.runner.runtime_services.range_bar_module
    tracker = application.runner.runtime_services.trade_data_integrity_tracker
    old_token = range_module.begin_repair(bucket_start)
    await application.market_data.start_prepared()

    new_bucket = bucket_start + interval_ms
    await stream.queue.put(_trade("first-live", new_bucket + 10_000))
    processor = application.market_data._event_processor
    await asyncio.wait_for(
        _wait_until(lambda: processor.stats.trades_processed == 1),
        timeout=1,
    )

    old_state = range_module.bucket_integrity(bucket_start)
    new_state = range_module.bucket_integrity(new_bucket)
    assert old_state.status is RangeBucketIntegrityStatus.REPAIRING
    assert old_state.repair_started_revision == old_token == 1
    assert new_state.status is RangeBucketIntegrityStatus.DEGRADED
    assert new_state.last_issue_revision == 2
    assert range_module.mark_repaired(
        bucket_start,
        through_revision=old_token,
    )
    assert range_module.bucket_integrity(new_bucket).status is (
        RangeBucketIntegrityStatus.DEGRADED
    )
    assert tracker.invalid_reason(
        new_bucket,
        new_bucket + interval_ms - 1,
    ) is not None
    await application.market_data.stop()


async def _wait_until(predicate) -> None:
    while not predicate():
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_formal_composition_processes_finite_trade_smoke(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "src.runtime.components.closed_bar.time.time",
        lambda: 0,
    )
    received = asyncio.Event()
    hold = asyncio.Event()
    calls = []
    trade = MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id="smoke-1",
        trade_time_ms=100,
        event_time_ms=100,
    )

    class FiniteTradeStream:
        async def stream_trades(self):
            yield trade
            await hold.wait()

    class SmokeStrategy(_Strategy):
        async def on_trade(self, event):
            calls.append(event.trade_id)
            received.set()
            return ()

    monkeypatch.setattr(
        "src.runtime.composition.create_trade_stream",
        lambda *_args, **_kwargs: FiniteTradeStream(),
    )
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        lambda *_args, **_kwargs: _IdleOrderBookStream(),
    )
    strategy = SmokeStrategy()
    application = _compose(
        tmp_path,
        _requirements(trades=True),
        strategy,
    )

    await application.market_data.start(
        application.runner._market_data_capabilities
    )
    await asyncio.wait_for(received.wait(), timeout=1)
    await application.market_data.stop()

    assert calls == ["smoke-1"]
    assert application.runner.stats.market_events_seen == 1


@pytest.mark.asyncio
async def test_formal_runtime_exits_when_trade_source_fails(
    tmp_path,
    monkeypatch,
) -> None:
    class FailingTradeStream:
        async def stream_trades(self):
            raise RuntimeError("injected formal source failure")
            if False:
                yield None

    monkeypatch.setattr(
        "src.runtime.composition.create_trade_stream",
        lambda *_args, **_kwargs: FailingTradeStream(),
    )
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        lambda *_args, **_kwargs: _IdleOrderBookStream(),
    )
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    strategy = _FeatureObserverStrategy()
    application = _compose(
        tmp_path,
        _requirements(),
        strategy,
        state_store=store,
    )
    bucket_start = 1_800_000_000_000
    monkeypatch.setattr(
        "src.runtime.components.closed_bar.time.time",
        lambda: (bucket_start + 120_000) / 1000,
    )

    async def no_op() -> None:
        return None

    application.runner._startup = no_op
    application.runner._start_producers = lambda: []
    application.runner._start_sync_tasks = lambda: []
    application.runner._stop_producers = no_op
    application.runner._stop_sync_tasks = no_op
    application.runner._stop_live_persistence_writer = no_op

    with pytest.raises(
        MarketDataRuntimeError,
        match="injected formal source failure",
    ):
        await application.run()

    rows = store.load_trade_integrity_windows(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
    )
    startup_rows = [
        row
        for row in rows
        if row["reason"] == "startup_partial_trade_window"
    ]
    assert startup_rows == [{
        "start_ms": bucket_start,
        "end_ms": bucket_start + 120_000,
        "last_issue_revision": 1,
        "repaired_through_revision": 0,
        "reason": "startup_partial_trade_window",
        "complete": False,
    }]
    assert strategy.events == []


@pytest.mark.asyncio
async def test_formal_runtime_exits_when_feature_handler_fails(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "src.runtime.components.closed_bar.time.time",
        lambda: 0,
    )
    hold = asyncio.Event()
    trade = MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id="feature-failure",
        trade_time_ms=100,
        event_time_ms=100,
    )

    class OneTradeStream:
        async def stream_trades(self):
            yield trade
            await hold.wait()

    class BrokenRangeFootprintBuilder:
        def __init__(self, **_kwargs) -> None:
            pass

        def on_trade(self, _trade):
            raise RuntimeError("injected feature failure")

    monkeypatch.setattr(
        "src.runtime.composition.create_trade_stream",
        lambda *_args, **_kwargs: OneTradeStream(),
    )
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        lambda *_args, **_kwargs: _IdleOrderBookStream(),
    )
    monkeypatch.setattr(
        "src.runtime.market_data.features.RangeFootprintBuilder",
        BrokenRangeFootprintBuilder,
    )
    application = _compose(
        tmp_path,
        _requirements(),
        _Strategy(trade_features=True),
    )

    async def no_op() -> None:
        return None

    application.runner._startup = no_op
    application.runner._start_producers = lambda: []
    application.runner._start_sync_tasks = lambda: []
    application.runner._stop_producers = no_op
    application.runner._stop_sync_tasks = no_op
    application.runner._stop_live_persistence_writer = no_op

    with pytest.raises(MarketDataRuntimeError, match="injected feature failure"):
        await application.run()
