"""Arch-04 State Ownership: tests for the shared-mutable-state fixes.

Each section verifies that exactly ONE canonical owner exists for each
piece of shared mutable state and that all other components read it
through the shared source (context / port) rather than a stale local copy.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from src.app import AppConfig, AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.planner import ExecutionPlanner
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime.components import (
    PersistenceComponent,
    StartupComponent,
)
from src.runtime.context import RuntimeContext
from src.runtime.health_state import RuntimeHealthState
from src.runtime.models import RuntimeHealth, RuntimePhase
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeRunner
from src.market_data.events import MarketFeatureEvent
from src.order_management.position_plan.models import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    PositionPlan,
    PositionPlanStatus,
)
from src.order_management.position_plan.store import SqlitePositionPlanStore
from src.signals import TradeSignal
from src.signals.models import SignalAction
from tests.runtime.test_live_runtime_runner import (
    FakeData,
    FakeExecution,
    FakeStateStore,
    FakeStrategy,
    _app_config,
)


# ═══════════════════════════════════════════════════════════════════════════
# 3.4  Managed Market Modules
# ═══════════════════════════════════════════════════════════════════════════


def _runner_managed_modules() -> LiveRuntimeRunner:
    config = _app_config()
    runner = LiveRuntimeRunner(
        app_config=config,
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        app_context=AppContext(
            data=FakeData(),
            execution=FakeExecution(),
            state_store=FakeStateStore(),
            strategy=FakeStrategy(),
            planner=ExecutionPlanner(),
            alerts=AsyncAlertDispatcher(NoopAlertSink()),
        ),
    )
    runner.runtime_state.market.modules_managed = True
    # Bypass frozen dataclass: use object.__setattr__
    object.__setattr__(runner.requirements.order_book, "enabled", True)
    object.__setattr__(runner.requirements.order_book, "stream_enabled", True)
    return runner


def test_managed_order_book_skips_legacy_producer() -> None:
    """When market modules are managed, _start_producers must NOT create a
    legacy order_book producer task."""
    runner = _runner_managed_modules()

    supervisor = MagicMock()
    runner.lifecycle._producer_supervisor = supervisor

    tasks = runner.lifecycle._start_producers()

    assert tasks == []
    supervisor.run_resilient_stream.assert_not_called()


def test_managed_range_uses_initial_recovery_not_initialize() -> None:
    """When modules are managed, Startup reads initial_recovery directly;
    it does NOT call initialize_recovery(), start checkpoint_writer, or
    repair_now()."""
    runner = _runner_managed_modules()
    object.__setattr__(runner.requirements.range_bars, "enabled", True)

    module = MagicMock()
    module.initial_recovery = MagicMock()
    module.initialize_recovery = MagicMock(
        side_effect=AssertionError("must not call")
    )
    module.checkpoint_writer = MagicMock()
    module.checkpoint_writer.start = MagicMock(
        side_effect=AssertionError("must not call")
    )
    module.repair_now = MagicMock(
        side_effect=AssertionError("must not call")
    )
    module._initial_bucket_ms = 0
    module.trust_start_bucket_ms = 0

    runner.startup.ports = SimpleNamespace(
        require_range_module=lambda: module,
        set_health=lambda *a, **k: None,
    )

    runner.startup._initialize_rangebar_trust_window()

    module.initialize_recovery.assert_not_called()
    module.checkpoint_writer.start.assert_not_called()
    module.repair_now.assert_not_called()


def test_managed_range_warmup_skips_live_warmup_call() -> None:
    """When modules are managed, _warmup_range_speed_history returns
    complete_history without calling warmup()."""
    runner = _runner_managed_modules()

    warmup = MagicMock()
    warmup.complete_history = 42
    warmup.warmup = AsyncMock(side_effect=AssertionError("must not call"))
    runner.startup._range_speed_warmup = warmup

    result = asyncio.run(runner.startup._warmup_range_speed_history())

    assert result == 42
    warmup.warmup.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
# 4.4  Runtime Health – Single Source of Truth
# ═══════════════════════════════════════════════════════════════════════════


def test_health_phase_preserved_after_startup_adds_metadata() -> None:
    """When a component adds metadata via set_health (which delegates to
    RuntimeHealthState.update), the phase must not revert to an earlier
    value.  This simulates what happens when Startup calls set_health
    with position_mode_requirements while Lifecycle has already entered
    WARMING_UP."""
    initial = RuntimeHealth(
        phase=RuntimePhase.WARMING_UP,
        healthy=True,
        metadata={"source": "enter_warming_up"},
    )
    state = RuntimeHealthState(initial)

    # Simulate Startup adding position_mode_requirements (as it would via
    # ports.set_health → lifecycle._set_health → state.update)
    state.update(
        RuntimePhase.WARMING_UP,
        metadata={
            **dict(state.current.metadata),
            "position_mode_requirements": {"ok": True, "strategy": "test"},
        },
    )

    current = state.current
    assert current.phase is RuntimePhase.WARMING_UP, (
        f"Expected WARMING_UP, got {current.phase}"
    )
    assert "position_mode_requirements" in current.metadata
    assert "source" in current.metadata  # preserved from initial


def test_health_metadata_accumulates_not_overwrites() -> None:
    """Sequential set_health calls by different components must preserve
    earlier metadata and only add new keys."""

    initial = RuntimeHealth(
        phase=RuntimePhase.WARMING_UP,
        healthy=True,
        metadata={},
    )
    state = RuntimeHealthState(initial)
    ctx = RuntimeContext()
    ctx.resources.lifecycle.health_state = state

    # Simulate Startup adding position_mode_requirements
    state.update(
        RuntimePhase.WARMING_UP,
        metadata={"position_mode_requirements": {"ok": True}},
    )
    # Simulate lifecycle adding feature_backfill_results
    state.update(
        RuntimePhase.WARMING_UP,
        metadata={
            **dict(state.current.metadata),
            "feature_backfill_results": {"mf": {"action": "none"}},
        },
    )
    # Simulate lifecycle recording a recovery result
    state.update(
        RuntimePhase.WARMING_UP,
        metadata={
            **dict(state.current.metadata),
            "recovery_result": {"recovered": True},
        },
    )

    # Now MarketEvents processes one event — it must preserve ALL prior keys
    current = state.current
    state.update(
        RuntimePhase.RUNNING,
        healthy=current.healthy,
        last_market_event_time_ms=123456,
        metadata={
            **dict(current.metadata),
            "last_event_type": "trade",
        },
    )

    final = state.current
    assert "feature_backfill_results" in final.metadata
    assert "position_mode_requirements" in final.metadata
    assert "recovery_result" in final.metadata
    assert final.metadata["last_event_type"] == "trade"
    assert final.phase is RuntimePhase.RUNNING


def test_health_metadata_survives_partial_execution_failure() -> None:
    """After a partial execution failure, existing metadata must be
    preserved and only partial_failures added."""
    initial = RuntimeHealth(
        phase=RuntimePhase.RUNNING,
        healthy=True,
        metadata={
            "position_mode_requirements": {"ok": True},
            "feature_backfill_results": {"mf": {"action": "none"}},
        },
    )
    state = RuntimeHealthState(initial)
    ctx = RuntimeContext()
    ctx.resources.lifecycle.health_state = state

    # Simulate partial failure
    current = state.current
    state.update(
        RuntimePhase.RUNNING,
        healthy=False,
        error="partial exchange execution failure",
        metadata={
            **dict(current.metadata),
            "partial_failures": 1,
        },
    )

    final = state.current
    assert "position_mode_requirements" in final.metadata
    assert "feature_backfill_results" in final.metadata
    assert final.metadata["partial_failures"] == 1
    assert final.healthy is False
    assert final.phase is RuntimePhase.RUNNING


# ═══════════════════════════════════════════════════════════════════════════
# 5.3  PositionPlanStore – Persistence as Sole Owner
# ═══════════════════════════════════════════════════════════════════════════


def test_account_resolves_position_plan_store_through_port(tmp_path: Path) -> None:
    """Account must obtain the store via self.ports.get_position_plan_store(),
    not a locally cached copy."""
    ctx = RuntimeContext()

    db_path = tmp_path / "test_plan_store.sqlite3"
    store = SqlitePositionPlanStore(str(db_path))

    persistence = PersistenceComponent(ctx)
    persistence._position_plan_store = store

    from src.runtime.components.account import AccountComponent
    account = AccountComponent(ctx)
    account.ports = SimpleNamespace(
        get_position_plan_store=lambda: persistence._get_position_plan_store(),
        execute_signals=AsyncMock(),
        get_execution_clients=lambda: (),
        get_order_journal=lambda: None,
        strategy_pending_work_provider=lambda: None,
        strategy_position_index=lambda: SimpleNamespace(active=False),
        has_unresolved_follower_close=lambda: False,
        has_account_config_entry_block=lambda: False,
    )

    resolved = account.ports.get_position_plan_store()
    assert resolved is store


def test_unresolved_follower_close_detected_via_port_based_store(
    tmp_path: Path,
) -> None:
    """When a PositionPlan has MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED status
    and Account reads through the port, _has_unresolved_follower_close()
    returns True."""
    db_path = tmp_path / "test_unresolved.sqlite3"
    store = SqlitePositionPlanStore(str(db_path))

    plan = PositionPlan(
        position_id="test-pos-1",
        strategy_id="test-strategy",
        entry_engine="lf",
        side="long",
        status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
        canonical_stop_price=None,
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("1.0"),
        metadata={"follower_close_generation": 0},
    )
    store.upsert_position(plan)
    leg = LegPlan(
        position_id="test-pos-1",
        exchange=ExchangeName.BINANCE,
        role=LegRole.FOLLOWER,
        target_qty_base=Decimal("1.0"),
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(leg)

    ctx = RuntimeContext()
    from src.runtime.components.account import AccountComponent
    account = AccountComponent(ctx)
    account.app_config = SimpleNamespace(symbol="ETH-USDT-PERP")

    account.ports = SimpleNamespace(
        get_position_plan_store=lambda: store,
        execute_signals=AsyncMock(),
        get_execution_clients=lambda: (),
        get_order_journal=lambda: None,
        strategy_pending_work_provider=lambda: None,
        strategy_position_index=lambda: SimpleNamespace(active=False),
        has_unresolved_follower_close=lambda: False,
        has_account_config_entry_block=lambda: False,
    )

    assert account._has_unresolved_follower_close() is True


def test_unresolved_follower_close_generates_signals_via_port(
    tmp_path: Path,
) -> None:
    """_build_unresolved_follower_close_signals must produce follower-only
    close signals using the port-based store."""
    db_path = tmp_path / "test_signals.sqlite3"
    store = SqlitePositionPlanStore(str(db_path))

    plan = PositionPlan(
        position_id="test-pos-2",
        strategy_id="test-strategy",
        entry_engine="lf",
        side="long",
        status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
        canonical_stop_price=None,
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("1.0"),
        metadata={"follower_close_generation": 0},
    )
    store.upsert_position(plan)
    leg = LegPlan(
        position_id="test-pos-2",
        exchange=ExchangeName.BINANCE,
        role=LegRole.FOLLOWER,
        target_qty_base=Decimal("2.0"),
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(leg)

    ctx = RuntimeContext()
    from src.runtime.components.account import AccountComponent
    account = AccountComponent(ctx)
    account.app_config = SimpleNamespace(
        symbol="ETH-USDT-PERP",
        data_exchange=ExchangeName.OKX,
    )

    account.ports = SimpleNamespace(
        get_position_plan_store=lambda: store,
        execute_signals=AsyncMock(),
        get_execution_clients=lambda: (),
        get_order_journal=lambda: None,
        strategy_pending_work_provider=lambda: None,
        strategy_position_index=lambda: SimpleNamespace(active=False),
        has_unresolved_follower_close=lambda: False,
        has_account_config_entry_block=lambda: False,
    )

    signals = account._build_unresolved_follower_close_signals()

    assert len(signals) == 1
    sig = signals[0]
    assert sig.action == SignalAction.CLOSE_LONG
    assert (
        sig.metadata["execution_purpose"]
        == "follower_close_after_master_close"
    )
    assert sig.metadata["position_id"] == "test-pos-2"
    assert sig.metadata["target_exchanges"] == ["binance"]


def test_new_open_signals_blocked_when_unresolved_follower_close_exists(
    tmp_path: Path,
) -> None:
    """SignalExecution must block new OPEN signals when an unresolved
    follower close is detected through the port."""
    db_path = tmp_path / "test_blocked.sqlite3"
    store = SqlitePositionPlanStore(str(db_path))

    plan = PositionPlan(
        position_id="test-pos-3",
        strategy_id="test-strategy",
        entry_engine="lf",
        side="long",
        status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
        canonical_stop_price=None,
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("1.0"),
    )
    store.upsert_position(plan)

    ctx = RuntimeContext()
    from src.runtime.components.signal_execution import SignalExecutionComponent
    signals_comp = SignalExecutionComponent(ctx)
    signals_comp.app_config = SimpleNamespace(
        symbol="ETH-USDT-PERP",
        dry_run=False,
    )
    signals_comp.stats = SimpleNamespace(signals_seen=0)
    signals_comp.context = SimpleNamespace(
        alerts=SimpleNamespace(emit=lambda a: None),
    )

    signals_comp.ports = SimpleNamespace(
        has_account_config_entry_block=lambda: False,
        has_unresolved_follower_close=lambda: True,
    )

    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("1.0"),
        reason="test",
    )
    from src.runtime.signal_execution_service import (
        RuntimeSignalExecutionRequest,
    )
    request = RuntimeSignalExecutionRequest(
        signals=(signal,),
        source="test",
        event_time_ms=None,
    )

    result = signals_comp._prepare_signal_execution(signal, request)
    assert result is False, (
        "New OPEN must be blocked when follower close unresolved"
    )


# ═══════════════════════════════════════════════════════════════════════════
# 6    Startup Catchup – Range Observed via Context
# ═══════════════════════════════════════════════════════════════════════════


def test_catchup_sets_range_observed_in_context_state() -> None:
    """When Catchup processes range events, the context state field
    startup_catchup_range_observed must be set to True."""
    from src.runtime.components.catchup import CatchupComponent

    ctx = RuntimeContext()
    catchup = CatchupComponent(ctx)

    catchup.runtime_state.range.startup_catchup_range_observed = True

    assert ctx.state.range.startup_catchup_range_observed is True


def test_startup_finish_after_catchup_reads_range_observed_from_context() -> None:
    """Startup._finish_range_speed_warmup_after_catchup reads
    startup_catchup_range_observed from RangeRuntimeState."""
    ctx = RuntimeContext()
    ctx.state.range.startup_catchup_range_observed = True

    startup = StartupComponent(ctx)
    warmup = MagicMock()
    warmup.finish_after_catchup = AsyncMock()
    startup._range_speed_warmup = warmup

    asyncio.run(startup._finish_range_speed_warmup_after_catchup())

    warmup.finish_after_catchup.assert_called_once_with(range_observed=True)


# ═══════════════════════════════════════════════════════════════════════════
# 7    Fixed-Time Trade Bar – Timestamp in Context
# ═══════════════════════════════════════════════════════════════════════════


def test_market_events_writes_fixed_time_trade_bar_timestamp_to_context() -> None:
    """When MarketEvents processes a fixed_time_trade_bar feature event,
    it writes open_time_ms into MarketRuntimeState."""
    from src.runtime.components.market_events import MarketEventsComponent

    ctx = RuntimeContext()

    market = MarketEventsComponent(ctx)
    market.stats = SimpleNamespace(feature_events_seen=0)
    market.ports = SimpleNamespace(
        get_market_feature_pipeline=lambda: SimpleNamespace(
            dispatch=AsyncMock(return_value=()),
        ),
        execute_signals=AsyncMock(),
        maybe_log_live_data_path_stats=lambda: None,
    )

    event = MarketFeatureEvent(
        event_type="fixed_time_trade_bar",
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="1m",
        event_time_ms=1000000,
        available_time_ms=1000000,
        data={"open_time_ms": 987654321},
    )

    asyncio.run(market._process_market_feature_event(event))

    assert (
        ctx.state.market.latest_fixed_time_trade_bar_open_time_ms
        == 987654321
    )


def test_persistence_reads_latest_fixed_time_trade_bar_timestamp_from_context() -> None:
    """Persistence._maybe_log_live_data_path_stats reads the latest
    fixed_time_trade_bar_open_time_ms from MarketRuntimeState."""
    ctx = RuntimeContext()
    ctx.state.market.latest_fixed_time_trade_bar_open_time_ms = 1717171717

    persistence = PersistenceComponent(ctx)
    persistence._last_live_data_path_log_ms = 0
    persistence._closed_bar_interval_ms = 14400000
    persistence._project_env = ProjectEnvConfig(
        values={"AETHER_LIVE_DATA_PATH_STATS_INTERVAL_SECONDS": "1800"},
        source_files=(),
        env_file=Path(".env"),
        example_file=None,
    )
    persistence._range_module = MagicMock()
    persistence._range_module.cached_rows_for_bucket = MagicMock(
        return_value=[]
    )

    class _FakeService:
        def metrics(self):
            return SimpleNamespace(
                pending_count=0,
                dropped=0,
                failures=0,
                written=0,
                submitted=0,
            )

    persistence._runtime_persistence_service = _FakeService()
    persistence._market_feature_pipeline = SimpleNamespace(
        resolve_observers=lambda: [],
    )
    persistence.stats = SimpleNamespace(
        market_events_seen=10,
        feature_events_seen=5,
    )

    # Patch logger to capture the formatted message
    with patch("src.runtime.components.persistence.logger") as mock_logger:
        persistence._maybe_log_live_data_path_stats()

    # The log message uses %s formatting; args include the timestamp value
    call_args = mock_logger.info.call_args
    assert call_args is not None
    args = call_args[0]
    # args[0] = format string, args[1:] = format values
    # The timestamp value 1717171717 should appear in the format args
    assert 1717171717 in args, (
        f"Expected 1717171717 in log format args, got: {args}"
    )
