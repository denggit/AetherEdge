from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.app import AppConfig
from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.platform.data.models import MarketEventType, MarketTrade, TradeSide
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime.components import catchup as catchup_component
from src.runtime.models import RuntimePhase
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeRunner


class _HeartbeatProbe:
    def __init__(self) -> None:
        self.start = Mock()
        self.read_previous = Mock()
        self.note_market_event = Mock()
        self.note_closed_bar = Mock()
        self.write_now = Mock()
        self.run_periodic = Mock(return_value=object())

    @property
    def store(self):
        raise AssertionError("Runner construction must not access heartbeat store")


def _runner(*, heartbeat_service=None) -> LiveRuntimeRunner:
    config = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="tests.fake:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )
    services = {
        "project_env_config": ProjectEnvConfig(
            values={},
            source_files=(),
            env_file=Path(".env"),
            example_file=None,
        ),
        "runtime_requirements": StrategyRuntimeRequirements.from_mapping(
            {
                "capabilities": {
                    "manifest_version": 1,
                    "strategy_id": "heartbeat-test",
                    "position_snapshots": False,
                    "recovery_status": False,
                    "market_features": False,
                    "range_speed_history": False,
                    "startup_preview": False,
                    "pending_work": False,
                }
            }
        ),
    }
    if heartbeat_service is not None:
        services["heartbeat_service"] = heartbeat_service
    return LiveRuntimeRunner(
        app_config=config,
        app_context=SimpleNamespace(
            strategy=SimpleNamespace(
                strategy_identity=lambda: "heartbeat-test"
            )
        ),
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services=services,
    )


def _assert_heartbeat_business_methods_not_called(
    heartbeat: _HeartbeatProbe,
) -> None:
    heartbeat.start.assert_not_called()
    heartbeat.read_previous.assert_not_called()
    heartbeat.note_market_event.assert_not_called()
    heartbeat.note_closed_bar.assert_not_called()
    heartbeat.write_now.assert_not_called()
    heartbeat.run_periodic.assert_not_called()


def test_injected_heartbeat_has_priority_and_no_constructor_side_effects(
    monkeypatch,
) -> None:
    heartbeat = _HeartbeatProbe()
    default_factory = Mock()
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeHeartbeatService",
        default_factory,
    )

    runner = _runner(heartbeat_service=heartbeat)

    default_factory.assert_not_called()
    assert runner._heartbeat_service is heartbeat
    assert runner.services["heartbeat_service"] is heartbeat
    _assert_heartbeat_business_methods_not_called(heartbeat)


def test_default_heartbeat_is_created_once_written_back_and_not_used(
    monkeypatch,
) -> None:
    heartbeat = _HeartbeatProbe()
    factory = Mock(return_value=heartbeat)
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeHeartbeatService",
        factory,
    )

    runner = _runner()

    factory.assert_called_once_with()
    assert runner._heartbeat_service is heartbeat
    assert runner.services["heartbeat_service"] is heartbeat
    _assert_heartbeat_business_methods_not_called(heartbeat)


def _async_stage(calls: list[str], name: str, result=None):
    async def stage(*args, **kwargs):
        calls.append(name)
        return result

    return stage


@pytest.mark.asyncio
async def test_startup_uses_injected_heartbeat_once_in_existing_order(
    monkeypatch,
) -> None:
    calls: list[str] = []
    heartbeat = _HeartbeatProbe()
    heartbeat.start.side_effect = lambda **kwargs: calls.append(
        "heartbeat.start"
    )
    runner = _runner(heartbeat_service=heartbeat)
    snapshots = (object(),)
    runner._account_config_new_entries_blocked = False
    monkeypatch.setattr(
        runner,
        "_initialize_rangebar_trust_window",
        lambda: calls.append("initialize_trust_window"),
    )
    monkeypatch.setattr(
        runner,
        "_set_health",
        lambda phase, **kwargs: calls.append(f"health.{phase.value}"),
    )
    monkeypatch.setattr(
        runner,
        "_bootstrap_account_config_if_enabled",
        _async_stage(calls, "account_config"),
    )
    monkeypatch.setattr(
        runner,
        "_check_strategy_position_mode_requirements",
        _async_stage(calls, "position_mode"),
    )
    monkeypatch.setattr(
        runner,
        "_run_warmup",
        _async_stage(calls, "warmup"),
    )
    monkeypatch.setattr(
        runner,
        "_warmup_range_speed_history",
        _async_stage(calls, "range_speed_warmup", 0),
    )
    monkeypatch.setattr(
        runner,
        "_check_startup_feature_backfills",
        _async_stage(calls, "feature_backfills"),
    )
    monkeypatch.setattr(
        runner,
        "_run_recovery",
        _async_stage(calls, "recovery", snapshots),
    )
    monkeypatch.setattr(
        runner,
        "_run_reconciliation",
        _async_stage(calls, "reconciliation"),
    )
    monkeypatch.setattr(
        runner,
        "_call_on_start",
        _async_stage(calls, "strategy.on_start"),
    )
    monkeypatch.setattr(
        runner,
        "_evaluate_startup_catchup_once",
        _async_stage(calls, "startup_catchup"),
    )
    monkeypatch.setattr(
        runner,
        "_finish_range_speed_warmup_after_catchup",
        _async_stage(calls, "finish_range_speed_warmup"),
    )
    monkeypatch.setattr(
        runner,
        "_start_range_speed_background_services",
        lambda: calls.append("start_range_speed_background_services"),
    )

    await runner._startup()

    assert calls == [
        "initialize_trust_window",
        "health.warming_up",
        "account_config",
        "position_mode",
        "warmup",
        "range_speed_warmup",
        "feature_backfills",
        "health.catching_up",
        "recovery",
        "reconciliation",
        "strategy.on_start",
        "startup_catchup",
        "finish_range_speed_warmup",
        "heartbeat.start",
        "start_range_speed_background_services",
        "health.running",
    ]
    heartbeat.start.assert_called_once_with(
        runtime_id="tests.fake:Strategy::ETH-USDT-PERP"
    )
    assert runner._heartbeat_service is runner.services["heartbeat_service"]


def test_periodic_heartbeat_is_unconditional_ordered_and_uses_stop_event() -> None:
    heartbeat = _HeartbeatProbe()
    runner = _runner(heartbeat_service=heartbeat)
    calls: list[tuple[str, object]] = []

    def task(name: str):
        def create(stop_event):
            calls.append((name, stop_event))
            return object()

        return create

    heartbeat.run_periodic.side_effect = task("heartbeat")
    runner.requirements = SimpleNamespace(
        account_state=SimpleNamespace(poll_enabled=True),
        order_state=SimpleNamespace(poll_when_position_enabled=True),
    )
    runner._get_account_sync_service = lambda: SimpleNamespace(
        run_periodic=task("account")
    )
    runner._get_order_sync_service = lambda: SimpleNamespace(
        run_periodic=task("order")
    )
    runner._periodic_follower_close_check = task("follower_close")
    runner._get_startup_feature_backfill_providers = lambda: (object(),)
    runner._periodic_feature_readiness_refresh = task("feature_readiness")

    class Lifecycle:
        def start(self, factories):
            return [factory() for factory in factories]

    runner._sync_lifecycle = Lifecycle()

    runner._start_sync_tasks()

    assert [name for name, _ in calls] == [
        "account",
        "order",
        "follower_close",
        "heartbeat",
        "feature_readiness",
    ]
    assert all(stop_event is runner._stop_event for _, stop_event in calls)
    heartbeat.run_periodic.assert_called_once_with(runner._stop_event)
    assert runner._heartbeat_service is runner.services["heartbeat_service"]


@pytest.mark.asyncio
async def test_stop_sync_tasks_only_delegates_to_lifecycle() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    heartbeat = _HeartbeatProbe()
    lifecycle = SimpleNamespace(stop=AsyncMock())
    runner._heartbeat_service = heartbeat
    runner._sync_lifecycle = lifecycle
    runner._sync_tasks = [object()]

    await runner._stop_sync_tasks()

    lifecycle.stop.assert_awaited_once_with()
    assert runner._sync_tasks == []
    _assert_heartbeat_business_methods_not_called(heartbeat)


def _trade(*, event_time_ms: int | None) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        event_time_ms=event_time_ms,
        trade_time_ms=event_time_ms,
    )


async def _process_market_event_with_order(
    monkeypatch,
    event,
) -> tuple[_HeartbeatProbe, list[str]]:
    calls: list[str] = []
    heartbeat = _HeartbeatProbe()
    heartbeat.note_market_event.side_effect = lambda value: calls.append(
        f"heartbeat.note_market_event:{value}"
    )
    runner = _runner(heartbeat_service=heartbeat)
    monkeypatch.setattr(
        runner,
        "_set_health",
        lambda *args, **kwargs: calls.append("health.update"),
    )
    monkeypatch.setattr(
        runner,
        "_call_strategy_market_event",
        _async_stage(calls, "strategy", ()),
    )
    monkeypatch.setattr(
        runner,
        "_execute_signals",
        _async_stage(calls, "signals"),
    )
    monkeypatch.setattr(
        runner,
        "_maybe_log_live_data_path_stats",
        lambda: calls.append("data_path"),
    )
    if isinstance(event, MarketTrade):
        monkeypatch.setattr(
            runner,
            "_process_trade",
            _async_stage(calls, "trade"),
        )
        monkeypatch.setattr(
            runner,
            "_trade_events_are_range_only",
            lambda: False,
        )

    await runner.process_market_event(event)
    return heartbeat, calls


@pytest.mark.asyncio
async def test_market_event_notes_exact_time_before_health(monkeypatch) -> None:
    heartbeat, calls = await _process_market_event_with_order(
        monkeypatch,
        _trade(event_time_ms=1234),
    )

    heartbeat.note_market_event.assert_called_once_with(1234)
    assert calls[:2] == [
        "heartbeat.note_market_event:1234",
        "health.update",
    ]


@pytest.mark.asyncio
async def test_market_event_notes_none_time_unchanged(monkeypatch) -> None:
    event = SimpleNamespace(event_type=MarketEventType.TICKER)

    heartbeat, calls = await _process_market_event_with_order(
        monkeypatch,
        event,
    )

    heartbeat.note_market_event.assert_called_once_with(None)
    assert calls[:2] == [
        "heartbeat.note_market_event:None",
        "health.update",
    ]


def _feature_event(*, event_type, open_time_ms) -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=event_type,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="4h",
        event_time_ms=5678,
        data={"open_time_ms": open_time_ms},
    )


@pytest.mark.asyncio
async def test_closed_bar_notes_open_time_before_pipeline_dispatch(
    monkeypatch,
) -> None:
    calls: list[str] = []
    heartbeat = _HeartbeatProbe()
    heartbeat.note_closed_bar.side_effect = lambda value: calls.append(
        f"heartbeat.note_closed_bar:{value}"
    )

    class Pipeline:
        async def dispatch(self, event):
            calls.append("pipeline.dispatch")
            return ()

    runner = _runner(heartbeat_service=heartbeat)
    runner._market_feature_pipeline = Pipeline()
    monkeypatch.setattr(
        runner,
        "_execute_signals",
        _async_stage(calls, "signals"),
    )
    monkeypatch.setattr(runner, "_maybe_log_live_data_path_stats", lambda: None)

    await runner.process_market_feature(
        _feature_event(
            event_type=MarketFeatureEventType.CLOSED_KLINE,
            open_time_ms=4321,
        )
    )

    heartbeat.note_closed_bar.assert_called_once_with(4321)
    assert calls[:2] == [
        "heartbeat.note_closed_bar:4321",
        "pipeline.dispatch",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event_type", "open_time_ms"),
    (
        (MarketFeatureEventType.RANGE_AGGREGATE, 4321),
        (MarketFeatureEventType.CLOSED_KLINE, "4321"),
    ),
)
async def test_non_closed_or_non_int_open_time_is_not_noted(
    event_type,
    open_time_ms,
) -> None:
    heartbeat = _HeartbeatProbe()
    runner = _runner(heartbeat_service=heartbeat)
    runner._market_feature_pipeline = SimpleNamespace(
        dispatch=AsyncMock(return_value=())
    )
    runner._execute_signals = AsyncMock()
    runner._maybe_log_live_data_path_stats = Mock()

    await runner.process_market_feature(
        _feature_event(
            event_type=event_type,
            open_time_ms=open_time_ms,
        )
    )

    heartbeat.note_closed_bar.assert_not_called()


@pytest.mark.asyncio
async def test_startup_catchup_reads_previous_heartbeat_once(monkeypatch) -> None:
    heartbeat = _HeartbeatProbe()
    runner = _runner(heartbeat_service=heartbeat)
    runner.requirements = SimpleNamespace(
        closed_kline=SimpleNamespace(enabled=True)
    )
    runner.runtime_config = SimpleNamespace(
        startup_catchup=SimpleNamespace(
            enabled=True,
            fresh_open_window_seconds=300,
        )
    )
    runner._startup_catchup_evaluated = False
    runner._closed_bar_interval_ms = 4 * 60 * 60 * 1000
    runner._last_snapshots = ()
    runner._has_any_active_position_for_catchup = lambda snapshots: True
    runner._closed_bar_scheduler = SimpleNamespace(mark_emitted=Mock())
    monkeypatch.setattr(catchup_component.time, "time", lambda: 1_728_000)

    await runner._evaluate_startup_catchup_once(object())

    heartbeat.read_previous.assert_called_once_with()
