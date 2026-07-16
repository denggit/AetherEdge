from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError, fields
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from src.app import AppConfig
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime import runner as runner_module
from src.runtime.models import RuntimePhase
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeRunner
from src.runtime.startup_phase_coordinator import (
    RuntimeStartupPhaseCoordinator,
    RuntimeStartupPhasePlan,
)


PLAN_FIELDS = (
    "initialize_rangebar_trust_window",
    "enter_warming_up",
    "bootstrap_account_config",
    "check_position_mode",
    "run_warmup",
    "warmup_range_speed_history",
    "handle_range_speed_history_result",
    "check_feature_backfills",
    "enter_catching_up",
    "run_recovery",
    "run_post_recovery_checks",
    "run_reconciliation",
    "call_strategy_on_start",
    "evaluate_startup_catchup",
    "finish_range_speed_warmup",
    "start_heartbeat",
    "start_range_speed_background_services",
    "enter_running",
)


class _Strategy:
    def strategy_identity(self) -> str:
        return "test-strategy"


def _runner(*, startup_phase_coordinator=None) -> LiveRuntimeRunner:
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
                    "strategy_id": "test-strategy",
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
    if startup_phase_coordinator is not None:
        services["startup_phase_coordinator"] = startup_phase_coordinator
    return LiveRuntimeRunner(
        app_config=config,
        app_context=SimpleNamespace(strategy=_Strategy()),
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services=services,
    )


def _plan(
    calls: list[str],
    snapshots: tuple[object, ...],
    *,
    range_speed_result: int = 7,
    overrides: dict[str, object] | None = None,
) -> tuple[RuntimeStartupPhasePlan, dict[str, object]]:
    received: dict[str, object] = {}

    def sync(name: str):
        def step() -> None:
            calls.append(name)

        return step

    def async_step(name: str, result=None):
        async def step():
            calls.append(name)
            return result

        return step

    def value_step(name: str):
        def step(value) -> None:
            calls.append(name)
            received[name] = value

        return step

    def async_value_step(name: str):
        async def step(value) -> None:
            calls.append(name)
            received[name] = value

        return step

    values = {
        "initialize_rangebar_trust_window": sync("initialize"),
        "enter_warming_up": sync("warming"),
        "bootstrap_account_config": async_step("account_config"),
        "check_position_mode": async_step("position_mode"),
        "run_warmup": async_step("warmup"),
        "warmup_range_speed_history": async_step(
            "range_speed_warmup",
            range_speed_result,
        ),
        "handle_range_speed_history_result": value_step(
            "range_speed_result"
        ),
        "check_feature_backfills": async_step("feature_backfills"),
        "enter_catching_up": sync("catching_up"),
        "run_recovery": async_step("recovery", snapshots),
        "run_post_recovery_checks": async_value_step("post_recovery"),
        "run_reconciliation": async_value_step("reconciliation"),
        "call_strategy_on_start": async_value_step("on_start"),
        "evaluate_startup_catchup": async_value_step("catchup"),
        "finish_range_speed_warmup": async_step("finish_range_speed"),
        "start_heartbeat": sync("heartbeat"),
        "start_range_speed_background_services": sync("range_background"),
        "enter_running": sync("running"),
    }
    values.update(overrides or {})
    return RuntimeStartupPhasePlan(**values), received


def test_coordinator_is_stateless_and_plan_is_frozen_callbacks_only() -> None:
    plan, _ = _plan([], (object(),))

    assert vars(RuntimeStartupPhaseCoordinator()) == {}
    assert tuple(field.name for field in fields(plan)) == PLAN_FIELDS
    with pytest.raises(FrozenInstanceError):
        plan.enter_running = lambda: None  # type: ignore[misc]


@pytest.mark.asyncio
async def test_all_phases_run_once_in_order_and_preserve_data_identity() -> None:
    calls: list[str] = []
    first = object()
    snapshots = (first, object())
    plan, received = _plan(calls, snapshots, range_speed_result=19)

    returned = await RuntimeStartupPhaseCoordinator().execute(plan)

    assert calls == [
        "initialize",
        "warming",
        "account_config",
        "position_mode",
        "warmup",
        "range_speed_warmup",
        "range_speed_result",
        "feature_backfills",
        "catching_up",
        "recovery",
        "post_recovery",
        "reconciliation",
        "on_start",
        "catchup",
        "finish_range_speed",
        "heartbeat",
        "range_background",
        "running",
    ]
    assert received["range_speed_result"] == 19
    assert received["post_recovery"] is snapshots
    assert received["reconciliation"] is snapshots
    assert received["on_start"] is first
    assert received["catchup"] is first
    assert returned is snapshots


@pytest.mark.asyncio
async def test_async_phase_completes_before_next_phase_starts() -> None:
    calls: list[str] = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def bootstrap() -> None:
        calls.append("bootstrap.start")
        started.set()
        await release.wait()
        calls.append("bootstrap.end")

    plan, _ = _plan(
        calls,
        (object(),),
        overrides={"bootstrap_account_config": bootstrap},
    )
    execution = asyncio.create_task(
        RuntimeStartupPhaseCoordinator().execute(plan)
    )

    await started.wait()
    assert calls == ["initialize", "warming", "bootstrap.start"]
    release.set()
    await execution
    assert calls.index("bootstrap.end") < calls.index("position_mode")


@pytest.mark.asyncio
async def test_callback_exception_propagates_and_stops_following_phases() -> None:
    calls: list[str] = []
    error = RuntimeError("startup failed")

    async def fail() -> None:
        calls.append("failed")
        raise error

    plan, _ = _plan(
        calls,
        (object(),),
        overrides={"check_position_mode": fail},
    )

    with pytest.raises(RuntimeError) as raised:
        await RuntimeStartupPhaseCoordinator().execute(plan)

    assert raised.value is error
    assert calls == ["initialize", "warming", "account_config", "failed"]


@pytest.mark.asyncio
async def test_cancelled_error_propagates_unchanged() -> None:
    cancelled = asyncio.CancelledError("cancelled")

    async def cancel() -> None:
        raise cancelled

    plan, _ = _plan(
        [],
        (object(),),
        overrides={"run_warmup": cancel},
    )

    with pytest.raises(asyncio.CancelledError) as raised:
        await RuntimeStartupPhaseCoordinator().execute(plan)

    assert raised.value is cancelled


@pytest.mark.asyncio
async def test_empty_snapshots_fail_after_snapshot_wide_steps() -> None:
    calls: list[str] = []
    snapshots: tuple[object, ...] = ()
    plan, received = _plan(calls, snapshots)

    with pytest.raises(IndexError):
        await RuntimeStartupPhaseCoordinator().execute(plan)

    assert calls[-3:] == ["recovery", "post_recovery", "reconciliation"]
    assert received["post_recovery"] is snapshots
    assert received["reconciliation"] is snapshots


def test_injected_coordinator_has_priority_and_no_constructor_execution(
    monkeypatch,
) -> None:
    coordinator = SimpleNamespace(execute=AsyncMock())
    default_factory = Mock()
    monkeypatch.setattr(
        runner_module,
        "RuntimeStartupPhaseCoordinator",
        default_factory,
    )

    runner = _runner(startup_phase_coordinator=coordinator)

    default_factory.assert_not_called()
    coordinator.execute.assert_not_called()
    assert runner._startup_phase_coordinator is coordinator
    assert runner.services["startup_phase_coordinator"] is coordinator


def test_default_coordinator_is_created_once_written_back_and_not_executed(
    monkeypatch,
) -> None:
    coordinator = SimpleNamespace(execute=AsyncMock())
    factory = Mock(return_value=coordinator)
    monkeypatch.setattr(
        runner_module,
        "RuntimeStartupPhaseCoordinator",
        factory,
    )

    runner = _runner()

    factory.assert_called_once_with()
    coordinator.execute.assert_not_called()
    assert runner._startup_phase_coordinator is coordinator
    assert runner.services["startup_phase_coordinator"] is coordinator


@pytest.mark.asyncio
async def test_runner_startup_builds_complete_plan_and_logs_around_delegate(
    monkeypatch,
) -> None:
    captured = []

    class Coordinator:
        async def execute(self, plan) -> tuple[object, ...]:
            captured.append(plan)
            return (object(),)

    runner = _runner(startup_phase_coordinator=Coordinator())
    logger = Mock()
    monkeypatch.setattr(runner_module, "logger", logger)

    result = await runner._startup()

    assert result is None
    assert len(captured) == 1
    plan = captured[0]
    expected = {
        "initialize_rangebar_trust_window": runner._initialize_rangebar_trust_window,
        "enter_warming_up": runner._enter_startup_warming_up,
        "bootstrap_account_config": runner._bootstrap_account_config_if_enabled,
        "check_position_mode": runner._check_strategy_position_mode_requirements,
        "run_warmup": runner._run_warmup,
        "warmup_range_speed_history": runner._warmup_range_speed_history,
        "handle_range_speed_history_result": runner._handle_startup_range_speed_history_result,
        "check_feature_backfills": runner._check_startup_feature_backfills,
        "enter_catching_up": runner._enter_startup_catching_up,
        "run_recovery": runner._run_recovery,
        "run_post_recovery_checks": runner._run_startup_post_recovery_checks,
        "run_reconciliation": runner._run_reconciliation,
        "call_strategy_on_start": runner._call_on_start,
        "evaluate_startup_catchup": runner._evaluate_startup_catchup_once,
        "finish_range_speed_warmup": runner._finish_range_speed_warmup_after_catchup,
        "start_heartbeat": runner._start_runtime_heartbeat,
        "start_range_speed_background_services": runner._start_range_speed_background_services,
        "enter_running": runner._enter_startup_running,
    }
    assert all(getattr(plan, name) == callback for name, callback in expected.items())
    assert logger.info.call_args_list == [
        call("Live runtime startup phase started"),
        call("Live runtime startup phase completed"),
    ]


@pytest.mark.asyncio
async def test_runner_startup_failure_omits_completed_log(monkeypatch) -> None:
    error = RuntimeError("delegate failed")
    coordinator = SimpleNamespace(execute=AsyncMock(side_effect=error))
    runner = _runner(startup_phase_coordinator=coordinator)
    logger = Mock()
    monkeypatch.setattr(runner_module, "logger", logger)

    with pytest.raises(RuntimeError) as raised:
        await runner._startup()

    assert raised.value is error
    logger.info.assert_called_once_with("Live runtime startup phase started")


def test_health_wrappers_keep_exact_phase_values() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner._set_health = Mock()

    runner._enter_startup_warming_up()
    runner._enter_startup_catching_up()
    runner._enter_startup_running()

    assert runner._set_health.call_args_list == [
        call(RuntimePhase.WARMING_UP, healthy=True),
        call(
            RuntimePhase.CATCHING_UP,
            healthy=True,
            warmup_complete=True,
        ),
        call(
            RuntimePhase.RUNNING,
            healthy=True,
            warmup_complete=True,
            caught_up=True,
        ),
    ]


def test_range_speed_result_wrapper_keeps_exact_warning(monkeypatch) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner._range_speed_min_periods = 10
    logger = Mock()
    monkeypatch.setattr(runner_module, "logger", logger)

    runner._handle_startup_range_speed_history_result(7)

    logger.warning.assert_called_once_with(
        "Range-speed history insufficient; live runtime continues | complete_history=%s min_periods=%s missing=%s",
        7,
        10,
        3,
    )


@pytest.mark.parametrize(
    ("minimum", "loaded"),
    ((0, 0), (10, 10), (10, 11)),
)
def test_range_speed_warning_condition_is_not_expanded(
    monkeypatch,
    minimum: int,
    loaded: int,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner._range_speed_min_periods = minimum
    logger = Mock()
    monkeypatch.setattr(runner_module, "logger", logger)

    runner._handle_startup_range_speed_history_result(loaded)

    logger.warning.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked", (False, True))
async def test_post_recovery_wrapper_only_rechecks_when_blocked(
    blocked: bool,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner._account_config_new_entries_blocked = blocked
    runner._recheck_account_config_after_recovery = AsyncMock()

    await runner._run_startup_post_recovery_checks((object(),))

    if blocked:
        runner._recheck_account_config_after_recovery.assert_awaited_once_with()
    else:
        runner._recheck_account_config_after_recovery.assert_not_called()


def test_heartbeat_wrapper_keeps_exact_runtime_id() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.app_config = SimpleNamespace(
        strategy="tests.fake:Strategy",
        symbol="ETH-USDT-PERP",
    )
    runner._heartbeat_service = SimpleNamespace(start=Mock())

    runner._start_runtime_heartbeat()

    runner._heartbeat_service.start.assert_called_once_with(
        runtime_id="tests.fake:Strategy::ETH-USDT-PERP"
    )
