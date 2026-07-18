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
from src.runtime.components import recovery as recovery_component
from src.runtime.recovery_coordinator import (
    RuntimeRecoveryCoordinator,
    RuntimeRecoveryPlan,
)
from src.runtime.runner import LiveRuntimeError, LiveRuntimeRunner
from src.runtime.services import DEFAULT_RUNTIME_SERVICE
from src.runtime.requirements import StrategyRuntimeRequirements
from src.signals.models import SignalAction
from src.strategy import StrategyRecoveryStatus


PLAN_FIELDS = (
    "resolve_service",
    "fallback_snapshots",
    "invoke_service",
    "record_run",
    "validate_report",
    "partition_signals",
    "capture_failure_counts",
    "execute_stop_signals",
    "validate_stop_execution",
    "validate_post_execution_protection",
    "execute_other_signals",
    "finalize_report",
)


def _runner(*, recovery_coordinator=None) -> LiveRuntimeRunner:
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
    strategy = object()
    services = {
        "project_env_config": ProjectEnvConfig(
            values={},
            source_files=(),
            env_file=Path(".env"),
            example_file=None,
        ),
        "runtime_requirements": StrategyRuntimeRequirements.from_mapping({}),
    }
    if recovery_coordinator is not None:
        services["recovery_coordinator"] = recovery_coordinator
    runner = LiveRuntimeRunner(
        app_config=config,
        app_context=SimpleNamespace(strategy=strategy),
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services=services,
    )
    return runner


def _plan(
    calls: list[str],
    *,
    service: object | None = None,
    report: object | None = None,
    stop_signals: list[object] | None = None,
    other_signals: list[object] | None = None,
    snapshots: tuple[object, ...] | None = None,
    overrides: dict[str, object] | None = None,
) -> tuple[RuntimeRecoveryPlan, dict[str, object]]:
    service = object() if service is None else service
    report = object() if report is None else report
    stop_signals = [object()] if stop_signals is None else stop_signals
    other_signals = [object()] if other_signals is None else other_signals
    snapshots = (object(),) if snapshots is None else snapshots
    received: dict[str, object] = {}
    failure_counts = (3, 5)

    def resolve_service() -> object:
        calls.append("resolve")
        return service

    def fallback_snapshots() -> tuple[object, ...]:
        calls.append("fallback")
        return snapshots

    async def invoke_service(value: object) -> object:
        calls.append("invoke")
        received["service"] = value
        return report

    def record_run() -> None:
        calls.append("record")

    def validate_report(value: object) -> None:
        calls.append("validate")
        received["validated_report"] = value

    def partition_signals(value: object):
        calls.append("partition")
        received["partitioned_report"] = value
        return stop_signals, other_signals

    def capture_failure_counts() -> tuple[int, int]:
        calls.append("capture")
        received["captured_failure_counts"] = failure_counts
        return failure_counts

    async def execute_stop_signals(signals: list[object]) -> None:
        calls.append("stops")
        received["stop_signals"] = signals

    def validate_stop_execution(counts: tuple[int, int]) -> None:
        calls.append("validate_stops")
        received["failure_counts"] = counts

    async def validate_post_execution_protection() -> None:
        calls.append("post_protection")

    async def execute_other_signals(signals: list[object]) -> None:
        calls.append("others")
        received["other_signals"] = signals

    def finalize_report(value: object) -> tuple[object, ...]:
        calls.append("finalize")
        received["finalized_report"] = value
        return snapshots

    values = {
        "resolve_service": resolve_service,
        "fallback_snapshots": fallback_snapshots,
        "invoke_service": invoke_service,
        "record_run": record_run,
        "validate_report": validate_report,
        "partition_signals": partition_signals,
        "capture_failure_counts": capture_failure_counts,
        "execute_stop_signals": execute_stop_signals,
        "validate_stop_execution": validate_stop_execution,
        "validate_post_execution_protection": (
            validate_post_execution_protection
        ),
        "execute_other_signals": execute_other_signals,
        "finalize_report": finalize_report,
    }
    values.update(overrides or {})
    return RuntimeRecoveryPlan(**values), received


def test_coordinator_is_stateless_and_plan_is_frozen_callbacks_only() -> None:
    plan, _ = _plan([])

    assert vars(RuntimeRecoveryCoordinator()) == {}
    assert tuple(field.name for field in fields(plan)) == PLAN_FIELDS
    with pytest.raises(FrozenInstanceError):
        plan.record_run = lambda: None  # type: ignore[misc]


@pytest.mark.asyncio
async def test_service_none_only_resolves_and_returns_fallback_identity() -> None:
    calls: list[str] = []
    snapshots = (object(),)
    plan, _ = _plan(calls, snapshots=snapshots)
    plan = RuntimeRecoveryPlan(
        **{
            **{field.name: getattr(plan, field.name) for field in fields(plan)},
            "resolve_service": lambda: calls.append("resolve") or None,
        }
    )

    returned = await RuntimeRecoveryCoordinator().execute(plan)

    assert calls == ["resolve", "fallback"]
    assert returned is snapshots


@pytest.mark.asyncio
async def test_full_path_runs_once_in_order_and_preserves_identity() -> None:
    calls: list[str] = []
    service = object()
    report = object()
    stop_signals = [object()]
    other_signals = [object()]
    snapshots = (object(), object())
    plan, received = _plan(
        calls,
        service=service,
        report=report,
        stop_signals=stop_signals,
        other_signals=other_signals,
        snapshots=snapshots,
    )

    returned = await RuntimeRecoveryCoordinator().execute(plan)

    assert calls == [
        "resolve",
        "invoke",
        "record",
        "validate",
        "partition",
        "capture",
        "stops",
        "validate_stops",
        "post_protection",
        "others",
        "finalize",
    ]
    assert received["service"] is service
    assert received["validated_report"] is report
    assert received["partitioned_report"] is report
    assert received["finalized_report"] is report
    assert received["stop_signals"] is stop_signals
    assert received["other_signals"] is other_signals
    assert received["failure_counts"] is received["captured_failure_counts"]
    assert returned is snapshots


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stop_signals", "other_signals", "absent"),
    [([], [object()], {"capture", "stops", "validate_stops", "post_protection"}),
     ([object()], [], {"others"})],
)
async def test_empty_signal_batches_skip_their_callbacks(
    stop_signals: list[object],
    other_signals: list[object],
    absent: set[str],
) -> None:
    calls: list[str] = []
    plan, _ = _plan(
        calls,
        stop_signals=stop_signals,
        other_signals=other_signals,
    )

    await RuntimeRecoveryCoordinator().execute(plan)

    assert absent.isdisjoint(calls)
    assert calls[-1] == "finalize"


@pytest.mark.asyncio
async def test_callback_exception_is_propagated_and_stops_later_steps() -> None:
    calls: list[str] = []
    error = RuntimeError("invalid report")

    def fail(_report: object) -> None:
        calls.append("failed")
        raise error

    plan, _ = _plan(calls, overrides={"validate_report": fail})

    with pytest.raises(RuntimeError) as raised:
        await RuntimeRecoveryCoordinator().execute(plan)

    assert raised.value is error
    assert calls == ["resolve", "invoke", "record", "failed"]


@pytest.mark.asyncio
async def test_cancelled_error_is_propagated_unchanged() -> None:
    cancelled = asyncio.CancelledError("cancelled")

    async def cancel(_service: object) -> object:
        raise cancelled

    plan, _ = _plan([], overrides={"invoke_service": cancel})
    with pytest.raises(asyncio.CancelledError) as raised:
        await RuntimeRecoveryCoordinator().execute(plan)
    assert raised.value is cancelled


def test_injected_coordinator_has_priority_without_default_construction(
    monkeypatch,
) -> None:
    coordinator = SimpleNamespace(execute=AsyncMock())
    default_factory = Mock()
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeRecoveryCoordinator",
        default_factory,
    )

    runner = _runner(recovery_coordinator=coordinator)

    default_factory.assert_not_called()
    coordinator.execute.assert_not_called()
    assert runner._recovery_coordinator is coordinator
    assert runner.services["recovery_coordinator"] is coordinator
    assert runner._recovery_service is DEFAULT_RUNTIME_SERVICE


def test_default_coordinator_is_created_once_and_not_executed(
    monkeypatch,
) -> None:
    coordinator = SimpleNamespace(execute=AsyncMock())
    factory = Mock(return_value=coordinator)
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeRecoveryCoordinator",
        factory,
    )

    runner = _runner()

    factory.assert_called_once_with()
    coordinator.execute.assert_not_called()
    assert runner._recovery_coordinator is coordinator
    assert runner.services["recovery_coordinator"] is coordinator
    assert runner._recovery_service is DEFAULT_RUNTIME_SERVICE


@pytest.mark.asyncio
async def test_runner_builds_complete_plan_and_delegates_once() -> None:
    captured: list[RuntimeRecoveryPlan] = []
    snapshots = (object(),)

    class Coordinator:
        async def execute(self, plan: RuntimeRecoveryPlan):
            captured.append(plan)
            return snapshots

    runner = _runner(recovery_coordinator=Coordinator())
    returned = await runner._run_recovery()

    assert returned is snapshots
    assert len(captured) == 1
    plan = captured[0]
    expected = {
        "resolve_service": runner._get_recovery_service,
        "fallback_snapshots": runner._recovery_fallback_snapshots,
        "invoke_service": runner._invoke_recovery_service,
        "record_run": runner._record_recovery_run,
        "validate_report": runner._validate_runtime_recovery_report,
        "partition_signals": runner._partition_recovery_signals,
        "capture_failure_counts": runner._capture_recovery_failure_counts,
        "execute_stop_signals": runner._execute_recovery_stop_signals,
        "validate_stop_execution": runner._validate_recovery_stop_execution,
        "validate_post_execution_protection": (
            runner._validate_post_execution_stop_protection
        ),
        "execute_other_signals": runner._execute_recovery_other_signals,
        "finalize_report": runner._finalize_recovery_report,
    }
    assert all(getattr(plan, name) == callback for name, callback in expected.items())


def test_fallback_reads_only_last_snapshot_and_keeps_identity() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    snapshot = object()
    runner._last_snapshot = snapshot
    runner._last_snapshots = (object(),)

    assert runner._recovery_fallback_snapshots() == (snapshot,)
    runner._last_snapshot = None
    with pytest.raises(
        LiveRuntimeError,
        match="startup snapshot is required before live trading",
    ):
        runner._recovery_fallback_snapshots()


@pytest.mark.asyncio
async def test_invoke_service_passes_same_strategy_once() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    strategy = object()
    report = object()
    runner.context = SimpleNamespace(strategy=strategy)
    service = SimpleNamespace(recover=AsyncMock(return_value=report))

    returned = await runner._invoke_recovery_service(service)

    assert returned is report
    service.recover.assert_awaited_once_with(strategy=strategy)


def test_record_and_report_validation_keep_order_and_messages() -> None:
    class BlockingStrategy:
        def recovery_status(self) -> StrategyRecoveryStatus:
            return StrategyRecoveryStatus(
                blocking_manual_required=True,
                alerts=("manual",),
            )

    runner = object.__new__(LiveRuntimeRunner)
    runner.stats = SimpleNamespace(recovery_runs=4)
    runner.context = SimpleNamespace(strategy=SimpleNamespace())
    runner.app_config = SimpleNamespace(
        strategy="tests.fake:Strategy",
        symbol="ETH-USDT-PERP",
    )
    runner.runtime_config = SimpleNamespace(mode=RuntimeMode.LIVE_RUNTIME)
    runner._validated_strategy_capabilities = SimpleNamespace(
        identity="test-strategy"
    )
    runner._validate_recovery_protection_postcondition = Mock()

    runner._record_recovery_run()
    assert runner.stats.recovery_runs == 5

    invalid = SimpleNamespace(ok=False, issues=["broken"])
    with pytest.raises(
        LiveRuntimeError,
        match=r"runtime recovery failed: \('broken',\)",
    ):
        runner._validate_runtime_recovery_report(invalid)
    runner._validate_recovery_protection_postcondition.assert_not_called()

    runner.context.strategy = BlockingStrategy()
    valid = SimpleNamespace(ok=True, issues=[])
    with pytest.raises(
        LiveRuntimeError,
        match=r"runtime recovery blocking manual required: alerts=\['manual'\]",
    ):
        runner._validate_runtime_recovery_report(valid)
    runner._validate_recovery_protection_postcondition.assert_not_called()

    runner.context.strategy = SimpleNamespace()
    runner._validate_runtime_recovery_report(valid)
    runner._validate_recovery_protection_postcondition.assert_called_once_with(valid)


def test_signal_partition_preserves_list_type_order_and_duplicates() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    stop_one = SimpleNamespace(action=SignalAction.PLACE_STOP_LOSS_LONG)
    other = SimpleNamespace(action=SignalAction.OPEN_LONG)
    stop_two = SimpleNamespace(action=SignalAction.PLACE_STOP_LOSS_SHORT)
    report = SimpleNamespace(
        strategy_signals=(stop_one, other, stop_one, stop_two)
    )

    stops, others = runner._partition_recovery_signals(report)

    assert isinstance(stops, list)
    assert isinstance(others, list)
    assert stops == [stop_one, stop_one, stop_two]
    assert others == [other]


@pytest.mark.asyncio
async def test_signal_wrappers_keep_exact_execution_arguments() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner._execute_signals = AsyncMock()
    stops = [object()]
    others = [object()]

    await runner._execute_recovery_stop_signals(stops)
    await runner._execute_recovery_other_signals(others)

    assert runner._execute_signals.await_args_list == [
        call(
            stops,
            source="recovery",
            event_time_ms=None,
            metadata={"feature_type": "recovery"},
        ),
        call(
            others,
            source="recovery",
            event_time_ms=None,
            metadata={"feature_type": "recovery"},
        ),
    ]


def test_failure_counts_and_validation_check_failed_before_partial() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.stats = SimpleNamespace(failed_intents=2, partial_failures=3)
    counts = runner._capture_recovery_failure_counts()
    assert counts == (2, 3)

    runner.stats.failed_intents = 3
    runner.stats.partial_failures = 4
    with pytest.raises(
        LiveRuntimeError,
        match="all target exchanges rejected the stop order",
    ):
        runner._validate_recovery_stop_execution(counts)

    runner.stats.failed_intents = 2
    with pytest.raises(
        LiveRuntimeError,
        match="some target exchanges rejected the stop order",
    ):
        runner._validate_recovery_stop_execution(counts)


def test_finalize_logs_and_updates_snapshot_compatibility_fields(
    monkeypatch,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    old = (object(),)
    runner._last_snapshots = old
    runner._last_snapshot = old[0]
    first = object()
    second = object()
    report = SimpleNamespace(
        snapshots=[first, second],
        strategy_signals=(object(),),
        issues=("warning",),
    )
    logger = Mock()
    monkeypatch.setattr(recovery_component, "logger", logger)

    returned = runner._finalize_recovery_report(report)

    assert returned is runner._last_snapshots
    assert returned == (first, second)
    assert runner._last_snapshot is first
    logger.info.assert_called_once_with(
        "Runtime recovery completed | snapshots=%s strategy_signals=%s issues=%s",
        2,
        1,
        1,
    )


def test_finalize_empty_snapshots_preserves_old_tuple_or_raises() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    old = (object(),)
    runner._last_snapshots = old
    runner._last_snapshot = old[0]
    report = SimpleNamespace(snapshots=(), strategy_signals=(), issues=())

    assert runner._finalize_recovery_report(report) is old
    assert runner._last_snapshot is old[0]

    runner._last_snapshots = ()
    with pytest.raises(
        LiveRuntimeError,
        match="recovery completed without a startup snapshot",
    ):
        runner._finalize_recovery_report(report)
