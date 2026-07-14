from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError, fields
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from src.app import AppConfig
from src.order_management.reconciliation.models import (
    FakeOrderRef,
    ReconciliationAction,
    ReconciliationVerdict,
)
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime import runner as runner_module
from src.runtime.reconciliation_coordinator import (
    RuntimeReconciliationCoordinator,
    RuntimeReconciliationPlan,
)
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeError, LiveRuntimeRunner


PLAN_FIELDS = (
    "resolve_service",
    "validate_snapshots",
    "begin_reconciliation",
    "apply_legacy_adoptions",
    "invoke_service",
    "handle_report",
)


def _runner(*, reconciliation_coordinator=None) -> LiveRuntimeRunner:
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
        "runtime_requirements": StrategyRuntimeRequirements.from_mapping({}),
    }
    if reconciliation_coordinator is not None:
        services["reconciliation_coordinator"] = reconciliation_coordinator
    return LiveRuntimeRunner(
        app_config=config,
        app_context=SimpleNamespace(strategy=object()),
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services=services,
    )


def _plan(
    calls: list[str],
    *,
    service: object | None = None,
    report: object | None = None,
    overrides: dict[str, object] | None = None,
) -> tuple[RuntimeReconciliationPlan, dict[str, object]]:
    service = object() if service is None else service
    report = object() if report is None else report
    received: dict[str, object] = {}

    def resolve_service() -> object:
        calls.append("resolve")
        return service

    def validate_snapshots(snapshots: tuple[object, ...]) -> None:
        calls.append("validate")
        received["validated_snapshots"] = snapshots

    def begin_reconciliation(snapshots: tuple[object, ...]) -> None:
        calls.append("begin")
        received["begun_snapshots"] = snapshots

    def apply_legacy_adoptions(value: object) -> None:
        calls.append("legacy")
        received["legacy_service"] = value

    async def invoke_service(
        value: object,
        snapshots: tuple[object, ...],
    ) -> object:
        calls.append("invoke")
        received["invoked_service"] = value
        received["invoked_snapshots"] = snapshots
        return report

    def handle_report(value: object) -> None:
        calls.append("handle")
        received["handled_report"] = value

    values = {
        "resolve_service": resolve_service,
        "validate_snapshots": validate_snapshots,
        "begin_reconciliation": begin_reconciliation,
        "apply_legacy_adoptions": apply_legacy_adoptions,
        "invoke_service": invoke_service,
        "handle_report": handle_report,
    }
    values.update(overrides or {})
    return RuntimeReconciliationPlan(**values), received


def _report(**overrides):
    values = {
        "stale_plans_closed": 0,
        "fake_order_refs_found": [],
        "unresolved_follower_positions": 0,
        "actions": [],
        "alerts": [],
        "verdict": ReconciliationVerdict.PASS,
        "ok": True,
        "issues": [],
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _adoption(position_id: str, exchange: str) -> dict[str, str]:
    return {
        "position_id": position_id,
        "exchange": exchange,
        "stop_order_id": f"{position_id}-stop",
        "stop_client_order_id": f"{position_id}-client-stop",
        "effective_stop_price": "1900",
        "canonical_theoretical_stop_price": "1895",
        "resolution_status": "adopted",
    }


def test_coordinator_is_stateless_and_plan_is_frozen_callbacks_only() -> None:
    plan, _ = _plan([])

    assert vars(RuntimeReconciliationCoordinator()) == {}
    assert tuple(field.name for field in fields(plan)) == PLAN_FIELDS
    with pytest.raises(FrozenInstanceError):
        plan.handle_report = lambda _report: None  # type: ignore[misc]


@pytest.mark.asyncio
async def test_service_none_calls_only_resolver_and_returns_none() -> None:
    calls: list[str] = []
    plan, _ = _plan(calls)
    plan = RuntimeReconciliationPlan(
        **{
            **{field.name: getattr(plan, field.name) for field in fields(plan)},
            "resolve_service": lambda: calls.append("resolve") or None,
        }
    )

    result = await RuntimeReconciliationCoordinator().execute((object(),), plan)

    assert result is None
    assert calls == ["resolve"]


@pytest.mark.asyncio
async def test_full_flow_is_ordered_once_and_preserves_all_identities() -> None:
    calls: list[str] = []
    service = object()
    report = object()
    snapshots = (object(), object())
    plan, received = _plan(calls, service=service, report=report)

    result = await RuntimeReconciliationCoordinator().execute(snapshots, plan)

    assert result is None
    assert calls == ["resolve", "validate", "begin", "legacy", "invoke", "handle"]
    assert received["validated_snapshots"] is snapshots
    assert received["begun_snapshots"] is snapshots
    assert received["invoked_snapshots"] is snapshots
    assert received["legacy_service"] is service
    assert received["invoked_service"] is service
    assert received["handled_report"] is report


@pytest.mark.asyncio
async def test_callback_exception_propagates_and_stops_following_steps() -> None:
    calls: list[str] = []
    error = RuntimeError("legacy adoption failed")

    def fail(_service: object) -> None:
        calls.append("failed")
        raise error

    plan, _ = _plan(calls, overrides={"apply_legacy_adoptions": fail})

    with pytest.raises(RuntimeError) as raised:
        await RuntimeReconciliationCoordinator().execute((object(),), plan)

    assert raised.value is error
    assert calls == ["resolve", "validate", "begin", "failed"]


@pytest.mark.asyncio
async def test_cancelled_error_propagates_unchanged() -> None:
    cancelled = asyncio.CancelledError("cancelled")

    async def cancel(_service: object, _snapshots: tuple[object, ...]):
        raise cancelled

    plan, _ = _plan([], overrides={"invoke_service": cancel})
    with pytest.raises(asyncio.CancelledError) as raised:
        await RuntimeReconciliationCoordinator().execute((object(),), plan)
    assert raised.value is cancelled


def test_injected_coordinator_has_priority_without_default_construction(
    monkeypatch,
) -> None:
    coordinator = SimpleNamespace(execute=AsyncMock())
    default_factory = Mock()
    monkeypatch.setattr(
        runner_module,
        "RuntimeReconciliationCoordinator",
        default_factory,
    )

    runner = _runner(reconciliation_coordinator=coordinator)

    default_factory.assert_not_called()
    coordinator.execute.assert_not_called()
    assert runner._reconciliation_coordinator is coordinator
    assert runner.services["reconciliation_coordinator"] is coordinator
    assert runner._reconciliation_service == "__default__"
    assert runner._position_plan_store is None
    assert runner._order_journal is None


def test_default_coordinator_is_created_once_written_back_and_not_executed(
    monkeypatch,
) -> None:
    coordinator = SimpleNamespace(execute=AsyncMock())
    factory = Mock(return_value=coordinator)
    monkeypatch.setattr(
        runner_module,
        "RuntimeReconciliationCoordinator",
        factory,
    )

    runner = _runner()

    factory.assert_called_once_with()
    coordinator.execute.assert_not_called()
    assert runner._reconciliation_coordinator is coordinator
    assert runner.services["reconciliation_coordinator"] is coordinator
    assert runner._reconciliation_service == "__default__"
    assert runner._position_plan_store is None
    assert runner._order_journal is None


@pytest.mark.asyncio
async def test_runner_builds_complete_plan_and_delegates_original_snapshots() -> None:
    captured: list[tuple[object, RuntimeReconciliationPlan]] = []

    class Coordinator:
        async def execute(self, snapshots, plan) -> None:
            captured.append((snapshots, plan))

    runner = _runner(reconciliation_coordinator=Coordinator())
    snapshots = (object(),)

    result = await runner._run_reconciliation(snapshots)

    assert result is None
    assert len(captured) == 1
    assert captured[0][0] is snapshots
    plan = captured[0][1]
    expected = {
        "resolve_service": runner._get_reconciliation_service,
        "validate_snapshots": runner._validate_startup_reconciliation_snapshots,
        "begin_reconciliation": runner._log_startup_reconciliation_begin,
        "apply_legacy_adoptions": runner._apply_startup_legacy_stop_adoptions,
        "invoke_service": runner._invoke_startup_reconciliation_service,
        "handle_report": runner._handle_startup_reconciliation_report,
    }
    assert all(getattr(plan, name) == callback for name, callback in expected.items())


def test_snapshot_validation_keeps_count_sorted_names_and_exact_error() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.app_config = SimpleNamespace(
        exchanges=(
            SimpleNamespace(value="okx"),
            SimpleNamespace(value="binance"),
            SimpleNamespace(value="third"),
        )
    )
    snapshots = (
        SimpleNamespace(
            leverage=SimpleNamespace(exchange=SimpleNamespace(value="zeta"))
        ),
        SimpleNamespace(
            leverage=SimpleNamespace(exchange=SimpleNamespace(value="alpha"))
        ),
    )

    with pytest.raises(
        LiveRuntimeError,
        match=(
            r"startup reconciliation missing exchange snapshots: expected 3 "
            r"exchanges \(okx, binance, third\), got 2 \(alpha, zeta\)"
        ),
    ):
        runner._validate_startup_reconciliation_snapshots(snapshots)

    runner.app_config.exchanges = (object(), object())
    runner._validate_startup_reconciliation_snapshots(snapshots)


def test_begin_log_keeps_snapshot_order_and_exact_arguments(monkeypatch) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    snapshots = (
        SimpleNamespace(
            leverage=SimpleNamespace(exchange=SimpleNamespace(value="binance"))
        ),
        SimpleNamespace(
            leverage=SimpleNamespace(exchange=SimpleNamespace(value="okx"))
        ),
    )
    logger = Mock()
    monkeypatch.setattr(runner_module, "logger", logger)

    runner._log_startup_reconciliation_begin(snapshots)

    logger.info.assert_called_once_with(
        "Startup reconciliation starting | exchanges=%s count=%s",
        "binance, okx",
        2,
    )


def test_empty_legacy_adoptions_do_not_read_clock_or_call_service(
    monkeypatch,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.context = SimpleNamespace(strategy=SimpleNamespace(_legacy_adoptions=[]))
    runner.app_config = SimpleNamespace(symbol="ETH-USDT-PERP")
    service = SimpleNamespace(_apply_actions=Mock())
    clock = Mock()
    monkeypatch.setattr(runner_module.time, "time", clock)

    runner._apply_startup_legacy_stop_adoptions(service)

    clock.assert_not_called()
    service._apply_actions.assert_not_called()
    assert runner.context.strategy._legacy_adoptions == []


def test_legacy_adoptions_keep_order_fields_clock_and_clear_after_success(
    monkeypatch,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    adoptions = [_adoption("first", "okx"), _adoption("second", "binance")]
    runner.context = SimpleNamespace(
        strategy=SimpleNamespace(_legacy_adoptions=adoptions)
    )
    runner.app_config = SimpleNamespace(symbol="ETH-USDT-PERP")
    service = SimpleNamespace(_apply_actions=Mock())
    clock = Mock(return_value=1.25)
    logger = Mock()
    monkeypatch.setattr(runner_module.time, "time", clock)
    monkeypatch.setattr(runner_module, "logger", logger)

    runner._apply_startup_legacy_stop_adoptions(service)

    clock.assert_called_once_with()
    assert len(service._apply_actions.call_args_list) == 2
    actions = [args.args[0][0] for args in service._apply_actions.call_args_list]
    assert all(isinstance(action, ReconciliationAction) for action in actions)
    assert [action.target for action in actions] == [
        "leg:first:okx",
        "leg:second:binance",
    ]
    assert [action.action_type for action in actions] == [
        "adopt_legacy_stop_reference",
        "adopt_legacy_stop_reference",
    ]
    assert actions[0].detail == {
        **adoptions[0],
        "adopted_at_ms": 1250,
    }
    assert actions[1].detail == {
        **adoptions[1],
        "adopted_at_ms": 1250,
    }
    assert all(
        args.args[1] == "ETH-USDT-PERP"
        for args in service._apply_actions.call_args_list
    )
    assert runner.context.strategy._legacy_adoptions == []
    assert logger.warning.call_args_list == [
        call(
            "Startup recovery: legacy stop adopted | position_id=%s exchange=%s stop_order_id=%s effective_stop_price=%s",
            "first",
            "okx",
            "first-stop",
            "1900",
        ),
        call(
            "Startup recovery: legacy stop adopted | position_id=%s exchange=%s stop_order_id=%s effective_stop_price=%s",
            "second",
            "binance",
            "second-stop",
            "1900",
        ),
    ]


def test_legacy_adoption_failure_preserves_original_list(monkeypatch) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    adoptions = [_adoption("first", "okx"), _adoption("second", "binance")]
    runner.context = SimpleNamespace(
        strategy=SimpleNamespace(_legacy_adoptions=adoptions)
    )
    runner.app_config = SimpleNamespace(symbol="ETH-USDT-PERP")
    error = RuntimeError("store write failed")
    service = SimpleNamespace(
        _apply_actions=Mock(side_effect=[None, error])
    )
    monkeypatch.setattr(runner_module.time, "time", Mock(return_value=2.0))

    with pytest.raises(RuntimeError) as raised:
        runner._apply_startup_legacy_stop_adoptions(service)

    assert raised.value is error
    assert runner.context.strategy._legacy_adoptions is adoptions
    assert service._apply_actions.call_count == 2


@pytest.mark.asyncio
async def test_invoke_service_receives_original_snapshots_once() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    report = object()
    service = SimpleNamespace(reconcile_and_apply=AsyncMock(return_value=report))
    snapshots = (object(), object())

    returned = await runner._invoke_startup_reconciliation_service(
        service,
        snapshots,
    )

    assert returned is report
    service.reconcile_and_apply.assert_awaited_once_with(snapshots)
    assert service.reconcile_and_apply.await_args.args[0] is snapshots


def test_report_handler_preserves_warning_alert_and_failure_order(
    monkeypatch,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    emit = Mock()
    runner.context = SimpleNamespace(alerts=SimpleNamespace(emit=emit))
    logger = Mock()
    monkeypatch.setattr(runner_module, "logger", logger)
    ref = FakeOrderRef(
        position_id="position-1",
        exchange="okx",
        leg_role="master",
        field="stop_order_id",
        value="fake-stop",
        reason="pattern_match",
    )
    action = ReconciliationAction(
        action_type="set_master_closed_follower_close_required",
        target="position-2",
    )
    report = _report(
        stale_plans_closed=1,
        fake_order_refs_found=[ref],
        unresolved_follower_positions=1,
        actions=[action],
        alerts=[
            {"subject": "first", "content": "one", "severity": "warning"},
            {"subject": "second", "content": "two"},
        ],
        verdict=ReconciliationVerdict.FAIL_CONFIG,
        ok=False,
        issues=["unsafe"],
    )

    with pytest.raises(
        LiveRuntimeError,
        match=(
            "startup reconciliation failed: verdict=fail_config "
            r"issues=\['unsafe'\]"
        ),
    ):
        runner._handle_startup_reconciliation_report(report)

    assert logger.warning.call_args_list == [
        call(
            "Startup reconciliation closed %s stale position plan(s) | fake_refs=%s verdict=%s",
            1,
            1,
            "fail_config",
        ),
        call(
            "Fake order ref cleaned | position_id=%s exchange=%s field=%s value=%s reason=%s",
            "position-1",
            "okx",
            "stop_order_id",
            "fake-stop",
            "pattern_match",
        ),
        call(
            "Startup reconciliation: %s unresolved follower position(s) | position_id(s)=%s",
            1,
            "position-2",
        ),
    ]
    assert [item.subject for item in (args.args[0] for args in emit.call_args_list)] == [
        "first",
        "second",
    ]
    assert [item.severity for item in (args.args[0] for args in emit.call_args_list)] == [
        "warning",
        "error",
    ]
    logger.error.assert_called_once_with(
        "Startup reconciliation failed | verdict=%s issues=%s",
        "fail_config",
        ["unsafe"],
    )
    logger.info.assert_not_called()


@pytest.mark.parametrize(
    ("verdict", "stale", "fake_refs"),
    [
        (ReconciliationVerdict.PASS_WITH_CLEANUP, 0, []),
        (ReconciliationVerdict.PASS, 1, []),
        (
            ReconciliationVerdict.PASS,
            0,
            [
                FakeOrderRef(
                    position_id="position-1",
                    exchange="okx",
                    leg_role="master",
                    field="stop_order_id",
                    value="fake-stop",
                    reason="pattern_match",
                )
            ],
        ),
    ],
)
def test_report_cleanup_log_all_three_conditions(
    monkeypatch,
    verdict,
    stale: int,
    fake_refs: list[object],
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.context = SimpleNamespace(alerts=SimpleNamespace(emit=Mock()))
    logger = Mock()
    monkeypatch.setattr(runner_module, "logger", logger)
    report = _report(
        verdict=verdict,
        stale_plans_closed=stale,
        fake_order_refs_found=fake_refs,
    )

    runner._handle_startup_reconciliation_report(report)

    normalized = verdict.value if hasattr(verdict, "value") else str(verdict)
    logger.info.assert_called_once_with(
        "Startup reconciliation passed with cleanup | verdict=%s stale_plans_closed=%s fake_refs=%s",
        normalized,
        stale,
        len(fake_refs),
    )


def test_report_plain_pass_accepts_string_verdict_and_logs_exactly(
    monkeypatch,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.context = SimpleNamespace(alerts=SimpleNamespace(emit=Mock()))
    logger = Mock()
    monkeypatch.setattr(runner_module, "logger", logger)

    runner._handle_startup_reconciliation_report(_report(verdict="pass"))

    logger.info.assert_called_once_with(
        "Startup reconciliation passed | verdict=%s",
        "pass",
    )
