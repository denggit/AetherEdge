from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.runtime.models import RuntimeMode, RuntimePhase
from src.runtime.recovery import RuntimeRecoveryService
from src.runtime.recovery.models import RecoveryReport
from src.runtime.recovery_coordinator import (
    RuntimeRecoveryCoordinator,
    RuntimeRecoveryPlan,
)
from src.runtime.runner import LiveRuntimeRunner
from src.signals import SignalAction, TradeSignal
from src.strategy import (
    StrategyContractError,
    StrategyPositionContractError,
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
    StrategyRecoveryStatus,
)


class _RecoveringContractStrategy:
    def __init__(self, invalid: str) -> None:
        self.invalid = invalid
        self.recovered = False
        self.position_calls = 0

    def position_snapshots(self):
        self.position_calls += 1
        if not self.recovered:
            return ()
        if self.invalid == "wrong_result":
            return object()
        values = {
            "position_id": "recovered-position",
            "side": StrategyPositionSide.LONG,
            "base_quantity": Decimal("1"),
        }
        if self.invalid == "position_id":
            values["position_id"] = ""
        elif self.invalid == "side":
            values["side"] = StrategyPositionSide.FLAT
        elif self.invalid == "quantity":
            values["base_quantity"] = Decimal("0")
        return (
            StrategyPositionSnapshot(
                strategy_id="recovery-contract",
                position_id=values["position_id"],
                symbol="ETH-USDT-PERP",
                side=values["side"],
                status=StrategyPositionStatus.ACTIVE,
                base_quantity=values["base_quantity"],
            ),
        )

    def recovery_status(self):
        if self.recovered and self.invalid == "recovery_status":
            return "invalid"
        return StrategyRecoveryStatus()

    def has_pending_strategy_work(self):
        if self.recovered and self.invalid == "pending_work":
            return 1
        return False

    async def recover(self, _context):
        self.recovered = True
        return (
            TradeSignal(
                symbol="ETH-USDT-PERP",
                action=SignalAction.OPEN_LONG,
                quantity=Decimal("1"),
            ),
        )


def _runner_for(strategy: object) -> LiveRuntimeRunner:
    runner = object.__new__(LiveRuntimeRunner)
    runner.context = SimpleNamespace(strategy=strategy)
    runner.app_config = SimpleNamespace(
        strategy="tests.contract:Strategy",
        symbol="ETH-USDT-PERP",
    )
    runner.runtime_config = SimpleNamespace(mode=RuntimeMode.LIVE_RUNTIME)
    runner._validated_strategy_capabilities = SimpleNamespace(
        identity="recovery-contract"
    )
    runner._health = SimpleNamespace(phase=RuntimePhase.CATCHING_UP)
    runner._producer_tasks = []
    runner._sync_tasks = []
    runner._validate_recovery_protection_postcondition = Mock()
    return runner


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("invalid", "error_type"),
    (
        ("position_id", StrategyPositionContractError),
        ("side", StrategyPositionContractError),
        ("quantity", StrategyPositionContractError),
        ("wrong_result", StrategyPositionContractError),
        ("recovery_status", StrategyContractError),
        ("pending_work", StrategyContractError),
    ),
)
async def test_recovery_revalidates_all_dynamic_provider_outputs(
    invalid: str,
    error_type: type[StrategyContractError],
) -> None:
    strategy = _RecoveringContractStrategy(invalid)

    with pytest.raises(error_type):
        await RuntimeRecoveryService().recover(strategy=strategy)

    assert strategy.recovered is True
    assert strategy.position_calls == 2


@pytest.mark.asyncio
async def test_recovery_contract_failure_stops_before_signal_partition_or_execution() -> None:
    strategy = _RecoveringContractStrategy("position_id")
    calls: list[str] = []
    service = RuntimeRecoveryService()

    async def invoke(_service):
        calls.append("invoke")
        return await service.recover(strategy=strategy)

    def forbidden(name: str):
        def callback(*_args, **_kwargs):
            calls.append(name)
            raise AssertionError(f"recovery continued to {name}")

        return callback

    async def forbidden_async(*_args, **_kwargs):
        raise AssertionError("recovery executed a signal")

    plan = RuntimeRecoveryPlan(
        resolve_service=lambda: service,
        fallback_snapshots=lambda: (),
        invoke_service=invoke,
        record_run=forbidden("record"),
        validate_report=forbidden("validate"),
        partition_signals=forbidden("partition"),
        capture_failure_counts=forbidden("capture"),
        execute_stop_signals=forbidden_async,
        validate_stop_execution=forbidden("validate_stops"),
        validate_post_execution_protection=forbidden_async,
        execute_other_signals=forbidden_async,
        finalize_report=forbidden("finalize"),
    )

    with pytest.raises(StrategyPositionContractError):
        await RuntimeRecoveryCoordinator().execute(plan)

    assert calls == ["invoke"]


def test_runner_revalidates_injected_recovery_report_before_any_signal_work() -> None:
    strategy = _RecoveringContractStrategy("position_id")
    strategy.recovered = True
    runner = _runner_for(strategy)
    report = SimpleNamespace(
        ok=True,
        issues=(),
        metadata={"strategy_dynamic_capabilities_validated": True},
        strategy_signals=(object(),),
    )

    with pytest.raises(StrategyPositionContractError):
        runner._validate_runtime_recovery_report(report)

    assert runner._health.phase is RuntimePhase.CATCHING_UP
    assert runner._producer_tasks == []
    assert runner._sync_tasks == []
    runner._validate_recovery_protection_postcondition.assert_not_called()


@pytest.mark.asyncio
async def test_default_recovery_report_is_revalidated_by_runner() -> None:
    class ValidStrategy(_RecoveringContractStrategy):
        def __init__(self) -> None:
            super().__init__("valid")

    strategy = ValidStrategy()
    report = await RuntimeRecoveryService().recover(strategy=strategy)
    assert strategy.position_calls == 2
    assert "strategy_dynamic_capabilities_validated" not in report.metadata

    runner = _runner_for(strategy)
    runner._validate_runtime_recovery_report(report)

    assert strategy.position_calls == 3
    runner._validate_recovery_protection_postcondition.assert_called_once_with(
        report
    )


def test_runner_dynamic_validation_requires_established_startup_identity() -> None:
    runner = _runner_for(_RecoveringContractStrategy("valid"))
    runner._validated_strategy_capabilities = None

    with pytest.raises(
        StrategyContractError,
        match="requires established startup capabilities",
    ):
        runner._validate_runtime_recovery_report(
            RecoveryReport(ok=True)
        )

    runner._validate_recovery_protection_postcondition.assert_not_called()


@pytest.mark.asyncio
async def test_forged_recovery_metadata_cannot_reach_partition_or_execution() -> None:
    strategy = _RecoveringContractStrategy("position_id")
    strategy.recovered = True
    runner = _runner_for(strategy)
    report = RecoveryReport(
        ok=True,
        strategy_signals=(object(),),
        metadata={"strategy_dynamic_capabilities_validated": True},
    )
    service = SimpleNamespace(
        recover=AsyncMock(return_value=report),
    )
    runner._recovery_coordinator = RuntimeRecoveryCoordinator()
    runner._get_recovery_service = Mock(return_value=service)
    runner._recovery_fallback_snapshots = Mock(return_value=())
    runner.stats = SimpleNamespace(recovery_runs=0)
    runner._partition_recovery_signals = Mock()
    runner._capture_recovery_failure_counts = Mock()
    runner._execute_recovery_stop_signals = AsyncMock()
    runner._validate_recovery_stop_execution = Mock()
    runner._validate_post_execution_stop_protection = AsyncMock()
    runner._execute_recovery_other_signals = AsyncMock()
    runner._finalize_recovery_report = Mock()

    with pytest.raises(StrategyPositionContractError):
        await runner._run_recovery()

    runner._partition_recovery_signals.assert_not_called()
    runner._execute_recovery_stop_signals.assert_not_awaited()
    runner._execute_recovery_other_signals.assert_not_awaited()
    runner._finalize_recovery_report.assert_not_called()
    assert runner._health.phase is RuntimePhase.CATCHING_UP
    assert runner._producer_tasks == []
    assert runner._sync_tasks == []
