from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.app import AppConfig
from src.platform import (
    Balance,
    ExchangeName,
    LeverageInfo,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    PositionMode,
    PositionSide,
)
from src.platform.snapshot import PlatformSnapshot
from src.order_management.reconciliation.models import (
    LiveStateReconciliationReport,
    ReconciliationVerdict,
)
from src.runtime.recovery.models import RecoveryReport
from src.runtime.recovery_coordinator import RuntimeRecoveryCoordinator
from src.runtime.models import RuntimeMode
from src.runtime.strategy_capabilities import ValidatedStrategyCapabilities
from src.runtime.reconciliation_coordinator import (
    RuntimeReconciliationCoordinator,
)
from src.runtime.runner import (
    LiveRuntimeError,
    LiveRuntimeRunner,
    _is_fatal_startup_error,
)
from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)
from src.strategy import StrategyRecoveryStatus


SYMBOL = "ETH-USDT-PERP"


class _Strategy:
    def __init__(
        self,
        snapshots: tuple[StrategyPositionSnapshot, ...],
    ) -> None:
        self._snapshots = snapshots
        self.recovery_blocking_manual_required = False
        self.recovery_alerts: list[str] = []

    def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
        return self._snapshots

    def recovery_status(self) -> StrategyRecoveryStatus:
        return StrategyRecoveryStatus(
            blocking_manual_required=self.recovery_blocking_manual_required,
            alerts=tuple(self.recovery_alerts),
        )


def _strategy_positions() -> tuple[StrategyPositionSnapshot, ...]:
    return (
        StrategyPositionSnapshot(
            strategy_id="eth_portfolio_v1",
            sleeve_id="lf",
            position_id="v9e-lf-runtime",
            symbol=SYMBOL,
            side=StrategyPositionSide.LONG,
            status=StrategyPositionStatus.ACTIVE,
            base_quantity=Decimal("0.6"),
            average_entry_price=Decimal("2000"),
            stop_price=Decimal("1900"),
            metadata={
                "active_exchanges": ["okx"],
                "protective_stop_required": True,
                "stop_order_ids": ["v9e-lf-runtime-stop"],
            },
        ),
        StrategyPositionSnapshot(
            strategy_id="eth_portfolio_v1",
            sleeve_id="mf",
            position_id="mf-low-sweep-time48-runtime",
            symbol=SYMBOL,
            side=StrategyPositionSide.LONG,
            status=StrategyPositionStatus.ACTIVE,
            base_quantity=Decimal("0.4"),
            average_entry_price=Decimal("2000"),
            stop_price=None,
            metadata={
                "active_exchanges": ["okx"],
                "protective_stop_required": False,
                "exit_variant": "time48",
            },
        ),
    )


def _snapshot(*, with_lf_stop: bool) -> PlatformSnapshot:
    stops = (
        (
            Order(
                exchange=ExchangeName.OKX,
                symbol=SYMBOL,
                raw_symbol="ETH-USDT-SWAP",
                order_id="v9e-lf-runtime-stop",
                client_order_id=None,
                status=OrderStatus.NEW,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                price=Decimal("1900"),
                quantity=Decimal("6"),
                raw={
                    "reduceOnly": "true",
                    "posSide": "long",
                    "position_id": "v9e-lf-runtime",
                },
            ),
        )
        if with_lf_stop
        else ()
    )
    return PlatformSnapshot(
        symbol=SYMBOL,
        balance=Balance(
            exchange=ExchangeName.OKX,
            asset="USDT",
            total=Decimal("10000"),
            available=Decimal("9000"),
        ),
        positions=(
            Position(
                exchange=ExchangeName.OKX,
                symbol=SYMBOL,
                raw_symbol="ETH-USDT-SWAP",
                side=PositionSide.LONG,
                quantity=Decimal("10"),
                entry_price=Decimal("2000"),
            ),
        ),
        open_orders=(),
        open_stop_orders=stops,
        leverage=LeverageInfo(
            exchange=ExchangeName.OKX,
            symbol=SYMBOL,
            raw_symbol="ETH-USDT-SWAP",
            leverage=Decimal("3"),
        ),
        position_mode=PositionMode.HEDGE,
    )


def _runner(strategy: _Strategy) -> LiveRuntimeRunner:
    runner = LiveRuntimeRunner.__new__(LiveRuntimeRunner)
    runner._recovery_coordinator = RuntimeRecoveryCoordinator()
    runner._reconciliation_coordinator = RuntimeReconciliationCoordinator()
    runner.app_config = AppConfig(
        symbol=SYMBOL,
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="eth_portfolio_v1",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )
    runner.context = SimpleNamespace(strategy=strategy)
    runner.runtime_config = SimpleNamespace(mode=RuntimeMode.LIVE_RUNTIME)
    runner._validated_strategy_capabilities = ValidatedStrategyCapabilities(
        identity="eth_portfolio_v1",
        position_snapshots=strategy,
        recovery_status=strategy,
        market_features=None,
        range_speed_history=None,
        startup_preview=None,
        pending_work=None,
    )
    return runner


def test_runtime_recovery_validates_all_v1_snapshots_with_aggregate_position() -> None:
    strategy = _Strategy(_strategy_positions())
    report = RecoveryReport(ok=True, snapshots=(_snapshot(with_lf_stop=True),))

    _runner(strategy)._validate_recovery_protection_postcondition(report)


def test_mf_explicit_no_stop_does_not_require_lf_stop_scope() -> None:
    mf_only = _strategy_positions()[1:]
    strategy = _Strategy(mf_only)
    report = RecoveryReport(ok=True, snapshots=(_snapshot(with_lf_stop=False),))

    _runner(strategy)._validate_recovery_protection_postcondition(report)


def test_lf_missing_stop_is_not_satisfied_by_mf_no_stop_policy() -> None:
    strategy = _Strategy(_strategy_positions())
    report = RecoveryReport(ok=True, snapshots=(_snapshot(with_lf_stop=False),))

    with pytest.raises(
        LiveRuntimeError,
        match="recovery protection postcondition failed",
    ):
        _runner(strategy)._validate_recovery_protection_postcondition(report)


@pytest.mark.asyncio
async def test_manual_required_blocks_startup_before_signal_execution() -> None:
    strategy = _Strategy(())

    class _RecoveryService:
        async def recover(self, *, strategy):
            strategy.recovery_blocking_manual_required = True
            strategy.recovery_alerts.append("missing_mf_recovery_metadata")
            return RecoveryReport(ok=True, snapshots=(_snapshot(with_lf_stop=False),))

    runner = _runner(strategy)
    runner.stats = SimpleNamespace(recovery_runs=0)
    runner._get_recovery_service = lambda: _RecoveryService()

    with pytest.raises(
        LiveRuntimeError,
        match="runtime recovery blocking manual required",
    ):
        await runner._run_recovery()


def _reconciliation_report(
    *,
    ok: bool,
    verdict: ReconciliationVerdict,
) -> LiveStateReconciliationReport:
    return LiveStateReconciliationReport(
        checked_at_ms=1,
        exchanges=("okx",),
        symbol=SYMBOL,
        ok=ok,
        verdict=verdict,
        issues=[] if ok else ["unsafe-startup-state"],
        stale_plans_closed=(
            1
            if verdict is ReconciliationVerdict.PASS_WITH_CLEANUP
            else 0
        ),
    )


@pytest.mark.parametrize(
    "verdict",
    (
        ReconciliationVerdict.FAIL_UNRESOLVED_FOLLOWER_POSITION,
        ReconciliationVerdict.FAIL_CONFIG,
        ReconciliationVerdict.FAIL_NEEDS_RECONCILE,
    ),
)
@pytest.mark.asyncio
async def test_startup_reconciliation_not_ok_is_fatal_hard_fail(
    verdict: ReconciliationVerdict,
    caplog,
) -> None:
    report = _reconciliation_report(ok=False, verdict=verdict)

    class _ReconciliationService:
        async def reconcile_and_apply(self, snapshots):
            return report

    runner = _runner(_Strategy(()))
    runner._get_reconciliation_service = lambda: _ReconciliationService()

    with caplog.at_level("ERROR"), pytest.raises(
        LiveRuntimeError,
        match=f"startup reconciliation failed: verdict={verdict.value}",
    ) as exc_info:
        await runner._run_reconciliation(
            (_snapshot(with_lf_stop=False),)
        )

    assert verdict.value in caplog.text
    assert "unsafe-startup-state" in caplog.text
    assert _is_fatal_startup_error(exc_info.value) is True


@pytest.mark.parametrize(
    "verdict",
    (
        ReconciliationVerdict.PASS,
        ReconciliationVerdict.PASS_WITH_CLEANUP,
    ),
)
@pytest.mark.asyncio
async def test_startup_reconciliation_ok_verdicts_continue(
    verdict: ReconciliationVerdict,
) -> None:
    report = _reconciliation_report(ok=True, verdict=verdict)

    class _ReconciliationService:
        async def reconcile_and_apply(self, snapshots):
            return report

    runner = _runner(_Strategy(()))
    runner._get_reconciliation_service = lambda: _ReconciliationService()

    await runner._run_reconciliation(
        (_snapshot(with_lf_stop=False),)
    )
