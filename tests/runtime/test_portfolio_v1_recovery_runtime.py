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
from src.runtime.recovery.models import RecoveryReport
from src.runtime.runner import LiveRuntimeError, LiveRuntimeRunner
from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)


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
