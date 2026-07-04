from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from src.runtime.recovery.service import RuntimeRecoveryService
from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)


def _snapshot(position_id: str, side: StrategyPositionSide) -> StrategyPositionSnapshot:
    return StrategyPositionSnapshot(
        strategy_id="test-strategy",
        position_id=position_id,
        symbol="ETH-USDT-PERP",
        side=side,
        status=StrategyPositionStatus.ACTIVE,
        base_quantity=Decimal("1"),
    )


class RecoveringProviderStrategy:
    def __init__(self, snapshots: tuple[StrategyPositionSnapshot, ...]) -> None:
        self.snapshots = snapshots
        self.recovery_contexts = []

    def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
        return self.snapshots

    async def recover(self, context) -> tuple:
        self.recovery_contexts.append(context)
        return ()


class RecoveringLegacyStrategy:
    def __init__(self, *, in_pos: bool) -> None:
        self.config = SimpleNamespace(
            strategy_id="legacy-strategy",
            symbol="ETH-USDT-PERP",
        )
        self.position = SimpleNamespace(
            in_pos=in_pos,
            position_id="legacy-position",
            side="long",
            qty=Decimal("2"),
            avg_entry=Decimal("2500"),
            stop_price=Decimal("2400"),
        )
        self.recovery_contexts = []

    async def recover(self, context) -> tuple:
        self.recovery_contexts.append(context)
        return ()


def test_recovery_context_receives_all_active_strategy_positions() -> None:
    first = _snapshot("first", StrategyPositionSide.LONG)
    second = _snapshot("second", StrategyPositionSide.SHORT)
    strategy = RecoveringProviderStrategy((first, second))

    report = asyncio.run(RuntimeRecoveryService().recover(strategy=strategy))

    context = strategy.recovery_contexts[0]
    assert context.metadata["strategy_positions"] == (first, second)
    assert context.metadata["active_strategy_positions"] == (first, second)
    assert report.strategy_positions == (first, second)
    assert report.active_strategy_positions == (first, second)


def test_recovery_does_not_reduce_multiple_positions_to_first_active() -> None:
    snapshots = (
        _snapshot("sleeve-a", StrategyPositionSide.LONG),
        _snapshot("sleeve-b", StrategyPositionSide.LONG),
    )
    strategy = RecoveringProviderStrategy(snapshots)

    report = asyncio.run(RuntimeRecoveryService().recover(strategy=strategy))

    assert tuple(item.position_id for item in report.active_strategy_positions) == (
        "sleeve-a",
        "sleeve-b",
    )


def test_recovery_uses_legacy_position_fallback() -> None:
    strategy = RecoveringLegacyStrategy(in_pos=True)

    report = asyncio.run(RuntimeRecoveryService().recover(strategy=strategy))

    assert len(report.active_strategy_positions) == 1
    assert report.active_strategy_positions[0].position_id == "legacy-position"
    assert strategy.recovery_contexts[0].metadata["active_strategy_positions"] == (
        report.active_strategy_positions
    )


def test_recovery_without_active_strategy_position_returns_empty_tuples() -> None:
    strategy = RecoveringLegacyStrategy(in_pos=False)

    report = asyncio.run(RuntimeRecoveryService().recover(strategy=strategy))

    assert report.strategy_positions == ()
    assert report.active_strategy_positions == ()
    assert strategy.recovery_contexts[0].metadata["active_strategy_positions"] == ()
