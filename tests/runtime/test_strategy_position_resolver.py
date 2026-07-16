from __future__ import annotations

from decimal import Decimal

import pytest

from src.runtime.strategy_positions import resolve_strategy_position_snapshots
from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)


def _snapshot(position_id: str) -> StrategyPositionSnapshot:
    return StrategyPositionSnapshot(
        strategy_id="test-strategy",
        position_id=position_id,
        symbol="ETH-USDT-PERP",
        side=StrategyPositionSide.LONG,
        status=StrategyPositionStatus.ACTIVE,
        base_quantity=Decimal("1"),
    )


def test_provider_returns_multiple_snapshots_in_original_order() -> None:
    first = _snapshot("position-2")
    second = _snapshot("position-1")

    class ProviderStrategy:
        def position_snapshots(self) -> list[StrategyPositionSnapshot]:
            return [first, second]

    assert resolve_strategy_position_snapshots(ProviderStrategy()) == (first, second)


def test_provider_preserves_duplicate_position_ids() -> None:
    first = _snapshot("duplicate")
    second = _snapshot("duplicate")

    class ProviderStrategy:
        def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
            return (first, second)

    assert resolve_strategy_position_snapshots(ProviderStrategy()) == (first, second)


def test_strategy_without_position_provider_returns_empty_tuple() -> None:
    class StrategyWithPrivatePositionState:
        position = object()

    assert resolve_strategy_position_snapshots(StrategyWithPrivatePositionState()) == ()


@pytest.mark.parametrize(
    "side",
    (StrategyPositionSide.FLAT, StrategyPositionSide.UNKNOWN),
)
def test_active_position_rejects_non_directional_side(
    side: StrategyPositionSide,
) -> None:
    with pytest.raises(ValueError, match="must not be FLAT or UNKNOWN"):
        StrategyPositionSnapshot(
            strategy_id="test-strategy",
            position_id="active-1",
            symbol="ETH-USDT-PERP",
            side=side,
            status=StrategyPositionStatus.ACTIVE,
            base_quantity=Decimal("1"),
        )


@pytest.mark.parametrize(
    "side",
    (StrategyPositionSide.LONG, StrategyPositionSide.SHORT),
)
def test_active_position_accepts_directional_side(
    side: StrategyPositionSide,
) -> None:
    snapshot = StrategyPositionSnapshot(
        strategy_id="test-strategy",
        position_id="active-1",
        symbol="ETH-USDT-PERP",
        side=side,
        status=StrategyPositionStatus.ACTIVE,
        base_quantity=Decimal("1"),
    )

    assert snapshot.side is side
