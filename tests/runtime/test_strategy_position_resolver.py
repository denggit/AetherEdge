from __future__ import annotations

from decimal import Decimal
from enum import Enum
from types import SimpleNamespace

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


def _legacy_strategy(*, in_pos: bool = True, side: object = 1) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(strategy_id="legacy-strategy", symbol="ETH-USDT-PERP"),
        position=SimpleNamespace(
            in_pos=in_pos,
            position_id="legacy-position-1",
            side=side,
            qty=Decimal("2.5"),
            avg_entry=Decimal("2500"),
            stop_price=Decimal("2400"),
            entry_engine="breakout",
            entry_time_ms=123456789,
        ),
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


def test_provider_takes_priority_over_legacy_position() -> None:
    strategy = _legacy_strategy()
    strategy.position_snapshots = lambda: (_snapshot("provider-position"),)

    snapshots = resolve_strategy_position_snapshots(strategy)

    assert tuple(snapshot.position_id for snapshot in snapshots) == ("provider-position",)


def test_flat_legacy_position_returns_empty_tuple() -> None:
    assert resolve_strategy_position_snapshots(_legacy_strategy(in_pos=False)) == ()


@pytest.mark.parametrize(
    ("side", "expected"),
    (
        (1, StrategyPositionSide.LONG),
        (-1, StrategyPositionSide.SHORT),
        ("long", StrategyPositionSide.LONG),
        ("SHORT", StrategyPositionSide.SHORT),
    ),
)
def test_legacy_side_is_normalized(side: object, expected: StrategyPositionSide) -> None:
    snapshots = resolve_strategy_position_snapshots(_legacy_strategy(side=side))

    assert len(snapshots) == 1
    assert snapshots[0].side is expected


def test_legacy_int_enum_side_uses_its_value() -> None:
    class LegacySide(int, Enum):
        LONG = 1

    snapshots = resolve_strategy_position_snapshots(_legacy_strategy(side=LegacySide.LONG))

    assert snapshots[0].side is StrategyPositionSide.LONG


def test_active_legacy_position_returns_generic_snapshot() -> None:
    snapshot = resolve_strategy_position_snapshots(_legacy_strategy())[0]

    assert snapshot == StrategyPositionSnapshot(
        strategy_id="legacy-strategy",
        position_id="legacy-position-1",
        symbol="ETH-USDT-PERP",
        side=StrategyPositionSide.LONG,
        status=StrategyPositionStatus.ACTIVE,
        base_quantity=Decimal("2.5"),
        average_entry_price=Decimal("2500"),
        stop_price=Decimal("2400"),
        engine="breakout",
        entry_time_ms=123456789,
    )


def test_unknown_legacy_side_is_safe() -> None:
    snapshot = resolve_strategy_position_snapshots(_legacy_strategy(side=object()))[0]

    assert snapshot.side is StrategyPositionSide.UNKNOWN


@pytest.mark.parametrize(
    "strategy",
    (
        object(),
        SimpleNamespace(position=object()),
        SimpleNamespace(position=SimpleNamespace(in_pos=True)),
        SimpleNamespace(
            strategy_id="legacy-strategy",
            symbol="ETH-USDT-PERP",
            position=SimpleNamespace(
                in_pos=True,
                position_id="position-1",
                side=1,
                qty="not-a-number",
                avg_entry=None,
                stop_price=None,
            ),
        ),
    ),
)
def test_missing_or_invalid_legacy_fields_do_not_raise(strategy: object) -> None:
    assert resolve_strategy_position_snapshots(strategy) == ()
