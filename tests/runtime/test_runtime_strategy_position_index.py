from __future__ import annotations

from decimal import Decimal

from src.runtime.strategy_positions import StrategyPositionSnapshotIndex
from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)


def _snapshot(
    position_id: str,
    *,
    symbol: str = "ETH-USDT-PERP",
    side: StrategyPositionSide = StrategyPositionSide.LONG,
    status: StrategyPositionStatus = StrategyPositionStatus.ACTIVE,
) -> StrategyPositionSnapshot:
    return StrategyPositionSnapshot(
        strategy_id="test-strategy",
        position_id=position_id,
        symbol=symbol,
        side=side,
        status=status,
        base_quantity=Decimal("1"),
    )


def test_active_returns_every_active_snapshot_in_provider_order() -> None:
    first = _snapshot("position-2")
    closing = _snapshot("closing", status=StrategyPositionStatus.CLOSING)
    second = _snapshot("position-1", side=StrategyPositionSide.SHORT)
    index = StrategyPositionSnapshotIndex((first, closing, second))

    assert index.active == (first, second)


def test_by_position_id_preserves_duplicates() -> None:
    first = _snapshot("duplicate")
    unrelated = _snapshot("other")
    second = _snapshot("duplicate", side=StrategyPositionSide.SHORT)
    index = StrategyPositionSnapshotIndex((first, unrelated, second))

    assert index.by_position_id("duplicate") == (first, second)


def test_by_symbol_returns_every_match_without_sorting() -> None:
    first = _snapshot("first", symbol="BTC-USDT-PERP")
    unrelated = _snapshot("other")
    second = _snapshot("second", symbol="BTC-USDT-PERP")
    index = StrategyPositionSnapshotIndex((first, unrelated, second))

    assert index.by_symbol("BTC-USDT-PERP") == (first, second)


def test_by_symbol_side_matches_both_fields() -> None:
    first = _snapshot("first", side=StrategyPositionSide.SHORT)
    wrong_side = _snapshot("wrong-side")
    wrong_symbol = _snapshot(
        "wrong-symbol",
        symbol="BTC-USDT-PERP",
        side=StrategyPositionSide.SHORT,
    )
    second = _snapshot("second", side=StrategyPositionSide.SHORT)
    index = StrategyPositionSnapshotIndex((first, wrong_side, wrong_symbol, second))

    assert index.by_symbol_side(
        "ETH-USDT-PERP",
        StrategyPositionSide.SHORT,
    ) == (first, second)


def test_multiple_active_snapshots_are_not_collapsed_to_one() -> None:
    first = _snapshot("first")
    second = _snapshot("second")
    index = StrategyPositionSnapshotIndex((first, second))

    assert index.active == (first, second)
    assert index.single_active_or_none_for_legacy() is None
