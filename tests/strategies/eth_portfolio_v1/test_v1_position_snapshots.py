from __future__ import annotations

import json
from decimal import Decimal

import pytest

from src.runtime.strategy_positions import resolve_strategy_position_snapshots
from src.strategy.positions import StrategyPositionSide, StrategyPositionStatus
from strategies.eth_portfolio_v1.domain.models import Side
from strategies.eth_portfolio_v1.strategy import Strategy


def test_v1_declares_position_snapshot_provider_used_before_legacy_fallback() -> None:
    strategy = Strategy()
    assert callable(strategy.position_snapshots)

    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=123,
        avg_entry=Decimal("2500"),
        qty=Decimal("0.40"),
        stop_price=Decimal("2400"),
        entry_engine="MOMENTUM_V3",
        position_id="original-lf-position",
    )

    snapshot = resolve_strategy_position_snapshots(strategy)[0]

    assert snapshot.sleeve_id == "lf"
    assert snapshot.metadata["active_exchanges"] == []


@pytest.mark.parametrize(
    ("side", "expected_side"),
    (
        (Side.LONG, StrategyPositionSide.LONG),
        (Side.SHORT, StrategyPositionSide.SHORT),
    ),
)
def test_active_lf_position_maps_to_generic_snapshot(
    side: Side,
    expected_side: StrategyPositionSide,
) -> None:
    strategy = Strategy()
    strategy.position.open_master(
        side=side,
        entry_time_ms=123,
        avg_entry=Decimal("2500"),
        qty=Decimal("0.40"),
        stop_price=Decimal("2400"),
        entry_engine="MOMENTUM_V3",
        position_id="original-lf-position",
    )
    strategy.position.mark_leg_open(
        exchange="okx",
        avg_fill_price=Decimal("2500"),
        base_qty=Decimal("0.40"),
    )

    snapshot = strategy.position_snapshots()[0]

    assert snapshot.strategy_id == "eth_portfolio_v1"
    assert snapshot.sleeve_id == "lf"
    assert snapshot.position_id == "original-lf-position"
    assert snapshot.symbol == strategy.config.symbol
    assert snapshot.status is StrategyPositionStatus.ACTIVE
    assert snapshot.side is expected_side
    assert snapshot.base_quantity == Decimal("0.40")
    assert snapshot.average_entry_price == Decimal("2500")
    assert snapshot.stop_price == Decimal("2400")
    assert snapshot.engine == "MOMENTUM_V3"
    assert snapshot.entry_time_ms == 123
    assert snapshot.metadata["active_exchanges"] == ["okx"]
    json.dumps(dict(snapshot.metadata))


def test_flat_lf_position_returns_no_active_snapshot() -> None:
    strategy = Strategy()

    snapshots = strategy.position_snapshots()

    assert snapshots == ()
    assert not any(
        snapshot.status is StrategyPositionStatus.ACTIVE
        for snapshot in snapshots
    )
