from __future__ import annotations

from decimal import Decimal

import pytest

from src.strategy import (
    StrategyPositionProvider,
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)


def _snapshot(
    *,
    strategy_id: str = "test-strategy",
    position_id: str = "position-1",
    symbol: str = "ETH-USDT-PERP",
    status: StrategyPositionStatus = StrategyPositionStatus.ACTIVE,
    base_quantity: Decimal = Decimal("1"),
) -> StrategyPositionSnapshot:
    return StrategyPositionSnapshot(
        strategy_id=strategy_id,
        position_id=position_id,
        symbol=symbol,
        side=StrategyPositionSide.LONG,
        status=status,
        base_quantity=base_quantity,
    )


def test_active_position_requires_position_id() -> None:
    with pytest.raises(ValueError, match="position_id"):
        _snapshot(position_id="")


@pytest.mark.parametrize(
    ("field", "value"),
    (("strategy_id", ""), ("symbol", "   ")),
)
def test_strategy_id_and_symbol_must_be_non_empty(field: str, value: str) -> None:
    kwargs = {field: value}
    with pytest.raises(ValueError, match=field):
        _snapshot(**kwargs)


def test_side_must_be_strategy_position_side() -> None:
    with pytest.raises(ValueError, match="side"):
        StrategyPositionSnapshot(
            strategy_id="test-strategy",
            position_id="position-1",
            symbol="ETH-USDT-PERP",
            side="long",  # type: ignore[arg-type]
            status=StrategyPositionStatus.ACTIVE,
            base_quantity=Decimal("1"),
        )


def test_status_must_be_strategy_position_status() -> None:
    with pytest.raises(ValueError, match="status"):
        StrategyPositionSnapshot(
            strategy_id="test-strategy",
            position_id="position-1",
            symbol="ETH-USDT-PERP",
            side=StrategyPositionSide.LONG,
            status="active",  # type: ignore[arg-type]
            base_quantity=Decimal("1"),
        )


def test_active_position_rejects_negative_quantity() -> None:
    with pytest.raises(ValueError, match="base_quantity"):
        _snapshot(base_quantity=Decimal("-0.1"))


def test_flat_snapshot_may_have_empty_position_id() -> None:
    snapshot = _snapshot(
        position_id="",
        status=StrategyPositionStatus.FLAT,
        base_quantity=Decimal("0"),
    )

    assert snapshot.position_id == ""


def test_provider_protocol_is_optional_and_runtime_checkable() -> None:
    class FakeStrategy:
        def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
            return (_snapshot(),)

    strategy = FakeStrategy()

    assert isinstance(strategy, StrategyPositionProvider)
    assert strategy.position_snapshots()[0].position_id == "position-1"
