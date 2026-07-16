from __future__ import annotations

from decimal import Decimal
from typing import Callable

import pytest

from src.strategy import (
    StrategyPositionContractError,
    StrategyPositionSide,
)
from strategies.eth_lf_portfolio_v8.strategy import Strategy as V8Strategy
from strategies.eth_lf_portfolio_v10b.strategy import Strategy as V10BStrategy
from strategies.eth_portfolio_v1.strategy import Strategy as PortfolioV1Strategy


StrategyFactory = Callable[[], object]


@pytest.fixture(
    params=(V8Strategy, V10BStrategy, PortfolioV1Strategy),
    ids=("v8", "v10b", "portfolio_v1"),
)
def strategy(request) -> object:
    return request.param()


def _valid_active_state(strategy: object, *, side_name: str = "LONG") -> object:
    position = strategy.position  # type: ignore[attr-defined]
    position.in_pos = True
    position.position_id = "contract-position"
    position.side = getattr(type(position.side), side_name)
    position.qty = Decimal("1")
    return position


def test_real_provider_returns_empty_only_for_inactive_position(
    strategy: object,
) -> None:
    strategy.position.in_pos = False  # type: ignore[attr-defined]
    strategy.position.position_id = None  # type: ignore[attr-defined]

    assert strategy.position_snapshots() == ()  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("side_name", "expected"),
    (
        ("LONG", StrategyPositionSide.LONG),
        ("SHORT", StrategyPositionSide.SHORT),
    ),
)
def test_real_provider_accepts_valid_directional_active_positions(
    strategy: object,
    side_name: str,
    expected: StrategyPositionSide,
) -> None:
    _valid_active_state(strategy, side_name=side_name)

    snapshot = strategy.position_snapshots()[0]  # type: ignore[attr-defined]

    assert snapshot.side is expected
    assert snapshot.base_quantity == Decimal("1")


@pytest.mark.parametrize(
    ("invalid_field", "invalid_value"),
    (
        ("position_id", None),
        ("side", "FLAT"),
        ("qty", Decimal("0")),
        ("qty", Decimal("-1")),
        ("qty", Decimal("NaN")),
        ("qty", Decimal("Infinity")),
    ),
)
def test_real_provider_rejects_every_invalid_active_position_state(
    strategy: object,
    invalid_field: str,
    invalid_value: object,
) -> None:
    position = _valid_active_state(strategy)
    if invalid_field == "side":
        invalid_value = getattr(type(position.side), str(invalid_value))
    setattr(position, invalid_field, invalid_value)

    with pytest.raises(StrategyPositionContractError) as exc_info:
        strategy.position_snapshots()  # type: ignore[attr-defined]

    message = str(exc_info.value)
    assert "strategy_id=" in message
    assert "position_id=" in message
    assert "in_pos=True" in message
    assert "side=" in message
    assert "quantity=" in message
    assert "provider=" in message
