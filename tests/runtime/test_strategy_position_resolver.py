from __future__ import annotations

from decimal import Decimal

import pytest

from src.runtime.strategy_positions import resolve_strategy_position_snapshots
from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)
from src.strategy import (
    StrategyCapabilityError,
    StrategyPositionContractError,
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


def test_position_contract_error_is_a_fatal_capability_error() -> None:
    assert issubclass(StrategyPositionContractError, StrategyCapabilityError)


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
    (
        StrategyPositionSide.BOTH,
        StrategyPositionSide.FLAT,
        StrategyPositionSide.UNKNOWN,
    ),
)
def test_active_position_rejects_non_directional_side(
    side: StrategyPositionSide,
) -> None:
    with pytest.raises(StrategyPositionContractError, match="LONG or SHORT"):
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


@pytest.mark.parametrize(
    "quantity",
    (
        Decimal("0"),
        Decimal("-1"),
        Decimal("NaN"),
        Decimal("Infinity"),
        1,
    ),
)
def test_active_position_requires_positive_finite_decimal_quantity(
    quantity: object,
) -> None:
    with pytest.raises(
        StrategyPositionContractError,
        match="positive finite Decimal",
    ):
        StrategyPositionSnapshot(
            strategy_id="test-strategy",
            position_id="active-1",
            symbol="ETH-USDT-PERP",
            side=StrategyPositionSide.LONG,
            status=StrategyPositionStatus.ACTIVE,
            base_quantity=quantity,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("provided", (None, object(), "bad", (object(),)))
def test_provider_malformed_output_is_a_fatal_position_contract(
    provided: object,
) -> None:
    class ProviderStrategy:
        def position_snapshots(self):
            return provided

    with pytest.raises(StrategyPositionContractError) as exc_info:
        resolve_strategy_position_snapshots(ProviderStrategy())

    assert isinstance(exc_info.value, StrategyCapabilityError)


def test_provider_internal_error_is_wrapped_and_contract_error_is_preserved() -> None:
    cause = RuntimeError("provider broke")

    class BrokenProvider:
        def position_snapshots(self):
            raise cause

    with pytest.raises(StrategyPositionContractError) as wrapped:
        resolve_strategy_position_snapshots(BrokenProvider())
    assert wrapped.value.__cause__ is cause

    contract = StrategyPositionContractError("already typed")

    class ContractProvider:
        def position_snapshots(self):
            raise contract

    with pytest.raises(StrategyPositionContractError) as preserved:
        resolve_strategy_position_snapshots(ContractProvider())
    assert preserved.value is contract
