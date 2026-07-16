from __future__ import annotations

from decimal import Decimal

import pytest

from src.runtime.strategy_capabilities import (
    validate_dynamic_strategy_capabilities,
)
from src.runtime.strategy_positions import (
    resolve_strategy_position_snapshots,
    validate_strategy_position_snapshot_set,
)
from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)
from src.strategy import (
    StrategyCapabilityError,
    StrategyPositionContractError,
)


def _snapshot(
    position_id: str,
    *,
    strategy_id: str = "test-strategy",
    symbol: str = "ETH-USDT-PERP",
    side: StrategyPositionSide = StrategyPositionSide.LONG,
    status: StrategyPositionStatus = StrategyPositionStatus.ACTIVE,
    sleeve_id: str | None = None,
) -> StrategyPositionSnapshot:
    return StrategyPositionSnapshot(
        strategy_id=strategy_id,
        position_id=position_id,
        symbol=symbol,
        side=side,
        status=status,
        base_quantity=Decimal("1"),
        sleeve_id=sleeve_id,
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


def test_snapshot_set_accepts_matching_identity_symbol_and_unique_active_ids() -> None:
    snapshots = (_snapshot("position-1"), _snapshot("position-2"))

    assert validate_strategy_position_snapshot_set(
        snapshots,
        expected_strategy_id="test-strategy",
        expected_symbol="ETH-USDT-PERP",
    ) is snapshots


def test_snapshot_set_rejects_wrong_strategy_identity_with_context() -> None:
    snapshot = _snapshot("position-1", strategy_id="wrong-strategy")

    with pytest.raises(StrategyPositionContractError) as exc_info:
        validate_strategy_position_snapshot_set(
            (snapshot,),
            expected_strategy_id="test-strategy",
            expected_symbol="ETH-USDT-PERP",
        )

    message = str(exc_info.value)
    assert "expected_strategy_id=test-strategy" in message
    assert "actual_strategy_id=wrong-strategy" in message
    assert "position_id=position-1" in message


def test_snapshot_set_rejects_wrong_symbol_with_context() -> None:
    snapshot = _snapshot("position-1", symbol="BTC-USDT-PERP")

    with pytest.raises(StrategyPositionContractError) as exc_info:
        validate_strategy_position_snapshot_set(
            (snapshot,),
            expected_strategy_id="test-strategy",
            expected_symbol="ETH-USDT-PERP",
        )

    message = str(exc_info.value)
    assert "expected_symbol=ETH-USDT-PERP" in message
    assert "actual_symbol=BTC-USDT-PERP" in message
    assert "position_id=position-1" in message


@pytest.mark.parametrize(
    "second",
    (
        _snapshot("duplicate", sleeve_id="sleeve-2"),
        _snapshot("duplicate", side=StrategyPositionSide.SHORT),
        _snapshot("duplicate", symbol="BTC-USDT-PERP"),
    ),
    ids=("different_sleeve", "different_side", "different_symbol"),
)
def test_snapshot_set_rejects_duplicate_active_position_ids(
    second: StrategyPositionSnapshot,
) -> None:
    first = _snapshot("duplicate", sleeve_id="sleeve-1")

    with pytest.raises(
        StrategyPositionContractError,
        match="duplicate active position_id",
    ) as exc_info:
        validate_strategy_position_snapshot_set(
            (first, second),
            expected_strategy_id="test-strategy",
            expected_symbol="ETH-USDT-PERP",
        )

    message = str(exc_info.value)
    assert "position_id=duplicate" in message
    assert "strategy_ids=" in message
    assert "symbols=" in message
    assert "sleeve_ids=" in message


def test_snapshot_set_allows_duplicate_non_active_position_ids() -> None:
    snapshots = (
        _snapshot(
            "historical",
            status=StrategyPositionStatus.CLOSING,
        ),
        _snapshot(
            "historical",
            side=StrategyPositionSide.FLAT,
            status=StrategyPositionStatus.FLAT,
        ),
    )

    assert validate_strategy_position_snapshot_set(
        snapshots,
        expected_strategy_id="test-strategy",
        expected_symbol="ETH-USDT-PERP",
    ) is snapshots


def test_dynamic_validation_applies_snapshot_set_context() -> None:
    snapshot = _snapshot("position-1", strategy_id="wrong-strategy")

    class ProviderStrategy:
        def position_snapshots(self):
            return (snapshot,)

    with pytest.raises(StrategyPositionContractError, match="identity mismatch"):
        validate_dynamic_strategy_capabilities(
            ProviderStrategy(),
            expected_strategy_id="test-strategy",
            expected_symbol="ETH-USDT-PERP",
        )
