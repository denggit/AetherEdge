from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, replace
from decimal import Decimal

import pytest

from src.signals import SignalAction
from src.strategy.targets import (
    StrategyDecision,
    StrategyTargetPosition,
    TargetIdentity,
    TargetPositionSide,
    TargetUpdateDisposition,
    TargetVersion,
    VirtualSleeveTarget,
    classify_target_update,
    is_target_stale,
)


def _position(
    side: TargetPositionSide = TargetPositionSide.LONG,
    quantity: Decimal = Decimal("1"),
) -> StrategyTargetPosition:
    return StrategyTargetPosition(side=side, quantity_base=quantity)


def _target(**overrides: object) -> VirtualSleeveTarget:
    values: dict[str, object] = {
        "strategy_id": "strategy-1",
        "sleeve_id": "sleeve-1",
        "symbol": "ETH-USDT-PERP",
        "generation": 0,
        "revision": 0,
        "position": _position(),
    }
    values.update(overrides)
    return VirtualSleeveTarget(**values)  # type: ignore[arg-type]


def _decision(*targets: VirtualSleeveTarget) -> StrategyDecision:
    return StrategyDecision(
        strategy_id="strategy-1",
        decision_id="decision-1",
        event_time_ms=100,
        available_time_ms=100,
        decision_time_ms=100,
        targets=targets,
    )


def test_target_identity_fields_frozen_slots_hash_and_value_equality() -> None:
    identity = TargetIdentity("strategy-1", "sleeve-1", "ETH-USDT-PERP")
    equal = TargetIdentity("strategy-1", "sleeve-1", "ETH-USDT-PERP")

    assert tuple(field.name for field in fields(TargetIdentity)) == (
        "strategy_id",
        "sleeve_id",
        "symbol",
    )
    assert identity == equal
    assert hash(identity) == hash(equal)
    assert {identity, equal} == {identity}
    assert {identity: "value"}[equal] == "value"
    assert not hasattr(identity, "__dict__")
    with pytest.raises(FrozenInstanceError):
        identity.symbol = "BTC-USDT-PERP"  # type: ignore[misc]


@pytest.mark.parametrize("field_name", ("strategy_id", "sleeve_id", "symbol"))
@pytest.mark.parametrize(
    "invalid",
    (
        None,
        1,
        "",
        "   ",
        " leading",
        "trailing ",
        SignalAction.OPEN_LONG,
        TargetPositionSide.LONG,
    ),
)
def test_target_identity_rejects_invalid_plain_string_fields(
    field_name: str, invalid: object
) -> None:
    values: dict[str, object] = {
        "strategy_id": "strategy-1",
        "sleeve_id": "sleeve-1",
        "symbol": "ETH-USDT-PERP",
    }
    values[field_name] = invalid
    with pytest.raises((TypeError, ValueError)):
        TargetIdentity(**values)  # type: ignore[arg-type]


def test_target_identity_is_case_sensitive_and_contains_only_identity_fields() -> None:
    upper = TargetIdentity("strategy-1", "sleeve-1", "ETH-USDT-PERP")
    lower = TargetIdentity("strategy-1", "sleeve-1", "eth-usdt-perp")

    assert upper != lower
    assert tuple(field.name for field in fields(TargetIdentity)) == (
        "strategy_id",
        "sleeve_id",
        "symbol",
    )


def test_target_version_fields_frozen_slots_hash_and_value_equality() -> None:
    version = TargetVersion(1, 2)
    equal = TargetVersion(1, 2)

    assert tuple(field.name for field in fields(TargetVersion)) == (
        "generation",
        "revision",
    )
    assert version == equal
    assert hash(version) == hash(equal)
    assert not hasattr(version, "__dict__")
    with pytest.raises(FrozenInstanceError):
        version.revision = 3  # type: ignore[misc]


@pytest.mark.parametrize("field_name", ("generation", "revision"))
@pytest.mark.parametrize("invalid", (-1, True, 1.0, "1", None))
def test_target_version_rejects_invalid_fields(
    field_name: str, invalid: object
) -> None:
    values: dict[str, object] = {"generation": 0, "revision": 0}
    values[field_name] = invalid
    with pytest.raises((TypeError, ValueError)):
        TargetVersion(**values)  # type: ignore[arg-type]


def test_target_version_uses_generation_revision_lexicographic_order() -> None:
    assert TargetVersion(0, 1) > TargetVersion(0, 0)
    assert TargetVersion(1, 0) > TargetVersion(0, 999)
    assert TargetVersion(2, 0) > TargetVersion(1, 100_000)
    assert TargetVersion(0, 999_999) < TargetVersion(1, 0)


def test_virtual_target_identity_and_version_properties_are_not_fields() -> None:
    target = _target(generation=3, revision=7)

    assert target.identity == TargetIdentity("strategy-1", "sleeve-1", "ETH-USDT-PERP")
    assert target.version == TargetVersion(3, 7)
    field_names = tuple(field.name for field in fields(VirtualSleeveTarget))
    assert "identity" not in field_names
    assert "version" not in field_names
    assert not hasattr(target.identity, "__dict__")
    assert not hasattr(target.version, "__dict__")


def test_identity_ignores_position_and_version_while_version_tracks_version_fields() -> None:
    baseline = _target()
    changed_position = _target(
        position=_position(TargetPositionSide.SHORT, Decimal("2"))
    )
    changed_version = _target(generation=2, revision=9)

    assert baseline.identity == changed_position.identity == changed_version.identity
    assert baseline.version == changed_position.version
    assert baseline.version != changed_version.version


def test_target_properties_preserve_symbol_case() -> None:
    upper = _target(symbol="ETH-USDT-PERP")
    lower = _target(symbol="eth-usdt-perp")
    assert upper.identity != lower.identity
    assert lower.identity.symbol == "eth-usdt-perp"


@pytest.mark.parametrize(
    "duplicate",
    (
        _target(),
        _target(revision=1),
        _target(generation=1),
        _target(position=_position(TargetPositionSide.SHORT)),
        _target(position=_position(quantity=Decimal("2"))),
    ),
)
def test_decision_rejects_duplicate_target_identity(
    duplicate: VirtualSleeveTarget,
) -> None:
    with pytest.raises(ValueError, match="duplicate target identity"):
        _decision(_target(), duplicate)


def test_decision_allows_distinct_sleeve_or_symbol_identity() -> None:
    decision = _decision(
        _target(),
        _target(sleeve_id="sleeve-2"),
        _target(symbol="BTC-USDT-PERP"),
    )
    assert len(decision.targets) == 3


def test_update_disposition_members_are_exact() -> None:
    assert tuple((member.name, member.value) for member in TargetUpdateDisposition) == (
        ("APPLY", "apply"),
        ("IDEMPOTENT", "idempotent"),
        ("STALE", "stale"),
        ("CONFLICT", "conflict"),
    )


def test_update_without_current_target_applies() -> None:
    assert classify_target_update(None, _target()) is TargetUpdateDisposition.APPLY


def test_update_rejects_wrong_input_types_and_identity_mismatch() -> None:
    with pytest.raises(TypeError, match="incoming"):
        classify_target_update(None, object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="current"):
        classify_target_update(object(), _target())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="identity"):
        classify_target_update(_target(), _target(sleeve_id="sleeve-2"))


@pytest.mark.parametrize(
    ("current", "incoming", "expected"),
    (
        (_target(revision=1), _target(revision=2), TargetUpdateDisposition.APPLY),
        (_target(revision=2), _target(revision=1), TargetUpdateDisposition.STALE),
        (
            _target(generation=1, revision=999_999),
            _target(generation=2, revision=0),
            TargetUpdateDisposition.APPLY,
        ),
        (
            _target(generation=2, revision=0),
            _target(generation=1, revision=999_999),
            TargetUpdateDisposition.STALE,
        ),
    ),
)
def test_update_classification_uses_lexicographic_version(
    current: VirtualSleeveTarget,
    incoming: VirtualSleeveTarget,
    expected: TargetUpdateDisposition,
) -> None:
    assert classify_target_update(current, incoming) is expected


def test_equal_target_replay_is_idempotent_for_same_or_independent_object() -> None:
    current = _target(metadata={"source": "test"})
    independent = _target(metadata={"source": "test"})

    assert classify_target_update(current, current) is TargetUpdateDisposition.IDEMPOTENT
    assert current is not independent
    assert current == independent
    assert classify_target_update(current, independent) is TargetUpdateDisposition.IDEMPOTENT


@pytest.mark.parametrize(
    "incoming",
    (
        _target(position=_position(TargetPositionSide.SHORT)),
        _target(valid_until_ms=500),
        _target(reason="changed"),
        _target(metadata={"changed": True}),
    ),
)
def test_same_version_with_different_content_conflicts_without_mutation(
    incoming: VirtualSleeveTarget,
) -> None:
    current = _target()
    current_before = _target()
    incoming_before = replace(incoming)

    assert classify_target_update(current, incoming) is TargetUpdateDisposition.CONFLICT
    assert current == current_before
    assert incoming == incoming_before


@pytest.mark.parametrize(
    ("valid_until_ms", "at_time_ms", "expected"),
    (
        (None, 0, False),
        (100, 0, False),
        (100, 99, False),
        (100, 100, False),
        (100, 101, True),
    ),
)
def test_target_stale_has_strict_after_expiry_boundary(
    valid_until_ms: int | None,
    at_time_ms: int,
    expected: bool,
) -> None:
    target = _target(valid_until_ms=valid_until_ms)
    before = _target(valid_until_ms=valid_until_ms)

    assert is_target_stale(target, at_time_ms=at_time_ms) is expected
    assert target == before


@pytest.mark.parametrize("invalid", (-1, True, 1.0, "1", None))
def test_target_stale_rejects_invalid_time(invalid: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        is_target_stale(_target(), at_time_ms=invalid)  # type: ignore[arg-type]


def test_target_stale_rejects_invalid_target_type() -> None:
    with pytest.raises(TypeError, match="target"):
        is_target_stale(object(), at_time_ms=0)  # type: ignore[arg-type]
