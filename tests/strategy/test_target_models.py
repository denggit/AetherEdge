from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from decimal import Decimal
from types import MappingProxyType

import pytest

from src.signals import SignalAction, TradeSignal
from src.strategy.positions import StrategyPositionSnapshot
from src.strategy.targets import (
    MAX_METADATA_DEPTH,
    MAX_METADATA_NODES,
    MAX_METADATA_STRING_LENGTH,
    StrategyDecision,
    StrategyTargetPosition,
    TargetPositionSide,
    VirtualSleeveTarget,
    freeze_metadata,
)


def _position(
    side: TargetPositionSide = TargetPositionSide.LONG,
    quantity: Decimal = Decimal("1.25"),
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


def _decision(**overrides: object) -> StrategyDecision:
    values: dict[str, object] = {
        "strategy_id": "strategy-1",
        "decision_id": "decision-1",
        "event_time_ms": 100,
        "available_time_ms": 110,
        "decision_time_ms": 120,
        "targets": (_target(valid_until_ms=120),),
    }
    values.update(overrides)
    return StrategyDecision(**values)  # type: ignore[arg-type]


def test_target_position_side_members_are_exact() -> None:
    assert tuple((member.name, member.value) for member in TargetPositionSide) == (
        ("FLAT", "flat"),
        ("LONG", "long"),
        ("SHORT", "short"),
    )


def test_target_position_fields_and_frozen_value_equality() -> None:
    position = _position()
    assert tuple(field.name for field in fields(StrategyTargetPosition)) == (
        "side",
        "quantity_base",
    )
    assert position == _position()
    with pytest.raises(FrozenInstanceError):
        position.quantity_base = Decimal("2")  # type: ignore[misc]


def test_flat_zero_and_directional_positive_positions_are_valid() -> None:
    assert _position(TargetPositionSide.FLAT, Decimal("0")).quantity_base == Decimal("0")
    assert _position(TargetPositionSide.LONG, Decimal("0.01")).side is TargetPositionSide.LONG
    assert _position(TargetPositionSide.SHORT, Decimal("0.01")).side is TargetPositionSide.SHORT


@pytest.mark.parametrize(
    ("side", "quantity"),
    (
        (TargetPositionSide.FLAT, Decimal("1")),
        (TargetPositionSide.LONG, Decimal("0")),
        (TargetPositionSide.SHORT, Decimal("0")),
        (TargetPositionSide.LONG, Decimal("-1")),
        (TargetPositionSide.SHORT, Decimal("-1")),
    ),
)
def test_target_position_rejects_invalid_side_quantity_combinations(
    side: TargetPositionSide, quantity: Decimal
) -> None:
    with pytest.raises(ValueError):
        _position(side, quantity)


@pytest.mark.parametrize("quantity", (1, 1.0, True, None))
def test_target_position_requires_decimal(quantity: object) -> None:
    with pytest.raises(TypeError, match="Decimal"):
        StrategyTargetPosition(TargetPositionSide.LONG, quantity)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "quantity", (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity"))
)
def test_target_position_rejects_non_finite_decimal(quantity: Decimal) -> None:
    with pytest.raises(ValueError, match="finite"):
        _position(TargetPositionSide.LONG, quantity)


def test_virtual_target_fields_frozen_and_equal() -> None:
    target = _target()
    assert tuple(field.name for field in fields(VirtualSleeveTarget)) == (
        "strategy_id",
        "sleeve_id",
        "symbol",
        "generation",
        "revision",
        "position",
        "valid_until_ms",
        "reason",
        "metadata",
    )
    assert target == _target()
    with pytest.raises(FrozenInstanceError):
        target.revision = 1  # type: ignore[misc]


@pytest.mark.parametrize("field_name", ("strategy_id", "sleeve_id", "symbol"))
def test_virtual_target_requires_identity_fields(field_name: str) -> None:
    values = {
        "strategy_id": "strategy-1",
        "sleeve_id": "sleeve-1",
        "symbol": "ETH-USDT-PERP",
        "generation": 0,
        "revision": 0,
        "position": _position(),
    }
    values.pop(field_name)
    with pytest.raises(TypeError):
        VirtualSleeveTarget(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("field_name", ("strategy_id", "sleeve_id", "symbol"))
@pytest.mark.parametrize("invalid", (None, 1, "", "   ", " leading", "trailing "))
def test_virtual_target_rejects_invalid_identity_values(
    field_name: str, invalid: object
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _target(**{field_name: invalid})


@pytest.mark.parametrize("field_name", ("generation", "revision"))
@pytest.mark.parametrize("invalid", (-1, True, 1.0, "1", None))
def test_virtual_target_rejects_invalid_version_fields(
    field_name: str, invalid: object
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _target(**{field_name: invalid})


@pytest.mark.parametrize(
    "invalid",
    (
        None,
        {},
        (),
        pytest.param(object.__new__(StrategyPositionSnapshot), id="strategy-snapshot"),
        pytest.param(object.__new__(TradeSignal), id="trade-signal"),
    ),
)
def test_virtual_target_requires_target_position(invalid: object) -> None:
    with pytest.raises(TypeError, match="StrategyTargetPosition"):
        _target(position=invalid)


@pytest.mark.parametrize("invalid", (-1, True, 1.0, "1"))
def test_virtual_target_rejects_invalid_valid_until(invalid: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        _target(valid_until_ms=invalid)
    assert _target(valid_until_ms=None).valid_until_ms is None


@pytest.mark.parametrize("invalid", (None, 1, "x" * (MAX_METADATA_STRING_LENGTH + 1)))
def test_virtual_target_rejects_invalid_reason(invalid: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        _target(reason=invalid)


def test_virtual_target_metadata_missing_empty_and_none_are_distinct() -> None:
    assert dict(_target().metadata) == {}
    assert dict(_target(metadata={}).metadata) == {}
    with pytest.raises(TypeError, match="metadata"):
        _target(metadata=None)


def test_decision_fields_frozen_and_times_have_no_defaults() -> None:
    decision = _decision()
    assert tuple(field.name for field in fields(StrategyDecision)) == (
        "strategy_id",
        "decision_id",
        "event_time_ms",
        "available_time_ms",
        "decision_time_ms",
        "targets",
        "reason",
        "metadata",
    )
    for name in ("event_time_ms", "available_time_ms", "decision_time_ms"):
        assert StrategyDecision.__dataclass_fields__[name].default.__class__.__name__ == "_MISSING_TYPE"
    with pytest.raises(FrozenInstanceError):
        decision.decision_time_ms = 999  # type: ignore[misc]


def test_decision_accepts_equal_and_normally_increasing_times() -> None:
    assert _decision(event_time_ms=100, available_time_ms=100, decision_time_ms=100)
    assert _decision(event_time_ms=100, available_time_ms=110, decision_time_ms=120)


@pytest.mark.parametrize("field_name", ("strategy_id", "decision_id"))
@pytest.mark.parametrize("invalid", (None, 1, "", "   ", " leading", "trailing "))
def test_decision_rejects_invalid_identity_values(
    field_name: str, invalid: object
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _decision(**{field_name: invalid})


@pytest.mark.parametrize(
    "overrides",
    (
        {"event_time_ms": 111, "available_time_ms": 110},
        {"available_time_ms": 121, "decision_time_ms": 120},
    ),
)
def test_decision_rejects_non_causal_time_order(overrides: dict[str, int]) -> None:
    with pytest.raises(ValueError, match="event_time_ms"):
        _decision(**overrides)


@pytest.mark.parametrize("field_name", ("event_time_ms", "available_time_ms", "decision_time_ms"))
def test_decision_requires_each_time_field(field_name: str) -> None:
    values = {
        "strategy_id": "strategy-1",
        "decision_id": "decision-1",
        "event_time_ms": 100,
        "available_time_ms": 110,
        "decision_time_ms": 120,
        "targets": (_target(valid_until_ms=120),),
    }
    values.pop(field_name)
    with pytest.raises(TypeError):
        StrategyDecision(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("field_name", ("event_time_ms", "available_time_ms", "decision_time_ms"))
@pytest.mark.parametrize("invalid", (None, -1, True, 1.0))
def test_decision_rejects_invalid_time_values(field_name: str, invalid: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        _decision(**{field_name: invalid})


def test_decision_copies_target_list_and_stores_tuple() -> None:
    target = _target(valid_until_ms=120)
    source = [target]
    decision = _decision(targets=source)
    source.clear()
    assert decision.targets == (target,)
    assert isinstance(decision.targets, tuple)
    with pytest.raises(FrozenInstanceError):
        decision.targets += (target,)  # type: ignore[misc]


def test_decision_accepts_target_tuple_and_has_value_equality() -> None:
    targets = (_target(valid_until_ms=120),)
    assert _decision(targets=targets) == _decision(targets=targets)


@pytest.mark.parametrize("invalid", (None, (), [], "target", b"target", {"target": 1}))
def test_decision_rejects_invalid_or_empty_target_collections(invalid: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        _decision(targets=invalid)


def test_decision_rejects_non_target_member_and_strategy_mismatch() -> None:
    with pytest.raises(TypeError, match="VirtualSleeveTarget"):
        _decision(targets=(object(),))
    with pytest.raises(ValueError, match="strategy_id"):
        _decision(targets=(_target(strategy_id="strategy-2"),))


def test_decision_rejects_target_already_stale_at_decision_time() -> None:
    with pytest.raises(ValueError, match="valid_until_ms"):
        _decision(targets=(_target(valid_until_ms=119),))


def test_decision_reason_and_metadata_use_shared_boundaries() -> None:
    assert _decision().reason == ""
    assert dict(_decision().metadata) == {}
    with pytest.raises(TypeError, match="reason"):
        _decision(reason=None)
    with pytest.raises(ValueError, match="reason"):
        _decision(reason="x" * (MAX_METADATA_STRING_LENGTH + 1))
    with pytest.raises(TypeError, match="metadata"):
        _decision(metadata=None)


def test_metadata_deep_freeze_and_input_mutation_isolation() -> None:
    source = {"nested": {"items": [1, {"tuple": (True, None)}]}}
    target = _target(metadata=source)
    source["nested"]["items"].append(2)  # type: ignore[index,union-attr]

    assert isinstance(target.metadata, MappingProxyType)
    nested = target.metadata["nested"]
    assert isinstance(nested, MappingProxyType)
    assert nested["items"] == (1, MappingProxyType({"tuple": (True, None)}))
    with pytest.raises(TypeError):
        target.metadata["new"] = 1  # type: ignore[index]
    with pytest.raises(TypeError):
        nested["new"] = 1  # type: ignore[index]


@pytest.mark.parametrize(
    "metadata",
    (
        {1: "value"},
        {SignalAction.OPEN_LONG: "value"},
        {"value": Decimal("1")},
        {"value": {1, 2}},
        {"value": b"bytes"},
        {"value": SignalAction.OPEN_LONG},
        {"value": _position()},
        {"value": object()},
        {"value": float("nan")},
        {"value": float("inf")},
        {"value": float("-inf")},
    ),
)
def test_metadata_rejects_non_json_values(metadata: object) -> None:
    with pytest.raises((TypeError, ValueError), match="metadata"):
        freeze_metadata(metadata)  # type: ignore[arg-type]


def test_metadata_rejects_circular_dict_and_list() -> None:
    circular_dict: dict[str, object] = {}
    circular_dict["self"] = circular_dict
    circular_list: list[object] = []
    circular_list.append(circular_list)

    with pytest.raises(ValueError, match="circular"):
        freeze_metadata(circular_dict)
    with pytest.raises(ValueError, match="circular"):
        freeze_metadata({"items": circular_list})


def test_metadata_depth_boundary() -> None:
    at_limit: dict[str, object] = {}
    for _ in range(MAX_METADATA_DEPTH):
        at_limit = {"child": at_limit}
    freeze_metadata(at_limit)

    over_limit = {"child": at_limit}
    with pytest.raises(ValueError, match="depth"):
        freeze_metadata(over_limit)


def test_metadata_node_count_boundary_includes_root_key_and_values() -> None:
    allowed_items = [None] * (MAX_METADATA_NODES - 3)
    frozen = freeze_metadata({"items": allowed_items})
    assert len(frozen["items"]) == MAX_METADATA_NODES - 3  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="node count"):
        freeze_metadata({"items": allowed_items + [None]})


def test_metadata_string_length_boundary_applies_to_key_and_value() -> None:
    at_limit = "x" * MAX_METADATA_STRING_LENGTH
    freeze_metadata({at_limit: at_limit})
    with pytest.raises(ValueError, match="metadata key"):
        freeze_metadata({at_limit + "x": "value"})
    with pytest.raises(ValueError, match="metadata string value"):
        freeze_metadata({"value": at_limit + "x"})


def test_metadata_root_requires_mapping() -> None:
    with pytest.raises(TypeError, match="metadata root"):
        freeze_metadata(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="metadata root"):
        freeze_metadata([])  # type: ignore[arg-type]
    assert dict(freeze_metadata({})) == {}
