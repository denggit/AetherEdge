from __future__ import annotations

import math
from enum import Enum
from types import MappingProxyType
from typing import Mapping, TypeAlias


MAX_METADATA_DEPTH = 8
MAX_METADATA_NODES = 256
MAX_METADATA_STRING_LENGTH = 4096

JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = (
    JsonScalar | Mapping[str, "JsonValue"] | list["JsonValue"] | tuple["JsonValue", ...]
)
FrozenJsonValue: TypeAlias = JsonScalar | Mapping[str, "FrozenJsonValue"] | tuple["FrozenJsonValue", ...]


class _FreezeState:
    def __init__(self) -> None:
        self.nodes = 0
        self.active_container_ids: set[int] = set()

    def count_node(self) -> None:
        self.nodes += 1
        if self.nodes > MAX_METADATA_NODES:
            raise ValueError(
                f"metadata exceeds maximum total node count {MAX_METADATA_NODES}"
            )


def freeze_metadata(metadata: Mapping[str, JsonValue]) -> Mapping[str, FrozenJsonValue]:
    """Return an independent, recursively immutable metadata mapping."""

    if not isinstance(metadata, Mapping):
        raise TypeError("metadata root must be a Mapping and cannot be None")
    state = _FreezeState()
    frozen = _freeze_mapping(metadata, depth=0, state=state)
    return frozen


def _freeze_mapping(
    value: Mapping[object, object],
    *,
    depth: int,
    state: _FreezeState,
) -> Mapping[str, FrozenJsonValue]:
    _check_depth(depth)
    state.count_node()
    container_id = id(value)
    if container_id in state.active_container_ids:
        raise ValueError("metadata contains a circular reference")
    state.active_container_ids.add(container_id)
    try:
        frozen: dict[str, FrozenJsonValue] = {}
        for key, item in value.items():
            if isinstance(key, Enum) or not isinstance(key, str):
                raise TypeError("metadata mapping keys must be strings")
            _check_string(key, description="metadata key")
            state.count_node()
            frozen[key] = _freeze_value(item, depth=depth + 1, state=state)
        return MappingProxyType(frozen)
    finally:
        state.active_container_ids.remove(container_id)


def _freeze_sequence(
    value: list[object] | tuple[object, ...],
    *,
    depth: int,
    state: _FreezeState,
) -> tuple[FrozenJsonValue, ...]:
    _check_depth(depth)
    state.count_node()
    container_id = id(value)
    if container_id in state.active_container_ids:
        raise ValueError("metadata contains a circular reference")
    state.active_container_ids.add(container_id)
    try:
        return tuple(
            _freeze_value(item, depth=depth + 1, state=state) for item in value
        )
    finally:
        state.active_container_ids.remove(container_id)


def _freeze_value(
    value: object,
    *,
    depth: int,
    state: _FreezeState,
) -> FrozenJsonValue:
    _check_depth(depth)
    if isinstance(value, Mapping):
        return _freeze_mapping(value, depth=depth, state=state)
    if isinstance(value, (list, tuple)):
        return _freeze_sequence(value, depth=depth, state=state)

    state.count_node()
    if isinstance(value, Enum):
        raise TypeError(
            f"metadata contains unsupported value type: {type(value).__name__}"
        )
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("metadata float values must be finite")
        return value
    if isinstance(value, str):
        _check_string(value, description="metadata string value")
        return value
    raise TypeError(f"metadata contains unsupported value type: {type(value).__name__}")


def _check_depth(depth: int) -> None:
    if depth > MAX_METADATA_DEPTH:
        raise ValueError(f"metadata exceeds maximum nesting depth {MAX_METADATA_DEPTH}")


def _check_string(value: str, *, description: str) -> None:
    if len(value) > MAX_METADATA_STRING_LENGTH:
        raise ValueError(
            f"{description} exceeds maximum length {MAX_METADATA_STRING_LENGTH}"
        )


__all__ = [
    "FrozenJsonValue",
    "JsonScalar",
    "JsonValue",
    "MAX_METADATA_DEPTH",
    "MAX_METADATA_NODES",
    "MAX_METADATA_STRING_LENGTH",
    "freeze_metadata",
]
