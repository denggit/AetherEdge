from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Mapping

from src.strategy.targets.metadata import (
    FrozenJsonValue,
    JsonValue,
    MAX_METADATA_STRING_LENGTH,
    freeze_metadata,
)


class TargetPositionSide(str, Enum):
    FLAT = "flat"
    LONG = "long"
    SHORT = "short"


@dataclass(frozen=True, slots=True)
class TargetIdentity:
    strategy_id: str
    sleeve_id: str
    symbol: str

    def __post_init__(self) -> None:
        _validate_identity(self.strategy_id, "strategy_id")
        _validate_identity(self.sleeve_id, "sleeve_id")
        _validate_identity(self.symbol, "symbol")


@dataclass(frozen=True, slots=True, order=True)
class TargetVersion:
    generation: int
    revision: int

    def __post_init__(self) -> None:
        _validate_non_negative_int(self.generation, "generation")
        _validate_non_negative_int(self.revision, "revision")


@dataclass(frozen=True, slots=True)
class StrategyTargetPosition:
    """A strategy's desired virtual sleeve position, not an execution action."""

    side: TargetPositionSide
    quantity_base: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.side, TargetPositionSide):
            raise TypeError("side must be TargetPositionSide")
        if not isinstance(self.quantity_base, Decimal):
            raise TypeError("quantity_base must be Decimal")
        if not self.quantity_base.is_finite():
            raise ValueError("quantity_base must be finite")
        if self.side is TargetPositionSide.FLAT:
            if self.quantity_base != Decimal("0"):
                raise ValueError("FLAT target quantity_base must equal Decimal('0')")
        elif self.quantity_base <= Decimal("0"):
            raise ValueError("LONG or SHORT target quantity_base must be positive")


@dataclass(frozen=True, slots=True)
class VirtualSleeveTarget:
    strategy_id: str
    sleeve_id: str
    symbol: str
    generation: int
    revision: int
    position: StrategyTargetPosition
    valid_until_ms: int | None = None
    reason: str = ""
    metadata: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_identity(self.strategy_id, "strategy_id")
        _validate_identity(self.sleeve_id, "sleeve_id")
        _validate_identity(self.symbol, "symbol")
        _validate_non_negative_int(self.generation, "generation")
        _validate_non_negative_int(self.revision, "revision")
        if not isinstance(self.position, StrategyTargetPosition):
            raise TypeError("position must be StrategyTargetPosition")
        if self.valid_until_ms is not None:
            _validate_non_negative_int(self.valid_until_ms, "valid_until_ms")
        _validate_reason(self.reason)
        object.__setattr__(self, "metadata", freeze_metadata(self.metadata))

    @property
    def identity(self) -> TargetIdentity:
        return TargetIdentity(
            strategy_id=self.strategy_id,
            sleeve_id=self.sleeve_id,
            symbol=self.symbol,
        )

    @property
    def version(self) -> TargetVersion:
        return TargetVersion(generation=self.generation, revision=self.revision)


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    strategy_id: str
    decision_id: str
    event_time_ms: int
    available_time_ms: int
    decision_time_ms: int
    targets: tuple[VirtualSleeveTarget, ...]
    reason: str = ""
    metadata: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_identity(self.strategy_id, "strategy_id")
        _validate_identity(self.decision_id, "decision_id")
        _validate_non_negative_int(self.event_time_ms, "event_time_ms")
        _validate_non_negative_int(self.available_time_ms, "available_time_ms")
        _validate_non_negative_int(self.decision_time_ms, "decision_time_ms")
        if not (
            self.event_time_ms <= self.available_time_ms <= self.decision_time_ms
        ):
            raise ValueError(
                "decision times must satisfy event_time_ms <= available_time_ms "
                "<= decision_time_ms"
            )
        if not isinstance(self.targets, (list, tuple)) or isinstance(
            self.targets, (str, bytes)
        ):
            raise TypeError("targets must be a list or tuple of VirtualSleeveTarget")
        targets = tuple(self.targets)
        if not targets:
            raise ValueError("targets must not be empty")
        identities: set[TargetIdentity] = set()
        for target in targets:
            if not isinstance(target, VirtualSleeveTarget):
                raise TypeError("targets must contain only VirtualSleeveTarget objects")
            if target.strategy_id != self.strategy_id:
                raise ValueError("target strategy_id must match decision strategy_id")
            if (
                target.valid_until_ms is not None
                and target.valid_until_ms < self.decision_time_ms
            ):
                raise ValueError("target valid_until_ms must be >= decision_time_ms")
            if target.identity in identities:
                raise ValueError(f"duplicate target identity: {target.identity}")
            identities.add(target.identity)
        _validate_reason(self.reason)
        object.__setattr__(self, "targets", targets)
        object.__setattr__(self, "metadata", freeze_metadata(self.metadata))


def _validate_identity(value: object, field_name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be a string")
    if not value or not value.strip():
        raise ValueError(f"{field_name} must be non-empty")
    if value != value.strip():
        raise ValueError(f"{field_name} must not have leading or trailing whitespace")


def _validate_non_negative_int(value: object, field_name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be an int and cannot be bool")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def _validate_reason(value: object) -> None:
    if type(value) is not str:
        raise TypeError("reason must be a string")
    if len(value) > MAX_METADATA_STRING_LENGTH:
        raise ValueError(
            f"reason exceeds maximum length {MAX_METADATA_STRING_LENGTH}"
        )


__all__ = [
    "StrategyDecision",
    "StrategyTargetPosition",
    "TargetIdentity",
    "TargetPositionSide",
    "TargetVersion",
    "VirtualSleeveTarget",
]
