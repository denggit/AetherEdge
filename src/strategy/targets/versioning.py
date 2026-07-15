from __future__ import annotations

from enum import Enum

from src.strategy.targets.models import VirtualSleeveTarget


class TargetUpdateDisposition(str, Enum):
    APPLY = "apply"
    IDEMPOTENT = "idempotent"
    STALE = "stale"
    CONFLICT = "conflict"


def classify_target_update(
    current: VirtualSleeveTarget | None,
    incoming: VirtualSleeveTarget,
) -> TargetUpdateDisposition:
    if not isinstance(incoming, VirtualSleeveTarget):
        raise TypeError("incoming must be VirtualSleeveTarget")
    if current is not None and not isinstance(current, VirtualSleeveTarget):
        raise TypeError("current must be VirtualSleeveTarget or None")
    if current is None:
        return TargetUpdateDisposition.APPLY
    if current.identity != incoming.identity:
        raise ValueError("current and incoming target identity must match")
    if incoming.version > current.version:
        return TargetUpdateDisposition.APPLY
    if incoming.version < current.version:
        return TargetUpdateDisposition.STALE
    if incoming == current:
        return TargetUpdateDisposition.IDEMPOTENT
    return TargetUpdateDisposition.CONFLICT


def is_target_stale(
    target: VirtualSleeveTarget,
    *,
    at_time_ms: int,
) -> bool:
    if not isinstance(target, VirtualSleeveTarget):
        raise TypeError("target must be VirtualSleeveTarget")
    if type(at_time_ms) is not int:
        raise TypeError("at_time_ms must be an int and cannot be bool")
    if at_time_ms < 0:
        raise ValueError("at_time_ms must be non-negative")
    return target.valid_until_ms is not None and at_time_ms > target.valid_until_ms


__all__ = [
    "TargetUpdateDisposition",
    "classify_target_update",
    "is_target_stale",
]
