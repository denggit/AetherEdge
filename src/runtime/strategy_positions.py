from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from src.strategy.positions import (
    format_strategy_position_snapshot_contexts,
    StrategyPositionProvider,
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)
from src.strategy.contracts import (
    StrategyContractError,
    StrategyPositionContractError,
)


@dataclass(frozen=True)
class StrategyPositionSnapshotIndex:
    """Stable, non-deduplicating index over strategy logical positions."""

    snapshots: tuple[StrategyPositionSnapshot, ...]

    @property
    def active(self) -> tuple[StrategyPositionSnapshot, ...]:
        return tuple(
            snapshot
            for snapshot in self.snapshots
            if snapshot.status is StrategyPositionStatus.ACTIVE
        )

    def by_position_id(self, position_id: str) -> tuple[StrategyPositionSnapshot, ...]:
        return tuple(
            snapshot
            for snapshot in self.snapshots
            if snapshot.position_id == position_id
        )

    def by_symbol(self, symbol: str) -> tuple[StrategyPositionSnapshot, ...]:
        return tuple(
            snapshot
            for snapshot in self.snapshots
            if snapshot.symbol == symbol
        )

    def by_symbol_side(
        self,
        symbol: str,
        side: StrategyPositionSide,
    ) -> tuple[StrategyPositionSnapshot, ...]:
        return tuple(
            snapshot
            for snapshot in self.snapshots
            if snapshot.symbol == symbol and snapshot.side is side
        )

    def single_active_or_none_for_legacy(self) -> StrategyPositionSnapshot | None:
        """Return the sole active snapshot, never an arbitrary first snapshot."""

        active = self.active
        return active[0] if len(active) == 1 else None


def resolve_strategy_position_snapshots(
    strategy: object,
) -> tuple[StrategyPositionSnapshot, ...]:
    """Resolve snapshots only through the public strategy capability."""

    if not isinstance(strategy, StrategyPositionProvider):
        return ()
    try:
        provided = strategy.position_snapshots()
    except StrategyContractError:
        raise
    except Exception as exc:
        raise StrategyPositionContractError(
            "strategy position provider failed | "
            f"provider={type(strategy).__module__}.{type(strategy).__qualname__} | "
            f"error={type(exc).__name__}: {exc}"
        ) from exc
    if (
        not isinstance(provided, Sequence)
        or isinstance(provided, (str, bytes, bytearray))
    ):
        raise StrategyPositionContractError(
            "position_snapshots() must return a sequence of snapshots | "
            f"provider={type(strategy).__module__}.{type(strategy).__qualname__}"
        )
    snapshots = tuple(provided)
    if any(
        not isinstance(snapshot, StrategyPositionSnapshot)
        for snapshot in snapshots
    ):
        raise StrategyPositionContractError(
            "position_snapshots() must return StrategyPositionSnapshot values | "
            f"provider={type(strategy).__module__}.{type(strategy).__qualname__}"
        )
    return snapshots


def validate_strategy_position_snapshot_set(
    snapshots: Sequence[StrategyPositionSnapshot],
    *,
    expected_strategy_id: str,
    expected_symbol: str,
) -> tuple[StrategyPositionSnapshot, ...]:
    """Validate runtime identity, symbol, and active position uniqueness."""

    resolved = tuple(snapshots)
    if any(
        not isinstance(snapshot, StrategyPositionSnapshot)
        for snapshot in resolved
    ):
        raise StrategyPositionContractError(
            "strategy position snapshot set must contain "
            "StrategyPositionSnapshot values"
        )

    active_by_position_id: dict[str, StrategyPositionSnapshot] = {}
    for snapshot in resolved:
        if snapshot.status is not StrategyPositionStatus.ACTIVE:
            continue
        previous = active_by_position_id.get(snapshot.position_id)
        if previous is not None:
            raise StrategyPositionContractError(
                "duplicate active position_id | "
                f"position_id={snapshot.position_id} | "
                f"{format_strategy_position_snapshot_contexts((previous, snapshot))}"
            )
        active_by_position_id[snapshot.position_id] = snapshot

    for snapshot in resolved:
        if snapshot.strategy_id != expected_strategy_id:
            raise StrategyPositionContractError(
                "strategy position snapshot identity mismatch | "
                f"expected_strategy_id={expected_strategy_id} | "
                f"actual_strategy_id={snapshot.strategy_id} | "
                f"position_id={snapshot.position_id}"
            )
        if snapshot.symbol != expected_symbol:
            raise StrategyPositionContractError(
                "strategy position snapshot symbol mismatch | "
                f"expected_symbol={expected_symbol} | "
                f"actual_symbol={snapshot.symbol} | "
                f"position_id={snapshot.position_id}"
            )
    return resolved


def resolve_strategy_position_snapshot_index(
    strategy: object,
) -> StrategyPositionSnapshotIndex:
    return StrategyPositionSnapshotIndex(
        resolve_strategy_position_snapshots(strategy)
    )


__all__ = [
    "StrategyPositionSnapshotIndex",
    "resolve_strategy_position_snapshot_index",
    "resolve_strategy_position_snapshots",
    "validate_strategy_position_snapshot_set",
]
