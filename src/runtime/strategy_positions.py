from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from src.strategy.positions import (
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
]
