from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar, Protocol

from src.strategy.positions import StrategyPositionSnapshot
from strategies.eth_portfolio_v1.domain.position_state import V8PositionState


LF_SLEEVE_ID = "lf"
MF_RESERVED_SLEEVE_ID = "mf"


class SleeveId(str):
    """Extensible string value with R005 compatibility constants."""

    LF: ClassVar[SleeveId]
    MF: ClassVar[SleeveId]

    def __new__(cls, value: str) -> SleeveId:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("sleeve_id must be non-empty")
        return str.__new__(cls, normalized)

    @property
    def value(self) -> str:
        return str(self)


SleeveId.LF = SleeveId(LF_SLEEVE_ID)
SleeveId.MF = SleeveId(MF_RESERVED_SLEEVE_ID)


class PortfolioSleeve(Protocol):
    """Plugin-private composition contract for portfolio sleeves."""

    sleeve_id: str
    enabled: bool

    def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
        ...


class _LfSnapshotAdapter(Protocol):
    def build_active(
        self,
        position: V8PositionState,
    ) -> StrategyPositionSnapshot | None:
        ...


@dataclass(frozen=True)
class LfSleeveState:
    """Compatibility boundary around the existing LF position state."""

    position: V8PositionState
    snapshot_adapter: _LfSnapshotAdapter
    sleeve_id: str = field(default=LF_SLEEVE_ID, init=False)
    enabled: bool = field(default=True, init=False)

    def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
        snapshot = self.snapshot_adapter.build_active(self.position)
        return () if snapshot is None else (snapshot,)


@dataclass(frozen=True)
class DisabledSleeve:
    """Null object reserved for a future sleeve implementation."""

    sleeve_id: str
    enabled: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "sleeve_id", SleeveId(self.sleeve_id).value)

    def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
        return ()


__all__ = [
    "DisabledSleeve",
    "LF_SLEEVE_ID",
    "LfSleeveState",
    "MF_RESERVED_SLEEVE_ID",
    "PortfolioSleeve",
    "SleeveId",
]
