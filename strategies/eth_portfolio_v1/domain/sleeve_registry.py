from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterator, Mapping

from src.strategy.positions import StrategyPositionSnapshot
from strategies.eth_portfolio_v1.domain.sleeves import PortfolioSleeve


@dataclass(frozen=True)
class SleeveRegistry:
    """Ordered registry of uniquely identified V1 portfolio sleeves."""

    sleeves: tuple[PortfolioSleeve, ...]
    _by_id: Mapping[str, PortfolioSleeve] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        sleeves = tuple(self.sleeves)
        by_id: dict[str, PortfolioSleeve] = {}
        for sleeve in sleeves:
            sleeve_id = sleeve.sleeve_id
            if not isinstance(sleeve_id, str) or not sleeve_id.strip():
                raise ValueError("sleeve_id must be a non-empty string")
            if sleeve_id != sleeve_id.strip():
                raise ValueError("sleeve_id must not contain surrounding whitespace")
            if sleeve_id in by_id:
                raise ValueError(f"duplicate sleeve_id: {sleeve_id}")
            by_id[sleeve_id] = sleeve

        object.__setattr__(self, "sleeves", sleeves)
        object.__setattr__(self, "_by_id", MappingProxyType(by_id))

    def __iter__(self) -> Iterator[PortfolioSleeve]:
        return iter(self.sleeves)

    def get(self, sleeve_id: str) -> PortfolioSleeve | None:
        return self._by_id.get(sleeve_id)

    def require(self, sleeve_id: str) -> PortfolioSleeve:
        sleeve = self.get(sleeve_id)
        if sleeve is None:
            raise KeyError(f"unknown sleeve_id: {sleeve_id}")
        return sleeve

    def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
        snapshots: list[StrategyPositionSnapshot] = []
        for sleeve in self.sleeves:
            if sleeve.enabled:
                snapshots.extend(sleeve.position_snapshots())
        return tuple(snapshots)


__all__ = ["SleeveRegistry"]
