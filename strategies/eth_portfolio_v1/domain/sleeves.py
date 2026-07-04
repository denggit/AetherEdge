from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterator

from strategies.eth_portfolio_v1.domain.position_state import V8PositionState


class SleeveId(str, Enum):
    LF = "lf"
    MF = "mf"


@dataclass(frozen=True)
class LfSleeveState:
    """Compatibility boundary around the existing LF position state."""

    position: V8PositionState
    sleeve_id: SleeveId = field(default=SleeveId.LF, init=False)
    enabled: bool = field(default=True, init=False)


@dataclass(frozen=True)
class MfSleeveState:
    """Reserved MF state with no event or signal behavior in R005."""

    sleeve_id: SleeveId = field(default=SleeveId.MF, init=False)
    enabled: bool = field(default=False, init=False)


@dataclass(frozen=True)
class PortfolioSleeves:
    """Small composite exposing the plugin-owned sleeve boundaries."""

    lf: LfSleeveState
    mf: MfSleeveState

    def __iter__(self) -> Iterator[LfSleeveState | MfSleeveState]:
        return iter((self.lf, self.mf))


__all__ = [
    "LfSleeveState",
    "MfSleeveState",
    "PortfolioSleeves",
    "SleeveId",
]
