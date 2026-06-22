from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Protocol

from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, EngineSignal, RoutedSignal, Side


class V8Engine(Protocol):
    name: str
    priority: int

    def evaluate(self, context: BarReadyContext) -> EngineSignal | None:
        ...


@dataclass
class PortfolioRouter:
    """Route V8 LF engine votes by original priority order.

    Final routing order is: Momentum V3 first, Bear V3 second, Bull Reclaim V2 third.
    Engines that return FLAT/None do not open a position.
    """

    engines: tuple[V8Engine, ...] = field(default_factory=tuple)

    def evaluate(self, context: BarReadyContext) -> RoutedSignal:
        return self.select([signal for signal in (engine.evaluate(context) for engine in self.engines) if signal is not None])

    def select(self, signals: Iterable[EngineSignal | RoutedSignal]) -> RoutedSignal:
        candidates = [signal for signal in signals if signal.side is not Side.FLAT]
        if not candidates:
            return RoutedSignal.flat()
        selected = sorted(candidates, key=lambda item: item.priority, reverse=True)[0]
        return RoutedSignal(
            side=selected.side,
            engine=selected.engine,
            priority=selected.priority,
            risk_mult=selected.risk_mult,
            quality_mult=selected.quality_mult,
            reason=selected.reason,
            metadata=selected.metadata,
        )
