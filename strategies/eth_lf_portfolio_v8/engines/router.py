from __future__ import annotations

from dataclasses import dataclass

from strategies.eth_lf_portfolio_v8.domain.models import Side


@dataclass(frozen=True)
class RoutedSignal:
    side: Side
    engine: str
    priority: int


class PortfolioRouter:
    """V8 engine conflict priority placeholder.

    Final routing order will be:
    Momentum V3 first, Bear V3 second, Bull Reclaim V2 third.
    """

    def select(self, signals: list[RoutedSignal]) -> RoutedSignal:
        if not signals:
            return RoutedSignal(side=Side.FLAT, engine="none", priority=0)
        return sorted(signals, key=lambda item: item.priority, reverse=True)[0]
