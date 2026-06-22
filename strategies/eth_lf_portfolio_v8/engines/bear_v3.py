from __future__ import annotations

from dataclasses import dataclass

from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, EngineSignal


@dataclass(frozen=True)
class BearV3OnlyEngine:
    name: str = "bear_v3_only"
    priority: int = 90

    def evaluate(self, context: BarReadyContext) -> EngineSignal | None:
        # Full V8 Bear V3 Only signal rules are migrated in the next package.
        return None
