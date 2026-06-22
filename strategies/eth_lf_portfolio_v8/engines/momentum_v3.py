from __future__ import annotations

from dataclasses import dataclass

from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, EngineSignal


@dataclass(frozen=True)
class MomentumV3Engine:
    name: str = "momentum_v3"
    priority: int = 150

    def evaluate(self, context: BarReadyContext) -> EngineSignal | None:
        # Full V8 Momentum V3 parity is migrated in the next package.
        return None
