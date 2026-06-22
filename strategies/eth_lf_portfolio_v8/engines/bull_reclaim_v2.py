from __future__ import annotations

from dataclasses import dataclass

from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, EngineSignal


@dataclass(frozen=True)
class BullReclaimV2Engine:
    name: str = "bull_reclaim_v2"
    priority: int = 50

    def evaluate(self, context: BarReadyContext) -> EngineSignal | None:
        # Full V8 Bull Reclaim V2 parity is migrated in the next package.
        return None
