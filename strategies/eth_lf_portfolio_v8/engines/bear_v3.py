from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, EngineSignal, Side


@dataclass(frozen=True)
class BearV3OnlyEngine:
    name: str = "BEAR_V3_ONLY"
    priority: int = 90

    def evaluate(self, context: BarReadyContext) -> EngineSignal | None:
        row = context.engine_features.get("bear") if context.engine_features else None
        if not row:
            return None
        signal = int(row.get("signal") or 0)
        if signal != -1:
            return None
        return EngineSignal(
            side=Side.SHORT,
            engine=self.name,
            priority=self.priority,
            risk_mult=_dec(row.get("risk_mult"), Decimal("1")),
            quality_mult=_dec(row.get("quality_mult"), Decimal("1")),
            reason="bear_v3_only_signal",
            metadata=_metadata(row),
        )


def _dec(value: Any, default: Decimal) -> Decimal:
    if value is None:
        return default
    return Decimal(str(value))


def _metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "short_signal": bool(row.get("short_signal", False)),
        "short_exit_channel": bool(row.get("short_exit_channel", False)),
        "bear_permission_v3": bool(row.get("bear_permission_v3", False)),
    }
