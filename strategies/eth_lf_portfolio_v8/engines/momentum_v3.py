from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, EngineSignal, Side


@dataclass(frozen=True)
class MomentumV3Engine:
    name: str = "MOMENTUM_V3"
    priority: int = 150

    def evaluate(self, context: BarReadyContext) -> EngineSignal | None:
        row = context.engine_features.get("momentum") if context.engine_features else None
        if not row:
            return None
        signal = int(row.get("signal") or 0)
        if signal == 0:
            return None
        return EngineSignal(
            side=Side.LONG if signal == 1 else Side.SHORT,
            engine=self.name,
            priority=self.priority,
            risk_mult=_dec(row.get("risk_mult"), Decimal("1")),
            quality_mult=_dec(row.get("quality_mult"), Decimal("1")),
            reason="momentum_v3_signal",
            metadata=_metadata(row),
        )


def _dec(value: Any, default: Decimal) -> Decimal:
    if value is None:
        return default
    return Decimal(str(value))


def _metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "long_signal": bool(row.get("long_signal", False)),
        "short_signal": bool(row.get("short_signal", False)),
        "long_exit_channel": bool(row.get("long_exit_channel", False)),
        "short_exit_channel": bool(row.get("short_exit_channel", False)),
    }
