from __future__ import annotations

from dataclasses import dataclass

from src.platform.exchanges.models import ExchangeName, PositionSide


@dataclass(frozen=True)
class StopScope:
    """Identity boundary for one strategy sleeve's protective stop."""

    strategy_id: str
    sleeve_id: str
    position_id: str
    symbol: str
    position_side: PositionSide | None = None
    target_exchanges: tuple[ExchangeName, ...] | None = None
    stop_client_order_id: str | None = None
    stop_order_id: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("strategy_id", "sleeve_id", "position_id", "symbol"):
            if not str(getattr(self, field_name) or "").strip():
                raise ValueError(f"{field_name} is required")
