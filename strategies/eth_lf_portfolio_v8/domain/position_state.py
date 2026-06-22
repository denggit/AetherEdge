from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping

from strategies.eth_lf_portfolio_v8.domain.models import Side


@dataclass
class ExchangeLegState:
    exchange: str
    is_open: bool = False
    avg_fill_price: Decimal | None = None
    base_qty: Decimal = Decimal("0")
    native_qty: Decimal | None = None
    entry_order_id: str | None = None
    entry_client_order_id: str | None = None
    stop_order_id: str | None = None
    stop_client_order_id: str | None = None
    stop_price: Decimal | None = None
    sync_status: str = "flat"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class V8PositionState:
    in_pos: bool = False
    side: Side = Side.FLAT
    entry_time_ms: int | None = None
    first_entry: Decimal | None = None
    avg_entry: Decimal | None = None
    initial_sl: Decimal | None = None
    stop_price: Decimal | None = None
    risk_per_coin: Decimal | None = None
    qty: Decimal = Decimal("0")
    units: int = 0
    max_fav: Decimal = Decimal("0")
    max_adv: Decimal = Decimal("0")
    entry_risk_mult: Decimal = Decimal("1")
    entry_engine: str = ""
    last_exit_time_ms: int | None = None
    legs: dict[str, ExchangeLegState] = field(default_factory=dict)

    def reset(self) -> None:
        self.in_pos = False
        self.side = Side.FLAT
        self.entry_time_ms = None
        self.first_entry = None
        self.avg_entry = None
        self.initial_sl = None
        self.stop_price = None
        self.risk_per_coin = None
        self.qty = Decimal("0")
        self.units = 0
        self.max_fav = Decimal("0")
        self.max_adv = Decimal("0")
        self.entry_risk_mult = Decimal("1")
        self.entry_engine = ""
        self.legs.clear()
