from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping

from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.exchanges.models import OrderSide, OrderStatus
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

    def open_master(
        self,
        *,
        side: Side,
        entry_time_ms: int,
        avg_entry: Decimal,
        qty: Decimal,
        stop_price: Decimal,
        entry_engine: str,
        entry_risk_mult: Decimal = Decimal("1"),
        units: int = 1,
    ) -> None:
        if side is Side.FLAT:
            raise ValueError("cannot open flat position")
        if avg_entry <= 0:
            raise ValueError("avg_entry must be positive")
        if qty <= 0:
            raise ValueError("qty must be positive")
        if stop_price <= 0:
            raise ValueError("stop_price must be positive")
        self.in_pos = True
        self.side = side
        self.entry_time_ms = entry_time_ms
        self.first_entry = avg_entry if self.first_entry is None else self.first_entry
        self.avg_entry = avg_entry
        self.initial_sl = stop_price if self.initial_sl is None else self.initial_sl
        self.stop_price = stop_price
        self.risk_per_coin = abs(avg_entry - stop_price)
        self.qty = qty
        self.units = units
        self.entry_engine = entry_engine
        self.entry_risk_mult = entry_risk_mult

    def update_stop(self, stop_price: Decimal) -> None:
        if stop_price <= 0:
            raise ValueError("stop_price must be positive")
        self.stop_price = stop_price
        for leg in self.legs.values():
            if leg.is_open:
                leg.stop_price = stop_price

    def mark_leg_open(
        self,
        *,
        exchange: str,
        avg_fill_price: Decimal,
        base_qty: Decimal,
        native_qty: Decimal | None = None,
        order_id: str | None = None,
        client_order_id: str | None = None,
        sync_status: str = "open",
    ) -> ExchangeLegState:
        if avg_fill_price <= 0:
            raise ValueError("avg_fill_price must be positive")
        if base_qty <= 0:
            raise ValueError("base_qty must be positive")
        leg = self.legs.get(exchange, ExchangeLegState(exchange=exchange))
        leg.is_open = True
        leg.avg_fill_price = avg_fill_price
        leg.base_qty = base_qty
        leg.native_qty = native_qty
        leg.entry_order_id = order_id or leg.entry_order_id
        leg.entry_client_order_id = client_order_id or leg.entry_client_order_id
        leg.stop_price = self.stop_price
        leg.sync_status = sync_status
        self.legs[exchange] = leg
        return leg

    def mark_leg_closed(self, *, exchange: str, sync_status: str = "closed") -> ExchangeLegState:
        leg = self.legs.get(exchange, ExchangeLegState(exchange=exchange))
        leg.is_open = False
        leg.base_qty = Decimal("0")
        leg.native_qty = Decimal("0") if leg.native_qty is not None else None
        leg.sync_status = sync_status
        self.legs[exchange] = leg
        return leg

    def apply_account_event(self, event: AccountEvent, *, master_exchange: str | None = None) -> None:
        """Update exchange-leg state from a generic private account event.

        The canonical strategy state is changed only for master exchange fills.
        Follower events update their own leg only.
        """
        if event.event_type is not AccountEventType.ORDER:
            return
        if event.symbol and event.symbol != "ETH-USDT-PERP":
            # This package is ETH-only for now. Future plugins can make symbol configurable.
            return
        exchange = event.exchange.value
        filled = event.filled_quantity or event.quantity or Decimal("0")
        price = event.price
        if event.order_status is OrderStatus.FILLED and filled > 0 and price and price > 0:
            if event.side is OrderSide.BUY:
                self.mark_leg_open(
                    exchange=exchange,
                    avg_fill_price=price,
                    base_qty=filled,
                    order_id=event.order_id,
                    client_order_id=event.client_order_id,
                )
                if master_exchange is not None and exchange == master_exchange and not self.in_pos:
                    # Stop is not inferred from account event. It must come from the strategy decision.
                    self.in_pos = True
                    self.side = Side.LONG
                    self.avg_entry = price
                    self.first_entry = self.first_entry or price
                    self.qty = filled
            elif event.side is OrderSide.SELL:
                self.mark_leg_closed(exchange=exchange)
                if master_exchange is not None and exchange == master_exchange:
                    self.reset()

    @property
    def open_legs(self) -> dict[str, ExchangeLegState]:
        return {exchange: leg for exchange, leg in self.legs.items() if leg.is_open}
