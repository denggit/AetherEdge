from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping

from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.exchanges.models import OrderSide, OrderStatus
from strategies.eth_portfolio_v1.domain.models import Side


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
    position_id: str | None = None
    in_pos: bool = False
    side: Side = Side.FLAT
    entry_time_ms: int | None = None
    first_entry: Decimal | None = None
    avg_entry: Decimal | None = None
    initial_sl: Decimal | None = None
    stop_price: Decimal | None = None
    confirmed_stop_price: Decimal | None = None
    desired_stop_price: Decimal | None = None
    pending_stop_replace: bool = False
    pending_stop_reason: str | None = None
    pending_stop_bar_close_time_ms: int | None = None
    risk_per_coin: Decimal | None = None
    qty: Decimal = Decimal("0")
    units: int = 0
    max_fav: Decimal = Decimal("0")
    max_adv: Decimal = Decimal("0")
    entry_risk_mult: Decimal = Decimal("1")
    entry_engine: str = ""
    last_exit_time_ms: int | None = None
    legs: dict[str, ExchangeLegState] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.confirmed_stop_price is None and self.stop_price is not None:
            self.confirmed_stop_price = self.stop_price
        elif self.stop_price is None and self.confirmed_stop_price is not None:
            self.stop_price = self.confirmed_stop_price

    def reset(self, *, keep_last_exit: bool = False) -> None:
        self.in_pos = False
        self.position_id = None
        self.side = Side.FLAT
        self.entry_time_ms = None
        self.first_entry = None
        self.avg_entry = None
        self.initial_sl = None
        self.stop_price = None
        self.confirmed_stop_price = None
        self.desired_stop_price = None
        self.pending_stop_replace = False
        self.pending_stop_reason = None
        self.pending_stop_bar_close_time_ms = None
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
        position_id: str | None = None,
        stop_confirmed: bool = True,
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
        self.position_id = position_id or self.position_id
        self.side = side
        self.entry_time_ms = entry_time_ms
        self.first_entry = avg_entry if self.first_entry is None else self.first_entry
        self.avg_entry = avg_entry
        self.initial_sl = stop_price if self.initial_sl is None else self.initial_sl
        if stop_confirmed:
            self.stop_price = stop_price
            self.confirmed_stop_price = stop_price
            self.desired_stop_price = None
            self.pending_stop_replace = False
            self.pending_stop_reason = None
            self.pending_stop_bar_close_time_ms = None
        else:
            self.stop_price = self.confirmed_stop_price
            self.desired_stop_price = stop_price
            self.pending_stop_replace = True
            self.pending_stop_reason = "MASTER_ENTRY_FILLED_REPLACE_STOP"
            self.pending_stop_bar_close_time_ms = entry_time_ms
        self.risk_per_coin = abs(self.first_entry - self.initial_sl)
        self.qty = qty
        self.units = units
        if units == 1:
            self.max_fav = avg_entry
            self.max_adv = avg_entry
        self.entry_engine = entry_engine
        self.entry_risk_mult = entry_risk_mult


    def add_master_fill(self, *, avg_fill_price: Decimal, add_qty: Decimal) -> None:
        if not self.in_pos:
            raise ValueError("cannot add when not in position")
        if avg_fill_price <= 0:
            raise ValueError("avg_fill_price must be positive")
        if add_qty <= 0:
            raise ValueError("add_qty must be positive")
        total_qty = self.qty + add_qty
        if total_qty <= 0:
            return
        old_avg = self.avg_entry or avg_fill_price
        self.avg_entry = (old_avg * self.qty + avg_fill_price * add_qty) / total_qty
        self.qty = total_qty
        self.units += 1

    def initialize_initial_risk_if_missing(self) -> None:
        if self.risk_per_coin is not None:
            return
        if self.first_entry is None or self.initial_sl is None:
            return
        self.risk_per_coin = abs(self.first_entry - self.initial_sl)

    def update_favorable_extremes(self, *, high: Decimal, low: Decimal) -> None:
        if not self.in_pos:
            return
        if self.max_fav == Decimal("0"):
            self.max_fav = self.first_entry or self.avg_entry or Decimal("0")
        if self.max_adv == Decimal("0"):
            self.max_adv = self.first_entry or self.avg_entry or Decimal("0")
        if self.side is Side.LONG:
            self.max_fav = max(self.max_fav, high)
            self.max_adv = min(self.max_adv, low)
        elif self.side is Side.SHORT:
            self.max_fav = min(self.max_fav, low)
            self.max_adv = max(self.max_adv, high)

    def close_master(self, *, exit_time_ms: int | None = None) -> None:
        self.reset()
        self.last_exit_time_ms = exit_time_ms

    def update_stop(self, stop_price: Decimal) -> None:
        if stop_price <= 0:
            raise ValueError("stop_price must be positive")
        self.stop_price = stop_price
        self.confirmed_stop_price = stop_price
        self.desired_stop_price = None
        self.pending_stop_replace = False
        self.pending_stop_reason = None
        self.pending_stop_bar_close_time_ms = None
        for leg in self.legs.values():
            if leg.is_open:
                leg.stop_price = stop_price

    def mark_pending_stop_replace(
        self,
        *,
        desired_stop_price: Decimal,
        reason: str,
        bar_close_time_ms: int | None,
    ) -> None:
        if desired_stop_price <= 0:
            raise ValueError("desired_stop_price must be positive")
        self.desired_stop_price = desired_stop_price
        self.pending_stop_replace = True
        self.pending_stop_reason = reason
        self.pending_stop_bar_close_time_ms = bar_close_time_ms

    def confirm_pending_stop_replace(self, *, stop_price: Decimal | None = None) -> None:
        confirmed = stop_price if stop_price is not None else self.desired_stop_price
        if confirmed is None or confirmed <= 0:
            raise ValueError("confirmed stop_price must be positive")
        self.update_stop(confirmed)

    def reject_pending_stop_replace(self) -> None:
        self.desired_stop_price = None
        self.pending_stop_replace = False
        self.pending_stop_reason = None
        self.pending_stop_bar_close_time_ms = None

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

    def add_leg_fill(
        self,
        *,
        exchange: str,
        avg_fill_price: Decimal,
        add_base_qty: Decimal,
        native_qty: Decimal | None = None,
        order_id: str | None = None,
        client_order_id: str | None = None,
        sync_status: str = "open",
    ) -> ExchangeLegState:
        if add_base_qty <= 0:
            raise ValueError("add_base_qty must be positive")
        leg = self.legs.get(exchange)
        if leg is None or not leg.is_open or leg.base_qty <= 0 or leg.avg_fill_price is None:
            return self.mark_leg_open(
                exchange=exchange,
                avg_fill_price=avg_fill_price,
                base_qty=add_base_qty,
                native_qty=native_qty,
                order_id=order_id,
                client_order_id=client_order_id,
                sync_status=sync_status,
            )
        total_qty = leg.base_qty + add_base_qty
        leg.avg_fill_price = (leg.avg_fill_price * leg.base_qty + avg_fill_price * add_base_qty) / total_qty
        leg.base_qty = total_qty
        if native_qty is not None:
            leg.native_qty = (leg.native_qty or Decimal("0")) + native_qty
        leg.entry_order_id = order_id or leg.entry_order_id
        leg.entry_client_order_id = client_order_id or leg.entry_client_order_id
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
                    self.close_master(exit_time_ms=event.event_time_ms)

    @property
    def open_legs(self) -> dict[str, ExchangeLegState]:
        return {exchange: leg for exchange, leg in self.legs.items() if leg.is_open}
