from __future__ import annotations

from typing import Protocol

from src.platform.account.events import AccountEvent
from src.platform.exchanges.models import ExchangeName, Order
from src.platform.snapshot import PlatformSnapshot
from src.platform.state.models import StoredAccountSnapshot, StoredEvent, StoredFill, StoredOrder


class StateStore(Protocol):
    """Local state persistence interface.

    This is storage only. It must not place orders, cancel orders, or run startup repair
    decisions.
    """

    def save_order(self, order: Order, *, is_stop_order: bool = False) -> None:
        ...

    def save_account_event(self, event: AccountEvent) -> None:
        ...

    def save_snapshot(self, snapshot: PlatformSnapshot) -> None:
        ...

    def get_order(
        self,
        *,
        exchange: ExchangeName,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> StoredOrder | None:
        ...

    def list_open_orders(self, *, exchange: ExchangeName, symbol: str, include_stop_orders: bool = True) -> list[StoredOrder]:
        ...

    def load_recent_events(self, *, exchange: ExchangeName, symbol: str | None = None, limit: int = 100) -> list[StoredEvent]:
        ...

    def load_recent_fills(self, *, exchange: ExchangeName, symbol: str, limit: int = 100) -> list[StoredFill]:
        ...

    def load_latest_account_snapshot(self, *, exchange: ExchangeName, symbol: str) -> StoredAccountSnapshot | None:
        ...
