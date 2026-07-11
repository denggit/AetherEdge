from __future__ import annotations

from typing import Protocol, Sequence

from src.order_management.models import ExchangeOrderResult, OrderIntent, OrderIntentStatus
from src.platform.exchanges.models import ExchangeName
from src.signals.models import SignalAction


class OrderIntentRepository(Protocol):
    def claim_intent(self, intent: OrderIntent) -> bool:
        ...

    def update_claimed_intent(self, intent: OrderIntent) -> None:
        ...

    def update_status(self, *, intent_id: str, status: OrderIntentStatus) -> None:
        ...

    def get_intent(self, intent_id: str) -> OrderIntent | None:
        ...


class ClientOrderIdFactory(Protocol):
    def create(
        self,
        *,
        intent_id: str,
        action: SignalAction,
        exchange: ExchangeName,
        sequence: int = 0,
    ) -> str:
        ...


class DuplicateOrderGuard(Protocol):
    def claim_or_raise(self, intent: OrderIntent) -> None:
        ...


class OrderCoordinatorPort(Protocol):
    async def execute(self, intent: OrderIntent) -> Sequence[ExchangeOrderResult]:
        ...
