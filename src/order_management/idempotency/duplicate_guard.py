from __future__ import annotations

from src.order_management.models import OrderIntent
from src.order_management.ports import OrderIntentRepository


class DuplicateIntentError(RuntimeError):
    pass


class RepositoryDuplicateOrderGuard:
    """Atomically claim an intent before any execution work begins."""

    def __init__(self, repository: OrderIntentRepository) -> None:
        self.repository = repository

    def claim_or_raise(self, intent: OrderIntent) -> None:
        if not self.repository.claim_intent(intent):
            raise DuplicateIntentError(f"duplicate order intent: {intent.intent_id}")
