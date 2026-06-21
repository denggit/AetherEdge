from __future__ import annotations

from src.order_management.models import OrderIntent
from src.order_management.ports import OrderIntentRepository


class DuplicateIntentError(RuntimeError):
    pass


class RepositoryDuplicateOrderGuard:
    """Prevent resubmitting an already journaled intent."""

    def __init__(self, repository: OrderIntentRepository) -> None:
        self.repository = repository

    def assert_not_duplicate(self, intent: OrderIntent) -> None:
        if self.repository.get_intent(intent.intent_id) is not None:
            raise DuplicateIntentError(f"duplicate order intent: {intent.intent_id}")
