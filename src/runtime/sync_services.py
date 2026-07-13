from __future__ import annotations

from collections.abc import Callable
from typing import Any


SyncServiceFactory = Callable[[], Any]


class RuntimeSyncServiceRegistry:
    """Own lazy Account and Order sync service identities."""

    def __init__(
        self,
        *,
        account_service: object | None = None,
        order_service: object | None = None,
    ) -> None:
        self._account_service = account_service
        self._order_service = order_service

    def get_account(self, factory: SyncServiceFactory) -> object:
        if self._account_service is None:
            self._account_service = factory()
        return self._account_service

    def get_order(self, factory: SyncServiceFactory) -> object:
        if self._order_service is None:
            self._order_service = factory()
        return self._order_service


__all__ = ["RuntimeSyncServiceRegistry", "SyncServiceFactory"]
