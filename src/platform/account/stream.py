from __future__ import annotations

from typing import AsyncIterator, Protocol

from src.platform.account.events import AccountEvent
from src.platform.exchanges.models import ExchangeName


class AccountEventStream(Protocol):
    """Private user-data stream interface.

    It only yields exchange events. It does not apply recovery, state mutation,
    strategy actions, or order management logic.
    """

    @property
    def exchange(self) -> ExchangeName:
        ...

    @property
    def symbol(self) -> str:
        ...

    async def stream_events(self) -> AsyncIterator[AccountEvent]:
        ...
