from __future__ import annotations

from typing import Protocol

from src.platform.account.events import AccountEvent
from src.platform.snapshot import PlatformSnapshot


class RuntimeEventHandler(Protocol):
    """Observer hook for future strategy/runtime plugins.

    Implementations may observe events, but this interface does not allow direct
    exchange access and does not define trading actions.
    """

    async def on_snapshot(self, snapshot: PlatformSnapshot) -> None:
        ...

    async def on_account_event(self, event: AccountEvent) -> None:
        ...


class NoopRuntimeEventHandler:
    async def on_snapshot(self, snapshot: PlatformSnapshot) -> None:
        return None

    async def on_account_event(self, event: AccountEvent) -> None:
        return None
