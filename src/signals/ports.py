from __future__ import annotations

from typing import Protocol, Sequence

from src.signals.models import TradeSignal


class SignalHandler(Protocol):
    async def on_signals(self, signals: Sequence[TradeSignal]) -> None:
        ...
