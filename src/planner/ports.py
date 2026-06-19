from __future__ import annotations

from typing import Iterable, Protocol

from src.planner.models import ExecutionPlan
from src.signals.models import TradeSignal


class PlannerPort(Protocol):
    def plan(self, signal: TradeSignal) -> ExecutionPlan:
        ...

    def plan_many(self, signals: Iterable[TradeSignal]) -> ExecutionPlan:
        ...
