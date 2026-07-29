from __future__ import annotations

from src.runtime.context import (
    AccountRuntimeState,
    ClosedBarRuntimeState,
    ExecutionRuntimeState,
    MarketRuntimeState,
    OperationalRuntimeState,
    RangeRuntimeState,
    RuntimeContext,
    RuntimeState,
)


RuntimeSharedState = RuntimeState


class RuntimeComponent:
    """Component constructed with one explicit runtime context."""

    def __init__(self, context: RuntimeContext) -> None:
        # Components operate on one explicitly composed context.  Sharing the
        # context namespace preserves the established state transitions while
        # removing owner fallback and arbitrary attribute interception.
        self.__dict__ = context.__dict__

    @property
    def market_state(self) -> MarketRuntimeState:
        return self.runtime_state.market


__all__ = [
    "AccountRuntimeState",
    "ClosedBarRuntimeState",
    "ExecutionRuntimeState",
    "MarketRuntimeState",
    "OperationalRuntimeState",
    "RangeRuntimeState",
    "RuntimeComponent",
    "RuntimeContext",
    "RuntimeSharedState",
    "RuntimeState",
]
