from __future__ import annotations

from typing import Any

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
        self._context = context

    def bind_dependencies(self, **dependencies: Any) -> None:
        """One-shot composition hook for explicitly selected dependencies."""

        overlap = set(dependencies).intersection(self.__dict__)
        if overlap:
            raise RuntimeError(
                "runtime dependency already bound: " + ", ".join(sorted(overlap))
            )
        for name, value in dependencies.items():
            setattr(self, name, value)

    def bind_ports(self, ports: Any) -> None:
        if "ports" in self.__dict__:
            raise RuntimeError("runtime ports are already bound")
        self.ports = ports

    @property
    def runtime_state(self) -> RuntimeState:
        return self._context.state

    @property
    def market_state(self) -> MarketRuntimeState:
        return self._context.state.market

    @property
    def _account_config_new_entries_blocked(self) -> bool:
        return (
            self._context.state.operational
            .account_config_new_entries_blocked
        )

    @_account_config_new_entries_blocked.setter
    def _account_config_new_entries_blocked(self, value: bool) -> None:
        (
            self._context.state.operational
            .account_config_new_entries_blocked
        ) = bool(value)


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
