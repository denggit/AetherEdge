from __future__ import annotations

from dataclasses import fields

from src.runtime.services import (
    DEFAULT_RUNTIME_SERVICE,
    RuntimeServiceBundle,
    RuntimeServices,
)


class LegacyRuntimeServiceView:
    """Mapping facade used only when the constructor receives a legacy dict."""

    def __init__(
        self,
        bundle: RuntimeServiceBundle,
        legacy: RuntimeServices,
    ) -> None:
        self.market = bundle.market
        self.execution = bundle.execution
        self.account = bundle.account
        self.persistence = bundle.persistence
        self.recovery = bundle.recovery
        self.lifecycle = bundle.lifecycle
        self.range = bundle.range
        self._legacy = legacy

    def __getitem__(self, key: str) -> object:
        if key not in self:
            raise KeyError(key)
        return getattr(self._legacy, key)

    def __setitem__(self, key: str, value: object) -> None:
        if key not in {item.name for item in fields(self._legacy)}:
            raise KeyError(key)
        setattr(self._legacy, key, value)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str) or key not in {
            item.name for item in fields(self._legacy)
        }:
            return False
        value = getattr(self._legacy, key)
        return value is not None and value is not DEFAULT_RUNTIME_SERVICE


__all__ = ["LegacyRuntimeServiceView"]
