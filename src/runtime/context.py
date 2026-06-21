from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from src.app import AppContext
from src.runtime.config import LiveRuntimeConfig


@dataclass(frozen=True)
class LiveRuntimeContext:
    """Composition root for live runtime services.

    The runtime context wraps the existing app context and can later carry
    market-data/order-management services without moving their implementation
    into ``src.app`` or ``src.platform``.
    """

    config: LiveRuntimeConfig
    app_context: AppContext
    services: Mapping[str, Any] = field(default_factory=dict)
