from __future__ import annotations

from src.runtime.models import RuntimeHealth, RuntimePhase


class LiveShutdownService:
    """Minimal shutdown hook for live runtime services."""

    async def shutdown(self) -> RuntimeHealth:
        return RuntimeHealth(phase=RuntimePhase.STOPPED, warmup_complete=True, caught_up=True)
