from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from src.market_data.events import MarketFeatureEvent


@runtime_checkable
class StartupFeatureBackfillProvider(Protocol):
    """Optional strategy hook consumed by the generic live runtime."""

    name: str
    poll_interval_seconds: float

    def check_and_launch(self) -> Mapping[str, Any]: ...

    def poll_readiness(self) -> Mapping[str, Any]: ...

    def market_feature_events(
        self,
        result: Mapping[str, Any],
    ) -> Sequence[MarketFeatureEvent]: ...


def resolve_startup_feature_backfill_providers(
    strategy: object,
) -> tuple[StartupFeatureBackfillProvider, ...]:
    provider_hook = getattr(
        strategy,
        "startup_feature_backfill_providers",
        None,
    )
    if not callable(provider_hook):
        return ()
    values = provider_hook()
    if values is None:
        return ()
    providers = tuple(values)
    invalid = tuple(
        type(provider).__name__
        for provider in providers
        if not isinstance(provider, StartupFeatureBackfillProvider)
    )
    if invalid:
        raise TypeError(
            "invalid startup feature backfill provider types: "
            f"{invalid}"
        )
    names = tuple(str(provider.name).strip() for provider in providers)
    if any(not name for name in names) or len(set(names)) != len(names):
        raise ValueError(
            "startup feature backfill provider names must be unique "
            "and non-empty"
        )
    return providers


__all__ = [
    "StartupFeatureBackfillProvider",
    "resolve_startup_feature_backfill_providers",
]
