from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from src.platform.data.models import MarketKline
from src.runtime.requirements import StrategyRuntimeRequirements


@dataclass(frozen=True)
class MarketModuleSpec:
    module_id: str
    after: frozenset[str] = frozenset()
    before: frozenset[str] = frozenset()
    execution_mode: str = "inline_stateful"

    def __post_init__(self) -> None:
        if not self.module_id.strip():
            raise ValueError("module_id must be non-empty")
        if self.execution_mode not in {"inline_stateful", "stateless_parallel"}:
            raise ValueError(f"unsupported execution_mode: {self.execution_mode}")


@dataclass(frozen=True)
class ResolvedMarketPipelinePlan:
    trades_enabled: bool
    closed_kline_enabled: bool
    order_book_enabled: bool
    enabled_module_ids: tuple[str, ...]
    execution_stages: tuple[tuple[str, ...], ...]
    module_specs: tuple[MarketModuleSpec, ...] = field(
        default_factory=tuple,
        compare=False,
        hash=False,
    )


@dataclass
class ClosedBarControlEvent:
    open_time_ms: int
    kline: MarketKline
    started: bool = False
    skip_reason: str | None = None
    result: object | None = None
    _completion: asyncio.Future | None = field(default=None, repr=False)

    @property
    def completion(self) -> asyncio.Future:
        if self._completion is None:
            self._completion = asyncio.get_running_loop().create_future()
        return self._completion


_DEFAULT_ORDER = (
    "range-footprint",
    "fixed-time-trade-bars",
    "trade-footprint",
    "range-bars",
    "raw-trade-callback",
)


def resolve_market_pipeline(
    requirements: StrategyRuntimeRequirements,
    *,
    extra_module_ids: frozenset[str] = frozenset(),
    custom_specs: tuple[MarketModuleSpec, ...] = (),
    feature_config: object | None = None,
) -> ResolvedMarketPipelinePlan:
    raw_trades = requirements.trades.enabled and requirements.trades.stream_enabled
    features = []
    for attribute, module_id in (
        ("range_footprint_enabled", "range-footprint"),
        ("fixed_time_trade_bars_enabled", "fixed-time-trade-bars"),
        ("trade_footprint_enabled", "trade-footprint"),
    ):
        if feature_config is not None and getattr(feature_config, attribute, False):
            features.append(module_id)

    trades_enabled = bool(raw_trades or requirements.range_bars.enabled or features)
    enabled = (["trade-stream"] if trades_enabled else []) + features
    if requirements.range_bars.enabled:
        enabled.append("range-bars")
    if raw_trades:
        enabled.append("raw-trade-callback")
    enabled.extend(sorted(extra_module_ids - set(enabled)))
    enabled_set = frozenset(enabled)

    custom_by_id = {spec.module_id: spec for spec in custom_specs}
    missing = extra_module_ids - custom_by_id.keys()
    if missing:
        raise ValueError("no module specification for: " + ", ".join(sorted(missing)))
    for spec in custom_specs:
        if spec.module_id not in enabled_set:
            raise ValueError(f"module dependency declared for disabled module: {spec.module_id}")
        invalid = (spec.after | spec.before) - enabled_set
        if invalid:
            raise ValueError(
                f"module {spec.module_id} depends on unavailable module(s): "
                + ", ".join(sorted(invalid))
            )

    ordered_defaults = [module_id for module_id in _DEFAULT_ORDER if module_id in enabled_set]
    specs = {
        module_id: MarketModuleSpec(
            module_id,
            after=frozenset(ordered_defaults[:index]),
        )
        for index, module_id in enumerate(ordered_defaults)
    }
    specs.update(custom_by_id)
    ordered = _topological_order(enabled_set, specs)
    stages = (ordered,) if ordered else ((),)
    return ResolvedMarketPipelinePlan(
        trades_enabled=trades_enabled,
        closed_kline_enabled=requirements.closed_kline.enabled,
        order_book_enabled=(
            requirements.order_book.enabled
            and requirements.order_book.stream_enabled
        ),
        enabled_module_ids=ordered,
        execution_stages=stages,
        module_specs=tuple(specs[module_id] for module_id in ordered if module_id in specs),
    )


def _topological_order(
    module_ids: frozenset[str],
    specs: dict[str, MarketModuleSpec],
) -> tuple[str, ...]:
    incoming = {module_id: set() for module_id in module_ids}
    for module_id, spec in specs.items():
        incoming[module_id].update(spec.after)
        for target in spec.before:
            incoming[target].add(module_id)
    ready = sorted(module_id for module_id, deps in incoming.items() if not deps)
    result: list[str] = []
    while ready:
        module_id = ready.pop(0)
        result.append(module_id)
        for target, deps in incoming.items():
            if module_id in deps:
                deps.remove(module_id)
                if not deps and target not in result and target not in ready:
                    ready.append(target)
        ready.sort()
    if len(result) != len(module_ids):
        cycle = ", ".join(sorted(module_ids - set(result)))
        raise ValueError(f"module dependency cycle detected: {cycle}")
    return tuple(result)


__all__ = [
    "ClosedBarControlEvent",
    "MarketModuleSpec",
    "ResolvedMarketPipelinePlan",
    "resolve_market_pipeline",
]
