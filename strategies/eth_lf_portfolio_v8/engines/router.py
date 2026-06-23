from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable, Protocol

from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, EngineSignal, RoutedSignal, Side


class V8Engine(Protocol):
    name: str
    priority: int

    def evaluate(self, context: BarReadyContext) -> EngineSignal | None:
        ...


@dataclass(frozen=True)
class PortfolioSelectionConfig:
    """V9C portfolio-level scaling applied after an engine wins routing."""

    min_risk_mult: Decimal = Decimal("0.35")
    max_risk_mult: Decimal = Decimal("10.0")
    quality_mult_cap: Decimal = Decimal("2.20")
    bear_standalone_risk_scale: Decimal = Decimal("1.0")
    bear_standalone_quality_scale: Decimal = Decimal("1.0")
    bull_reclaim_risk_scale: Decimal = Decimal("1.0")
    bull_reclaim_quality_scale: Decimal = Decimal("1.0")


@dataclass
class PortfolioRouter:
    """Route LF engine votes by V9C reclaim-first priority order.

    Final routing order is: Bull Reclaim V2 first, Momentum V3 second, Bear V3 third.
    Engines that return FLAT/None do not open a position.
    """

    engines: tuple[V8Engine, ...] = field(default_factory=tuple)
    selection_config: PortfolioSelectionConfig = field(default_factory=PortfolioSelectionConfig)

    def evaluate(self, context: BarReadyContext) -> RoutedSignal:
        return self.select([signal for signal in (engine.evaluate(context) for engine in self.engines) if signal is not None])

    def select(self, signals: Iterable[EngineSignal | RoutedSignal]) -> RoutedSignal:
        candidates = [signal for signal in signals if signal.side is not Side.FLAT]
        if not candidates:
            return RoutedSignal.flat()
        selected = sorted(candidates, key=lambda item: item.priority, reverse=True)[0]
        risk_mult, quality_mult = self._portfolio_scaled_multipliers(selected)
        return RoutedSignal(
            side=selected.side,
            engine=selected.engine,
            priority=selected.priority,
            risk_mult=risk_mult,
            quality_mult=quality_mult,
            reason=selected.reason,
            metadata=selected.metadata,
        )

    def _portfolio_scaled_multipliers(self, selected: EngineSignal | RoutedSignal) -> tuple[Decimal, Decimal]:
        """Mirror CoinBacktest V9C ``select_portfolio_signals`` sizing columns.

        Momentum keeps its own ``risk_mult`` / ``quality_mult`` untouched. Bear
        standalone and Bull Reclaim signals are scaled and clipped at the
        portfolio layer after they win routing.
        """

        engine = str(selected.engine).upper()
        cfg = self.selection_config
        risk_mult = selected.risk_mult
        quality_mult = selected.quality_mult
        if engine == "BEAR_V3_ONLY":
            return (
                _clip_decimal(risk_mult * cfg.bear_standalone_risk_scale, cfg.min_risk_mult, cfg.max_risk_mult),
                _clip_decimal(quality_mult * cfg.bear_standalone_quality_scale, Decimal("0.20"), cfg.quality_mult_cap),
            )
        if engine == "BULL_RECLAIM_V2":
            return (
                _clip_decimal(risk_mult * cfg.bull_reclaim_risk_scale, cfg.min_risk_mult, cfg.max_risk_mult),
                _clip_decimal(quality_mult * cfg.bull_reclaim_quality_scale, Decimal("0.10"), cfg.quality_mult_cap),
            )
        return risk_mult, quality_mult


def _clip_decimal(value: Decimal, lower: Decimal, upper: Decimal) -> Decimal:
    return max(lower, min(upper, value))
