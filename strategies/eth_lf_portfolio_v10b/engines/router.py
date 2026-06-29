from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Mapping, Protocol

from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, EngineSignal, RoutedSignal, Side
from strategies.eth_lf_portfolio_v10b.features.range_speed import RangeSpeedEvaluation


class V8Engine(Protocol):
    name: str
    priority: int

    def evaluate(self, context: BarReadyContext) -> EngineSignal | None:
        ...


class MicroEvaluator(Protocol):
    def evaluate(self, *, signal_side: Side | int, aggregate: Any) -> Any:
        ...


@dataclass(frozen=True)
class MomentumEntryFilterConfig:
    enable_momentum_long_not_aligned_block: bool = True
    enable_momentum_short_fast_speed_block: bool = True
    range_speed_rolling_window_bars: int = 1080
    range_speed_min_periods: int = 100
    range_speed_fast_quantile: float = 0.75

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "MomentumEntryFilterConfig":
        defaults = cls()
        return cls(
            enable_momentum_long_not_aligned_block=bool(
                values.get(
                    "enable_momentum_long_not_aligned_block",
                    defaults.enable_momentum_long_not_aligned_block,
                )
            ),
            enable_momentum_short_fast_speed_block=bool(
                values.get(
                    "enable_momentum_short_fast_speed_block",
                    defaults.enable_momentum_short_fast_speed_block,
                )
            ),
            range_speed_rolling_window_bars=int(
                values.get(
                    "range_speed_rolling_window_bars",
                    defaults.range_speed_rolling_window_bars,
                )
            ),
            range_speed_min_periods=int(
                values.get("range_speed_min_periods", defaults.range_speed_min_periods)
            ),
            range_speed_fast_quantile=float(
                values.get("range_speed_fast_quantile", defaults.range_speed_fast_quantile)
            ),
        )


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
    entry_filter_config: MomentumEntryFilterConfig = field(default_factory=MomentumEntryFilterConfig)
    micro_evaluator: MicroEvaluator | None = None

    def evaluate(
        self,
        context: BarReadyContext,
        *,
        range_speed: RangeSpeedEvaluation | None = None,
    ) -> RoutedSignal:
        return self.select(
            [signal for signal in (engine.evaluate(context) for engine in self.engines) if signal is not None],
            context=context,
            range_speed=range_speed,
        )

    def select(
        self,
        signals: Iterable[EngineSignal | RoutedSignal],
        *,
        context: BarReadyContext | None = None,
        range_speed: RangeSpeedEvaluation | None = None,
    ) -> RoutedSignal:
        candidates = [signal for signal in signals if signal.side is not Side.FLAT]
        gate_audit = self._base_gate_audit(range_speed)
        eligible: list[EngineSignal | RoutedSignal] = []
        for candidate in candidates:
            engine = str(candidate.engine).upper()
            if engine == "MOMENTUM_V3" and candidate.side is Side.LONG:
                micro_action = self._candidate_micro_action(candidate, context)
                gate_audit["v10_momentum_long_micro_filter_action"] = micro_action
                if (
                    self.entry_filter_config.enable_momentum_long_not_aligned_block
                    and micro_action == "NOT_ALIGNED_RISK_REDUCED"
                ):
                    gate_audit.update(
                        {
                            "blocked_by_v10_momentum_long_not_aligned": True,
                            "v10_blocked_engine": "MOMENTUM_V3",
                            "v10_blocked_side": "LONG",
                        }
                    )
                    continue
            if (
                engine == "MOMENTUM_V3"
                and candidate.side is Side.SHORT
                and self.entry_filter_config.enable_momentum_short_fast_speed_block
                and range_speed is not None
                and range_speed.available
                and range_speed.is_fast_range_speed
            ):
                gate_audit.update(
                    {
                        "blocked_by_v10a_momentum_short_fast_speed": True,
                        "v10a_blocked_engine": "MOMENTUM_V3",
                        "v10a_blocked_side": "SHORT",
                    }
                )
                continue
            eligible.append(candidate)

        if not eligible:
            return RoutedSignal(
                side=Side.FLAT,
                engine="none",
                priority=0,
                metadata=gate_audit,
            )
        selected = sorted(eligible, key=lambda item: item.priority, reverse=True)[0]
        risk_mult, quality_mult = self._portfolio_scaled_multipliers(selected)
        return RoutedSignal(
            side=selected.side,
            engine=selected.engine,
            priority=selected.priority,
            risk_mult=risk_mult,
            quality_mult=quality_mult,
            reason=selected.reason,
            metadata={**dict(selected.metadata), **gate_audit},
        )

    def _candidate_micro_action(
        self,
        candidate: EngineSignal | RoutedSignal,
        context: BarReadyContext | None,
    ) -> str:
        if context is not None and self.micro_evaluator is not None:
            decision = self.micro_evaluator.evaluate(
                signal_side=candidate.side,
                aggregate=context.range_aggregate,
            )
            return str(decision.action)
        value = candidate.metadata.get("micro_filter_action")
        if value is not None:
            return str(value)
        if context is not None:
            return str(context.micro.action)
        return "NEUTRAL"

    def _base_gate_audit(self, range_speed: RangeSpeedEvaluation | None) -> dict[str, Any]:
        cfg = self.entry_filter_config
        return {
            "blocked_by_v10_momentum_long_not_aligned": False,
            "blocked_by_v10a_momentum_short_fast_speed": False,
            "v10_blocked_engine": None,
            "v10_blocked_side": None,
            "v10a_blocked_engine": None,
            "v10a_blocked_side": None,
            "v10_momentum_long_micro_filter_action": None,
            "v10a_fast_speed_available": bool(range_speed is not None and range_speed.available),
            "rf_bar_count": None if range_speed is None else range_speed.rf_bar_count,
            "rf_bar_count_fast_threshold": None if range_speed is None else range_speed.fast_threshold,
            "is_fast_range_speed": bool(range_speed is not None and range_speed.is_fast_range_speed),
            "range_speed_historical_periods": 0 if range_speed is None else range_speed.historical_periods,
            "range_speed_history_warmup_count": 0 if range_speed is None else range_speed.historical_periods,
            "v10a_fast_speed_unavailable_reason": (
                "range_speed_not_evaluated"
                if range_speed is None
                else range_speed.unavailable_reason
            ),
            "v10a_fast_speed_degraded_margin": (
                1.0 if range_speed is None else range_speed.degraded_fast_margin
            ),
            "range_speed_rolling_window_bars": cfg.range_speed_rolling_window_bars,
            "range_speed_min_periods": cfg.range_speed_min_periods,
            "range_speed_fast_quantile": cfg.range_speed_fast_quantile,
        }

    def _portfolio_scaled_multipliers(self, selected: EngineSignal | RoutedSignal) -> tuple[Decimal, Decimal]:
        """Apply the frozen V9C ``select_portfolio_signals`` sizing semantics.

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
