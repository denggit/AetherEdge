from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.platform.execution.rules import round_to_step
from strategies.eth_lf_portfolio_v10b.domain.models import Side


STRUCTURAL_STOP_SOURCE = "STRUCTURAL_STOP"
STRUCTURAL_STOP_VARIANT = "struct_stop_all_swing_n21_buf0p0_trig0p0_h0"


@dataclass(frozen=True)
class StructuralStopConfig:
    enabled: bool = True
    engine_scope: str = "ALL"
    source: str = "swing"
    lookback_bars: int = 21
    buffer_atr: Decimal = Decimal("0.0")
    trigger_mfe_r: Decimal = Decimal("0.0")
    min_hold_bars: int = 0
    require_full_window: bool = True
    closed_bar_only: bool = True
    effective_from_next_bar: bool = True
    price_tick: Decimal | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "StructuralStopConfig":
        raw = dict(value or {})
        config = cls(
            enabled=_bool(raw.get("enabled", True)),
            engine_scope=str(raw.get("engine_scope", "ALL")).strip().upper(),
            source=str(raw.get("source", "swing")).strip().lower(),
            lookback_bars=int(raw.get("lookback_bars", 21)),
            buffer_atr=Decimal(str(raw.get("buffer_atr", "0.0"))),
            trigger_mfe_r=Decimal(str(raw.get("trigger_mfe_r", "0.0"))),
            min_hold_bars=int(raw.get("min_hold_bars", 0)),
            require_full_window=_bool(raw.get("require_full_window", True)),
            closed_bar_only=_bool(raw.get("closed_bar_only", True)),
            effective_from_next_bar=_bool(raw.get("effective_from_next_bar", True)),
            price_tick=_optional_decimal(raw.get("price_tick")),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.lookback_bars <= 0:
            raise ValueError("structural_stop.lookback_bars must be positive")
        if self.buffer_atr < 0:
            raise ValueError("structural_stop.buffer_atr must be non-negative")
        if self.trigger_mfe_r < 0:
            raise ValueError("structural_stop.trigger_mfe_r must be non-negative")
        if self.min_hold_bars < 0:
            raise ValueError("structural_stop.min_hold_bars must be non-negative")
        if self.source != "swing":
            raise ValueError("V10B structural_stop.source must be swing")
        if self.engine_scope != "ALL":
            raise ValueError("V10B structural_stop.engine_scope must be ALL")
        if not self.require_full_window:
            raise ValueError("V10B structural_stop.require_full_window must be true")
        if not self.closed_bar_only:
            raise ValueError("V10B structural_stop.closed_bar_only must be true")
        if not self.effective_from_next_bar:
            raise ValueError("V10B structural_stop.effective_from_next_bar must be true")
        if self.price_tick is not None and self.price_tick <= 0:
            raise ValueError("structural_stop.price_tick must be positive when set")


@dataclass(frozen=True)
class StructuralStopDecision:
    strategy: str
    bar_close_time: int | None
    side: str
    engine: str
    lookback_bars: int
    available_closed_bars: int
    current_close: Decimal | None
    old_stop: Decimal | None
    base_v10a_stop: Decimal | None
    swing_low_21: Decimal | None
    swing_high_21: Decimal | None
    raw_candidate: Decimal | None
    rounded_candidate: Decimal | None
    accepted: bool
    reject_reason: str
    final_stop: Decimal | None
    stop_source: str
    effective_from_next_bar: bool = True

    def as_audit_fields(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy,
            "bar_close_time": self.bar_close_time,
            "side": self.side,
            "engine": self.engine,
            "entry_engine": self.engine,
            "lookback_bars": self.lookback_bars,
            "available_closed_bars": self.available_closed_bars,
            "current_close": _text(self.current_close),
            "old_stop": _text(self.old_stop),
            "base_v10a_stop": _text(self.base_v10a_stop),
            "swing_low_21": _text(self.swing_low_21),
            "swing_high_21": _text(self.swing_high_21),
            "raw_candidate": _text(self.raw_candidate),
            "rounded_candidate": _text(self.rounded_candidate),
            "accepted": self.accepted,
            "reject_reason": self.reject_reason,
            "final_stop": _text(self.final_stop),
            "stop_source": self.stop_source,
            "effective_from_next_bar": self.effective_from_next_bar,
        }


def evaluate_swing_structural_stop(
    *,
    closed_bars: Sequence[Any],
    side: Side,
    old_stop: Decimal | None,
    base_v10a_stop: Decimal | None,
    current_close: Decimal | None,
    atr: Decimal | None,
    engine: str,
    hold_bars: int | None,
    mfe_r: Decimal | None,
    bar_close_time: int | None,
    config: StructuralStopConfig,
    current_bar_exit: bool = False,
    precondition_reject_reason: str | None = None,
    strategy: str = "eth_lf_portfolio_v10b",
) -> StructuralStopDecision:
    """Evaluate the completed bar, producing a stop that is active only later.

    ``closed_bars`` must be ordered oldest to newest and include the just-closed
    strategy bar. The function never mutates strategy or order state.
    """

    available = len(closed_bars)
    final_without_structure = base_v10a_stop if base_v10a_stop is not None else old_stop
    window = tuple(closed_bars[-config.lookback_bars :])
    swing_low = _window_extreme(window, "low", minimum=True) if len(window) == config.lookback_bars else None
    swing_high = _window_extreme(window, "high", minimum=False) if len(window) == config.lookback_bars else None

    def rejected(
        reason: str,
        *,
        raw: Decimal | None = None,
        rounded: Decimal | None = None,
    ) -> StructuralStopDecision:
        return StructuralStopDecision(
            strategy=strategy,
            bar_close_time=bar_close_time,
            side=_side_label(side),
            engine=engine,
            lookback_bars=config.lookback_bars,
            available_closed_bars=available,
            current_close=current_close,
            old_stop=old_stop,
            base_v10a_stop=base_v10a_stop,
            swing_low_21=swing_low,
            swing_high_21=swing_high,
            raw_candidate=raw,
            rounded_candidate=rounded,
            accepted=False,
            reject_reason=reason,
            final_stop=final_without_structure,
            stop_source="V10A_STOP",
            effective_from_next_bar=config.effective_from_next_bar,
        )

    if not config.enabled:
        return rejected("disabled")
    if side not in (Side.LONG, Side.SHORT):
        return rejected("unknown_position_side")
    if not str(engine or "").strip():
        return rejected("missing_entry_engine")
    if old_stop is None or old_stop <= 0:
        return rejected("missing_old_stop")
    if current_close is None or current_close <= 0:
        return rejected("missing_current_close")
    if precondition_reject_reason:
        return rejected(precondition_reject_reason)
    if current_bar_exit:
        return rejected("current_bar_exit")
    if hold_bars is None:
        return rejected("missing_hold_bars")
    if hold_bars < config.min_hold_bars:
        return rejected("min_hold_bars_not_reached")
    if mfe_r is None:
        return rejected("missing_mfe_r")
    if mfe_r < config.trigger_mfe_r:
        return rejected("trigger_mfe_r_not_reached")
    if config.require_full_window and available < config.lookback_bars:
        return rejected("insufficient_closed_bars")
    if swing_low is None or swing_high is None:
        return rejected("invalid_swing_window")
    if config.buffer_atr > 0 and (atr is None or atr <= 0):
        return rejected("missing_atr_for_buffer")

    atr_value = atr or Decimal("0")
    raw = (
        swing_low - config.buffer_atr * atr_value
        if side is Side.LONG
        else swing_high + config.buffer_atr * atr_value
    )
    if raw <= 0:
        return rejected("invalid_raw_candidate", raw=raw)
    if side is Side.LONG and raw >= current_close:
        return rejected("raw_candidate_crosses_close", raw=raw)
    if side is Side.SHORT and raw <= current_close:
        return rejected("raw_candidate_crosses_close", raw=raw)

    rounded = round_to_step(raw, config.price_tick)
    if side is Side.LONG and rounded < old_stop:
        return rejected("rounded_candidate_loosens_old_stop", raw=raw, rounded=rounded)
    if side is Side.SHORT and rounded > old_stop:
        return rejected("rounded_candidate_loosens_old_stop", raw=raw, rounded=rounded)
    if side is Side.LONG and rounded >= current_close:
        return rejected("rounded_candidate_crosses_close", raw=raw, rounded=rounded)
    if side is Side.SHORT and rounded <= current_close:
        return rejected("rounded_candidate_crosses_close", raw=raw, rounded=rounded)

    comparison_stop = final_without_structure
    if comparison_stop is not None:
        if side is Side.LONG and rounded <= comparison_stop:
            reason = (
                "not_more_protective_than_old_stop"
                if rounded <= old_stop and comparison_stop == old_stop
                else "not_more_protective_than_base_v10a_stop"
            )
            return rejected(reason, raw=raw, rounded=rounded)
        if side is Side.SHORT and rounded >= comparison_stop:
            reason = (
                "not_more_protective_than_old_stop"
                if rounded >= old_stop and comparison_stop == old_stop
                else "not_more_protective_than_base_v10a_stop"
            )
            return rejected(reason, raw=raw, rounded=rounded)

    return StructuralStopDecision(
        strategy=strategy,
        bar_close_time=bar_close_time,
        side=_side_label(side),
        engine=engine,
        lookback_bars=config.lookback_bars,
        available_closed_bars=available,
        current_close=current_close,
        old_stop=old_stop,
        base_v10a_stop=base_v10a_stop,
        swing_low_21=swing_low,
        swing_high_21=swing_high,
        raw_candidate=raw,
        rounded_candidate=rounded,
        accepted=True,
        reject_reason="",
        final_stop=rounded,
        stop_source=STRUCTURAL_STOP_SOURCE,
        effective_from_next_bar=config.effective_from_next_bar,
    )


def _window_extreme(window: Sequence[Any], field: str, *, minimum: bool) -> Decimal | None:
    values: list[Decimal] = []
    for bar in window:
        value = getattr(bar, field, None)
        if value is None and isinstance(bar, Mapping):
            value = bar.get(field)
        try:
            decimal_value = Decimal(str(value))
        except (ValueError, TypeError):
            return None
        if decimal_value <= 0:
            return None
        values.append(decimal_value)
    if not values:
        return None
    return min(values) if minimum else max(values)


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _side_label(side: Side) -> str:
    if side is Side.LONG:
        return "long"
    if side is Side.SHORT:
        return "short"
    return "unknown"


def _text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
