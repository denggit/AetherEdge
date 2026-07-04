from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping

from strategies.eth_portfolio_v1.domain.models import Side


RANGE_EXIT_REASON = "RANGE_EXIT_NEXT_OPEN"


@dataclass(frozen=True)
class RangeExitConfig:
    enabled: bool = True
    mode: str = "soft"
    min_mfe_r: Decimal = Decimal("2.0")
    giveback_frac: Decimal = Decimal("0.65")
    min_hold_bars: int = 2
    contra_imbalance: Decimal = Decimal("0.05")
    bad_close_pos: Decimal = Decimal("0.35")
    require_reversal: bool = True

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> "RangeExitConfig":
        data = dict(raw or {})
        delay_bars = int(data.get("delay_bars", 0) or 0)
        if delay_bars != 0:
            raise ValueError("range_exit.delay_bars is not supported in live and must be 0")
        mode = str(data.get("mode", "soft")).strip().lower()
        if mode not in {"off", "soft"}:
            raise ValueError("range_exit.mode must be off or soft")
        return cls(
            enabled=bool(data.get("enabled", True)),
            mode=mode,
            min_mfe_r=Decimal(str(data.get("min_mfe_r", "2.0"))),
            giveback_frac=Decimal(str(data.get("giveback_frac", "0.65"))),
            min_hold_bars=int(data.get("min_hold_bars", 2)),
            contra_imbalance=abs(Decimal(str(data.get("contra_imbalance", "0.05")))),
            bad_close_pos=Decimal(str(data.get("bad_close_pos", "0.35"))),
            require_reversal=bool(data.get("require_reversal", True)),
        )


@dataclass(frozen=True)
class RangeExitDecision:
    should_exit: bool
    reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def evaluate_range_exit(
    *,
    side: Side,
    avg_entry: Decimal,
    risk_per_coin: Decimal,
    max_fav: Decimal,
    hold_bars: int,
    close: Decimal,
    micro_context_available: bool,
    rf_imbalance: Decimal | None,
    rf_close_pos: Decimal | None,
    config: RangeExitConfig,
) -> RangeExitDecision:
    metadata: dict[str, Any] = _base_metadata(
        config=config,
        micro_context_available=micro_context_available,
        rf_imbalance=rf_imbalance,
        rf_close_pos=rf_close_pos,
    )
    if not config.enabled or config.mode == "off":
        return RangeExitDecision(False, metadata=metadata)
    if side not in {Side.LONG, Side.SHORT} or risk_per_coin <= 0:
        return RangeExitDecision(False, metadata=metadata)
    if hold_bars < config.min_hold_bars:
        return RangeExitDecision(False, metadata=metadata)

    if side is Side.LONG:
        peak_r = (max_fav - avg_entry) / risk_per_coin
        current_r = (close - avg_entry) / risk_per_coin
    else:
        peak_r = (avg_entry - max_fav) / risk_per_coin
        current_r = (avg_entry - close) / risk_per_coin

    giveback_frac = (peak_r - current_r) / max(abs(peak_r), Decimal("1e-12"))
    metadata.update(
        {
            "range_exit_peak_r": str(peak_r),
            "range_exit_current_r": str(current_r),
            "range_exit_giveback_frac": str(giveback_frac),
        }
    )
    if peak_r < config.min_mfe_r:
        return RangeExitDecision(False, metadata=metadata)
    if giveback_frac < config.giveback_frac:
        return RangeExitDecision(False, metadata=metadata)
    if not micro_context_available:
        return RangeExitDecision(False, metadata=metadata)

    reversal = _hostile_reversal(
        side=side,
        rf_imbalance=rf_imbalance,
        rf_close_pos=rf_close_pos,
        contra_imbalance=config.contra_imbalance,
        bad_close_pos=config.bad_close_pos,
    )
    metadata["range_exit_reversal"] = reversal
    if config.require_reversal and not reversal:
        return RangeExitDecision(False, metadata=metadata)

    metadata["range_exit_triggered"] = True
    metadata["range_exit_reason"] = RANGE_EXIT_REASON
    return RangeExitDecision(True, RANGE_EXIT_REASON, metadata)


def _hostile_reversal(
    *,
    side: Side,
    rf_imbalance: Decimal | None,
    rf_close_pos: Decimal | None,
    contra_imbalance: Decimal,
    bad_close_pos: Decimal,
) -> bool:
    if side is Side.LONG:
        hostile_imb = rf_imbalance is not None and rf_imbalance <= -contra_imbalance
        hostile_close = rf_close_pos is not None and rf_close_pos <= bad_close_pos
    elif side is Side.SHORT:
        hostile_imb = rf_imbalance is not None and rf_imbalance >= contra_imbalance
        hostile_close = rf_close_pos is not None and rf_close_pos >= Decimal("1") - bad_close_pos
    else:
        return False
    return bool(hostile_imb or hostile_close)


def _base_metadata(
    *,
    config: RangeExitConfig,
    micro_context_available: bool,
    rf_imbalance: Decimal | None,
    rf_close_pos: Decimal | None,
) -> dict[str, Any]:
    return {
        "range_exit_triggered": False,
        "range_exit_reason": "",
        "range_exit_peak_r": None,
        "range_exit_current_r": None,
        "range_exit_giveback_frac": None,
        "range_exit_reversal": False,
        "range_exit_min_mfe_r": str(config.min_mfe_r),
        "range_exit_giveback_threshold": str(config.giveback_frac),
        "range_exit_contra_imbalance": str(config.contra_imbalance),
        "range_exit_bad_close_pos": str(config.bad_close_pos),
        "rf_imbalance": None if rf_imbalance is None else str(rf_imbalance),
        "rf_close_pos": None if rf_close_pos is None else str(rf_close_pos),
        "micro_context_available": micro_context_available,
    }
