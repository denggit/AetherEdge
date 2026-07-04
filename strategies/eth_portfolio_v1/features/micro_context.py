from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from strategies.eth_portfolio_v1.domain.models import MicroDecision, RangeAggregateContext, Side


@dataclass(frozen=True)
class MicroContextConfig:
    mode: str = "soft"
    min_range_bars: int = 5
    contra_imbalance: Decimal = Decimal("0.05")
    aligned_imbalance: Decimal = Decimal("0.05")
    bad_close_pos: Decimal = Decimal("0.35")
    good_close_pos: Decimal = Decimal("0.65")
    contra_risk_scale: Decimal = Decimal("0.50")
    not_aligned_risk_scale: Decimal = Decimal("0.50")


class MicroContextEngine:
    """V8 range-bar micro confirmation layer.

    This engine does not create direction. It only converts the current 4H range
    aggregate into an entry-risk scale for an LF signal side.
    """

    def __init__(self, config: MicroContextConfig | None = None) -> None:
        self.config = config or MicroContextConfig()

    def evaluate(self, *, signal_side: Side | int, aggregate: RangeAggregateContext | None) -> MicroDecision:
        side = signal_side if isinstance(signal_side, Side) else Side(signal_side)
        mode = self.config.mode.lower()
        if side is Side.FLAT:
            return MicroDecision(signal_side=side, context_available=False, aligned=False, contra=False, entry_risk_scale=Decimal("1"), action="NO_SIGNAL")
        if mode == "off":
            return MicroDecision(signal_side=side, context_available=False, aligned=False, contra=False, entry_risk_scale=Decimal("1"), action="OFF")
        coverage_status = (
            "COMPLETE"
            if aggregate is None
            else str(aggregate.coverage_status).strip().upper()
        )
        if coverage_status in {"COLD_START_PARTIAL", "RECOVERED_INCOMPLETE"}:
            return MicroDecision(
                signal_side=side,
                context_available=False,
                aligned=False,
                contra=False,
                entry_risk_scale=Decimal("1"),
                action="NEUTRAL",
                metadata={"range_coverage_status": coverage_status},
            )
        if aggregate is None or aggregate.bar_count < self.config.min_range_bars:
            return MicroDecision(signal_side=side, context_available=False, aligned=False, contra=False, entry_risk_scale=Decimal("1"), action="NEUTRAL")

        imbalance = aggregate.imbalance
        close_pos = aggregate.close_pos
        long_contra = side is Side.LONG and imbalance <= -abs(self.config.contra_imbalance) and close_pos <= self.config.bad_close_pos
        short_contra = side is Side.SHORT and imbalance >= abs(self.config.contra_imbalance) and close_pos >= Decimal("1") - self.config.bad_close_pos
        long_aligned = side is Side.LONG and imbalance >= abs(self.config.aligned_imbalance) and close_pos >= self.config.good_close_pos
        short_aligned = side is Side.SHORT and imbalance <= -abs(self.config.aligned_imbalance) and close_pos <= Decimal("1") - self.config.good_close_pos

        contra = bool(long_contra or short_contra)
        aligned = bool(long_aligned or short_aligned)
        if mode == "strict":
            if aligned:
                risk_scale = Decimal("1")
                action = "NEUTRAL"
            else:
                risk_scale = Decimal("0")
                action = "NOT_ALIGNED_BLOCKED"
        elif mode == "soft":
            if contra:
                risk_scale = self.config.contra_risk_scale
                action = "CONTRA_RISK_REDUCED"
            elif aligned:
                risk_scale = Decimal("1")
                action = "NEUTRAL"
            else:
                risk_scale = self.config.not_aligned_risk_scale
                action = "NOT_ALIGNED_RISK_REDUCED"
        else:
            raise ValueError(f"Unsupported micro context mode: {self.config.mode}")

        if (
            coverage_status == "RECOVERED_DEGRADED_MINOR"
            and action
            not in {"NOT_ALIGNED_RISK_REDUCED", "CONTRA_RISK_REDUCED"}
        ):
            aligned = False
            contra = False
            risk_scale = Decimal("1")
            action = "NEUTRAL"

        return MicroDecision(
            signal_side=side,
            context_available=True,
            aligned=aligned,
            contra=contra,
            entry_risk_scale=risk_scale,
            action=action,
            metadata={
                "rf_bar_count": aggregate.bar_count,
                "rf_imbalance": str(aggregate.imbalance),
                "rf_close_pos": str(aggregate.close_pos),
                "rf_taker_buy_ratio": str(aggregate.taker_buy_ratio),
                "rf_micro_return_pct": str(aggregate.micro_return_pct),
                "range_coverage_status": coverage_status,
            },
        )
