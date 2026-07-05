from __future__ import annotations

import copy
import json
from decimal import Decimal

from strategies.eth_portfolio_v1.domain.models import (
    BarReadyContext,
    ClosedKlineContext,
    MicroDecision,
    RoutedSignal,
    Side,
)
from strategies.eth_portfolio_v1.engines.momentum_v3 import MomentumV3Engine
from strategies.eth_portfolio_v1.strategy import Strategy


def _context() -> BarReadyContext:
    engine_features = {
        "momentum": {
            "signal": 1,
            "long_signal": True,
            "short_signal": False,
            "close_gt_entry_high": True,
            "close_lt_entry_low": False,
            "vol_ok": True,
            "atr_ok": True,
            "adx": 18.3,
            "adx_long_ok": True,
            "adx_short_ok": True,
            "short_enabled": True,
            "d1_bull": True,
            "d1_bear": False,
            "ema20_gt_ema50": True,
            "ema20_lt_ema50": False,
            "close_gt_ema50": True,
            "close_lt_ema50": False,
            "close_gt_open": True,
            "close_lt_open": False,
            "risk_mult": Decimal("1.2"),
            "quality_mult": Decimal("0.5"),
            "atr": Decimal("25"),
        },
        "bull": {
            "signal": 0,
            "long_signal": False,
            "recent_pullback": True,
            "reclaim": False,
            "macro_bull_ok": True,
            "range_ok": True,
            "volume_ok": True,
            "not_extended": True,
            "quality_bucket_a": False,
            "quality_bucket_b": False,
            "risk_mult": Decimal("1"),
            "quality_mult": Decimal("0.2"),
        },
        "bear": {
            "signal": 0,
            "short_signal": False,
            "bear_permission_v3": False,
            "four_h_bear": False,
            "weekly_bear": False,
            "breakdown": False,
            "permission_continuation": False,
            "risk_mult": Decimal("1"),
            "quality_mult": Decimal("1"),
        },
    }
    return BarReadyContext(
        kline=ClosedKlineContext(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            timeframe="4h",
            open_time_ms=1,
            close_time_ms=2,
            open=Decimal("2000"),
            high=Decimal("2100"),
            low=Decimal("1950"),
            close=Decimal("2050"),
            volume=Decimal("100"),
        ),
        range_aggregate=None,
        micro=MicroDecision(
            signal_side=Side.LONG,
            context_available=False,
            aligned=False,
            contra=False,
            entry_risk_scale=Decimal("1"),
            action="NEUTRAL",
        ),
        global_risk_scale=Decimal("1"),
        routed_signal=RoutedSignal(
            side=Side.LONG,
            engine="MOMENTUM_V3",
            priority=100,
            risk_mult=Decimal("1.2"),
            quality_mult=Decimal("0.5"),
        ),
        engine_features=engine_features,
    )


def test_v1_decision_audit_contains_json_safe_engine_diagnostics() -> None:
    strategy = Strategy()
    context = _context()
    original_features = copy.deepcopy(context.engine_features)
    signal_before = MomentumV3Engine().evaluate(context)

    strategy.last_decision_audit = strategy._build_decision_audit(context, ())

    signal_after = MomentumV3Engine().evaluate(context)
    assert strategy.last_decision_audit["engine_diag"]["momentum"]["signal"] == 1
    assert strategy.last_decision_audit["engine_diag_text"].startswith("engine_diag:")
    json.dumps(strategy.last_decision_audit["engine_diag"])
    assert signal_after == signal_before
    assert context.engine_features == original_features
