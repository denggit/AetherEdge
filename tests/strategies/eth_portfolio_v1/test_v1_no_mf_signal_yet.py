from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from src.signals import SignalAction
from strategies.eth_portfolio_v1.domain.models import Side, V8DecisionType, V8TradeDecision
from strategies.eth_portfolio_v1.domain.sleeves import (
    LF_SLEEVE_ID,
    MF_RESERVED_SLEEVE_ID,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState
from strategies.eth_portfolio_v1.strategy import Strategy


def test_mf_sleeve_is_enabled_by_default() -> None:
    strategy = Strategy()
    mf = strategy.mf_sleeve

    assert isinstance(mf, MfSleeveState)
    assert mf.enabled is True
    assert mf.sleeve_id == MF_RESERVED_SLEEVE_ID
    assert mf.position_snapshots() == ()
    assert strategy.config.mf.exit_variant == "time48"


def test_portfolio_v1_default_config_enables_mf() -> None:
    config_path = (
        Path(__file__).resolve().parents[3]
        / "strategies"
        / "eth_portfolio_v1"
        / "config.json"
    )
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload["mf"]["enabled"] is True


def test_lf_entry_signal_metadata_contains_v1_sleeve_scope() -> None:
    strategy = Strategy()
    signal = strategy.signal_mapper.map_decision(
        V8TradeDecision(
            decision_type=V8DecisionType.OPEN,
            side=Side.LONG,
            symbol=strategy.config.symbol,
            quantity=Decimal("0.20"),
            reason="TEST_LF_ENTRY",
            metadata={
                "position_id": "existing-lf-position-id",
                "existing_key": "preserved",
            },
        )
    )[0]

    assert signal.action is SignalAction.OPEN_LONG
    assert signal.quantity == Decimal("0.20")
    assert signal.metadata["strategy_id"] == "eth_portfolio_v1"
    assert signal.metadata["sleeve_id"] == LF_SLEEVE_ID
    assert signal.metadata["position_id"] == "existing-lf-position-id"
    assert signal.metadata["existing_key"] == "preserved"


def test_lf_scoped_stop_signal_keeps_position_scope_and_sleeve_metadata() -> None:
    strategy = Strategy()
    strategy.position.open_master(
        side=Side.SHORT,
        entry_time_ms=1,
        avg_entry=Decimal("2500"),
        qty=Decimal("0.30"),
        stop_price=Decimal("2600"),
        entry_engine="MOMENTUM_V3",
        position_id="existing-lf-short-position",
    )

    signal = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("0.30"),
        stop_price=Decimal("2550"),
        reason="TEST_LF_STOP",
        bar_close_time_ms=2,
    )[0]

    assert signal.action is SignalAction.PLACE_STOP_LOSS_SHORT
    assert signal.quantity == Decimal("0.30")
    assert signal.trigger_price == Decimal("2550")
    assert signal.metadata["strategy_id"] == "eth_portfolio_v1"
    assert signal.metadata["sleeve_id"] == LF_SLEEVE_ID
    assert signal.metadata["position_id"] == "existing-lf-short-position"
