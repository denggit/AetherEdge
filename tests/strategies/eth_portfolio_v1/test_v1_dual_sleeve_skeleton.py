from __future__ import annotations

from decimal import Decimal

import pytest

from src.strategy.positions import StrategyPositionStatus
from strategies.eth_portfolio_v1.domain.models import Side
from strategies.eth_portfolio_v1.domain.sleeve_registry import SleeveRegistry
from strategies.eth_portfolio_v1.domain.sleeves import (
    DisabledSleeve,
    LF_SLEEVE_ID,
    MF_RESERVED_SLEEVE_ID,
    SleeveId,
)
from strategies.eth_portfolio_v1.strategy import Strategy


def test_v1_registry_preserves_registration_order_and_supports_lookup() -> None:
    strategy = Strategy()

    assert tuple(sleeve.sleeve_id for sleeve in strategy.sleeves) == ("lf", "mf")
    assert strategy.sleeves.get(LF_SLEEVE_ID) is strategy.lf_sleeve
    assert strategy.sleeves.get(MF_RESERVED_SLEEVE_ID) is strategy.mf_sleeve
    assert strategy.sleeves.get("hf") is None
    assert strategy.sleeves.require(LF_SLEEVE_ID) is strategy.lf_sleeve


def test_registry_rejects_duplicate_sleeve_ids() -> None:
    with pytest.raises(ValueError, match="duplicate sleeve_id: duplicate"):
        SleeveRegistry(
            (
                DisabledSleeve("duplicate"),
                DisabledSleeve("duplicate"),
            )
        )


def test_registry_accepts_future_placeholders_without_type_changes() -> None:
    mf = DisabledSleeve("mf")
    hf = DisabledSleeve("hf")

    registry = SleeveRegistry((mf, hf))

    assert tuple(sleeve.sleeve_id for sleeve in registry) == ("mf", "hf")
    assert registry.get("mf") is mf
    assert registry.get("hf") is hf
    assert registry.position_snapshots() == ()
    assert SleeveId("mf_low_sweep").value == "mf_low_sweep"


def test_strategy_position_snapshots_aggregate_registry_sleeves() -> None:
    strategy = Strategy()
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2500"),
        qty=Decimal("0.25"),
        stop_price=Decimal("2400"),
        entry_engine="MOMENTUM_V3",
        position_id="lf-position-stays-stable",
    )

    snapshots = strategy.position_snapshots()

    assert snapshots == strategy.sleeves.position_snapshots()
    assert len(snapshots) == 1
    assert snapshots[0].sleeve_id == LF_SLEEVE_ID
    assert snapshots[0].position_id == "lf-position-stays-stable"
    assert snapshots[0].status is StrategyPositionStatus.ACTIVE
    assert not any(
        snapshot.sleeve_id == MF_RESERVED_SLEEVE_ID
        for snapshot in snapshots
    )
