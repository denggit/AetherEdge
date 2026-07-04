from __future__ import annotations

from decimal import Decimal

from src.strategy.positions import StrategyPositionStatus
from strategies.eth_portfolio_v1.domain.models import Side
from strategies.eth_portfolio_v1.domain.sleeves import SleeveId
from strategies.eth_portfolio_v1.strategy import Strategy


def test_v1_composes_lf_and_disabled_mf_state_boundaries() -> None:
    strategy = Strategy()

    assert tuple(sleeve.sleeve_id for sleeve in strategy.sleeves) == (
        SleeveId.LF,
        SleeveId.MF,
    )
    assert strategy.sleeves.lf is strategy.lf_sleeve
    assert strategy.sleeves.mf is strategy.mf_sleeve
    assert strategy.lf_sleeve.enabled is True
    assert strategy.lf_sleeve.position is strategy.position
    assert strategy.mf_sleeve.enabled is False


def test_disabled_mf_does_not_change_active_lf_snapshot() -> None:
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

    assert len(snapshots) == 1
    assert snapshots[0].sleeve_id == SleeveId.LF.value
    assert snapshots[0].position_id == "lf-position-stays-stable"
    assert snapshots[0].status is StrategyPositionStatus.ACTIVE
    assert not any(snapshot.sleeve_id == SleeveId.MF.value for snapshot in snapshots)
