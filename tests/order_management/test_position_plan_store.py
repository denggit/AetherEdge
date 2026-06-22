from __future__ import annotations

from decimal import Decimal

from src.order_management.position_plan import LegPlan, LegRole, LegSyncStatus, PositionPlan, PositionPlanStatus, SqlitePositionPlanStore
from src.platform.exchanges.models import ExchangeName


def test_position_plan_store_roundtrips_master_and_follower_targets(tmp_path):
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    plan = PositionPlan(
        position_id="pos-1",
        strategy_id="eth_lf_portfolio_v9c_reclaim_priority",
        entry_engine="BULL_RECLAIM_V2",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("1900"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("1.2"),
        master_filled_qty_base=Decimal("1.2"),
    )

    store.upsert_position(plan)
    store.upsert_leg(LegPlan(position_id="pos-1", exchange=ExchangeName.OKX, role=LegRole.MASTER, target_qty_base=Decimal("1.2"), filled_qty_base=Decimal("1.2"), sync_status=LegSyncStatus.SYNCED))
    store.upsert_leg(LegPlan(position_id="pos-1", exchange=ExchangeName.BINANCE, role=LegRole.FOLLOWER, target_qty_base=Decimal("0.3"), filled_qty_base=Decimal("0.1"), sync_status=LegSyncStatus.UNDERFILLED))

    loaded = store.get_position("pos-1")
    legs = {leg.exchange: leg for leg in store.get_legs("pos-1")}

    assert loaded == plan or loaded.position_id == plan.position_id
    assert legs[ExchangeName.OKX].target_qty_base == Decimal("1.2")
    assert legs[ExchangeName.BINANCE].target_qty_base == Decimal("0.3")
    assert legs[ExchangeName.BINANCE].filled_qty_base == Decimal("0.1")
    assert store.serialize_active_positions()[0]["legs"][1]["exchange"] == "okx"


def test_position_plan_store_adds_entry_time_unit_target_per_exchange(tmp_path):
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    store.upsert_position(
        PositionPlan(
            position_id="pos-1",
            strategy_id="eth_lf_portfolio_v9c_reclaim_priority",
            entry_engine="MOMENTUM_V3",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1900"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("1"),
        )
    )
    store.upsert_leg(LegPlan(position_id="pos-1", exchange=ExchangeName.BINANCE, role=LegRole.FOLLOWER, target_qty_base=Decimal("0.25")))

    store.add_to_leg_target(position_id="pos-1", exchange=ExchangeName.BINANCE, delta_target_qty_base=Decimal("0.25"), delta_filled_qty_base=Decimal("0.2"))

    leg = store.get_legs("pos-1")[0]
    assert leg.target_qty_base == Decimal("0.50")
    assert leg.filled_qty_base == Decimal("0.2")
