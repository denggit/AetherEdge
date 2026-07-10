from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    MultiExchangeOrderCoordinator,
    OrderIntent,
    OrderIntentStatus,
    PositionPlan,
    PositionPlanStatus,
    SqliteOrderJournalStore,
    SqlitePositionPlanStore,
)
from src.platform import ExchangeName, InstrumentRule, get_market_profile
from src.signals import SignalAction, TradeSignal


class _NoWriteBinanceClient:
    exchange = ExchangeName.BINANCE
    symbol = "ETH-USDT-PERP"

    def __init__(self) -> None:
        self.place_order_calls = 0

    @property
    def market_profile(self):
        return get_market_profile(self.symbol)

    async def fetch_instrument_rule(self):
        return InstrumentRule(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol="ETHUSDT",
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            min_notional=Decimal("5"),
        )

    async def place_order(self, request):
        self.place_order_calls += 1
        raise AssertionError("non-executable recovery dust reached exchange write")


@pytest.mark.asyncio
async def test_coordinator_skips_non_executable_recovery_topup_without_failure(
    tmp_path,
) -> None:
    journal = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    plans = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _seed_plan(plans)
    client = _NoWriteBinanceClient()
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=journal,
        position_plan_store=plans,
    )
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.00095841427016089856221634149"),
        metadata={
            "target_exchanges": ["binance"],
            "execution_purpose": "follower_recovery_topup",
            "position_id": "p-dust-guard",
            "reference_price": "1800",
            "raw_target_qty": "0.06195841427016089856221634149",
            "confirmed_filled_qty": "0.061",
            "actual_exchange_qty": "0.061",
            "raw_delta": "0.00095841427016089856221634149",
        },
    )
    intent = OrderIntent(
        intent_id="i-dust-guard",
        strategy_id="eth_portfolio_v1",
        signal=signal,
        target_exchanges=(ExchangeName.BINANCE,),
    )

    results = await coordinator.execute(intent)

    assert client.place_order_calls == 0
    assert len(results) == 1
    assert results[0].ok is True
    assert results[0].raw["execution_outcome"] == (
        "skipped_non_executable_quantity"
    )
    assert results[0].raw["normalized_base_quantity"] == "0"
    assert journal.get_intent(intent.intent_id).status is OrderIntentStatus.RECOVERED
    follower = {
        leg.exchange: leg for leg in plans.get_legs("p-dust-guard")
    }[ExchangeName.BINANCE]
    assert follower.sync_status is LegSyncStatus.SYNCED
    assert follower.metadata["execution_outcome"] == (
        "skipped_non_executable_quantity"
    )
    assert follower.metadata["reason"] == "non_executable_rounding_dust"


def _seed_plan(store: SqlitePositionPlanStore) -> None:
    store.upsert_position(
        PositionPlan(
            position_id="p-dust-guard",
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1738.25"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.07"),
            master_filled_qty_base=Decimal("0.07"),
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id="p-dust-guard",
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.06195841427016089856221634149"),
            filled_qty_base=Decimal("0.061"),
            sync_status=LegSyncStatus.TOPUP_FAILED,
        )
    )
