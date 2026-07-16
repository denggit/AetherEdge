from __future__ import annotations

from typing import Any, Mapping

from src.platform.account.events import AccountEvent
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.snapshot import PlatformSnapshot
from src.signals import TradeSignal


class Strategy:
    """Empty plugin used to verify the app runner wiring.

    It intentionally emits no signal and contains no trading logic.
    """

    def runtime_requirements(self) -> Mapping[str, Any]:
        return {
            "capabilities": {
                "manifest_version": 1,
                "strategy_id": "empty_strategy",
                "position_snapshots": False,
                "recovery_status": False,
                "market_features": False,
                "range_speed_history": False,
                "startup_preview": False,
                "pending_work": False,
            },
            "account_state": {
                "startup_snapshot_enabled": False,
                "poll_enabled": False,
                "post_order_sync_enabled": False,
            },
            "order_state": {
                "post_submit_sync_enabled": False,
                "poll_when_position_enabled": False,
                "sync_open_orders": False,
                "sync_open_stop_orders": False,
                "sync_position": False,
            },
        }

    def strategy_identity(self) -> str:
        return "empty_strategy"

    async def on_start(self, snapshot: PlatformSnapshot) -> list[TradeSignal]:
        return []

    async def on_kline(self, kline: MarketKline) -> list[TradeSignal]:
        return []

    async def on_ticker(self, ticker: MarketTicker) -> list[TradeSignal]:
        return []

    async def on_trade(self, trade: MarketTrade) -> list[TradeSignal]:
        return []

    async def on_order_book(self, order_book: MarketOrderBook) -> list[TradeSignal]:
        return []

    async def on_account_event(self, event: AccountEvent) -> list[TradeSignal]:
        return []
