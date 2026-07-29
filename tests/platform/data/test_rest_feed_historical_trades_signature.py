from __future__ import annotations

from decimal import Decimal

import pytest

from src.platform.data.rest_feed import RestMarketDataFeed
from src.platform.exchanges.models import ExchangeName, OrderSide, Trade
from src.platform.markets import MarketProfile


class _ExchangeClient:
    exchange = ExchangeName.OKX

    def __init__(self) -> None:
        self.calls = []

    async def fetch_trades(self, symbol: str, *, start_time_ms=None, end_time_ms=None, limit: int = 1000, oldest_first: bool = True):
        self.calls.append((symbol, start_time_ms, end_time_ms, limit, oldest_first))
        return [
            Trade(
                exchange=ExchangeName.OKX,
                symbol=symbol,
                raw_symbol="ETH-USDT-SWAP",
                price=Decimal("2000"),
                quantity=Decimal("1"),
                side=OrderSide.BUY,
                trade_id="1",
                event_time_ms=start_time_ms,
                trade_time_ms=start_time_ms,
            )
        ]


@pytest.mark.asyncio
async def test_rest_feed_fetch_trades_accepts_historical_trade_feed_symbol_keyword() -> None:
    client = _ExchangeClient()
    feed = RestMarketDataFeed(
        exchange_client=client,
        symbol="ETH-USDT-PERP",
        market_profile=MarketProfile(symbol="ETH-USDT-PERP", base_asset="ETH", quote_asset="USDT"),
    )

    rows = await feed.fetch_trades(symbol="ETH-USDT-PERP", start_time_ms=1, end_time_ms=2, limit=100, oldest_first=True)

    assert len(rows) == 1
    assert client.calls == [("ETH-USDT-PERP", 1, 2, 100, True)]


@pytest.mark.asyncio
async def test_rest_feed_fetch_trades_rejects_wrong_bound_symbol() -> None:
    feed = RestMarketDataFeed(
        exchange_client=_ExchangeClient(),
        symbol="ETH-USDT-PERP",
        market_profile=MarketProfile(symbol="ETH-USDT-PERP", base_asset="ETH", quote_asset="USDT"),
    )

    with pytest.raises(ValueError):
        await feed.fetch_trades(symbol="BTC-USDT-PERP")
