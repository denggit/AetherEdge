from __future__ import annotations

from decimal import Decimal

import pytest

from src.platform.data.rest_feed import RestMarketDataFeed
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.data.ports import TradeIdAnchoredHistoryFetcher
from src.platform.exchanges.models import ExchangeName
from src.platform.markets import MarketProfile


class _ExchangeClient:
    exchange = ExchangeName.OKX

    def __init__(self) -> None:
        self.calls = []
        self.last_historical_trade_pages = 1

    async def fetch_trades(self, symbol: str, *, start_time_ms=None, end_time_ms=None, limit: int = 1000, oldest_first: bool = True, max_pages=None):
        self.calls.append((symbol, start_time_ms, end_time_ms, limit, oldest_first, max_pages))
        return [
            MarketTrade(
                exchange=ExchangeName.OKX,
                symbol=symbol,
                raw_symbol="ETH-USDT-SWAP",
                price=Decimal("2000"),
                quantity=Decimal("1"),
                side=TradeSide.BUY,
                trade_id="1",
                event_time_ms=start_time_ms,
                trade_time_ms=start_time_ms,
            )
        ]


class _AnchoredFetcher:
    def __init__(self) -> None:
        self.last_historical_trade_pages = 4

    async def fetch_trades_between_ids(self, symbol: str, **kwargs):
        return []


@pytest.mark.asyncio
async def test_rest_feed_fetch_trades_accepts_historical_trade_feed_symbol_keyword() -> None:
    client = _ExchangeClient()
    feed = RestMarketDataFeed(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        market_profile=MarketProfile(symbol="ETH-USDT-PERP", base_asset="ETH", quote_asset="USDT"),
        kline_fetcher=client,
        ticker_fetcher=client,
        historical_trade_fetcher=client,
    )

    rows = await feed.fetch_trades(symbol="ETH-USDT-PERP", start_time_ms=1, end_time_ms=2, limit=100, oldest_first=True)

    assert len(rows) == 1
    assert client.calls == [("ETH-USDT-PERP", 1, 2, 100, True, None)]
    assert feed.last_historical_trade_pages == 1


@pytest.mark.asyncio
async def test_rest_feed_propagates_normal_fetcher_page_count() -> None:
    client = _ExchangeClient()
    client.last_historical_trade_pages = 7
    feed = RestMarketDataFeed(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        market_profile=MarketProfile(
            symbol="ETH-USDT-PERP",
            base_asset="ETH",
            quote_asset="USDT",
        ),
        kline_fetcher=client,
        ticker_fetcher=client,
        historical_trade_fetcher=client,
    )

    await feed.fetch_trades()

    assert feed.last_historical_trade_pages == 7


@pytest.mark.asyncio
async def test_rest_feed_propagates_anchored_fetcher_page_count() -> None:
    client = _ExchangeClient()
    anchored: TradeIdAnchoredHistoryFetcher = _AnchoredFetcher()
    feed = RestMarketDataFeed(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        market_profile=MarketProfile(
            symbol="ETH-USDT-PERP",
            base_asset="ETH",
            quote_asset="USDT",
        ),
        kline_fetcher=client,
        ticker_fetcher=client,
        anchored_trade_fetcher=anchored,
    )

    await feed.fetch_trades_between_ids(
        newer_trade_id="20",
        older_trade_id="10",
    )

    assert anchored.last_historical_trade_pages == 4
    assert feed.last_historical_trade_pages == 4


@pytest.mark.asyncio
async def test_rest_feed_fetch_trades_rejects_wrong_bound_symbol() -> None:
    feed = RestMarketDataFeed(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        market_profile=MarketProfile(symbol="ETH-USDT-PERP", base_asset="ETH", quote_asset="USDT"),
        kline_fetcher=_ExchangeClient(),
        ticker_fetcher=_ExchangeClient(),
        historical_trade_fetcher=_ExchangeClient(),
    )

    with pytest.raises(ValueError):
        await feed.fetch_trades(symbol="BTC-USDT-PERP")
