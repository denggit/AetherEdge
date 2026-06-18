import asyncio
from decimal import Decimal

from src.platform.data import MarketDataSource, MarketEventType, RestMarketDataFeed, create_market_data_feed
from src.platform.exchanges.models import ExchangeName, Kline, Ticker


class FakeExchangeClient:
    exchange = ExchangeName.OKX

    def __init__(self):
        self.kline_calls = []
        self.ticker_calls = []

    async def get_server_time_ms(self):  # pragma: no cover - not used by feed
        return 1

    async def fetch_klines(self, symbol, *, interval, limit=100, start_time_ms=None, end_time_ms=None, oldest_first=False):
        self.kline_calls.append(
            {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
                "start_time_ms": start_time_ms,
                "end_time_ms": end_time_ms,
                "oldest_first": oldest_first,
            }
        )
        return [
            Kline(
                exchange=ExchangeName.OKX,
                symbol=symbol,
                raw_symbol="ETH-USDT-SWAP",
                interval=interval,
                open_time_ms=1710000000000,
                close_time_ms=1710000059999,
                open=Decimal("3000"),
                high=Decimal("3010"),
                low=Decimal("2990"),
                close=Decimal("3005"),
                volume=Decimal("12"),
                quote_volume=Decimal("36000"),
                raw={"source": "fake"},
            )
        ]

    async def fetch_ticker(self, symbol):
        self.ticker_calls.append(symbol)
        return Ticker(
            exchange=ExchangeName.OKX,
            symbol=symbol,
            raw_symbol="ETH-USDT-SWAP",
            price=Decimal("3005.5"),
            time_ms=1710000001000,
            raw={"source": "fake"},
        )

    async def fetch_balance(self, asset="USDT"):  # pragma: no cover - boundary only
        raise AssertionError("data_feed must not use private balance API")

    async def fetch_positions(self, symbol=None):  # pragma: no cover - boundary only
        raise AssertionError("data_feed must not use private position API")

    async def place_order(self, request):  # pragma: no cover - boundary only
        raise AssertionError("data_feed must not place orders")

    async def cancel_order(self, request):  # pragma: no cover - boundary only
        raise AssertionError("data_feed must not cancel orders")


def test_rest_feed_fetches_klines_through_exchange_public_method():
    exchange_client = FakeExchangeClient()
    feed = RestMarketDataFeed(exchange_client=exchange_client, symbol="ETH-USDT-PERP")

    rows = asyncio.run(feed.fetch_klines(interval="1m", limit=1, start_time_ms=1, end_time_ms=2))

    assert exchange_client.kline_calls == [
        {
            "symbol": "ETH-USDT-PERP",
            "interval": "1m",
            "limit": 1,
            "start_time_ms": 1,
            "end_time_ms": 2,
            "oldest_first": False,
        }
    ]
    assert rows[0].event_type is MarketEventType.KLINE
    assert rows[0].source is MarketDataSource.REST
    assert rows[0].close == Decimal("3005")
    assert rows[0].raw_symbol == "ETH-USDT-SWAP"


def test_rest_feed_fetches_ticker_through_exchange_public_method():
    exchange_client = FakeExchangeClient()
    feed = RestMarketDataFeed(exchange_client=exchange_client, symbol="ETH-USDT-PERP")

    ticker = asyncio.run(feed.fetch_ticker())

    assert exchange_client.ticker_calls == ["ETH-USDT-PERP"]
    assert ticker.event_type is MarketEventType.TICKER
    assert ticker.source is MarketDataSource.REST
    assert ticker.price == Decimal("3005.5")


def test_market_data_feed_factory_can_reuse_existing_exchange_client():
    exchange_client = FakeExchangeClient()

    feed = create_market_data_feed(
        "okx",
        symbol="ETH-USDT-PERP",
        exchange_client=exchange_client,
    )

    assert feed.exchange is ExchangeName.OKX
    assert feed.symbol == "ETH-USDT-PERP"
