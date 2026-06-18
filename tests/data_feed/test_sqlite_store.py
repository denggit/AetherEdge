import asyncio
from decimal import Decimal

from src.platform.data import MarketDataSource, MarketEventType, SqliteMarketDataStore, create_market_data_feed
from src.platform.exchanges.models import ExchangeName, Kline


class FakeExchangeClient:
    exchange = ExchangeName.OKX

    def __init__(self):
        self.calls = 0

    async def fetch_klines(self, symbol, *, interval, limit=100, start_time_ms=None, end_time_ms=None, oldest_first=False):
        self.calls += 1
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
                raw={"source": "remote"},
            )
        ]

    async def fetch_ticker(self, symbol):  # pragma: no cover
        raise AssertionError("not used")


def test_sqlite_store_saves_and_loads_klines(tmp_path):
    store = SqliteMarketDataStore(tmp_path / "market.sqlite3")
    feed = create_market_data_feed(
        "okx",
        symbol="ETH-USDT-PERP",
        exchange_client=FakeExchangeClient(),
        store=store,
        enable_trade_stream=False,
        enable_order_book_stream=False,
    )

    first = asyncio.run(feed.fetch_klines(interval="1m", limit=1))
    cached = asyncio.run(feed.fetch_klines(interval="1m", limit=1))

    assert first[0].event_type is MarketEventType.KLINE
    assert cached[0].source is MarketDataSource.REST
    assert cached[0].close == Decimal("3005")


from src.platform.data import MarketOrderBook, MarketTrade, OrderBookLevel, TradeSide


def test_sqlite_store_saves_and_loads_trades_and_orderbooks(tmp_path):
    store = SqliteMarketDataStore(tmp_path / "market.sqlite3")
    trade = MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        trade_id="t1",
        event_time_ms=1710000000000,
        trade_time_ms=1710000000000,
        price=Decimal("3000"),
        quantity=Decimal("0.1"),
        side=TradeSide.BUY,
    )
    order_book = MarketOrderBook(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        event_time_ms=1710000000000,
        bids=[OrderBookLevel(price=Decimal("2999"), quantity=Decimal("1"))],
        asks=[OrderBookLevel(price=Decimal("3001"), quantity=Decimal("2"))],
    )

    store.save_trade(trade)
    store.save_order_book(order_book)

    trades = store.load_trades(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", limit=10)
    books = store.load_order_books(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", limit=10)

    assert trades[0].price == Decimal("3000")
    assert trades[0].side is TradeSide.BUY
    assert books[0].bids[0].price == Decimal("2999")
    assert books[0].asks[0].quantity == Decimal("2")
