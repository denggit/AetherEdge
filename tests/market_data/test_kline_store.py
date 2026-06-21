from __future__ import annotations

from decimal import Decimal

from src.market_data.models import TimeRange
from src.market_data.storage import SqliteKlineStore
from src.platform.data.models import MarketDataSource, MarketKline
from src.platform.exchanges.models import ExchangeName


def _kline(open_time_ms: int, *, symbol: str = "ETH-USDT-PERP", interval: str = "4H", closed: bool = True) -> MarketKline:
    return MarketKline(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol="ETH-USDT-SWAP",
        interval=interval,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 4 * 60 * 60_000 - 1,
        open=Decimal("1000"),
        high=Decimal("1010"),
        low=Decimal("990"),
        close=Decimal("1005"),
        volume=Decimal("12.5"),
        quote_volume=Decimal("12500"),
        is_closed=closed,
        source=MarketDataSource.REST,
        raw={"ts": open_time_ms},
    )


def test_sqlite_kline_store_saves_loads_and_upserts(tmp_path):
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    assert store.save([_kline(0), _kline(14_400_000)]) == 2
    assert store.save([_kline(0)]) == 1

    rows = store.load(symbol="ETH-USDT-PERP", interval="4H", time_range=TimeRange(0, 14_400_000))

    assert [row.open_time_ms for row in rows] == [0, 14_400_000]
    assert rows[0].exchange is ExchangeName.OKX
    assert rows[0].quote_volume == Decimal("12500")
    assert rows[0].raw == {"ts": 0}


def test_sqlite_kline_store_latest_time_ignores_open_bar(tmp_path):
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    store.save([_kline(0), _kline(14_400_000, closed=False)])

    assert store.latest_time_ms(symbol="ETH-USDT-PERP", interval="4H") == 0
