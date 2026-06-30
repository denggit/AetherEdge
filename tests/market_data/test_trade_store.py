from __future__ import annotations

from decimal import Decimal
import sqlite3

from src.market_data.models import TimeRange
from src.market_data.storage import SqliteTradeStore
from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName


def _trade(trade_id: str | None, time_ms: int, *, side: TradeSide = TradeSide.BUY) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("2000.5"),
        quantity=Decimal("3"),
        side=side,
        trade_id=trade_id,
        event_time_ms=time_ms,
        trade_time_ms=time_ms,
        source=MarketDataSource.WEBSOCKET,
        raw={"id": trade_id or "synthetic"},
    )


def test_sqlite_trade_store_saves_loads_and_upserts_by_trade_id(tmp_path):
    store = SqliteTradeStore(
        tmp_path / "market.sqlite3",
        save_raw_trades=True,
    )
    assert store.save([_trade("1", 1000), _trade("2", 2000, side=TradeSide.SELL)]) == 2
    assert store.save([_trade("1", 1000)]) == 1

    rows = store.load(symbol="ETH-USDT-PERP", time_range=TimeRange(0, 2000))

    assert [row.trade_id for row in rows] == ["1", "2"]
    assert rows[0].price == Decimal("2000.5")
    assert rows[1].side is TradeSide.SELL
    assert store.latest_time_ms(symbol="ETH-USDT-PERP") == 2000


def test_sqlite_trade_store_supports_trades_without_exchange_trade_id(tmp_path):
    store = SqliteTradeStore(
        tmp_path / "market.sqlite3",
        save_raw_trades=True,
    )
    store.save([_trade(None, 3000)])

    rows = store.load(symbol="ETH-USDT-PERP", time_range=TimeRange(3000, 3000))

    assert len(rows) == 1
    assert rows[0].trade_id is None


def test_sqlite_trade_store_defaults_to_write_protected(tmp_path):
    db_path = tmp_path / "market.sqlite3"
    store = SqliteTradeStore(db_path)

    assert store.save([_trade("1", 1000)]) == 0
    assert store.save_trades([_trade("2", 2000)]) == 0
    store.mark_coverage(
        symbol="ETH-USDT-PERP",
        time_range=TimeRange(1000, 2000),
        source="historical_current_bucket",
    )

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM trade_coverage").fetchone()[0] == 0
