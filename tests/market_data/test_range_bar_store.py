from __future__ import annotations

from decimal import Decimal

from src.market_data.models import RangeBar, TimeRange
from src.market_data.storage import SqliteRangeBarStore


def _bar(bar_id: int, end_time_ms: int) -> RangeBar:
    return RangeBar(
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        bar_id=bar_id,
        start_time_ms=end_time_ms - 1000,
        end_time_ms=end_time_ms,
        open=Decimal("1000"),
        high=Decimal("1002"),
        low=Decimal("999"),
        close=Decimal("1001"),
        volume=Decimal("10"),
        buy_notional=Decimal("100"),
        sell_notional=Decimal("50"),
        trade_count=2,
    )


def test_sqlite_range_bar_store_saves_loads_and_upserts(tmp_path):
    store = SqliteRangeBarStore(tmp_path / "market.sqlite3")
    assert store.save([_bar(1, 1000), _bar(2, 2000)]) == 2
    assert store.save([_bar(1, 1000)]) == 1

    rows = store.load(symbol="ETH-USDT-PERP", range_pct="0.0020", time_range=TimeRange(0, 2000))

    assert [row.bar_id for row in rows] == [1, 2]
    assert rows[0].notional == Decimal("150")
    assert store.latest_end_time_ms(symbol="ETH-USDT-PERP", range_pct="0.002") == 2000


def test_repair_replace_does_not_overwrite_later_live_bar_id(tmp_path):
    store = SqliteRangeBarStore(tmp_path / "market.sqlite3")
    repaired = _bar(2, 2_000)
    later_live = _bar(2, 5_000)
    store.save([later_live])

    assert (
        store.replace_range_for_repair(
            symbol="ETH-USDT-PERP",
            range_pct="0.002",
            time_range=TimeRange(1_000, 3_000),
            rows=[repaired],
        )
        == 1
    )

    rows = store.load(
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        time_range=TimeRange(1_000, 6_000),
    )
    assert [row.end_time_ms for row in rows] == [2_000, 5_000]
    assert rows[0].bar_id >= 500_000
    assert rows[1].bar_id == 2
