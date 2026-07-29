from __future__ import annotations

import asyncio
from decimal import Decimal

from src.market_data.derived import (
    FixedTimeTradeBarBuilder,
    RangeBarBuilder,
    RangeFootprintBuilder,
    TradeFootprintBuilder,
)
from src.platform.data import create_market_data_feed
from src.platform.data.models import TradeSide
from src.platform.exchanges import ExchangeConfig, ExchangeName
from src.platform.exchanges.factory import create_exchange_client


class _HttpClient:
    def __init__(self) -> None:
        self.responses = [
            {
                "code": "0",
                "data": [
                    {
                        "tradeId": "2",
                        "px": "102",
                        "sz": "1",
                        "side": "sell",
                        "ts": "1700000002000",
                    },
                    {
                        "tradeId": "1",
                        "px": "100",
                        "sz": "2",
                        "side": "buy",
                        "ts": "1700000001000",
                    },
                ],
            }
        ]

    async def request(self, *args, **kwargs):
        return self.responses.pop(0)


def _rest_trades():
    client = create_exchange_client(
        ExchangeName.OKX,
        ExchangeConfig(),
        http_client=_HttpClient(),
    )
    feed = create_market_data_feed(
        ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        exchange_client=client,
        enable_trade_stream=False,
        enable_order_book_stream=False,
    )
    return asyncio.run(
        feed.fetch_trades(limit=2, max_pages=1, oldest_first=True)
    )


def test_okx_rest_trade_side_drives_fixed_time_volume_delta() -> None:
    trades = _rest_trades()
    assert [trade.side for trade in trades] == [
        TradeSide.BUY,
        TradeSide.SELL,
    ]

    builder = FixedTimeTradeBarBuilder(contract_value="1")
    for trade in trades:
        builder.on_trade(trade)
    [bar] = builder.drain()

    assert bar.buy_volume == Decimal("2")
    assert bar.sell_volume == Decimal("1")
    assert bar.delta_volume == Decimal("1")


def test_okx_rest_trade_side_is_recognized_by_range_and_footprint_builders() -> None:
    trades = _rest_trades()

    range_bar_builder = RangeBarBuilder(
        range_pct="0.002",
        contract_value="1",
    )
    range_footprint_builder = RangeFootprintBuilder(
        range_pct="0.002",
        contract_value="1",
    )
    trade_footprint_builder = TradeFootprintBuilder(contract_value="1")

    range_bars = ()
    range_footprints = ()
    for trade in trades:
        range_bars = range_bar_builder.on_trade(trade)
        range_footprints = range_footprint_builder.on_trade(trade)
        trade_footprint_builder.on_trade(trade)
    [trade_footprint] = trade_footprint_builder.drain()

    assert range_bars[0].buy_notional == Decimal("200")
    assert range_bars[0].sell_notional == Decimal("102")
    assert range_footprints[0].fp_delta_pressure > Decimal("0")
    assert trade_footprint.delta_notional == Decimal("98")
