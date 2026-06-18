"""Read-only public API smoke test.

Usage:
  PYTHONPATH=. python tools/smoke_public.py okx
  PYTHONPATH=. python tools/smoke_public.py binance
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.platform import create_market_data_feed


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("exchange", choices=["okx", "binance"])
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    args = parser.parse_args()

    data = create_market_data_feed(args.exchange, symbol=args.symbol, enable_trade_stream=False, enable_order_book_stream=False)
    ticker = await data.fetch_ticker()
    klines = await data.fetch_klines(interval="1m", limit=2)
    print({"exchange": args.exchange, "symbol": args.symbol, "ticker": str(ticker.price), "klines": len(klines)})


if __name__ == "__main__":
    asyncio.run(main())
