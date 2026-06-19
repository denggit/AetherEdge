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
from src.platform.config import load_env_config
from src.platform.exchanges.errors import ExchangeApiError


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("exchange", nargs="?", choices=["okx", "binance"], help="default: AETHER_DATA_EXCHANGE or okx")
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    args = parser.parse_args()
    env = load_env_config()
    exchange = args.exchange or env.get("AETHER_DATA_EXCHANGE") or "okx"

    data = create_market_data_feed(exchange, symbol=args.symbol, enable_trade_stream=False, enable_order_book_stream=False)
    try:
        ticker = await data.fetch_ticker()
        klines = await data.fetch_klines(interval="1m", limit=2)
    except ExchangeApiError as exc:
        print(
            {
                "exchange": exchange,
                "symbol": args.symbol,
                "error": str(exc),
                "status_code": exc.status_code,
                "payload": exc.payload,
                "hint": "If OKX public API returns HTTP 403 while Binance works, test curl/requests from the same server. It is usually HTTP client fingerprint or server IP/geolocation blocking, not API key.",
            }
        )
        raise SystemExit(1) from exc
    print({"exchange": exchange, "symbol": args.symbol, "ticker": str(ticker.price), "klines": len(klines)})


if __name__ == "__main__":
    asyncio.run(main())
