"""Read-only private API smoke test. It does not place, amend, or cancel orders.

Usage:
  PYTHONPATH=. python tools/smoke_private_readonly.py okx
  PYTHONPATH=. python tools/smoke_private_readonly.py binance
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.platform import create_account_client, create_execution_client, fetch_platform_snapshot


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("exchange", choices=["okx", "binance"])
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    parser.add_argument("--asset", default="USDT")
    args = parser.parse_args()

    account = create_account_client(args.exchange, symbol=args.symbol)
    execution = create_execution_client(args.exchange, symbol=args.symbol, validate_orders=False)
    snapshot = await fetch_platform_snapshot(account=account, execution=execution, asset=args.asset)
    print(
        {
            "exchange": args.exchange,
            "symbol": snapshot.symbol,
            "available": str(snapshot.balance.available),
            "positions": len(snapshot.positions),
            "open_orders": len(snapshot.open_orders),
            "open_stop_orders": len(snapshot.open_stop_orders),
            "leverage": str(snapshot.leverage.leverage),
            "position_mode": snapshot.position_mode.value,
        }
    )


if __name__ == "__main__":
    asyncio.run(main())
