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
from src.platform.config import load_env_config
from src.platform.exchanges.errors import ExchangeApiError
from src.utils.log import get_logger

logger = get_logger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("exchange", nargs="?", choices=["okx", "binance"], help="default: first AETHER_EXCHANGES item or okx")
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    parser.add_argument("--asset", default="USDT")
    args = parser.parse_args()
    env = load_env_config()
    exchange = args.exchange or (env.get("AETHER_EXCHANGES", "okx").split(",")[0].strip() or "okx")

    account = create_account_client(exchange, symbol=args.symbol)
    execution = create_execution_client(exchange, symbol=args.symbol, validate_orders=False)
    try:
        snapshot = await fetch_platform_snapshot(account=account, execution=execution, asset=args.asset)
    except ExchangeApiError as exc:
        logger.error(
            "Private readonly smoke failed | exchange=%s symbol=%s status_code=%s payload=%s hint=%s error=%s",
            exchange,
            args.symbol,
            exc.status_code,
            exc.payload,
            "If this is OKX HTTP 403, first check API key environment mismatch, IP whitelist, and whether your server IP/User-Agent is blocked.",
            exc,
        )
        raise SystemExit(1) from exc
    logger.info(
        "Private readonly smoke ok | exchange=%s symbol=%s available=%s positions=%s open_orders=%s open_stop_orders=%s leverage=%s position_mode=%s",
        exchange,
        snapshot.symbol,
        snapshot.balance.available,
        len(snapshot.positions),
        len(snapshot.open_orders),
        len(snapshot.open_stop_orders),
        snapshot.leverage.leverage,
        snapshot.position_mode.value,
    )


if __name__ == "__main__":
    asyncio.run(main())
