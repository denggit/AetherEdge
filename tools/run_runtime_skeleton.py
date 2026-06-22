"""Run the platform runtime skeleton without strategy logic.

This collects a startup snapshot, stores it, then optionally consumes private
account events into the state store.

Examples:
  PYTHONPATH=. python tools/run_runtime_skeleton.py okx --no-event-stream
  PYTHONPATH=. python tools/run_runtime_skeleton.py binance --max-events 10
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.platform import ExchangeName, RuntimeConfig, PlatformRuntime, build_runtime_context
from src.utils.log import get_logger

logger = get_logger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("exchange", choices=["okx", "binance"])
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    parser.add_argument("--asset", default="USDT")
    parser.add_argument("--state-db", default="data/state/aether_state.sqlite3")
    parser.add_argument("--no-event-stream", action="store_true")
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args()

    config = RuntimeConfig(
        exchange=ExchangeName(args.exchange),
        symbol=args.symbol,
        asset=args.asset,
        state_db_path=args.state_db,
        enable_private_event_stream=not args.no_event_stream,
    )
    runtime = PlatformRuntime(config=config, context=build_runtime_context(config))
    logger.info("Platform runtime skeleton starting | exchange=%s symbol=%s event_stream=%s", args.exchange, args.symbol, not args.no_event_stream)
    result = await runtime.run(max_account_events=args.max_events)
    logger.info(
        "Platform runtime skeleton stopped | exchange=%s symbol=%s snapshots_saved=%s account_events_saved=%s handler_errors=%s",
        args.exchange,
        args.symbol,
        result.stats.snapshots_saved,
        result.stats.account_events_saved,
        result.stats.handler_errors,
    )


if __name__ == "__main__":
    asyncio.run(main())
