from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import AsyncIterator
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.platform.data import (
    MarketFullOrderBook,
    MarketOpenInterest,
    MarketOrderBookL2,
    create_full_order_book_stream,
    create_open_interest_stream,
    create_order_book_l2_stream,
)
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.exchanges.symbols import to_canonical_symbol
from src.platform.markets import get_market_profile


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only OKX public market-data smoke tool",
    )
    parser.add_argument(
        "--stream",
        required=True,
        choices=(
            "order-book-l2",
            "full-order-book",
            "open-interest",
        ),
    )
    parser.add_argument("--symbol", default="ETH-USDT-SWAP")
    parser.add_argument("--events", type=int, default=2)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--depth", type=int, default=5000)
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=3.0,
    )
    parser.add_argument("--sandbox", action="store_true")
    return parser


async def _run(args: argparse.Namespace) -> None:
    if args.events <= 0:
        raise ValueError("--events must be positive")
    if args.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be positive")
    config = ExchangeConfig(sandbox=args.sandbox)
    try:
        symbol = get_market_profile(args.symbol).symbol
    except ValueError:
        symbol = to_canonical_symbol(ExchangeName.OKX, args.symbol)
    stream: object
    iterator: AsyncIterator[Any]
    if args.stream == "order-book-l2":
        stream = create_order_book_l2_stream(
            ExchangeName.OKX,
            symbol=symbol,
            config=config,
        )
        iterator = stream.stream_order_book_l2()
    elif args.stream == "full-order-book":
        stream = create_full_order_book_stream(
            ExchangeName.OKX,
            symbol=symbol,
            config=config,
            depth=args.depth,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        iterator = stream.stream_full_order_book()
    else:
        stream = create_open_interest_stream(
            ExchangeName.OKX,
            symbol=symbol,
            config=config,
        )
        iterator = stream.stream_open_interest()

    async def collect() -> None:
        count = 0
        async for event in iterator:
            print(
                json.dumps(
                    _summary(event, stream=stream),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                flush=True,
            )
            count += 1
            if count >= args.events:
                return

    try:
        await asyncio.wait_for(
            collect(),
            timeout=args.timeout_seconds,
        )
    finally:
        close = getattr(stream, "close", None)
        if not callable(close):
            close = getattr(stream, "stop", None)
        if callable(close):
            result = close()
            if asyncio.iscoroutine(result):
                await result


def _summary(event: object, *, stream: object) -> dict[str, object]:
    if isinstance(event, MarketOrderBookL2):
        return {
            "event_time_ms": event.event_time_ms,
            "sequence_id": event.sequence_id,
            "previous_sequence_id": event.previous_sequence_id,
            "bid_count": len(event.bids),
            "ask_count": len(event.asks),
            "best_bid": _decimal_text(
                event.bids[0].price if event.bids else None
            ),
            "best_ask": _decimal_text(
                event.asks[0].price if event.asks else None
            ),
            "sequence_gap_count": int(
                getattr(stream, "sequence_gap_count", 0)
            ),
            "resync_count": int(getattr(stream, "resync_count", 0)),
        }
    if isinstance(event, MarketFullOrderBook):
        return {
            "event_time_ms": event.event_time_ms,
            "requested_depth": event.requested_depth,
            "bid_count": len(event.bids),
            "ask_count": len(event.asks),
            "best_bid": _decimal_text(
                event.bids[0].price if event.bids else None
            ),
            "best_ask": _decimal_text(
                event.asks[0].price if event.asks else None
            ),
        }
    if isinstance(event, MarketOpenInterest):
        return {
            "event_time_ms": event.event_time_ms,
            "open_interest_contracts": _decimal_text(
                event.open_interest_contracts
            ),
            "open_interest_base": _decimal_text(
                event.open_interest_base
            ),
            "open_interest_usd": _decimal_text(
                event.open_interest_usd
            ),
        }
    raise TypeError(f"unsupported smoke event: {type(event).__name__}")


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def main() -> int:
    args = _parser().parse_args()
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
