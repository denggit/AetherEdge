#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Preflight tool: check live warmup kline data without starting live trading.

Usage::

    python tools/check_live_warmup_data.py \\
        --symbol ETH-USDT-PERP \\
        --interval 4h \\
        --warmup-days 365 \\
        --min-records 1000

    # With optional REST backfill
    python tools/check_live_warmup_data.py \\
        --symbol ETH-USDT-PERP \\
        --interval 4h \\
        --warmup-days 365 \\
        --min-records 1000 \\
        --backfill

Exit codes:
    0 — enough data
    2 — insufficient data
    3 — config / provider error
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check AetherEdge live warmup kline data",
    )
    parser.add_argument("--symbol", default="ETH-USDT-PERP", help="Canonical symbol")
    parser.add_argument("--interval", default="4h", help="Kline interval (e.g. 4h, 1h)")
    parser.add_argument("--warmup-days", type=int, default=365, help="Warmup lookback in days")
    parser.add_argument("--min-records", type=int, default=1000, help="Minimum required records")
    parser.add_argument("--backfill", action="store_true", help="Attempt public REST backfill if insufficient")
    parser.add_argument("--kline-store-path", default=None, help="Override KlineStore SQLite path")
    return parser.parse_args()


def _check_local(
    *,
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    kline_store_path: str | None,
) -> tuple[int, str, str]:
    """Check local KlineStore and return (count, store_class, store_path)."""
    from src.market_data.models import TimeRange
    from src.market_data.storage.kline_store import SqliteKlineStore

    path = kline_store_path or "data/market_data/aether_market_data.sqlite3"
    store = SqliteKlineStore(path)
    rows = store.load(symbol=symbol, interval=interval, time_range=TimeRange(start_open_ms, end_open_ms))
    closed = [r for r in rows if r.is_closed]
    return len(closed), type(store).__name__, str(store.path)


def _print_diagnostics(
    *,
    symbol: str,
    interval: str,
    warmup_days: int,
    min_records: int,
    start_open_ms: int,
    end_open_ms: int,
    store_class: str,
    store_path: str,
    raw_aliases: str,
    local_count: int,
    fetched: int = 0,
    saved: int = 0,
    after_count: int = 0,
    backfill_attempted: bool = False,
) -> None:
    start_utc = datetime.fromtimestamp(start_open_ms / 1000, tz=timezone.utc).isoformat()
    end_utc = datetime.fromtimestamp(end_open_ms / 1000, tz=timezone.utc).isoformat()

    print("=== AetherEdge Live Warmup Data Check ===")
    print(f"  Symbol:              {symbol}")
    print(f"  Raw aliases:         {raw_aliases}")
    print(f"  Interval:            {interval}")
    print(f"  Warmup days:         {warmup_days}")
    print(f"  Min records:         {min_records}")
    print(f"  Start open (ms):     {start_open_ms}")
    print(f"  End open (ms):       {end_open_ms}")
    print(f"  Start open (UTC):    {start_utc}")
    print(f"  End open (UTC):      {end_utc}")
    print(f"  KlineStore class:    {store_class}")
    print(f"  KlineStore path:     {store_path}")
    print(f"  Available closed rows (before backfill): {local_count}")
    if backfill_attempted:
        print(f"  Backfill fetched:    {fetched}")
        print(f"  Backfill saved:      {saved}")
        print(f"  Available closed rows (after backfill):  {after_count}")
    final_count = after_count if backfill_attempted else local_count
    print(f"  Sufficient:          {'YES' if final_count >= min_records else 'NO'}")


async def _run() -> int:
    args = _parse_args()

    # Resolve warmup time range
    from src.market_data.warmup.gap_detector import interval_to_ms

    interval_ms = interval_to_ms(args.interval)
    now_ms = int(__import__("time").time() * 1000)
    # Compute the end_open as the most recent closed bar boundary.
    end_open = (now_ms // interval_ms) * interval_ms - interval_ms
    start_open = max(0, end_open - args.warmup_days * 24 * 60 * 60_000)

    # Resolve raw aliases
    raw_aliases_str = "N/A"
    try:
        from src.platform.markets import get_market_profile

        profile = get_market_profile(args.symbol)
        raw_aliases_str = ", ".join(
            f"{exchange.value}:{profile.raw_symbol(exchange)}"
            for exchange in profile.exchange_symbols
        )
    except Exception as exc:
        print(f"ERROR: Cannot resolve market profile for {args.symbol}: {exc}", file=sys.stderr)
        return 3

    # Check local store
    try:
        local_count, store_class, store_path = _check_local(
            symbol=args.symbol,
            interval=args.interval,
            start_open_ms=start_open,
            end_open_ms=end_open,
            kline_store_path=args.kline_store_path,
        )
    except Exception as exc:
        print(f"ERROR: Cannot query local KlineStore: {exc}", file=sys.stderr)
        return 3

    fetched = 0
    saved = 0
    after_count = local_count
    backfill_attempted = False

    if local_count < args.min_records and args.backfill:
        backfill_attempted = True
        print(f"Local records ({local_count}) < min ({args.min_records}). Attempting REST backfill...")

        try:
            from src.market_data.models import TimeRange
            from src.market_data.storage.kline_store import SqliteKlineStore
            from src.platform.data.factory import create_market_data_feed
            from src.platform.exchanges.models import ExchangeName
            from src.market_data.warmup.kline_provider import MarketDataKlineProvider

            # Create a read-only data feed for backfill (no WebSocket streams).
            data_exchange = ExchangeName.OKX
            data_feed = create_market_data_feed(
                data_exchange,
                symbol=args.symbol,
                enable_trade_stream=False,
                enable_order_book_stream=False,
            )
            store = SqliteKlineStore(args.kline_store_path or "data/market_data/aether_market_data.sqlite3")
            provider = MarketDataKlineProvider(data_feed=data_feed, repository=store)

            diag = await provider.backfill_and_reload(
                symbol=args.symbol,
                interval=args.interval,
                time_range=TimeRange(start_open, end_open),
                min_records=args.min_records,
                store_class=store_class,
                store_path=store_path,
            )
            fetched = diag.fetched_records
            saved = diag.saved_records
            after_count = diag.records_loaded_after

        except Exception as exc:
            print(f"ERROR: REST backfill failed: {exc}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            return 3

    _print_diagnostics(
        symbol=args.symbol,
        interval=args.interval,
        warmup_days=args.warmup_days,
        min_records=args.min_records,
        start_open_ms=start_open,
        end_open_ms=end_open,
        store_class=store_class,
        store_path=store_path,
        raw_aliases=raw_aliases_str,
        local_count=local_count,
        fetched=fetched,
        saved=saved,
        after_count=after_count,
        backfill_attempted=backfill_attempted,
    )

    final_count = after_count if backfill_attempted else local_count
    if final_count >= args.min_records:
        print("PASS: Sufficient warmup data available.")
        return 0
    else:
        print(f"FAIL: Insufficient warmup data ({final_count} < {args.min_records}).")
        return 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
