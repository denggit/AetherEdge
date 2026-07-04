"""MF feature backfill worker tool.

Reads raw trades from OKX daily zip archives, normalizes trades, feeds EVERY
trade into FixedTimeTradeBarBuilder AND TradeFootprintBuilder independently,
and writes both closed 1m tradebars and footprints to SQLite.

Usage:
  python tools/mf_feature_backfill_worker.py --once --symbol ETH-USDT-PERP
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Sequence

# ---------------------------------------------------------------------------
# Repo-root bootstrap
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.backfill.coordinator import (  # noqa: E402
    MF_FEATURE_BACKFILL_PRIORITY,
    RawTradeBackfillCoordinator,
)
from src.market_data.backfill.status_store import now_ms  # noqa: E402
from src.market_data.derived import (  # noqa: E402
    FixedTimeTradeBarBuilder,
    TradeFootprintBuilder,
)
from src.market_data.historical_trades.importer import (  # noqa: E402
    iter_trade_csv_chunks,
    normalize_okx_trade_chunk,
)
from src.market_data.historical_trades.okx_archive import (  # noqa: E402
    OkxHistoricalTradeArchive,
    iter_okx_archive_dates_for_utc_range,
    okx_raw_symbol_from_canonical,
)
from src.market_data.models import (  # noqa: E402
    FixedTimeTradeBar,
    TradeFootprintFeature,
)
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore  # noqa: E402
from src.market_data.trade_features.coverage import compute_backfill_target  # noqa: E402
from src.platform.data.models import MarketTrade  # noqa: E402
from src.platform.exchanges.models import ExchangeName  # noqa: E402

logger = logging.getLogger(__name__)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MF 1m trade-derived feature backfill worker")
    # Mode
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument("--mode", choices=("live", "prebuild"), default="prebuild")
    parser.add_argument("--direction", choices=("recent-to-oldest", "oldest-to-recent"),
                        default="recent-to-oldest")
    # Limits
    parser.add_argument("--max-minutes-per-cycle", type=int, default=1440)
    parser.add_argument("--max-days-per-cycle", type=int, default=1)
    parser.add_argument("--max-trades-per-cycle", type=int, default=500_000)
    parser.add_argument("--max-seconds-per-cycle", type=float, default=60.0)
    parser.add_argument("--chunk-sleep-seconds", type=float, default=0.0)
    # Symbol
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    parser.add_argument("--exchange", default="okx")
    # Paths
    parser.add_argument("--raw-root", default="data/okx/raw/trades")
    parser.add_argument("--market-db", default="data/market_data/aether_market_data.sqlite3")
    parser.add_argument("--status-path", default="data/state/mf_feature_backfill_status.json")
    parser.add_argument("--lock-path", default="data/state/mf_feature_backfill.lock")
    parser.add_argument("--global-lock-path",
                        default="data/state/raw_trade_backfill_global.lock")
    parser.add_argument("--global-status-path",
                        default="data/state/raw_trade_backfill_global_status.json")
    parser.add_argument("--log-file", default=None)
    # Flags
    parser.add_argument("--no-download", action="store_true")
    parser.add_argument("--save-raw-trades", action="store_true", default=False)
    parser.add_argument("--contract-value", type=str, default="0.01")
    parser.add_argument("--large-trade-threshold", type=str, default="10000")
    return parser.parse_args(argv)


def run_cycle(
    *,
    symbol: str,
    exchange: str,
    market_db: str,
    raw_root: str,
    status_path: str,
    lock_path: str,
    global_lock_path: str,
    global_status_path: str,
    mode: str,
    direction: str,
    max_minutes_per_cycle: int,
    max_days_per_cycle: int,
    max_trades_per_cycle: int,
    max_seconds_per_cycle: float,
    chunk_sleep_seconds: float,
    no_download: bool,
    save_raw_trades: bool,
    contract_value: Decimal,
    large_trade_threshold: Decimal,
) -> dict:
    # -------- guard --------
    if save_raw_trades:
        if mode == "live":
            raise ValueError("--save-raw-trades is forbidden in live mode")
        logger.warning("save_raw_trades=True is NOT recommended")

    cycle_start = time.time()
    store = SqliteTradeFeatureStore(path=market_db)
    raw_symbol = okx_raw_symbol_from_canonical(symbol)

    # -------- global raw-trade coordinator --------
    coordinator = RawTradeBackfillCoordinator(
        lock_path=global_lock_path,
        status_path=global_status_path,
    )
    acquired = coordinator.try_acquire(
        owner="mf_feature_backfill",
        priority=MF_FEATURE_BACKFILL_PRIORITY,
        symbol=symbol,
        raw_days=max_days_per_cycle,
    )
    if not acquired:
        return {"status": "skipped", "reason": "global_lock_not_acquired"}

    try:
        # -------- gap-driven target --------
        target = compute_backfill_target(
            symbol=symbol,
            exchange=exchange,
            store=store,
            max_minutes_per_cycle=max_minutes_per_cycle,
            direction=direction,
        )

        if target is None:
            return {"status": "up_to_date", "reason": "no_gap_found"}

        start_ms = target.start_ms
        end_ms = target.end_ms
        reason = target.reason

        # Collect archive dates
        archive_dates = list(iter_okx_archive_dates_for_utc_range(start_ms, end_ms))
        if not archive_dates:
            return {"status": "no_archive_dates", "target_start_ms": start_ms,
                    "target_end_ms": end_ms, "reason": reason}

        archive = OkxHistoricalTradeArchive(
            root=Path(raw_root),
            timeout_seconds=20.0,
            retries=3,
        )

        # -------- builders with same contract_value --------
        bar_builder = FixedTimeTradeBarBuilder(
            contract_value=contract_value,
            large_trade_threshold_notional=large_trade_threshold,
        )
        fp_builder = TradeFootprintBuilder(contract_value=contract_value)

        total_trades = 0
        total_bars = 0
        total_footprints = 0
        downloaded = 0
        failed_downloads: list[str] = []
        missing_raw_days: list[str] = []
        safe_end_ms: int | None = None

        # Identify current-day boundary for conservative handling
        current_okx_day = _current_okx_day_utc8()

        for day in archive_dates:
            elapsed = time.time() - cycle_start
            if elapsed > max_seconds_per_cycle:
                logger.info("Cycle time limit reached")
                break
            if total_trades >= max_trades_per_cycle:
                logger.info("Trade limit reached")
                break

            # Current-day guard: don't pretend archive is ready
            if str(day) >= current_okx_day.isoformat():
                if no_download:
                    missing_raw_days.append(day.isoformat())
                    continue
                # Attempt download but track failure explicitly
                try:
                    hf = archive.ensure_daily_file(
                        symbol=symbol,
                        raw_symbol=raw_symbol,
                        day=day,
                        allow_download=True,
                    )
                    if hf.downloaded:
                        downloaded += 1
                except Exception as exc:
                    failed_downloads.append(day.isoformat())
                    logger.warning("Current-day archive not available: %s (%s)", day, exc)
                    continue
            else:
                try:
                    hf = archive.ensure_daily_file(
                        symbol=symbol,
                        raw_symbol=raw_symbol,
                        day=day,
                        allow_download=not no_download,
                    )
                    if hf.downloaded:
                        downloaded += 1
                except Exception as exc:
                    logger.warning("Failed to get archive for %s: %s", day, exc)
                    failed_downloads.append(day.isoformat())
                    missing_raw_days.append(day.isoformat())
                    continue

            # Process chunks
            for chunk in iter_trade_csv_chunks(hf.path, chunksize=50_000):
                if total_trades >= max_trades_per_cycle:
                    break
                if (time.time() - cycle_start) > max_seconds_per_cycle:
                    break

                trades = normalize_okx_trade_chunk(
                    chunk,
                    symbol=symbol,
                    raw_symbol=raw_symbol,
                    exchange=exchange,
                    min_valid_trade_time_ms=start_ms,
                    max_valid_trade_time_ms=end_ms,
                )
                total_trades += len(trades)

                batch_bars: list[FixedTimeTradeBar] = []
                batch_fps: list[TradeFootprintFeature] = []
                for trade in trades:
                    # Feed EVERY trade to BOTH builders independently
                    closed_bars = bar_builder.on_trade(trade)
                    for bar in closed_bars:
                        batch_bars.append(bar)

                    closed_fps = fp_builder.on_trade(trade)
                    for fp in closed_fps:
                        batch_fps.append(fp)

                # Batch-write
                if batch_bars:
                    store.upsert_tradebars_many(batch_bars)
                    total_bars += len(batch_bars)
                if batch_fps:
                    store.upsert_footprints_many(batch_fps)
                    total_footprints += len(batch_fps)

                if chunk_sleep_seconds > 0:
                    time.sleep(chunk_sleep_seconds)

            # Track safe end after processing each day
            safe_end_ms = end_ms

        # -------- Drain ONLY closed bars (never active) --------
        remaining_bars = bar_builder.drain_closed_only()
        remaining_fps = fp_builder.drain_closed_only()

        if remaining_bars:
            store.upsert_tradebars_many(list(remaining_bars))
            total_bars += len(remaining_bars)
        if remaining_fps:
            store.upsert_footprints_many(list(remaining_fps))
            total_footprints += len(remaining_fps)

        # Discard active to prevent accidental writes
        bar_builder.discard_active()
        fp_builder.discard_active()

        coordinator.heartbeat()

        # Determine status
        has_current_day_gap = _current_okx_day_utc8().isoformat() in [
            d.isoformat() for d in archive_dates
        ] if archive_dates else False

        if failed_downloads and has_current_day_gap:
            actual_status = "partial"
            status_reason = "current_day_archive_not_ready"
        elif failed_downloads:
            actual_status = "partial"
            status_reason = "download_failures"
        else:
            actual_status = "ok"
            status_reason = "cycle_complete"

        return {
            "status": actual_status,
            "reason": status_reason,
            "total_trades": total_trades,
            "total_bars_written": total_bars,
            "total_footprints_written": total_footprints,
            "downloaded_files": downloaded,
            "failed_downloads": failed_downloads,
            "missing_raw_days": missing_raw_days,
            "target_start_ms": start_ms,
            "target_end_ms": end_ms,
            "safe_end_ms": safe_end_ms,
            "elapsed_seconds": time.time() - cycle_start,
        }

    finally:
        coordinator.release()


def _update_status(status_path: str, **kwargs: object) -> None:
    path = Path(status_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "version": 1,
        "pid": os.getpid(),
        "worker_heartbeat_ms": now_ms(),
        "running": True,
        **kwargs,
    }
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _current_okx_day_utc8() -> date:
    from datetime import UTC, datetime, timedelta
    utc_now = datetime.now(UTC)
    cst_now = utc_now + timedelta(hours=8)
    return cst_now.date()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    log_file = args.log_file
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        filename=log_file,
    )
    if not log_file:
        logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))

    if args.save_raw_trades and args.mode == "live":
        logger.error("--save-raw-trades is forbidden in live mode")
        return 1

    _update_status(args.status_path, running=True, mode=args.mode, symbol=args.symbol)

    try:
        result = run_cycle(
            symbol=args.symbol,
            exchange=args.exchange,
            market_db=args.market_db,
            raw_root=args.raw_root,
            status_path=args.status_path,
            lock_path=args.lock_path,
            global_lock_path=args.global_lock_path,
            global_status_path=args.global_status_path,
            mode=args.mode,
            direction=args.direction,
            max_minutes_per_cycle=args.max_minutes_per_cycle,
            max_days_per_cycle=args.max_days_per_cycle,
            max_trades_per_cycle=args.max_trades_per_cycle,
            max_seconds_per_cycle=args.max_seconds_per_cycle,
            chunk_sleep_seconds=args.chunk_sleep_seconds,
            no_download=args.no_download,
            save_raw_trades=args.save_raw_trades,
            contract_value=Decimal(args.contract_value),
            large_trade_threshold=Decimal(args.large_trade_threshold),
        )
        _update_status(
            args.status_path,
            running=False,
            last_result=result,
            worker_heartbeat_ms=now_ms(),
        )
        logger.info("Cycle result: %s", json.dumps(result, default=str))
        return 0
    except Exception:
        logger.exception("MF feature backfill worker failed")
        _update_status(args.status_path, running=False, error=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
