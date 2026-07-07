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
from datetime import date, datetime, timedelta, timezone
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
    EXPEDITED_BACKFILL_PRIORITY,
    RawTradeBackfillCoordinator,
)
from src.market_data.backfill.status_store import now_ms  # noqa: E402
from src.market_data.derived import (  # noqa: E402
    FixedTimeTradeBarBuilder,
    RangeFootprintBuilder,
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
    RangeFootprintFeature,
    TradeFootprintFeature,
)
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore  # noqa: E402
from src.market_data.trade_features.coverage import (  # noqa: E402
    compute_mf_signal_backfill_target,
    resolve_trade_feature_readiness,
    safe_okx_archive_end_ms,
)
from src.platform.data.models import MarketTrade  # noqa: E402
from src.platform.exchanges.models import ExchangeName  # noqa: E402

logger = logging.getLogger(__name__)
_ONE_MINUTE_MS = 60_000
_OKX_ARCHIVE_TIMEZONE = timezone(timedelta(hours=8))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MF 1m trade-derived feature backfill worker")
    # Mode
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument("--mode", choices=("live", "prebuild"), default="prebuild")
    parser.add_argument("--direction", choices=("recent-to-oldest", "oldest-to-recent"),
                        default="recent-to-oldest")
    # Limits
    parser.add_argument("--max-minutes-per-cycle", type=int, default=1440)
    parser.add_argument("--required-minutes", type=int, default=4320)
    parser.add_argument(
        "--archive-publish-lag-hours",
        type=float,
        default=8.0,
    )
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
    parser.add_argument("--global-lock-path",
                        default="data/state/raw_trade_backfill_global.lock")
    parser.add_argument("--global-status-path",
                        default="data/state/raw_trade_backfill_global_status.json")
    parser.add_argument("--log-file", default=None)
    # Flags
    parser.add_argument("--no-download", action="store_true")
    parser.add_argument("--save-raw-trades", action="store_true", default=False)
    parser.add_argument("--contract-value", type=str, default="0.01")
    parser.add_argument("--price-bucket-size", type=str, default="1")
    parser.add_argument("--range-footprint-range-pct", type=str, default="0.002")
    parser.add_argument("--range-footprint-price-step", type=str, default="1")
    parser.add_argument("--range-footprint-warmup-days", type=int, default=1)
    parser.add_argument("--large-trade-threshold", type=str, default="10000")
    return parser.parse_args(argv)


def run_cycle(
    *,
    symbol: str,
    exchange: str,
    market_db: str,
    raw_root: str,
    status_path: str,
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
    price_bucket_size: Decimal = Decimal("1"),
    range_footprint_range_pct: Decimal = Decimal("0.002"),
    range_footprint_price_step: Decimal = Decimal("1"),
    range_footprint_warmup_days: int = 1,
    required_minutes: int = 4320,
    archive_publish_lag_hours: float = 8.0,
) -> dict:
    # -------- guard --------
    normalized_mode = str(mode).strip().lower()
    if normalized_mode == "live" and not no_download:
        raise ValueError(
            "live mode requires --no-download; raw archive downloads "
            "must run in prebuild/background mode"
        )
    if save_raw_trades:
        if normalized_mode == "live":
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
        priority=EXPEDITED_BACKFILL_PRIORITY,
        symbol=symbol,
        raw_days=max_days_per_cycle,
    )
    if not acquired:
        holder = coordinator.current_owner() or {}
        holder_priority = int(holder.get("priority", 0) or 0)
        reason = (
            "waiting_for_lower_priority_worker"
            if holder_priority < EXPEDITED_BACKFILL_PRIORITY
            else "global_lock_not_acquired"
        )
        return {
            "status": "skipped",
            "reason": reason,
            "global_lock_owner": holder.get("owner"),
            "global_lock_pid": holder.get("pid"),
            "global_lock_priority": holder_priority,
        }

    try:
        # -------- gap-driven target --------
        lag_hours = max(0.0, float(archive_publish_lag_hours))
        calendar_safe_archive_end = safe_okx_archive_end_ms(
            archive_publish_lag_hours=0.0,
        )
        safe_archive_end = safe_okx_archive_end_ms(
            archive_publish_lag_hours=lag_hours,
        )
        protected_archive_dates = (
            set(
                iter_okx_archive_dates_for_utc_range(
                    safe_archive_end + 1,
                    calendar_safe_archive_end,
                )
            )
            if calendar_safe_archive_end > safe_archive_end
            else set()
        )
        target = compute_mf_signal_backfill_target(
            symbol=symbol,
            exchange=exchange,
            store=store,
            max_minutes_per_cycle=min(
                max(1, int(max_minutes_per_cycle)),
                max(1, int(max_days_per_cycle)) * 1_440,
            ),
            required_minutes=max(1, int(required_minutes)),
            direction=direction,
            safe_archive_end_ms=safe_archive_end,
            range_pct=str(range_footprint_range_pct),
            price_step=str(range_footprint_price_step),
        )

        if target is None:
            return {
                "status": "up_to_date",
                "reason": "no_gap_found",
                "archive_publish_lag_hours": lag_hours,
                "calendar_safe_archive_end_ms": (
                    calendar_safe_archive_end
                ),
                "safe_archive_end_ms": safe_archive_end,
            }

        start_ms = target.start_ms
        requested_end_ms = target.end_ms
        end_ms = min(requested_end_ms, safe_archive_end)
        reason = target.reason
        current_day_gap = requested_end_ms > safe_archive_end
        if start_ms > safe_archive_end:
            return {
                "status": "deferred",
                "reason": "archive_not_published_yet",
                "archive_not_published_days": sorted(
                    day.isoformat() for day in protected_archive_dates
                ),
                "target_start_ms": start_ms,
                "requested_target_end_ms": requested_end_ms,
                "target_end_ms": safe_archive_end,
                "archive_publish_lag_hours": lag_hours,
                "calendar_safe_archive_end_ms": (
                    calendar_safe_archive_end
                ),
                "safe_archive_end_ms": safe_archive_end,
                "processed_through_ms": None,
                "current_day_gap_unrecoverable_until_archive": True,
            }

        # CoinBacktest's resumed range-footprint prebuild replays one prior day
        # so cross-day active range state is reconstructed before target rows.
        warmup_ms = max(0, int(range_footprint_warmup_days)) * 86_400_000
        effective_start_ms = max(0, start_ms - warmup_ms)
        archive_dates = list(
            iter_okx_archive_dates_for_utc_range(effective_start_ms, end_ms)
        )
        if not archive_dates:
            return {
                "status": "no_archive_dates",
                "target_start_ms": start_ms,
                "target_end_ms": end_ms,
                "reason": reason,
                "archive_publish_lag_hours": lag_hours,
                "calendar_safe_archive_end_ms": (
                    calendar_safe_archive_end
                ),
                "safe_archive_end_ms": safe_archive_end,
            }

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
        fp_builder = TradeFootprintBuilder(
            contract_value=contract_value,
            price_bucket_size=price_bucket_size,
        )
        range_fp_builder = RangeFootprintBuilder(
            range_pct=range_footprint_range_pct,
            price_step=range_footprint_price_step,
            contract_value=contract_value,
        )
        store.delete_range_footprint_window(
            symbol=symbol,
            exchange=exchange,
            range_pct=range_footprint_range_pct,
            price_step=range_footprint_price_step,
            start_ms=start_ms,
            end_ms=end_ms,
        )

        total_trades = 0
        total_bars = 0
        total_footprints = 0
        total_range_footprints = 0
        latest_range_seed: RangeFootprintFeature | None = None
        downloaded = 0
        failed_downloads: list[str] = []
        archive_not_published_days: list[str] = []
        missing_raw_days: list[str] = []
        processed_through_ms: int | None = None
        cycle_truncated = False

        for day in archive_dates:
            elapsed = time.time() - cycle_start
            if elapsed > max_seconds_per_cycle:
                logger.info("Cycle time limit reached")
                cycle_truncated = True
                break
            if total_trades >= max_trades_per_cycle:
                logger.info("Trade limit reached")
                cycle_truncated = True
                break

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
                if day in protected_archive_dates:
                    archive_not_published_days.append(day.isoformat())
                else:
                    failed_downloads.append(day.isoformat())
                missing_raw_days.append(day.isoformat())
                break

            # Process chunks
            for chunk in iter_trade_csv_chunks(hf.path, chunksize=50_000):
                if total_trades >= max_trades_per_cycle:
                    cycle_truncated = True
                    break
                if (time.time() - cycle_start) > max_seconds_per_cycle:
                    cycle_truncated = True
                    break

                trades = normalize_okx_trade_chunk(
                    chunk,
                    symbol=symbol,
                    raw_symbol=raw_symbol,
                    exchange=exchange,
                    min_valid_trade_time_ms=effective_start_ms,
                    max_valid_trade_time_ms=end_ms,
                )
                total_trades += len(trades)

                batch_bars: list[FixedTimeTradeBar] = []
                batch_fps: list[TradeFootprintFeature] = []
                batch_range_fps: list[RangeFootprintFeature] = []
                for trade in trades:
                    # Feed every trade to all independent derived builders.
                    closed_bars = bar_builder.on_trade(trade)
                    for bar in closed_bars:
                        if start_ms <= bar.close_time_ms <= end_ms:
                            batch_bars.append(bar)

                    closed_fps = fp_builder.on_trade(trade)
                    for fp in closed_fps:
                        if start_ms <= fp.close_time_ms <= end_ms:
                            batch_fps.append(fp)

                    closed_range_fps = range_fp_builder.on_trade(trade)
                    for range_fp in closed_range_fps:
                        if range_fp.available_time_ms < start_ms:
                            if (
                                latest_range_seed is None
                                or range_fp.available_time_ms
                                > latest_range_seed.available_time_ms
                            ):
                                latest_range_seed = range_fp
                        elif (
                            start_ms
                            <= range_fp.available_time_ms
                            <= end_ms
                        ):
                            batch_range_fps.append(range_fp)

                # Batch-write
                if batch_bars:
                    store.upsert_tradebars_many(batch_bars)
                    total_bars += len(batch_bars)
                if batch_fps:
                    store.upsert_footprints_many(batch_fps)
                    total_footprints += len(batch_fps)
                if batch_range_fps:
                    store.upsert_range_footprints_many(batch_range_fps)
                    total_range_footprints += len(batch_range_fps)

                if chunk_sleep_seconds > 0:
                    time.sleep(chunk_sleep_seconds)
                coordinator.heartbeat()

            if cycle_truncated:
                break

            # Track safe end after processing each day
            day_end_ms = _okx_archive_day_end_ms(day)
            if day_end_ms >= start_ms:
                processed_through_ms = min(day_end_ms, end_ms)
            coordinator.heartbeat()

        # A fully read safe archive proves the final historical minute closed.
        # A truncated/failed cycle must never promote its active minute.
        can_finalize_safe_history = (
            not cycle_truncated
            and not failed_downloads
            and not archive_not_published_days
            and not missing_raw_days
            and processed_through_ms == end_ms
        )
        if can_finalize_safe_history:
            remaining_bars = bar_builder.drain_completed_through(end_ms)
            remaining_fps = fp_builder.drain_completed_through(end_ms)
        else:
            remaining_bars = bar_builder.drain_closed_only()
            remaining_fps = fp_builder.drain_closed_only()

        if remaining_bars:
            target_bars = [
                row
                for row in remaining_bars
                if start_ms <= row.close_time_ms <= end_ms
            ]
            store.upsert_tradebars_many(target_bars)
            total_bars += len(target_bars)
        if remaining_fps:
            target_fps = [
                row
                for row in remaining_fps
                if start_ms <= row.close_time_ms <= end_ms
            ]
            store.upsert_footprints_many(target_fps)
            total_footprints += len(target_fps)
        if latest_range_seed is not None:
            store.upsert_range_footprints_many([latest_range_seed])
            total_range_footprints += 1

        # Discard active to prevent accidental writes
        bar_builder.discard_active()
        fp_builder.discard_active()
        range_fp_builder.discard_active()

        coordinator.heartbeat()

        target_minutes = max(
            1,
            (end_ms - start_ms + 1 + _ONE_MINUTE_MS - 1)
            // _ONE_MINUTE_MS,
        )
        if can_finalize_safe_history:
            store.mark_range_footprint_coverage(
                symbol=symbol,
                exchange=exchange,
                range_pct=range_footprint_range_pct,
                price_step=range_footprint_price_step,
                start_ms=start_ms,
                end_ms=end_ms,
                complete=True,
            )
        readiness_after = resolve_trade_feature_readiness(
            symbol=symbol,
            exchange=exchange,
            store=store,
            required_minutes=target_minutes,
            reference_end_ms=end_ms,
            range_pct=str(range_footprint_range_pct),
            price_step=str(range_footprint_price_step),
            archive_publish_lag_hours=lag_hours,
        )
        coverage_after = readiness_after.coverage
        if coverage_after is None:
            raise RuntimeError("feature coverage missing after worker cycle")
        range_coverage_after = {
            key: value
            for key, value in dict(coverage_after.extra or {}).items()
            if key
            in {
                "range_footprint_ready",
                "range_footprint_complete_count",
                "missing_range_footprint_count",
                "degraded_range_footprint_count",
                "latest_range_footprint_available_time_ms",
                "range_footprint_context_seed_available_time_ms",
                "range_footprint_coverage_marker_present",
                "range_pct",
                "price_step",
            }
        }
        next_signal_gap = compute_mf_signal_backfill_target(
            symbol=symbol,
            exchange=exchange,
            store=store,
            max_minutes_per_cycle=target_minutes,
            required_minutes=target_minutes,
            direction=direction,
            safe_archive_end_ms=end_ms,
            range_pct=str(range_footprint_range_pct),
            price_step=str(range_footprint_price_step),
        )
        mf_signal_feature_ready = next_signal_gap is None

        # Determine status
        if archive_not_published_days or current_day_gap:
            actual_status = "deferred"
            status_reason = "archive_not_published_yet"
        elif failed_downloads:
            actual_status = "partial"
            status_reason = "download_failures"
        elif cycle_truncated:
            actual_status = "partial"
            status_reason = "cycle_limit_reached"
        elif not mf_signal_feature_ready:
            actual_status = "partial"
            status_reason = "mf_signal_feature_incomplete"
        else:
            actual_status = "ok"
            status_reason = "cycle_complete"

        return {
            "status": actual_status,
            "reason": status_reason,
            "total_trades": total_trades,
            "total_bars_written": total_bars,
            "total_footprints_written": total_footprints,
            "tradebars_written": total_bars,
            "fixed_footprints_written": total_footprints,
            "range_footprints_written": total_range_footprints,
            "downloaded_files": downloaded,
            "failed_downloads": failed_downloads,
            "archive_not_published_days": (
                archive_not_published_days
                if archive_not_published_days
                else (
                    sorted(
                        day.isoformat()
                        for day in protected_archive_dates
                    )
                    if current_day_gap
                    else []
                )
            ),
            "missing_raw_days": missing_raw_days,
            "target_start_ms": start_ms,
            "target_end_ms": end_ms,
            "requested_target_end_ms": requested_end_ms,
            "safe_end_ms": safe_archive_end,
            "safe_archive_end_ms": safe_archive_end,
            "calendar_safe_archive_end_ms": (
                calendar_safe_archive_end
            ),
            "archive_publish_lag_hours": lag_hours,
            "processed_through_ms": processed_through_ms,
            "cycle_truncated": cycle_truncated,
            "can_finalize_safe_history": can_finalize_safe_history,
            "coverage_after": {
                "complete_minutes": coverage_after.complete_minutes,
                "missing_minutes": coverage_after.missing_minutes,
                "degraded_minutes": coverage_after.degraded_minutes,
                "available": coverage_after.available,
                "reason": coverage_after.reason,
            },
            "range_footprint_coverage_after": range_coverage_after,
            "mf_signal_feature_ready": bool(mf_signal_feature_ready),
            "current_day_gap_unrecoverable_until_archive": current_day_gap,
            "elapsed_seconds": time.time() - cycle_start,
            "mode": normalized_mode,
            "no_download": bool(no_download),
        }

    finally:
        coordinator.release()


def _okx_archive_day_end_ms(day: date) -> int:
    next_day_start = datetime(
        day.year,
        day.month,
        day.day,
        tzinfo=_OKX_ARCHIVE_TIMEZONE,
    ) + timedelta(days=1)
    return int(next_day_start.timestamp() * 1_000) - 1


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

    if args.mode == "live" and not args.no_download:
        logger.error("live mode requires --no-download")
        return 1
    if args.save_raw_trades and args.mode == "live":
        logger.error("--save-raw-trades is forbidden in live mode")
        return 1

    _update_status(
        args.status_path,
        running=True,
        mode=args.mode,
        symbol=args.symbol,
        no_download=bool(args.no_download),
        archive_publish_lag_hours=args.archive_publish_lag_hours,
    )

    try:
        result = run_cycle(
            symbol=args.symbol,
            exchange=args.exchange,
            market_db=args.market_db,
            raw_root=args.raw_root,
            status_path=args.status_path,
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
            price_bucket_size=Decimal(args.price_bucket_size),
            range_footprint_range_pct=Decimal(
                args.range_footprint_range_pct
            ),
            range_footprint_price_step=Decimal(
                args.range_footprint_price_step
            ),
            range_footprint_warmup_days=args.range_footprint_warmup_days,
            required_minutes=args.required_minutes,
            archive_publish_lag_hours=args.archive_publish_lag_hours,
        )
        result.setdefault("mode", args.mode)
        result.setdefault("no_download", bool(args.no_download))
        _update_status(
            args.status_path,
            running=False,
            last_result=result,
            worker_heartbeat_ms=now_ms(),
            mode=args.mode,
            symbol=args.symbol,
            no_download=bool(args.no_download),
            archive_publish_lag_hours=args.archive_publish_lag_hours,
        )
        logger.info("Cycle result: %s", json.dumps(result, default=str))
        return 0
    except Exception:
        logger.exception("MF feature backfill worker failed")
        _update_status(
            args.status_path,
            running=False,
            error=True,
            mode=args.mode,
            symbol=args.symbol,
            no_download=bool(args.no_download),
            archive_publish_lag_hours=args.archive_publish_lag_hours,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
