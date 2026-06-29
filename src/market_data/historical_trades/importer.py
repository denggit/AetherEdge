from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Sequence

from src.market_data.historical_trades.models import HistoricalTradeImportSummary
from src.market_data.models import TimeRange
from src.market_data.storage import SqliteTradeStore
from src.platform.exchanges.okx.historical_data import (
    DownloadedArchive,
    OkxHistoricalTradesArchiveClient,
)


class HistoricalTradeImportService:
    """Import raw historical trades into AetherEdge storage and mark coverage."""

    def __init__(
        self,
        *,
        trade_store: SqliteTradeStore,
        archive_client: OkxHistoricalTradesArchiveClient | None = None,
        archive_client_kwargs: dict[str, object] | None = None,
        raw_kind: str = "trades",
        manifest_filename: str = "download_manifest.jsonl",
    ) -> None:
        self.trade_store = trade_store
        self.archive_client = archive_client or OkxHistoricalTradesArchiveClient(
            **(archive_client_kwargs or {})
        )
        self.raw_kind = str(raw_kind)
        self.manifest_filename = str(manifest_filename)

    def import_missing_buckets(
        self,
        *,
        symbol: str,
        raw_symbol: str,
        exchange: str,
        bucket_starts: list[int],
        bucket_ms: int,
        time_range: TimeRange,
        raw_root: Path,
        trade_source: str,
        dry_run: bool,
        dry_run_download_network: bool,
        current_bucket_start_ms: int | None = None,
        overwrite_raw: bool = False,
        raw_chunksize: int = 300_000,
        coverage_edge_tolerance_ms: int = 300_000,
        coverage_max_gap_ms: int = 1_800_000,
        download_limit: int = 100,
        download_max_pages: int | None = None,
    ) -> HistoricalTradeImportSummary:
        if str(exchange).lower() != "okx":
            raise ValueError("historical trade import currently supports exchange='okx'")
        if trade_source not in {"okx_cdn_daily", "local_raw", "rest_history"}:
            raise ValueError(f"unsupported trade_source: {trade_source!r}")

        cutoff = current_bucket_start_ms
        eligible_buckets = [
            bucket_start
            for bucket_start in sorted(bucket_starts)
            if cutoff is None or bucket_start < cutoff
        ]
        summary = HistoricalTradeImportSummary(requested_buckets=len(bucket_starts))
        summary.skipped_buckets = len(bucket_starts) - len(eligible_buckets)

        if trade_source == "rest_history":
            return self._import_rest_history(
                symbol=symbol,
                raw_symbol=raw_symbol,
                bucket_starts=eligible_buckets,
                bucket_ms=bucket_ms,
                dry_run=dry_run,
                dry_run_download_network=dry_run_download_network,
                coverage_edge_tolerance_ms=coverage_edge_tolerance_ms,
                coverage_max_gap_ms=coverage_max_gap_ms,
                download_limit=download_limit,
                download_max_pages=download_max_pages,
                initial=summary,
            )

        summary.raw_dates_required = _raw_dates_for_buckets(eligible_buckets, bucket_ms)
        manifest_path = Path(raw_root) / self.manifest_filename
        summary.raw_manifest_path = str(manifest_path)

        if dry_run and not dry_run_download_network:
            summary.would_download_buckets = len(eligible_buckets)
            return summary

        date_paths: dict[str, Path] = {}
        for date_text in summary.raw_dates_required:
            raw_path = self.raw_zip_path(raw_root=Path(raw_root), raw_symbol=raw_symbol, date_text=date_text)
            raw_date = date.fromisoformat(date_text)
            if raw_path.exists() and raw_path.stat().st_size > 0 and not overwrite_raw:
                summary.raw_files_found.append(str(raw_path))
                self._append_manifest(
                    manifest_path,
                    DownloadedArchive(
                        date=date_text,
                        url=self.archive_client.build_daily_trades_url(raw_symbol, raw_date),
                        path=str(raw_path),
                        sha256=None,
                        size=raw_path.stat().st_size,
                        status="found",
                    ),
                )
                date_paths[date_text] = raw_path
                continue

            if trade_source != "okx_cdn_daily":
                summary.raw_files_missing.append(str(raw_path))
                self._append_manifest(
                    manifest_path,
                    DownloadedArchive(
                        date=date_text,
                        url=self.archive_client.build_daily_trades_url(raw_symbol, raw_date),
                        path=str(raw_path),
                        sha256=None,
                        size=None,
                        status="missing",
                    ),
                )
                continue

            try:
                archive = self.archive_client.download_daily_trades_zip(
                    raw_symbol,
                    raw_date,
                    raw_path,
                    overwrite=overwrite_raw,
                )
            except Exception as exc:
                status = "download_failed"
                if _is_raw_404(exc):
                    if _is_incomplete_raw_day(raw_date, current_bucket_start_ms):
                        status = "skipped_incomplete_day"
                        summary.raw_files_skipped_incomplete_day.append(str(raw_path))
                    else:
                        status = "not_yet_published"
                        summary.raw_files_not_yet_published.append(str(raw_path))
                else:
                    summary.raw_files_missing.append(str(raw_path))
                    summary.errors.append(f"raw_download_failed date={date_text} path={raw_path} error={exc}")
                self._append_manifest(
                    manifest_path,
                    DownloadedArchive(
                        date=date_text,
                        url=self.archive_client.build_daily_trades_url(raw_symbol, raw_date),
                        path=str(raw_path),
                        sha256=None,
                        size=None,
                        status=status,
                        error=str(exc),
                    ),
                )
                continue

            summary.raw_files_downloaded.append(str(raw_path))
            self._append_manifest(manifest_path, archive)
            date_paths[date_text] = raw_path

        for date_text in summary.raw_dates_required:
            raw_path = date_paths.get(date_text)
            if raw_path is None:
                continue
            try:
                rows_read, trades_saved = self._import_raw_zip(
                    raw_path,
                    symbol=symbol,
                    raw_symbol=raw_symbol,
                    time_range=time_range,
                    chunksize=raw_chunksize,
                    dry_run=dry_run,
                )
                summary.rows_read += rows_read
                summary.trades_saved += trades_saved
            except Exception as exc:
                summary.errors.append(f"raw_import_failed date={date_text} path={raw_path} error={exc}")

        if dry_run:
            return summary

        self._validate_and_mark_buckets(
            summary=summary,
            symbol=symbol,
            bucket_starts=eligible_buckets,
            bucket_ms=bucket_ms,
            coverage_edge_tolerance_ms=coverage_edge_tolerance_ms,
            coverage_max_gap_ms=coverage_max_gap_ms,
        )
        return summary

    def raw_zip_path(self, *, raw_root: Path, raw_symbol: str, date_text: str) -> Path:
        return Path(raw_root) / self.raw_kind / raw_symbol / f"{raw_symbol}-{self.raw_kind}-{date_text}.zip"

    def _import_raw_zip(
        self,
        path: Path,
        *,
        symbol: str,
        raw_symbol: str,
        time_range: TimeRange,
        chunksize: int,
        dry_run: bool,
    ) -> tuple[int, int]:
        rows_read = 0
        trades_saved = 0
        for trades in self.archive_client.iter_daily_trades_zip(
            path,
            raw_symbol=raw_symbol,
            symbol=symbol,
            chunksize=chunksize,
        ):
            rows_read += len(trades)
            filtered = [
                trade
                for trade in trades
                if _trade_time_ms(trade) is not None
                and time_range.start_time_ms <= int(_trade_time_ms(trade)) <= time_range.end_time_ms
            ]
            if filtered and not dry_run:
                trades_saved += self.trade_store.save(filtered)
        return rows_read, trades_saved

    def _import_rest_history(
        self,
        *,
        symbol: str,
        raw_symbol: str,
        bucket_starts: list[int],
        bucket_ms: int,
        dry_run: bool,
        dry_run_download_network: bool,
        coverage_edge_tolerance_ms: int,
        coverage_max_gap_ms: int,
        download_limit: int,
        download_max_pages: int | None,
        initial: HistoricalTradeImportSummary,
    ) -> HistoricalTradeImportSummary:
        summary = initial
        if dry_run and not dry_run_download_network:
            summary.would_download_buckets = len(bucket_starts)
            return summary

        for bucket_start in sorted(bucket_starts, reverse=True):
            bucket_end = bucket_start + bucket_ms - 1
            try:
                trades, _pages, _complete = self.archive_client.download_history_bucket_trades(
                    raw_symbol,
                    bucket_start,
                    bucket_end,
                    limit=download_limit,
                    max_pages=download_max_pages,
                    symbol=symbol,
                )
            except Exception as exc:
                summary.failed_buckets += 1
                summary.errors.append(f"download_failed bucket_start_ms={bucket_start} error={exc}")
                continue

            if not trades:
                summary.failed_buckets += 1
                summary.errors.append(f"no_trades_returned bucket_start_ms={bucket_start}")
                continue

            if dry_run:
                summary.would_download_buckets += 1
                summary.would_download_trade_count += len(trades)
                continue

            summary.trades_saved += self.trade_store.save(trades)
            ok, reason = validate_bucket_trade_coverage(
                db_path=self.trade_store.path,
                symbol=symbol,
                bucket_start_ms=bucket_start,
                bucket_end_ms=bucket_end,
                edge_tolerance_ms=coverage_edge_tolerance_ms,
                max_gap_ms=coverage_max_gap_ms,
            )
            if ok:
                self.trade_store.mark_coverage(
                    symbol=symbol,
                    time_range=TimeRange(bucket_start, bucket_end),
                    source="historical",
                )
                summary.imported_buckets += 1
                summary.coverage_validated_buckets += 1
            else:
                summary.failed_buckets += 1
                summary.coverage_validation_failed_buckets += 1
                summary.coverage_validation_failed_examples.append({
                    "bucket_start_ms": bucket_start,
                    "bucket_end_ms": bucket_end,
                    "trades_downloaded": len(trades),
                    "reason": reason,
                })
        return summary

    def _validate_and_mark_buckets(
        self,
        *,
        summary: HistoricalTradeImportSummary,
        symbol: str,
        bucket_starts: Sequence[int],
        bucket_ms: int,
        coverage_edge_tolerance_ms: int,
        coverage_max_gap_ms: int,
    ) -> None:
        for bucket_start in bucket_starts:
            bucket_end = bucket_start + bucket_ms - 1
            ok, reason = validate_bucket_trade_coverage(
                db_path=self.trade_store.path,
                symbol=symbol,
                bucket_start_ms=bucket_start,
                bucket_end_ms=bucket_end,
                edge_tolerance_ms=coverage_edge_tolerance_ms,
                max_gap_ms=coverage_max_gap_ms,
            )
            if ok:
                self.trade_store.mark_coverage(
                    symbol=symbol,
                    time_range=TimeRange(bucket_start, bucket_end),
                    source="historical",
                )
                summary.imported_buckets += 1
                summary.coverage_validated_buckets += 1
            else:
                summary.failed_buckets += 1
                summary.coverage_validation_failed_buckets += 1
                summary.coverage_validation_failed_examples.append({
                    "bucket_start_ms": bucket_start,
                    "bucket_end_ms": bucket_end,
                    "reason": reason,
                })

    @staticmethod
    def _append_manifest(path: Path, archive: DownloadedArchive) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(archive.to_manifest_json() + "\n")


def validate_bucket_trade_coverage(
    *,
    db_path: Path,
    symbol: str,
    bucket_start_ms: int,
    bucket_end_ms: int,
    edge_tolerance_ms: int,
    max_gap_ms: int,
) -> tuple[bool, str]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*),
                   MIN(COALESCE(trade_time_ms, event_time_ms)),
                   MAX(COALESCE(trade_time_ms, event_time_ms))
            FROM trades
            WHERE symbol = ?
              AND COALESCE(trade_time_ms, event_time_ms) BETWEEN ? AND ?
            """,
            (symbol, bucket_start_ms, bucket_end_ms),
        ).fetchone()
    count = int(row[0] or 0)
    earliest = row[1]
    latest = row[2]
    if count == 0:
        return False, "no trades in bucket"
    if earliest is None or latest is None:
        return False, "no timestamps in bucket trades"

    earliest_ms = int(earliest)
    latest_ms = int(latest)
    if earliest_ms > bucket_start_ms + edge_tolerance_ms:
        return False, (
            f"earliest trade {earliest_ms} too far from bucket_start {bucket_start_ms} "
            f"(gap={earliest_ms - bucket_start_ms}ms > tolerance={edge_tolerance_ms}ms)"
        )
    if latest_ms < bucket_end_ms - edge_tolerance_ms:
        return False, (
            f"latest trade {latest_ms} too far from bucket_end {bucket_end_ms} "
            f"(gap={bucket_end_ms - latest_ms}ms > tolerance={edge_tolerance_ms}ms)"
        )

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(trade_time_ms, event_time_ms)
            FROM trades
            WHERE symbol = ?
              AND COALESCE(trade_time_ms, event_time_ms) BETWEEN ? AND ?
            ORDER BY COALESCE(trade_time_ms, event_time_ms) ASC
            """,
            (symbol, bucket_start_ms, bucket_end_ms),
        ).fetchall()

    prev: int | None = None
    max_gap_found = 0
    for (ts,) in rows:
        ts_int = int(ts)
        if prev is not None:
            max_gap_found = max(max_gap_found, ts_int - prev)
        prev = ts_int
    if max_gap_found > max_gap_ms:
        return False, f"max inter-trade gap {max_gap_found}ms exceeds threshold {max_gap_ms}ms"
    return True, "ok"


def _raw_dates_for_buckets(bucket_starts: Sequence[int], bucket_ms: int) -> list[str]:
    dates: set[str] = set()
    for bucket_start in bucket_starts:
        bucket_end = bucket_start + bucket_ms - 1
        start_day = datetime.fromtimestamp(bucket_start / 1000, tz=UTC).date()
        end_day = datetime.fromtimestamp(bucket_end / 1000, tz=UTC).date()
        current = start_day
        while current <= end_day:
            dates.add(current.isoformat())
            current += timedelta(days=1)
    return sorted(dates)


def _trade_time_ms(trade) -> int | None:
    if trade.trade_time_ms is not None:
        return trade.trade_time_ms
    return trade.event_time_ms


def _is_raw_404(exc: Exception) -> bool:
    raw = repr(exc)
    text = str(exc)
    return (
        "HTTP Error 404" in raw
        or "HTTP Error 404" in text
        or "HTTPError 404" in raw
        or "HTTPError 404" in text
        or "HTTP 404" in raw
        or "HTTP 404" in text
        or "code=404" in raw
        or "code=404" in text
    )


def _is_incomplete_raw_day(raw_date: date, current_bucket_start_ms: int | None) -> bool:
    if current_bucket_start_ms is None:
        return False
    current_utc_day = datetime.fromtimestamp(current_bucket_start_ms / 1000, tz=UTC).date()
    return raw_date >= current_utc_day
