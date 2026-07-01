from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.models import RangeCoverageStatus, TimeRange
from src.market_data.ports import HistoricalTradeProvider
from src.market_data.range_checkpoint import (
    RangeMicroRepairJob,
    SqliteRangeCheckpointStore,
)
from src.market_data.range_repair_journal import JOURNAL_FINALIZED
from src.market_data.storage import SqliteRangeBarStore
# Legacy compatibility: normalized MarketTrade still lives under platform.
# Keep this dependency contained until the shared model migration is done.
from src.platform.data.models import MarketTrade
from src.utils.log import get_logger

logger = get_logger(__name__)


class RangeMicroRepairError(RuntimeError):
    """Raised when REST history cannot conservatively prove full coverage."""


@dataclass(frozen=True)
class RangeMicroRepairFetchResult:
    trades: tuple[MarketTrade, ...]
    rest_pages: int
    rest_raw_trades: int
    rest_deduped_trades: int


@dataclass(frozen=True)
class RangeMicroRepairRebuildResult:
    bucket_start_ms: int
    bucket_end_ms: int
    repair_start_ms: int
    repair_end_ms: int
    repair_gap_start_ms: int
    repair_gap_end_ms: int
    repair_gap_ms: int
    journal_start_ms: int
    journal_end_ms: int
    journal_trade_count: int
    rest_pages: int
    rest_raw_trades: int
    rest_deduped_trades: int
    replayed_rest_trades: int
    replayed_journal_trades: int
    range_bars_written: int
    aggregate_written: bool


class RangeMicroRepairService:
    """Fetch a bounded trade interval through an exchange-agnostic port."""

    def __init__(
        self,
        provider: HistoricalTradeProvider,
        *,
        page_limit: int = 100,
        max_pages: int = 20,
        max_seconds: float = 30.0,
    ) -> None:
        if page_limit <= 0 or max_pages <= 0 or max_seconds <= 0:
            raise ValueError("micro repair limits must be positive")
        self.provider = provider
        self.page_limit = int(page_limit)
        self.max_pages = int(max_pages)
        self.max_seconds = float(max_seconds)

    async def fetch(
        self,
        *,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
    ) -> RangeMicroRepairFetchResult:
        if end_time_ms < start_time_ms:
            return RangeMicroRepairFetchResult((), 0, 0, 0)

        started = time.monotonic()
        cursor_start_ms = int(start_time_ms)
        pages = 0
        raw_count = 0
        by_identity: dict[tuple[object, ...], MarketTrade] = {}

        while cursor_start_ms <= end_time_ms:
            if pages >= self.max_pages:
                raise RangeMicroRepairError(
                    f"REST pagination limit reached before interval coverage: max_pages={self.max_pages}"
                )
            remaining = self.max_seconds - (time.monotonic() - started)
            if remaining <= 0:
                raise RangeMicroRepairError(
                    f"REST micro repair timed out after {self.max_seconds:.3f}s"
                )
            supports_page_budget = _accepts_keyword(
                self.provider.fetch_trades, "max_pages"
            )
            remaining_pages = self.max_pages - pages
            fetch_limit = (
                self.page_limit * remaining_pages
                if supports_page_budget
                else self.page_limit
            )
            fetch_kwargs = {
                "symbol": symbol,
                "start_time_ms": cursor_start_ms,
                "end_time_ms": end_time_ms,
                "limit": fetch_limit,
            }
            if supports_page_budget:
                fetch_kwargs["max_pages"] = remaining_pages
            try:
                page = await asyncio.wait_for(
                    self.provider.fetch_trades(**fetch_kwargs),
                    timeout=remaining,
                )
            except asyncio.TimeoutError as exc:
                raise RangeMicroRepairError(
                    f"REST micro repair timed out after {self.max_seconds:.3f}s"
                ) from exc

            fetched_pages = max(
                1,
                int(
                    getattr(
                        self.provider,
                        "last_historical_trade_pages",
                        1,
                    )
                    or 1
                ),
            )
            pages += fetched_pages
            if pages > self.max_pages:
                raise RangeMicroRepairError(
                    f"REST pagination limit exceeded: pages={pages} max_pages={self.max_pages}"
                )
            normalized = [
                trade
                for trade in page
                if _trade_time_ms(trade) is not None
                and cursor_start_ms <= int(_trade_time_ms(trade) or 0) <= end_time_ms
            ]
            normalized.sort(key=_trade_sort_key)
            raw_count += len(normalized)
            before = len(by_identity)
            for trade in normalized:
                by_identity.setdefault(trade_identity(trade), trade)

            if len(normalized) < fetch_limit:
                trades = tuple(sorted(by_identity.values(), key=_trade_sort_key))
                return RangeMicroRepairFetchResult(
                    trades=trades,
                    rest_pages=pages,
                    rest_raw_trades=raw_count,
                    rest_deduped_trades=len(trades),
                )

            max_time_ms = max(int(_trade_time_ms(trade) or 0) for trade in normalized)
            if len(by_identity) == before:
                raise RangeMicroRepairError(
                    "REST pagination stalled on duplicate trades; interval coverage is unproven"
                )
            # Keep the boundary inclusive once. This prevents silently skipping
            # multiple trades that share a millisecond. A provider that cannot
            # advance at that boundary fails closed on the next iteration.
            cursor_start_ms = max_time_ms

        trades = tuple(sorted(by_identity.values(), key=_trade_sort_key))
        return RangeMicroRepairFetchResult(
            trades=trades,
            rest_pages=pages,
            rest_raw_trades=raw_count,
            rest_deduped_trades=len(trades),
        )


class RangeMicroRepairRebuildService:
    """Rebuild one closed degraded bucket without touching live memory."""

    def __init__(
        self,
        *,
        provider: HistoricalTradeProvider,
        range_bar_store: SqliteRangeBarStore,
        checkpoint_store: SqliteRangeCheckpointStore,
        contract_value: Decimal | str,
        page_limit: int = 100,
        max_pages: int = 20,
        max_seconds: float = 30.0,
    ) -> None:
        self.provider = provider
        self.range_bar_store = range_bar_store
        self.checkpoint_store = checkpoint_store
        self.contract_value = Decimal(str(contract_value))
        self.fetcher = RangeMicroRepairService(
            provider,
            page_limit=page_limit,
            max_pages=max_pages,
            max_seconds=max_seconds,
        )

    async def rebuild(
        self,
        job: RangeMicroRepairJob,
        *,
        journal_trades: Sequence[MarketTrade],
        completed_at_ms: int,
    ) -> RangeMicroRepairRebuildResult:
        if job.checkpoint_last_trade_ts_ms is None or not job.builder_state:
            raise RangeMicroRepairError(
                "checkpoint builder_state is required for micro repair"
            )
        if job.first_live_trade_ts_ms is None:
            raise RangeMicroRepairError(
                "first_live_trade_ts_ms is required for micro repair"
            )
        if job.journal_required and job.journal_status != JOURNAL_FINALIZED:
            raise RangeMicroRepairError(
                f"repair journal is not finalized: {job.journal_status}"
            )
        builder = RangeBarBuilder.restore_state(job.builder_state)
        repair_start_ms = int(job.checkpoint_last_trade_ts_ms) + 1
        suffix_end_ms = int(job.bucket_end_ms)
        repair_gap_start_ms = repair_start_ms
        repair_gap_end_ms = int(job.first_live_trade_ts_ms) - 1
        repair_gap_ms = max(
            0, repair_gap_end_ms - repair_gap_start_ms + 1
        )
        journal_start_ms = int(job.first_live_trade_ts_ms)
        journal_end_ms = suffix_end_ms
        logger.info(
            "range_micro_repair_rest_gap_fetch_started | symbol=%s exchange=%s "
            "range_pct=%s bucket_start_ms=%s bucket_end_ms=%s "
            "checkpoint_last_trade_ts_ms=%s checkpoint_last_trade_id=%s "
            "first_live_trade_ts_ms=%s first_live_trade_id=%s "
            "repair_gap_start_ms=%s repair_gap_end_ms=%s repair_gap_ms=%s "
            "coverage_before=%s",
            job.symbol,
            job.exchange,
            job.range_pct,
            job.bucket_start_ms,
            job.bucket_end_ms,
            job.checkpoint_last_trade_ts_ms,
            job.checkpoint_last_trade_id,
            job.first_live_trade_ts_ms,
            job.first_live_trade_id,
            repair_gap_start_ms,
            repair_gap_end_ms,
            repair_gap_ms,
            job.coverage_status,
        )
        fetch = await self.fetcher.fetch(
            symbol=job.symbol,
            start_time_ms=repair_gap_start_ms,
            end_time_ms=repair_gap_end_ms,
        )
        logger.info(
            "range_micro_repair_rest_gap_fetch_completed | symbol=%s "
            "exchange=%s bucket_start_ms=%s bucket_end_ms=%s "
            "repair_gap_start_ms=%s repair_gap_end_ms=%s repair_gap_ms=%s "
            "rest_pages=%s rest_raw_trades=%s rest_deduped_trades=%s",
            job.symbol,
            job.exchange,
            job.bucket_start_ms,
            job.bucket_end_ms,
            repair_gap_start_ms,
            repair_gap_end_ms,
            repair_gap_ms,
            fetch.rest_pages,
            fetch.rest_raw_trades,
            fetch.rest_deduped_trades,
        )

        journal_rows = tuple(journal_trades)
        journal_deduped = dedupe_and_sort_trades(journal_rows)
        if len(journal_deduped) != len(journal_rows):
            raise RangeMicroRepairError(
                "journal contains duplicate trade identities"
            )
        if any(
            (_trade_time_ms(trade) or -1) < journal_start_ms
            or (_trade_time_ms(trade) or -1) > journal_end_ms
            for trade in journal_deduped
        ):
            raise RangeMicroRepairError(
                "journal contains trades outside the required interval"
            )
        if not _contains_first_live_trade(job, journal_deduped):
            raise RangeMicroRepairError(
                "journal does not contain the recorded first live trade"
            )
        logger.info(
            "range_micro_repair_journal_load_completed | symbol=%s "
            "exchange=%s bucket_start_ms=%s journal_start_ms=%s "
            "journal_end_ms=%s journal_trade_count=%s journal_status=%s",
            job.symbol,
            job.exchange,
            job.bucket_start_ms,
            journal_start_ms,
            journal_end_ms,
            len(journal_deduped),
            job.journal_status,
        )
        combined = tuple(fetch.trades) + journal_deduped
        _assert_strict_replay_order(combined)
        generated = []
        logger.info(
            "range_micro_repair_replay_started | symbol=%s exchange=%s "
            "bucket_start_ms=%s bucket_end_ms=%s replayed_rest_trades=%s "
            "replayed_journal_trades=%s",
            job.symbol,
            job.exchange,
            job.bucket_start_ms,
            job.bucket_end_ms,
            len(fetch.trades),
            len(journal_deduped),
        )
        for trade in combined:
            generated.extend(builder.on_trade(trade))
        generated = [
            bar
            for bar in generated
            if repair_start_ms <= bar.end_time_ms <= suffix_end_ms
        ]
        replace_start_ms = min(repair_start_ms, suffix_end_ms)
        existing_bars = self.range_bar_store.load(
            symbol=job.symbol,
            range_pct=job.range_pct,
            time_range=TimeRange(job.bucket_start_ms, job.bucket_end_ms),
        )
        all_bars = [
            bar
            for bar in existing_bars
            if not replace_start_ms <= bar.end_time_ms <= suffix_end_ms
        ]
        all_bars.extend(generated)
        all_bars.sort(key=lambda row: (row.end_time_ms, row.bar_id))
        bucket_ms = int(job.bucket_end_ms) - int(job.bucket_start_ms) + 1
        aggregate = next(
            (
                row
                for row in RangeBarAggregator().aggregate(
                    all_bars, bucket_ms=bucket_ms
                )
                if row.bucket_start_ms == job.bucket_start_ms
                and row.bucket_end_ms == job.bucket_end_ms
            ),
            None,
        )
        if aggregate is None:
            raise RangeMicroRepairError(
                "rebuild produced no completed range aggregate"
            )
        written = self.range_bar_store.replace_range_for_repair(
            symbol=job.symbol,
            range_pct=job.range_pct,
            time_range=TimeRange(replace_start_ms, suffix_end_ms),
            rows=generated,
        )
        aggregate_written = self.checkpoint_store.save_completed_aggregate(
            exchange=job.exchange,
            aggregate=aggregate,
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            missing_gap_ms=0,
            completed_at_ms=completed_at_ms,
        )
        if not aggregate_written:
            raise RangeMicroRepairError(
                "repaired aggregate failed validation and was not written"
            )
        logger.info(
            "range_micro_repair_replay_completed | symbol=%s exchange=%s "
            "bucket_start_ms=%s bucket_end_ms=%s "
            "repair_gap_ms=%s rest_deduped_trades=%s "
            "replayed_rest_trades=%s replayed_journal_trades=%s "
            "range_bars_written=%s aggregates_written=1 "
            "coverage_before=%s coverage_after=%s",
            job.symbol,
            job.exchange,
            job.bucket_start_ms,
            job.bucket_end_ms,
            repair_gap_ms,
            fetch.rest_deduped_trades,
            len(fetch.trades),
            len(journal_deduped),
            written,
            job.coverage_status,
            RangeCoverageStatus.COMPLETE.value,
        )
        return RangeMicroRepairRebuildResult(
            bucket_start_ms=job.bucket_start_ms,
            bucket_end_ms=job.bucket_end_ms,
            repair_start_ms=repair_start_ms,
            repair_end_ms=suffix_end_ms,
            repair_gap_start_ms=repair_gap_start_ms,
            repair_gap_end_ms=repair_gap_end_ms,
            repair_gap_ms=repair_gap_ms,
            journal_start_ms=journal_start_ms,
            journal_end_ms=journal_end_ms,
            journal_trade_count=len(journal_deduped),
            rest_pages=fetch.rest_pages,
            rest_raw_trades=fetch.rest_raw_trades,
            rest_deduped_trades=fetch.rest_deduped_trades,
            replayed_rest_trades=len(fetch.trades),
            replayed_journal_trades=len(journal_deduped),
            range_bars_written=written,
            aggregate_written=True,
        )


def trade_identity(trade: MarketTrade) -> tuple[object, ...]:
    exchange = getattr(trade.exchange, "value", str(trade.exchange))
    if trade.trade_id:
        return (exchange, trade.symbol, "id", str(trade.trade_id))
    return (
        exchange,
        trade.symbol,
        "fields",
        _trade_time_ms(trade),
        str(trade.price),
        str(trade.quantity),
        getattr(trade.side, "value", str(trade.side)),
    )


def dedupe_and_sort_trades(rows: Sequence[MarketTrade]) -> tuple[MarketTrade, ...]:
    by_identity: dict[tuple[object, ...], MarketTrade] = {}
    for trade in rows:
        by_identity.setdefault(trade_identity(trade), trade)
    return tuple(sorted(by_identity.values(), key=_trade_sort_key))


def _trade_time_ms(trade: MarketTrade) -> int | None:
    return trade.trade_time_ms if trade.trade_time_ms is not None else trade.event_time_ms


def _trade_sort_key(trade: MarketTrade) -> tuple[object, ...]:
    trade_id = str(trade.trade_id or "")
    id_key: tuple[int, object] = (0, int(trade_id)) if trade_id.isdigit() else (1, trade_id)
    return (int(_trade_time_ms(trade) or 0), *id_key)


def _accepts_keyword(callable_obj, keyword: str) -> bool:
    try:
        parameters = inspect.signature(callable_obj).parameters
    except (TypeError, ValueError):
        return False
    return keyword in parameters


def _contains_first_live_trade(
    job: RangeMicroRepairJob,
    trades: Sequence[MarketTrade],
) -> bool:
    expected_ts = int(job.first_live_trade_ts_ms or -1)
    expected_id = job.first_live_trade_id
    return any(
        int(_trade_time_ms(trade) or -1) == expected_ts
        and (
            expected_id is None
            or str(trade.trade_id or "") == str(expected_id)
        )
        for trade in trades
    )


def _assert_strict_replay_order(trades: Sequence[MarketTrade]) -> None:
    identities: set[tuple[object, ...]] = set()
    previous = None
    for trade in trades:
        identity = trade_identity(trade)
        if identity in identities:
            raise RangeMicroRepairError(
                "replay contains duplicate trade identity"
            )
        identities.add(identity)
        key = _trade_sort_key(trade)
        if previous is not None and key < previous:
            raise RangeMicroRepairError("replay trades are out of order")
        previous = key
