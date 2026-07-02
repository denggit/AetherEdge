from __future__ import annotations

"""Data contracts and statuses for the range repair journal."""

from dataclasses import dataclass

DEFAULT_RANGE_REPAIR_JOURNAL_DB = (
    "data/state/range_repair_trade_journal.sqlite3"
)

JOURNAL_OPEN = "journal_open"
JOURNAL_FINALIZED = "journal_finalized"
JOURNAL_INVALID_DROPPED_TRADE = "journal_invalid_dropped_trade"
JOURNAL_INVALID_WRITER_ERROR = "journal_invalid_writer_error"
JOURNAL_INVALID_QUEUE_OVERFLOW = "journal_invalid_queue_overflow"
JOURNAL_INVALID_PRODUCER_STALE = "journal_invalid_producer_stale"
JOURNAL_INVALID_PRODUCER_FAILED = "journal_invalid_producer_failed"
JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE = (
    "journal_invalid_market_queue_drain_incomplete"
)


def journal_status_is_invalid(status: str) -> bool:
    return str(status).startswith("journal_invalid_")


@dataclass(frozen=True)
class RangeRepairTrade:
    exchange: str
    symbol: str
    range_pct: str
    bucket_start_ms: int
    trade_time_ms: int
    event_time_ms: int | None
    trade_id: str | None
    raw_symbol: str
    side: str
    price: str
    quantity: str
    source: str
    created_at_ms: int


@dataclass(frozen=True)
class RangeRepairJournalState:
    exchange: str
    symbol: str
    range_pct: str
    bucket_start_ms: int
    bucket_end_ms: int
    checkpoint_last_trade_ts_ms: int | None
    checkpoint_last_trade_id: str | None
    first_live_trade_ts_ms: int | None
    first_live_trade_id: str | None
    first_live_trade_recorded_at_ms: int | None
    last_journal_trade_ts_ms: int | None
    journal_trade_count: int
    dropped_trades: int
    writer_failures: int
    finalized: bool
    finalized_at_ms: int | None
    status: str
    last_error: str | None
    updated_at_ms: int

    @property
    def valid_for_repair(self) -> bool:
        return (
            self.finalized
            and self.status == JOURNAL_FINALIZED
            and self.first_live_trade_ts_ms is not None
            and self.dropped_trades == 0
            and self.writer_failures == 0
        )
