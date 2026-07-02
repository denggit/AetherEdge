"""Compatibility facade for the short-lived range repair journal."""

from __future__ import annotations

from src.market_data.range_repair import (
    DEFAULT_RANGE_REPAIR_JOURNAL_DB,
    JOURNAL_FINALIZED,
    JOURNAL_INVALID_DROPPED_TRADE,
    JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
    JOURNAL_INVALID_PRODUCER_FAILED,
    JOURNAL_INVALID_PRODUCER_STALE,
    JOURNAL_INVALID_QUEUE_OVERFLOW,
    JOURNAL_INVALID_WRITER_ERROR,
    JOURNAL_OPEN,
    RangeRepairJournalState,
    RangeRepairJournalWriter,
    RangeRepairTrade,
    SqliteRangeRepairJournalStore,
    journal_status_is_invalid,
)

# These imports preserve the module attributes that existed before the
# implementation was split. Core journal behavior lives in the three modules
# re-exported below.

__all__ = [
    "DEFAULT_RANGE_REPAIR_JOURNAL_DB",
    "JOURNAL_OPEN",
    "JOURNAL_FINALIZED",
    "JOURNAL_INVALID_DROPPED_TRADE",
    "JOURNAL_INVALID_WRITER_ERROR",
    "JOURNAL_INVALID_QUEUE_OVERFLOW",
    "JOURNAL_INVALID_PRODUCER_STALE",
    "JOURNAL_INVALID_PRODUCER_FAILED",
    "JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE",
    "journal_status_is_invalid",
    "RangeRepairTrade",
    "RangeRepairJournalState",
    "SqliteRangeRepairJournalStore",
    "RangeRepairJournalWriter",
]
