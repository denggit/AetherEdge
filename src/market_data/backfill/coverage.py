from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.market_data.warmup.gap_detector import interval_to_ms


def current_closed_bucket_end_ms(now_ms: int, bucket_interval: str) -> int:
    bucket_ms = interval_to_ms(bucket_interval)
    current_start = (int(now_ms) // bucket_ms) * bucket_ms
    return max(0, current_start - 1)


def bucket_start_from_end(bucket_end_ms: int, bucket_interval: str) -> int:
    return int(bucket_end_ms) - interval_to_ms(bucket_interval) + 1


def utc_day_start_ms(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    start = datetime(dt.year, dt.month, dt.day, tzinfo=UTC)
    return int(start.timestamp() * 1000)


def previous_utc_day_start_ms(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(utc_day_start_ms(ts_ms) / 1000, tz=UTC)
    return int((dt - timedelta(days=1)).timestamp() * 1000)


def iter_utc_dates(start_ms: int, end_ms: int):
    start = datetime.fromtimestamp(start_ms / 1000, tz=UTC).date()
    end = datetime.fromtimestamp(end_ms / 1000, tz=UTC).date()
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)
