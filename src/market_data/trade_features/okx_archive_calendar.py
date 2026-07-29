from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

_OKX_ARCHIVE_TIMEZONE = timezone(timedelta(hours=8))


def safe_okx_archive_end_ms(
    now_ms: int | None = None,
    *,
    archive_publish_lag_hours: float = 8.0,
) -> int:
    """Return the last published-safe millisecond of an OKX UTC+8 day."""

    now = (
        datetime.now(UTC)
        if now_ms is None
        else datetime.fromtimestamp(int(now_ms) / 1000, tz=UTC)
    )
    effective_now = now - timedelta(
        hours=max(0.0, float(archive_publish_lag_hours))
    )
    okx_now = effective_now.astimezone(_OKX_ARCHIVE_TIMEZONE)
    current_day_start = datetime(
        okx_now.year,
        okx_now.month,
        okx_now.day,
        tzinfo=_OKX_ARCHIVE_TIMEZONE,
    )
    return int(current_day_start.timestamp() * 1000) - 1


def format_okx_archive_time(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(
        int(timestamp_ms) / 1_000,
        tz=UTC,
    ).astimezone(_OKX_ARCHIVE_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S+08")


__all__ = ["format_okx_archive_time", "safe_okx_archive_end_ms"]
