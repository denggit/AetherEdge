from __future__ import annotations

import logging

from src.market_data.backfill.status_store import RangeBackfillStatusStore, now_ms
from src.runtime.range_speed_history import RangeSpeedHistoryRefresher, RangeSpeedHistoryStatus


class DummyStrategy:
    config = type(
        "Config",
        (),
        {"entry_filters": type("EntryFilters", (), {"range_speed_rolling_window_bars": 100, "range_speed_min_periods": 3})()},
    )()


def test_warning_log_is_rate_limited(tmp_path, caplog) -> None:
    refresher = RangeSpeedHistoryRefresher(
        strategy=DummyStrategy(),
        store=object(),
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        warning_seconds=600,
        status_path=str(tmp_path / "status.json"),
    )
    status = RangeSpeedHistoryStatus(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=1,
        min_periods=3,
        missing_periods=2,
        rolling_window_bars=100,
        available=False,
        latest_complete_bucket_end_ms=None,
        current_closed_bucket_end_ms=123,
    )

    with caplog.at_level(logging.WARNING):
        refresher._log_status_if_needed(status)
        refresher._log_status_if_needed(status)

    messages = [record.message for record in caplog.records if "range-speed history still insufficient" in record.message]
    assert len(messages) == 1
    assert "backfill_process_running" in messages[0]


def test_warning_stops_when_history_available(tmp_path, caplog) -> None:
    refresher = RangeSpeedHistoryRefresher(
        strategy=DummyStrategy(),
        store=object(),
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        warning_seconds=600,
        status_path=str(tmp_path / "status.json"),
    )
    insufficient = RangeSpeedHistoryStatus(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=1,
        min_periods=3,
        missing_periods=2,
        rolling_window_bars=100,
        available=False,
        latest_complete_bucket_end_ms=None,
        current_closed_bucket_end_ms=123,
    )
    available = RangeSpeedHistoryStatus(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=3,
        min_periods=3,
        missing_periods=0,
        rolling_window_bars=100,
        available=True,
        latest_complete_bucket_end_ms=123,
        current_closed_bucket_end_ms=123,
        refreshed=True,
    )

    with caplog.at_level(logging.WARNING):
        refresher._log_status_if_needed(insufficient)
        refresher._log_status_if_needed(available)

    warnings = [record.message for record in caplog.records if "still insufficient" in record.message]
    assert len(warnings) == 1


def test_warning_does_not_report_missing_pid_as_running(
    tmp_path,
    caplog,
    monkeypatch,
) -> None:
    status_path = tmp_path / "status.json"
    RangeBackfillStatusStore(status_path).write(
        {
            "running": True,
            "pid": 909230,
            "worker_heartbeat_ms": now_ms(),
            "mode": "live",
            "direction": "recent-to-oldest",
        }
    )
    monkeypatch.setattr(
        "src.market_data.backfill.status_store.process_id_exists",
        lambda pid: False,
    )
    refresher = RangeSpeedHistoryRefresher(
        strategy=DummyStrategy(),
        store=object(),
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        warning_seconds=600,
        status_path=str(status_path),
    )
    status = RangeSpeedHistoryStatus(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=96,
        min_periods=100,
        missing_periods=4,
        rolling_window_bars=100,
        available=False,
        latest_complete_bucket_end_ms=None,
        current_closed_bucket_end_ms=123,
    )

    with caplog.at_level(logging.WARNING):
        refresher._log_status_if_needed(status)

    message = next(
        record.message
        for record in caplog.records
        if "range-speed history still insufficient" in record.message
    )
    assert "backfill_process_running=False" in message
    assert "backfill_pid=None" in message
