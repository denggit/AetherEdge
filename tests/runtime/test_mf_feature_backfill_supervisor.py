from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path

from src.market_data.models import FixedTimeTradeBar
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.market_data.trade_features.coverage import safe_okx_archive_end_ms
from src.runtime.mf_feature_backfill_supervisor import MfFeatureBackfillSupervisor

_MINUTE = 60_000


def _now_aligned() -> int:
    now = int(time.time() * 1000)
    return now - (now % _MINUTE)


def _make_bar(open_time_ms: int, close_time_ms: int | None = None) -> FixedTimeTradeBar:
    if close_time_ms is None:
        close_time_ms = open_time_ms + _MINUTE - 1
    return FixedTimeTradeBar(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        timeframe="1m",
        open_time_ms=open_time_ms,
        close_time_ms=close_time_ms,
        available_time_ms=close_time_ms,
        open=Decimal("1000"),
        high=Decimal("1005"),
        low=Decimal("995"),
        close=Decimal("1002"),
        volume=Decimal("10"),
        buy_volume=Decimal("6"),
        sell_volume=Decimal("4"),
        buy_notional=Decimal("6000"),
        sell_notional=Decimal("4000"),
        delta_volume=Decimal("2"),
        delta_notional=Decimal("2000"),
        abs_delta_notional=Decimal("2000"),
        trade_count=5,
    )


def _make_fp(open_time_ms: int, close_time_ms: int | None = None, *,
             quality: str = "COMPLETE", context_available: bool = True) -> "TradeFootprintFeature":
    from src.market_data.models import TradeFootprintFeature as TFF
    if close_time_ms is None:
        close_time_ms = open_time_ms + _MINUTE - 1
    delta = Decimal("2000")
    return TFF(
        exchange="okx", symbol="ETH-USDT-PERP", timeframe="1m",
        open_time_ms=open_time_ms, close_time_ms=close_time_ms, available_time_ms=close_time_ms,
        delta_notional=delta, abs_delta_notional=abs(delta),
        taker_buy_ratio=Decimal("0.6"), close_pos=Decimal("0.5"),
        range_pct=Decimal("0.01"), return_pct=Decimal("0.002"),
        fp_max_bucket_abs_delta_pressure=Decimal("0"),
        context_available=context_available, quality=quality,
    )


def _write_pair(store: SqliteTradeFeatureStore, open_time_ms: int) -> None:
    store.upsert_tradebars_many([_make_bar(open_time_ms)])
    store.upsert_footprints_many([_make_fp(open_time_ms)])


def test_supervisor_scan_coverage_returns_dict(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite3"
    status_path = tmp_path / "status.json"
    lock_path = tmp_path / "lock.lock"
    global_lock = tmp_path / "global.lock"
    global_status = tmp_path / "global_status.json"
    log_path = tmp_path / "worker.out"

    store = SqliteTradeFeatureStore(path=db_path)
    base = safe_okx_archive_end_ms() - 5 * _MINUTE + 1
    for i in range(5):
        _write_pair(store, base + i * 60_000)

    supervisor = MfFeatureBackfillSupervisor(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        market_db=str(db_path),
        status_path=str(status_path),
        lock_path=str(lock_path),
        global_lock_path=str(global_lock),
        global_status_path=str(global_status),
        worker_log_path=str(log_path),
        required_minutes=5,
    )
    coverage = supervisor.scan_coverage()
    assert isinstance(coverage, dict)
    assert "coverage_ready" in coverage
    assert "mf_signal_ready" in coverage
    assert coverage["mf_signal_ready"] is False


def test_supervisor_check_and_launch_when_coverage_complete(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite3"
    status_path = tmp_path / "status.json"
    lock_path = tmp_path / "lock.lock"
    global_lock = tmp_path / "global.lock"
    global_status = tmp_path / "global_status.json"
    log_path = tmp_path / "worker.out"

    store = SqliteTradeFeatureStore(path=db_path)
    base = safe_okx_archive_end_ms() - 10 * _MINUTE + 1
    for i in range(10):
        _write_pair(store, base + i * 60_000)

    supervisor = MfFeatureBackfillSupervisor(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        market_db=str(db_path),
        status_path=str(status_path),
        lock_path=str(lock_path),
        global_lock_path=str(global_lock),
        global_status_path=str(global_status),
        worker_log_path=str(log_path),
        required_minutes=10,
    )
    result = supervisor.check_and_launch()
    assert result["action"] == "none"
    assert result["reason"] == "coverage_complete"


def test_supervisor_worker_running_detection(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite3"
    status_path = tmp_path / "status.json"
    lock_path = tmp_path / "lock.lock"
    global_lock = tmp_path / "global.lock"
    global_status = tmp_path / "global_status.json"
    log_path = tmp_path / "worker.out"

    status_payload = {
        "version": 1,
        "pid": 99999,
        "running": True,
        "worker_heartbeat_ms": int(time.time() * 1000),
    }
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(status_payload))

    supervisor = MfFeatureBackfillSupervisor(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        market_db=str(db_path),
        status_path=str(status_path),
        lock_path=str(lock_path),
        global_lock_path=str(global_lock),
        global_status_path=str(global_status),
        worker_log_path=str(log_path),
    )
    # The PID 99999 doesn't exist, so worker should be detected as not running
    assert supervisor._worker_running() is False


def test_supervisor_reports_mf_signal_ready_false_always(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite3"
    status_path = tmp_path / "status.json"
    lock_path = tmp_path / "lock.lock"
    global_lock = tmp_path / "global.lock"
    global_status = tmp_path / "global_status.json"
    log_path = tmp_path / "worker.out"

    supervisor = MfFeatureBackfillSupervisor(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        market_db=str(db_path),
        status_path=str(status_path),
        lock_path=str(lock_path),
        global_lock_path=str(global_lock),
        global_status_path=str(global_status),
        worker_log_path=str(log_path),
    )
    coverage = supervisor.scan_coverage()
    assert coverage["mf_signal_ready"] is False


def test_supervisor_launches_worker_when_global_lf_lock_is_stale(
    tmp_path: Path, monkeypatch
) -> None:
    global_lock = tmp_path / "global.lock"
    global_status = tmp_path / "global_status.json"
    stale_ms = int(time.time() * 1000) - 3_600_000
    global_lock.write_text(
        json.dumps(
            {
                "pid": 99999,
                "owner": "range_backfill",
                "priority": 10,
                "symbol": "ETH-USDT-PERP",
                "raw_days": 1,
                "started_at_ms": stale_ms,
            }
        ),
        encoding="utf-8",
    )
    global_status.write_text(
        json.dumps(
            {
                "pid": 99999,
                "owner": "range_backfill",
                "priority": 10,
                "running": False,
                "worker_heartbeat_ms": stale_ms,
            }
        ),
        encoding="utf-8",
    )
    supervisor = MfFeatureBackfillSupervisor(
        symbol="ETH-USDT-PERP",
        market_db=str(tmp_path / "market.sqlite3"),
        status_path=str(tmp_path / "mf_status.json"),
        lock_path=str(tmp_path / "mf.lock"),
        global_lock_path=str(global_lock),
        global_status_path=str(global_status),
        worker_log_path=str(tmp_path / "worker.out"),
        stale_after_seconds=1,
        restart_cooldown_seconds=0,
        failure_cooldown_seconds=0,
    )
    monkeypatch.setattr(
        supervisor,
        "scan_coverage",
        lambda: {
            "coverage_ready": False,
            "current_day_archive_not_ready": True,
            "mf_signal_ready": False,
        },
    )
    monkeypatch.setattr(supervisor, "_launch_worker", lambda: True)

    result = supervisor.check_and_launch()

    assert result["action"] == "launched"
    assert result["reason"] == "coverage_gap"
