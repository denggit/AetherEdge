from __future__ import annotations

from src.market_data.backfill.status_store import RangeBackfillStatusStore, now_ms
from tools import prebuild_range_speed_history as tool


def test_prebuild_defaults_do_not_require_db_args(monkeypatch) -> None:
    monkeypatch.delenv("AETHER_MARKET_DATA_DB", raising=False)

    args = tool.build_parser().parse_args(["--check-only"])
    request = tool.request_from_args(args)

    assert request.symbol == "ETH-USDT-PERP"
    assert str(request.market_db_path).endswith("aether_market_data.sqlite3")
    assert request.save_raw_trades is True
    assert request.chunk_sleep_seconds == 0.0
    assert request.max_seconds_per_cycle == 0.0
    assert request.max_trades_per_cycle == 0


def test_prebuild_buckets_100_takes_effect() -> None:
    args = tool.build_parser().parse_args(["--buckets", "100"])
    request = tool.request_from_args(args)

    assert request.required_buckets == 100
    assert request.max_buckets_per_cycle == 100


def test_prebuild_exits_when_live_worker_lock_exists(tmp_path) -> None:
    lock_path = tmp_path / "range.lock"
    status_path = tmp_path / "status.json"
    lock_path.write_text("running", encoding="utf-8")
    RangeBackfillStatusStore(status_path).write(
        {"running": True, "heartbeat_ms": now_ms(), "phase": "sleeping"}
    )

    result = tool.main(
        [
            "--buckets",
            "1",
            "--market-db",
            str(tmp_path / "market.sqlite3"),
            "--checkpoint-db",
            str(tmp_path / "checkpoint.sqlite3"),
            "--raw-root",
            str(tmp_path / "raw"),
            "--status-path",
            str(status_path),
            "--lock-path",
            str(lock_path),
            "--no-download",
        ]
    )

    assert result == 1
