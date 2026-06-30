from __future__ import annotations

from src.market_data.backfill.models import RangeBackfillSummary
from tools import range_backfill_worker as worker


def test_worker_cli_defaults_parse() -> None:
    args = worker.build_parser().parse_args([])
    request = worker.request_from_args(args)

    assert request.mode == "live"
    assert request.direction == "recent-to-oldest"
    assert request.required_buckets == 100
    assert request.save_raw_trades is False
    assert request.chunk_sleep_seconds == 0.05
    assert request.max_seconds_per_cycle == 120
    assert request.max_trades_per_cycle == 2_000_000


def test_live_worker_default_once_false_and_prebuild_once_true() -> None:
    live_args = worker.build_parser().parse_args(["--mode", "live"])
    prebuild_args = worker.build_parser().parse_args(["--mode", "prebuild"])

    assert worker.resolve_once(live_args) is False
    assert worker.resolve_once(prebuild_args) is True


def test_worker_low_priority_skips_os_nice_on_windows(monkeypatch) -> None:
    called = False

    def fake_nice(value):
        nonlocal called
        called = True

    monkeypatch.setattr(worker.platform, "system", lambda: "Windows")
    monkeypatch.setattr(worker.os, "nice", fake_nice, raising=False)

    worker.maybe_lower_priority(True)

    assert called is False


def test_worker_check_only_does_not_request_writes(monkeypatch) -> None:
    args = worker.build_parser().parse_args(["--check-only"])
    request = worker.request_from_args(args)

    assert request.dry_run is False
    assert args.check_only is True


def test_worker_loops_until_missing_after_zero(monkeypatch) -> None:
    calls = 0

    class FakeService:
        def __init__(self, request) -> None:
            pass

        def run_once(self):
            nonlocal calls
            calls += 1
            missing = 1 if calls == 1 else 0
            return RangeBackfillSummary(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                range_pct="0.002",
                bucket_interval="4h",
                target_buckets=3,
                complete_before=1,
                complete_after=3 - missing,
                missing_before=2,
                missing_after=missing,
                status="ok",
            )

    monkeypatch.setattr(worker, "RangeBackfillService", FakeService)
    monkeypatch.setattr(worker.time, "sleep", lambda value: None)

    assert worker.main(["--mode", "live", "--sleep-seconds", "0"]) == 0
    assert calls == 2


def test_worker_exits_zero_when_cycle_reaches_required_coverage(monkeypatch) -> None:
    class FakeService:
        def __init__(self, request) -> None:
            pass

        def run_once(self):
            return RangeBackfillSummary(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                range_pct="0.002",
                bucket_interval="4h",
                target_buckets=3,
                complete_before=2,
                complete_after=3,
                missing_before=1,
                missing_after=0,
                status="ok",
            )

    monkeypatch.setattr(worker, "RangeBackfillService", FakeService)

    assert worker.main(["--mode", "live"]) == 0
