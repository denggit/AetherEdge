from __future__ import annotations

from tools import range_backfill_worker as worker


def test_worker_cli_defaults_parse() -> None:
    args = worker.build_parser().parse_args([])
    request = worker.request_from_args(args)

    assert request.mode == "live"
    assert request.direction == "recent-to-oldest"
    assert request.required_buckets == 100


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
