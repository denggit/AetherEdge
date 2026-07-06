from __future__ import annotations

import json
import os

from tools import prebuild_mf_feature_history as tool


def _readiness(ready: bool) -> dict[str, bool]:
    return {
        "tradebar_ready": ready,
        "fixed_time_footprint_ready": ready,
        "range_footprint_ready": ready,
        "coverage_ready": ready,
        "degraded_footprint": False,
        "ready": ready,
    }


def _args(tmp_path, *extra: str):
    return tool.build_parser().parse_args(
        [
            "--status-path",
            str(tmp_path / "status.json"),
            "--market-db",
            str(tmp_path / "market.sqlite3"),
            "--sleep-seconds",
            "0",
            *extra,
        ]
    )


def test_already_ready_exits_without_run_cycle(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: _readiness(True),
    )
    calls = []
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: calls.append(kwargs),
    )

    result = tool.run_prebuild(_args(tmp_path))

    assert result == 0
    assert calls == []


def test_partial_cycle_then_ready_exits_zero(
    tmp_path,
    monkeypatch,
) -> None:
    readiness = iter((_readiness(False), _readiness(True)))
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: next(readiness),
    )
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: {
            "status": "partial",
            "reason": "cycle_limit_reached",
            "target_end_ms": 2,
        },
    )

    result = tool.run_prebuild(_args(tmp_path))

    assert result == 0


def test_repeated_cycle_failures_exit_nonzero(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: _readiness(False),
    )
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: {
            "status": "error",
            "reason": "download_failures",
        },
    )

    result = tool.run_prebuild(
        _args(tmp_path, "--max-failures", "2")
    )

    assert result != 0
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["error"] is True
    assert status["error_detail"] == "max_failures_reached"


def test_max_cycles_not_ready_exits_nonzero(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: _readiness(False),
    )
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: {
            "status": "partial",
            "reason": "cycle_limit_reached",
        },
    )

    result = tool.run_prebuild(
        _args(tmp_path, "--max-cycles", "2")
    )

    assert result != 0
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["cycles"] == 2
    assert status["running"] is False


def test_status_file_is_written_with_atomic_replace(
    tmp_path,
    monkeypatch,
) -> None:
    calls = []
    real_replace = os.replace

    def capture(source, target):
        calls.append((source, target))
        real_replace(source, target)

    monkeypatch.setattr(tool.os, "replace", capture)
    target = tmp_path / "nested" / "status.json"

    tool._write_status(target, {"running": True})

    assert calls == [
        (target.with_name("status.json.tmp"), target)
    ]
    assert not target.with_name("status.json.tmp").exists()
    assert json.loads(target.read_text())["running"] is True


def test_no_download_passes_true_to_worker(
    tmp_path,
    monkeypatch,
) -> None:
    readiness = iter((_readiness(False), _readiness(True)))
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: next(readiness),
    )
    captured = {}

    def cycle(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "reason": "cycle_complete"}

    monkeypatch.setattr(tool, "run_cycle", cycle)

    result = tool.run_prebuild(
        _args(tmp_path, "--no-download")
    )

    assert result == 0
    assert captured["no_download"] is True


def test_effective_required_minutes_covers_large_share_window(
    tmp_path,
    monkeypatch,
) -> None:
    readiness_calls = []
    readiness = iter((_readiness(False), _readiness(True)))

    def capture_readiness(_args, *, required_minutes):
        readiness_calls.append(required_minutes)
        return next(readiness)

    monkeypatch.setattr(tool, "_readiness_audit", capture_readiness)
    captured = {}
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: captured.update(kwargs)
        or {"status": "ok", "reason": "cycle_complete"},
    )

    result = tool.run_prebuild(
        _args(tmp_path, "--target-days", "3")
    )

    assert result == 0
    assert readiness_calls == [129_600, 129_600]
    assert captured["required_minutes"] == 129_600
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["requested_minutes"] == 4_320
    assert status["effective_required_minutes"] == 129_600


def test_default_command_arguments() -> None:
    args = tool.build_parser().parse_args([])

    assert args.symbol == "ETH-USDT-PERP"
    assert args.exchange == "okx"
    assert args.target_days == 120
    assert args.max_minutes_per_cycle == 4320
    assert args.max_days_per_cycle == 3
    assert args.max_trades_per_cycle == 2_000_000
    assert args.max_seconds_per_cycle == 600
    assert args.max_cycles == 200
    assert args.max_seconds == 0
    assert args.download is True
    assert args.large_share_min_samples == 43_200
    assert args.large_share_window_days == 90


def test_progress_summary_contains_cycle_status_and_ready(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    readiness = iter((_readiness(False), _readiness(True)))
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: next(readiness),
    )
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: {
            "status": "ok",
            "reason": "cycle_complete",
        },
    )

    assert tool.run_prebuild(_args(tmp_path)) == 0

    output = capsys.readouterr().out
    assert "[prebuild-mf] cycle=1" in output
    assert "status=ok" in output
    assert "ready=True" in output
