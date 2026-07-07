from __future__ import annotations

import json
import os
import time
from dataclasses import replace
from pathlib import Path

from tools import mf_feature_backfill_worker as worker
from src.market_data.backfill.coordinator import (
    BACKGROUND_BACKFILL_PRIORITY,
)
from src.market_data.trade_features import (
    backfill_supervisor as supervisor_module,
)
from src.market_data.trade_features.backfill_supervisor import (
    TradeFeatureBackfillConfig,
    TradeFeatureBackfillSupervisor,
)


def _config(tmp_path: Path) -> TradeFeatureBackfillConfig:
    return TradeFeatureBackfillConfig(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        worker_script=tmp_path / "worker.py",
        repository_root=tmp_path,
        market_db=str(tmp_path / "market.sqlite3"),
        status_path=tmp_path / "status.json",
        global_lock_path=tmp_path / "global.lock",
        global_status_path=tmp_path / "global-status.json",
        worker_log_path=tmp_path / "worker.out",
        restart_cooldown_seconds=0,
        failure_cooldown_seconds=0,
    )


def test_complete_coverage_does_not_launch(tmp_path, monkeypatch) -> None:
    supervisor = TradeFeatureBackfillSupervisor(
        config=_config(tmp_path),
        coverage_reader=lambda: {"coverage_ready": True},
    )
    launches = []
    monkeypatch.setattr(
        supervisor,
        "_launch_worker",
        lambda: launches.append(True),
    )

    result = supervisor.check_and_launch()

    assert result["reason"] == "coverage_complete"
    assert launches == []


def test_coverage_gap_launches_injected_worker(tmp_path, monkeypatch) -> None:
    supervisor = TradeFeatureBackfillSupervisor(
        config=_config(tmp_path),
        coverage_reader=lambda: {"coverage_ready": False},
    )
    monkeypatch.setattr(
        supervisor,
        "_launch_worker",
        lambda: True,
    )

    result = supervisor.check_and_launch()

    assert result["action"] == "launched"
    assert result["reason"] == "coverage_gap"


def test_running_worker_prevents_duplicate_launch(
    tmp_path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    config.status_path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "running": True,
                "worker_heartbeat_ms": int(time.time() * 1000),
            }
        ),
        encoding="utf-8",
    )
    supervisor = TradeFeatureBackfillSupervisor(
        config=config,
        coverage_reader=lambda: {"coverage_ready": False},
    )
    launches = []
    monkeypatch.setattr(
        supervisor,
        "_launch_worker",
        lambda: launches.append(True),
    )

    result = supervisor.check_and_launch()

    assert result["reason"] == "worker_already_running"
    assert launches == []


def test_scan_coverage_returns_detached_mapping(tmp_path) -> None:
    source = {"coverage_ready": False}
    supervisor = TradeFeatureBackfillSupervisor(
        config=_config(tmp_path),
        coverage_reader=lambda: source,
    )

    result = supervisor.scan_coverage()
    result["coverage_ready"] = True

    assert source["coverage_ready"] is False


def test_worker_command_required_minutes_is_parser_compatible(
    tmp_path,
    monkeypatch,
) -> None:
    config = replace(
        _config(tmp_path),
        required_minutes=172800,
    )
    supervisor = TradeFeatureBackfillSupervisor(
        config=config,
        coverage_reader=lambda: {"coverage_ready": False},
    )
    captured = {}

    def capture_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(
        supervisor_module.subprocess,
        "Popen",
        capture_popen,
    )

    assert supervisor._launch_worker() is True
    command = captured["command"]
    assert "--required-minutes" in command

    parsed = worker.parse_args(command[2:])

    assert parsed.required_minutes == 172800


def test_worker_command_uses_configured_mode_and_no_download(
    tmp_path,
    monkeypatch,
) -> None:
    config = replace(
        _config(tmp_path),
        worker_mode="live",
        no_download=True,
    )
    supervisor = TradeFeatureBackfillSupervisor(
        config=config,
        coverage_reader=lambda: {"coverage_ready": False},
    )
    captured = {}

    def capture_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(
        supervisor_module.subprocess,
        "Popen",
        capture_popen,
    )

    assert supervisor._launch_worker() is True
    command = captured["command"]
    parsed = worker.parse_args(command[2:])

    assert parsed.mode == "live"
    assert parsed.no_download is True


def test_default_supervisor_launches_live_recent_to_oldest_no_once(
    tmp_path,
    monkeypatch,
) -> None:
    """Default config must launch a live recent-to-oldest no-once worker
    with BACKGROUND_BACKFILL_PRIORITY and without --no-download."""
    config = _config(tmp_path)
    supervisor = TradeFeatureBackfillSupervisor(
        config=config,
        coverage_reader=lambda: {"coverage_ready": False},
    )
    captured = {}

    def capture_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(
        supervisor_module.subprocess,
        "Popen",
        capture_popen,
    )

    assert supervisor._launch_worker() is True
    command = captured["command"]
    parsed = worker.parse_args(command[2:])

    assert parsed.mode == "live"
    assert parsed.direction == "recent-to-oldest"
    assert parsed.once is False
    assert parsed.no_download is False
    assert parsed.global_lock_priority == BACKGROUND_BACKFILL_PRIORITY
    assert "--no-once" in command
    assert "--no-download" not in command
