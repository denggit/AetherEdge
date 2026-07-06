from __future__ import annotations

import json

import pytest

from scripts.live_launch_gate import (
    live_reports_required,
    validate_live_launch_reports,
)
from src.app import AppConfig
from src.platform import ExchangeName


NOW_MS = 1_800_000_000_000


def _app(
    *,
    strategy: str = "strategies.eth_portfolio_v1:Strategy",
) -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        strategy=strategy,
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=False,
        enable_email_alerts=False,
    )


def _report(kind: str) -> dict:
    return {
        "generated_at_ms": NOW_MS - 1_000,
        "report_kind": kind,
        "strategy": "eth_portfolio_v1",
        "symbol": "ETH-USDT-PERP",
        "runtime_mode": "live_runtime",
        "exchanges": ["okx", "binance"],
        "data_exchange": "okx",
        "ok": True,
        "verdict": "pass",
        "exit_code": 0,
        "mutation_attempted": False,
        "startup_gate_results": [
            {"name": "direct_live_startup_gates", "status": "ok"}
        ],
    }


def _write_reports(tmp_path, *, preflight=None, smoke=None):
    preflight_path = tmp_path / "preflight.json"
    smoke_path = tmp_path / "smoke.json"
    if preflight is not None:
        preflight_path.write_text(
            json.dumps(preflight),
            encoding="utf-8",
        )
    if smoke is not None:
        smoke_path.write_text(json.dumps(smoke), encoding="utf-8")
    return preflight_path, smoke_path


def _validate(tmp_path, *, preflight=None, smoke=None):
    preflight_path, smoke_path = _write_reports(
        tmp_path,
        preflight=_report("preflight") if preflight is None else preflight,
        smoke=_report("smoke") if smoke is None else smoke,
    )
    return validate_live_launch_reports(
        app_config=_app(),
        preflight_report_path=preflight_path,
        smoke_report_path=smoke_path,
        max_age_seconds=600,
        now_ms=NOW_MS,
    )


def test_valid_reports_allow_live_bootstrap(tmp_path) -> None:
    assert _validate(tmp_path).ok is True


def test_missing_report_blocks_live_bootstrap(tmp_path) -> None:
    smoke_path = tmp_path / "smoke.json"
    smoke_path.write_text(json.dumps(_report("smoke")), encoding="utf-8")

    result = validate_live_launch_reports(
        app_config=_app(),
        preflight_report_path=tmp_path / "missing.json",
        smoke_report_path=smoke_path,
        now_ms=NOW_MS,
    )

    assert result.ok is False
    assert "preflight_report_missing_or_invalid" in result.issues


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    (
        ("ok", False, "preflight_report_not_ok"),
        ("mutation_attempted", True, "preflight_mutation_attempted"),
        ("strategy", "eth_lf_portfolio_v10b", "preflight_strategy_mismatch"),
        ("symbol", "BTC-USDT-PERP", "preflight_symbol_mismatch"),
        ("exchanges", ["okx"], "preflight_exchanges_mismatch"),
        ("data_exchange", "binance", "preflight_data_exchange_mismatch"),
    ),
)
def test_report_mismatch_blocks_live_bootstrap(
    tmp_path,
    field,
    value,
    expected,
) -> None:
    preflight = _report("preflight")
    preflight[field] = value

    result = _validate(tmp_path, preflight=preflight)

    assert result.ok is False
    assert expected in result.issues


def test_stale_report_blocks_live_bootstrap(tmp_path) -> None:
    preflight = _report("preflight")
    preflight["generated_at_ms"] = NOW_MS - 601_000

    result = _validate(tmp_path, preflight=preflight)

    assert "preflight_report_stale" in result.issues


def test_failed_startup_gate_blocks_live_bootstrap(tmp_path) -> None:
    smoke = _report("smoke")
    smoke["startup_gate_results"][0]["status"] = "fail"

    result = _validate(tmp_path, smoke=smoke)

    assert "smoke_startup_gate_failed" in result.issues


def test_non_live_mode_does_not_require_reports() -> None:
    assert (
        live_reports_required(
            runtime_mode="legacy_app",
            strategy="strategies.eth_portfolio_v1:Strategy",
            configured=True,
        )
        is False
    )
