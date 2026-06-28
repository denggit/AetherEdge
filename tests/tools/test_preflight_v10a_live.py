from __future__ import annotations

from pathlib import Path

import pytest

from tools.preflight_v10a_live import (
    EXPECTED_STRATEGY,
    render_report,
    run_preflight,
)


BASE_ENV = {
    "AETHER_RUNTIME_MODE": "live_runtime",
    "AETHER_MARKET": "ETH-USDT-PERP",
    "AETHER_EXCHANGES": "okx,binance",
    "AETHER_DATA_EXCHANGE": "okx",
    "AETHER_MASTER_EXCHANGE": "okx",
    "AETHER_FOLLOWER_EXCHANGES": "binance",
    "AETHER_STRATEGY": EXPECTED_STRATEGY,
    "AETHER_DATA_STREAMS": "trades",
    "AETHER_WARMUP_ENABLED": "true",
    "AETHER_CLOSED_BAR_INTERVAL": "4h",
    "AETHER_CLOSED_BAR_BUFFER_MS": "5000",
    "AETHER_RANGE_PCT": "0.002",
    "AETHER_SCHEDULER_POLL_SECONDS": "1.0",
    "AETHER_PRODUCER_STALE_TIMEOUT_MS": "60000",
    "AETHER_DRY_RUN": "false",
    "AETHER_LIVE_TRADING": "true",
    "OKX_SANDBOX": "false",
    "BINANCE_SANDBOX": "false",
    "MARGIN_MODE": "isolated",
    "OKX_LEVERAGE": "10",
    "BINANCE_LEVERAGE": "10",
}


def _run(tmp_path: Path, overrides: dict[str, str] | None = None):
    values = {**BASE_ENV, **(overrides or {})}
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )
    return run_preflight(env_file=env_file, environ={}, repo_root=tmp_path)


def _one(report, name: str):
    matches = report.named(name)
    assert len(matches) == 1
    return matches[0]


def test_correct_env_returns_pass(tmp_path: Path) -> None:
    report = _run(tmp_path)

    assert report.ok is True
    assert "PASS_READY_FOR_MANUAL_LIVE_START" in render_report(report)


def test_wrong_strategy_fails(tmp_path: Path) -> None:
    report = _run(tmp_path, {"AETHER_STRATEGY": "strategies.eth_lf_portfolio_v9c:Strategy"})

    assert report.ok is False
    assert _one(report, "AETHER_STRATEGY").status == "FAIL"


def test_dry_run_true_fails(tmp_path: Path) -> None:
    report = _run(tmp_path, {"AETHER_DRY_RUN": "true"})

    assert report.ok is False
    assert _one(report, "AETHER_DRY_RUN").status == "FAIL"


def test_live_trading_false_fails(tmp_path: Path) -> None:
    report = _run(tmp_path, {"AETHER_LIVE_TRADING": "false"})

    assert report.ok is False
    assert _one(report, "AETHER_LIVE_TRADING").status == "FAIL"


@pytest.mark.parametrize(
    "strategy_key",
    [
        "enable_momentum_long_not_aligned_block",
        "range_speed_min_periods",
        "global_risk_scale",
        "range_exit",
    ],
)
def test_strategy_parameter_in_env_fails(tmp_path: Path, strategy_key: str) -> None:
    report = _run(tmp_path, {strategy_key: "true"})

    assert report.ok is False
    check = _one(report, "strategy_params_absent_from_env")
    assert check.status == "FAIL"
    assert strategy_key in check.detail


def test_nonstandard_closed_bar_buffer_warns(tmp_path: Path) -> None:
    report = _run(tmp_path, {"AETHER_CLOSED_BAR_BUFFER_MS": "1000"})

    assert report.ok is True
    assert _one(report, "AETHER_CLOSED_BAR_BUFFER_MS").status == "WARN"


def test_leverage_mismatch_fails(tmp_path: Path) -> None:
    report = _run(tmp_path, {"OKX_LEVERAGE": "15", "BINANCE_LEVERAGE": "10"})

    assert report.ok is False
    assert _one(report, "leverage_match").status == "FAIL"


def test_leverage_15_warns(tmp_path: Path) -> None:
    report = _run(tmp_path, {"OKX_LEVERAGE": "15", "BINANCE_LEVERAGE": "15"})

    assert report.ok is True
    assert _one(report, "configured_leverage").status == "WARN"


def test_strategy_config_checks_pass(tmp_path: Path) -> None:
    report = _run(tmp_path)

    for name in (
        "strategy_load",
        "strategy_id",
        "v10_long_block_enabled",
        "v10a_short_speed_block_enabled",
        "range_speed_rolling_window_bars",
        "range_speed_min_periods",
        "range_speed_fast_quantile",
    ):
        assert _one(report, name).status == "PASS"


def test_runtime_requirements_checks_pass(tmp_path: Path) -> None:
    report = _run(tmp_path)

    for name in (
        "closed_kline.enabled",
        "closed_kline.interval",
        "trades.enabled",
        "trades.stream_enabled",
        "range_bars.enabled",
        "range_bars.range_pct",
        "range_bars.aggregate_interval",
        "account_state.poll_enabled",
        "order_state.poll_when_position_enabled",
        "env_runtime_alignment",
    ):
        assert _one(report, name).status == "PASS"


def test_exchange_read_is_explicitly_skipped(tmp_path: Path) -> None:
    report = _run(tmp_path)

    assert _one(report, "EXCHANGE_READ_CHECK_SKIPPED").status == "SKIPPED"
    output = render_report(report)
    assert "no existing ETH-USDT-PERP positions" in output
    assert "no stale open orders" in output
    assert "no stale stop orders" in output


def test_tool_has_no_exchange_or_order_mutation_entrypoints() -> None:
    source = (
        Path(__file__).resolve().parents[2] / "tools" / "preflight_v10a_live.py"
    ).read_text(encoding="utf-8")

    for forbidden in (
        "create_account_client",
        "create_execution_client",
        "fetch_platform_snapshot",
        "build_app_context",
        "place_order",
        "cancel_order",
        "send_order",
    ):
        assert forbidden not in source
