from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pytest

from tools.preflight_v10a_live import (
    EXPECTED_STRATEGY,
    main,
    parse_args,
    render_report,
    run_preflight,
    write_json_report,
)


BASE_ENV = {
    "AETHER_RUNTIME_MODE": "live_runtime",
    "AETHER_MARKET": "ETH-USDT-PERP",
    "AETHER_EXCHANGES": "okx,binance",
    "AETHER_DATA_EXCHANGE": "okx",
    "AETHER_MASTER_EXCHANGE": "okx",
    "AETHER_FOLLOWER_EXCHANGES": "binance",
    "AETHER_ENTRY_DEVIATION_ALERT_PCT": "0.005",
    "AETHER_FOLLOWER_ENTRY_MAX_ATTEMPTS": "3",
    "AETHER_FOLLOWER_ENTRY_RETRY_DELAY_SECONDS": "10",
    "AETHER_MASTER_ENTRY_MAX_ATTEMPTS": "3",
    "AETHER_MASTER_ENTRY_RETRY_DELAY_SECONDS": "10",
    "AETHER_MASTER_FAIL_MANUAL_GRACE_SECONDS": "1800",
    "AETHER_CLOSE_ORPHAN_FOLLOWER_AFTER_GRACE": "true",
    "AETHER_DO_NOT_REJOIN_MID_POSITION_AFTER_FOLLOWER_DESYNC": "true",
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
    "AETHER_STATE_DB": "data/state/test_state.sqlite3",
    "AETHER_ORDER_JOURNAL_DB": "data/state/test_order_journal.sqlite3",
    "OKX_API_KEY": "test-okx-api-key",
    "OKX_SECRET_KEY": "test-okx-secret-key",
    "OKX_PASSPHRASE": "test-okx-passphrase",
    "BINANCE_API_KEY": "test-binance-api-key",
    "BINANCE_SECRET_KEY": "test-binance-secret-key",
}


def _write_env(
    tmp_path: Path,
    overrides: dict[str, str] | None = None,
    *,
    remove: tuple[str, ...] = (),
) -> tuple[Path, dict[str, str]]:
    values = {**BASE_ENV, **(overrides or {})}
    for key in remove:
        values.pop(key, None)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )
    return env_file, values


def _run(
    tmp_path: Path,
    overrides: dict[str, str] | None = None,
    *,
    remove: tuple[str, ...] = (),
    expect_real_live: bool = True,
    check_exchange_read: bool = False,
):
    env_file, _ = _write_env(tmp_path, overrides, remove=remove)
    return run_preflight(
        env_file=env_file,
        environ={},
        repo_root=tmp_path,
        expect_real_live=expect_real_live,
        check_exchange_read=check_exchange_read,
    )


def _one(report, name: str):
    matches = report.named(name)
    assert len(matches) == 1
    return matches[0]


def test_new_cli_arguments_parse() -> None:
    args = parse_args(
        [
            "--expect-real-live",
            "--report",
            "data/state/report.json",
            "--env-file",
            ".env",
            "--check-exchange-read",
        ]
    )

    assert args.expect_real_live is True
    assert args.report == "data/state/report.json"
    assert args.env_file == ".env"
    assert args.check_exchange_read is True


def test_report_argument_writes_json_and_creates_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file, values = _write_env(tmp_path)
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    report_path = tmp_path / "nested" / "v10a-report.json"

    exit_code = main(
        [
            "--expect-real-live",
            "--env-file",
            str(env_file),
            "--report",
            str(report_path),
        ]
    )

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["final_status"] == "PASS_READY_FOR_MANUAL_LIVE_START"
    assert payload["expect_real_live"] is True
    assert payload["env_file"] == str(env_file)
    assert isinstance(payload["checks"], list)
    assert isinstance(payload["manual_checklist"], list)
    assert payload["fail_count"] == 0
    assert payload["skipped_count"] == 1
    assert payload["generated_at"].endswith("Z")


def test_report_redacts_secret_values(tmp_path: Path) -> None:
    secret = "DO-NOT-LEAK-THIS-SECRET"
    env_file, _ = _write_env(
        tmp_path,
        {
            "OKX_API_KEY": secret,
            "OKX_SECRET_KEY": "another-secret",
            "EMAIL_PASSWORD": "mail-secret",
        },
    )
    report = run_preflight(
        env_file=env_file,
        environ={},
        repo_root=tmp_path,
        expect_real_live=True,
    )
    report.add("ENV", "WARN", "redaction_probe", f"value={secret}")
    report_path = tmp_path / "report.json"

    write_json_report(report_path, report)

    raw = report_path.read_text(encoding="utf-8")
    assert secret not in raw
    assert "another-secret" not in raw
    assert "mail-secret" not in raw
    assert "<redacted>" in raw


def test_correct_real_live_env_returns_pass(tmp_path: Path) -> None:
    report = _run(tmp_path)

    assert report.ok is True
    assert report.expect_real_live is True
    assert "PASS_READY_FOR_MANUAL_LIVE_START" in render_report(report)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("AETHER_DRY_RUN", "true"),
        ("AETHER_LIVE_TRADING", "false"),
    ],
)
def test_real_live_safety_switch_mismatch_fails(
    tmp_path: Path,
    key: str,
    value: str,
) -> None:
    report = _run(tmp_path, {key: value})

    assert report.ok is False
    assert _one(report, key).status == "FAIL"


def test_wrong_strategy_fails(tmp_path: Path) -> None:
    report = _run(
        tmp_path,
        {"AETHER_STRATEGY": "strategies.eth_lf_portfolio_v9c:Strategy"},
    )

    assert report.ok is False
    assert _one(report, "AETHER_STRATEGY").status == "FAIL"


@pytest.mark.parametrize(
    "strategy_key",
    [
        "enable_momentum_long_not_aligned_block",
        "enable_momentum_short_fast_speed_block",
        "range_speed_rolling_window_bars",
        "range_speed_min_periods",
        "range_speed_fast_quantile",
        "global_risk_scale",
        "range_exit",
        "micro_context",
        "bull_reclaim",
        "momentum_v3",
        "bear_v3",
    ],
)
def test_strategy_parameter_in_env_fails(
    tmp_path: Path,
    strategy_key: str,
) -> None:
    report = _run(tmp_path, {strategy_key: "true"})

    check = _one(report, "strategy_params_absent_from_env")
    assert report.ok is False
    assert check.status == "FAIL"
    assert strategy_key in check.detail


def test_leverage_mismatch_fails(tmp_path: Path) -> None:
    report = _run(tmp_path, {"OKX_LEVERAGE": "15", "BINANCE_LEVERAGE": "10"})

    assert report.ok is False
    assert _one(report, "leverage_match").status == "FAIL"


def test_leverage_15_warns_and_adds_manual_confirmation(tmp_path: Path) -> None:
    report = _run(tmp_path, {"OKX_LEVERAGE": "15", "BINANCE_LEVERAGE": "15"})

    assert report.ok is True
    assert _one(report, "configured_leverage").status == "WARN"
    assert any("15x" in item for item in report.manual_checklist)


@pytest.mark.parametrize("value", [None, "not-a-number", "-0.1"])
def test_entry_deviation_missing_or_invalid_fails(
    tmp_path: Path,
    value: str | None,
) -> None:
    if value is None:
        report = _run(tmp_path, remove=("AETHER_ENTRY_DEVIATION_ALERT_PCT",))
    else:
        report = _run(tmp_path, {"AETHER_ENTRY_DEVIATION_ALERT_PCT": value})

    assert _one(report, "AETHER_ENTRY_DEVIATION_ALERT_PCT").status == "FAIL"


def test_entry_deviation_nonrecommended_value_warns(tmp_path: Path) -> None:
    report = _run(tmp_path, {"AETHER_ENTRY_DEVIATION_ALERT_PCT": "0.01"})

    assert report.ok is True
    assert _one(report, "AETHER_ENTRY_DEVIATION_ALERT_PCT").status == "WARN"


def test_entry_deviation_recommended_value_passes(tmp_path: Path) -> None:
    report = _run(tmp_path)

    assert _one(report, "AETHER_ENTRY_DEVIATION_ALERT_PCT").status == "PASS"


@pytest.mark.parametrize("key", ["AETHER_STATE_DB", "AETHER_ORDER_JOURNAL_DB"])
def test_state_db_path_missing_fails(tmp_path: Path, key: str) -> None:
    report = _run(tmp_path, remove=(key,))

    assert report.ok is False
    assert _one(report, key).status == "FAIL"


def test_existing_state_db_warns_backup_manually(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE harmless (id INTEGER)")
    report = _run(tmp_path, {"AETHER_STATE_DB": "state.sqlite3"})

    check = _one(report, "AETHER_STATE_DB")
    assert report.ok is True
    assert check.status == "WARN"
    assert "backup manually" in check.detail
    assert _one(report, "state_db_pending_orders").status == "SKIPPED"


def test_pending_state_order_fails_read_only_inspection(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE orders (status TEXT NOT NULL)")
        connection.execute("INSERT INTO orders(status) VALUES ('new')")
    before = db_path.read_bytes()
    report = _run(tmp_path, {"AETHER_STATE_DB": "state.sqlite3"})

    assert report.ok is False
    assert _one(report, "state_db_pending_orders").status == "FAIL"
    assert db_path.read_bytes() == before


def test_pending_journal_intent_fails_read_only_inspection(tmp_path: Path) -> None:
    db_path = tmp_path / "journal.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE order_intents (status TEXT NOT NULL)")
        connection.execute("INSERT INTO order_intents(status) VALUES ('submitted')")
    report = _run(
        tmp_path,
        {"AETHER_ORDER_JOURNAL_DB": "journal.sqlite3"},
    )

    assert report.ok is False
    assert _one(report, "journal_pending_intents").status == "FAIL"


def test_exchange_read_default_is_explicitly_skipped(tmp_path: Path) -> None:
    report = _run(tmp_path)

    check = _one(report, "EXCHANGE_READ_CHECK_SKIPPED")
    assert check.status == "SKIPPED"
    assert "not requested" in check.detail
    output = render_report(report)
    assert "Confirm OKX has no ETH-USDT-SWAP position" in output
    assert "Confirm Binance has no stale open/stop orders" in output


def test_requested_exchange_read_stays_safe_and_skipped(tmp_path: Path) -> None:
    report = _run(tmp_path, check_exchange_read=True)

    check = _one(report, "EXCHANGE_READ_CHECK_SKIPPED")
    assert check.status == "SKIPPED"
    assert "no isolated read-only V10A adapter" in check.detail


def test_strategy_and_runtime_requirement_checks_pass(tmp_path: Path) -> None:
    report = _run(tmp_path)

    for name in (
        "strategy_load",
        "strategy_id",
        "enable_momentum_long_not_aligned_block",
        "enable_momentum_short_fast_speed_block",
        "range_speed_rolling_window_bars",
        "range_speed_min_periods",
        "range_speed_fast_quantile",
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


def test_nonstandard_closed_bar_buffer_warns(tmp_path: Path) -> None:
    report = _run(tmp_path, {"AETHER_CLOSED_BAR_BUFFER_MS": "1000"})

    assert report.ok is True
    assert _one(report, "AETHER_CLOSED_BAR_BUFFER_MS").status == "WARN"


def _create_range_state_db(
    tmp_path: Path,
    *,
    complete_history: int,
    current_checkpoint: bool,
) -> None:
    path = tmp_path / "data" / "state" / "range_builder_checkpoint.sqlite3"
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE completed_range_aggregates (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                range_pct TEXT NOT NULL,
                bucket_start_ms INTEGER NOT NULL,
                bucket_end_ms INTEGER NOT NULL,
                rf_bar_count INTEGER NOT NULL,
                imbalance TEXT,
                close_pos TEXT,
                taker_buy_ratio TEXT,
                micro_return_pct TEXT,
                delta_notional_sum TEXT,
                notional_sum TEXT,
                coverage_status TEXT NOT NULL,
                missing_gap_ms INTEGER NOT NULL DEFAULT 0,
                completed_at_ms INTEGER NOT NULL,
                PRIMARY KEY (exchange, symbol, range_pct, bucket_end_ms)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE range_builder_checkpoints (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                range_pct TEXT NOT NULL,
                bucket_start_ms INTEGER NOT NULL,
                bucket_end_ms INTEGER NOT NULL,
                last_trade_id TEXT,
                last_trade_ts_ms INTEGER,
                last_ws_recv_ts_ms INTEGER,
                range_bar_count INTEGER NOT NULL,
                aggregate_json TEXT NOT NULL,
                builder_state_json TEXT NOT NULL,
                coverage_status TEXT NOT NULL,
                missing_gap_ms INTEGER NOT NULL DEFAULT 0,
                checkpoint_updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (exchange, symbol, range_pct, bucket_start_ms)
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO completed_range_aggregates (
                exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                rf_bar_count, coverage_status, completed_at_ms
            ) VALUES ('okx', 'ETH-USDT-PERP', '0.002', ?, ?, 10, 'COMPLETE', ?)
            """,
            [
                (index * 100, index * 100 + 99, index * 100 + 100)
                for index in range(complete_history)
            ],
        )
        if current_checkpoint:
            now_ms = int(time.time() * 1000)
            bucket_ms = 4 * 60 * 60 * 1000
            bucket_start_ms = (now_ms // bucket_ms) * bucket_ms
            connection.execute(
                """
                INSERT INTO range_builder_checkpoints (
                    exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                    range_bar_count, aggregate_json, builder_state_json,
                    coverage_status, checkpoint_updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "okx",
                    "ETH-USDT-PERP",
                    "0.002",
                    bucket_start_ms,
                    bucket_start_ms + bucket_ms - 1,
                    10,
                    "{}",
                    "{}",
                    "COMPLETE",
                    now_ms,
                ),
            )


def test_range_history_at_min_periods_passes(tmp_path: Path) -> None:
    _create_range_state_db(
        tmp_path, complete_history=100, current_checkpoint=False
    )

    report = _run(tmp_path)

    assert _one(report, "completed_range_aggregate_history_count").status == "PASS"
    assert _one(report, "complete_range_history_min_periods").status == "PASS"


def test_range_history_below_min_periods_warns(tmp_path: Path) -> None:
    _create_range_state_db(
        tmp_path, complete_history=99, current_checkpoint=False
    )

    report = _run(tmp_path)

    check = _one(report, "complete_range_history_min_periods")
    assert check.status == "WARN"
    assert "unavailable until range history reaches min_periods" in check.detail


def test_no_current_range_checkpoint_warns_cold_start(tmp_path: Path) -> None:
    _create_range_state_db(
        tmp_path, complete_history=100, current_checkpoint=False
    )

    report = _run(tmp_path)

    check = _one(report, "current_bucket_checkpoint")
    assert check.status == "WARN"
    assert "COLD_START_PARTIAL" in check.detail


def test_fresh_current_range_checkpoint_is_recoverable(tmp_path: Path) -> None:
    _create_range_state_db(
        tmp_path, complete_history=100, current_checkpoint=True
    )

    report = _run(tmp_path)

    check = _one(report, "current_bucket_checkpoint")
    assert check.status == "PASS"
    assert "RECOVERED_DEGRADED_MINOR" in check.detail
    assert _one(report, "current_bucket_checkpoint_age_ms").status == "PASS"


def test_tool_source_has_no_forbidden_write_operation() -> None:
    source = (
        Path(__file__).resolve().parents[2] / "tools" / "preflight_v10a_live.py"
    ).read_text(encoding="utf-8")

    for forbidden in (
        "place_order",
        "cancel_order",
        "create_order",
        "set_leverage",
        "set_margin_mode",
        "close_position",
        "build_app_context",
        "live_runtime start",
    ):
        assert forbidden not in source


# ---------------------------------------------------------------------------
# SQLite read-only inspection – immutable=1 removal
# ---------------------------------------------------------------------------


def test_sqlite_read_only_inspection_does_not_use_immutable() -> None:
    source = (
        Path(__file__).resolve().parents[2] / "tools" / "preflight_v10a_live.py"
    ).read_text(encoding="utf-8")
    assert "immutable=1" not in source


def test_wal_uncheckpointed_pending_rows_visible_in_read_only(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "wal_state.sqlite3"

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("CREATE TABLE orders (status TEXT NOT NULL)")
        conn.execute("INSERT INTO orders(status) VALUES ('new')")

    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO orders(status) VALUES ('partially_filled')")

    wal_path = Path(str(db_path) + "-wal")
    assert wal_path.exists(), "WAL file must exist for this test to be meaningful"

    before = db_path.read_bytes()
    report = _run(tmp_path, {"AETHER_STATE_DB": "wal_state.sqlite3"})

    assert not report.ok
    check = _one(report, "state_db_pending_orders")
    assert check.status == "FAIL"
    assert "2" in check.detail
    assert db_path.read_bytes() == before


def test_read_only_inspection_preserves_db_bytes(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE orders (status TEXT NOT NULL)")
        connection.execute("INSERT INTO orders(status) VALUES ('new')")
    before = db_path.read_bytes()
    report = _run(tmp_path, {"AETHER_STATE_DB": "state.sqlite3"})

    assert report.ok is False
    assert _one(report, "state_db_pending_orders").status == "FAIL"
    assert db_path.read_bytes() == before


# ---------------------------------------------------------------------------
# Credentials presence check
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "missing_key",
    [
        "OKX_API_KEY",
        "OKX_SECRET_KEY",
        "OKX_PASSPHRASE",
        "BINANCE_API_KEY",
        "BINANCE_SECRET_KEY",
    ],
)
def test_missing_credential_fails(tmp_path: Path, missing_key: str) -> None:
    report = _run(tmp_path, remove=(missing_key,))

    check = _one(report, f"{missing_key} missing")
    assert check.status == "FAIL"
    assert check.section == "CREDENTIALS"


def test_credentials_present_pass(tmp_path: Path) -> None:
    report = _run(tmp_path)

    for key in (
        "OKX_API_KEY",
        "OKX_SECRET_KEY",
        "OKX_PASSPHRASE",
        "BINANCE_API_KEY",
        "BINANCE_SECRET_KEY",
    ):
        check = _one(report, f"{key} present")
        assert check.status == "PASS"
        assert check.section == "CREDENTIALS"


def test_credentials_no_secret_in_report_or_stdout(tmp_path: Path) -> None:
    secret = "MY-SECRET-API-KEY-DO-NOT-LEAK"
    creds = {
        "OKX_API_KEY": secret,
        "OKX_SECRET_KEY": "another-secret-value",
        "OKX_PASSPHRASE": "passphrase-secret",
        "BINANCE_API_KEY": "binance-key-secret",
        "BINANCE_SECRET_KEY": "binance-secret-secret",
    }
    report = run_preflight(
        env_file=_write_env(tmp_path, creds)[0],
        environ={},
        repo_root=tmp_path,
        expect_real_live=True,
    )

    # stdout must not leak secrets
    output = render_report(report)
    assert secret not in output
    assert "another-secret-value" not in output
    assert "passphrase-secret" not in output
    assert "binance-key-secret" not in output
    assert "binance-secret-secret" not in output

    # JSON report must not leak secrets
    report_path = tmp_path / "report.json"
    write_json_report(report_path, report)
    raw = report_path.read_text(encoding="utf-8")
    assert secret not in raw
    assert "another-secret-value" not in raw
    assert "passphrase-secret" not in raw
    assert "binance-key-secret" not in raw
    assert "binance-secret-secret" not in raw


def test_email_alert_enabled_missing_email_fields_fails(tmp_path: Path) -> None:
    creds = {"AETHER_ENABLE_EMAIL_ALERT": "true"}
    report = _run(tmp_path, creds)

    for key in ("EMAIL_SENDER", "EMAIL_PASSWORD", "EMAIL_RECEIVER"):
        check = _one(report, f"{key} missing")
        assert check.status == "FAIL"
        assert check.section == "CREDENTIALS"


def test_email_alert_disabled_missing_email_fields_ok(tmp_path: Path) -> None:
    creds = {"AETHER_ENABLE_EMAIL_ALERT": "false"}
    report = _run(tmp_path, creds)

    for key in ("EMAIL_SENDER", "EMAIL_PASSWORD", "EMAIL_RECEIVER"):
        assert not report.named(f"{key} present")
        assert not report.named(f"{key} missing")
