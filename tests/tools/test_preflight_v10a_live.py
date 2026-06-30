from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pytest

from tools.preflight_v10a_live import (
    EXPECTED_STRATEGY,
    PreflightReport,
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
    skip_exchange_read: bool = True,
):
    env_file, _ = _write_env(tmp_path, overrides, remove=remove)
    return run_preflight(
        env_file=env_file,
        environ={},
        repo_root=tmp_path,
        expect_real_live=expect_real_live,
        check_exchange_read=check_exchange_read,
        skip_exchange_read=skip_exchange_read,
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
    assert args.skip_exchange_read is False


def test_skip_exchange_read_cli_flag_parse() -> None:
    args = parse_args(
        [
            "--expect-real-live",
            "--skip-exchange-read",
        ]
    )
    assert args.expect_real_live is True
    assert args.skip_exchange_read is True
    assert args.check_exchange_read is False


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
            "--skip-exchange-read",
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


def test_leverage_15_passes_as_expected_buffer(tmp_path: Path) -> None:
    report = _run(tmp_path, {"OKX_LEVERAGE": "15", "BINANCE_LEVERAGE": "15"})

    assert report.ok is True
    check = _one(report, "configured_leverage")
    assert check.status == "PASS"
    assert "15" in check.detail
    assert "expected buffer" in check.detail
    assert not any("Confirm leverage" in item for item in report.manual_checklist)


def test_leverage_below_12_warns(tmp_path: Path) -> None:
    report = _run(tmp_path, {"OKX_LEVERAGE": "10", "BINANCE_LEVERAGE": "10"})

    check = _one(report, "configured_leverage")
    assert check.status == "WARN"
    assert "below expected strategy max leverage buffer" in check.detail


def test_leverage_above_20_warns(tmp_path: Path) -> None:
    report = _run(tmp_path, {"OKX_LEVERAGE": "25", "BINANCE_LEVERAGE": "25"})

    check = _one(report, "configured_leverage")
    assert check.status == "WARN"
    assert "unusually high leverage" in check.detail


def test_leverage_12_passes_as_expected_buffer(tmp_path: Path) -> None:
    report = _run(tmp_path, {"OKX_LEVERAGE": "12", "BINANCE_LEVERAGE": "12"})

    check = _one(report, "configured_leverage")
    assert check.status == "PASS"
    assert "expected buffer" in check.detail


def test_leverage_20_passes_as_expected_buffer(tmp_path: Path) -> None:
    report = _run(tmp_path, {"OKX_LEVERAGE": "20", "BINANCE_LEVERAGE": "20"})

    check = _one(report, "configured_leverage")
    assert check.status == "PASS"
    assert "expected buffer" in check.detail


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


def test_existing_state_db_creates_central_backup(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "state.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE harmless (id INTEGER)")
    report = _run(tmp_path, {"AETHER_STATE_DB": "state.sqlite3"})

    output = capsys.readouterr().out
    check = _one(report, "AETHER_STATE_DB")
    backups = sorted((tmp_path / "data" / "state" / "backups").glob("state.*.sqlite3"))
    assert report.ok is True
    assert check.status == "WARN"
    assert "backup created at" in check.detail
    assert len(backups) == 1
    assert f"backup={backups[0]}" in output
    assert not list((tmp_path / "data" / "state" / "backups").glob("*-wal"))
    assert not list((tmp_path / "data" / "state" / "backups").glob("*-shm"))
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
    assert "explicitly skipped" in check.detail
    output = render_report(report)
    assert "Confirm OKX has no ETH-USDT-SWAP position" in output
    assert "Confirm Binance has no stale open/stop orders" in output


def test_skip_exchange_read_with_flag(tmp_path: Path) -> None:
    report = _run(tmp_path, skip_exchange_read=True)

    check = _one(report, "EXCHANGE_READ_CHECK_SKIPPED")
    assert check.status == "SKIPPED"
    assert "explicitly skipped by --skip-exchange-read" in check.detail


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


# ---------------------------------------------------------------------------
# Fake exchange clients for testing exchange read checks
# ---------------------------------------------------------------------------


class FakeExchangeClient:
    """Fake read-only exchange client for preflight testing.

    Tracks any write-method calls so tests can assert none were made.
    """

    def __init__(
        self,
        exchange_name: str = "okx",
        *,
        balance_available: float = 1000,
        positions: list | None = None,
        open_orders: list | None = None,
        open_stop_orders: list | None = None,
        leverage: float | None = None,
        margin_mode: str | None = None,
        position_mode: str = "one_way",
        raise_on: dict | None = None,
    ):
        from decimal import Decimal

        from src.platform.exchanges.models import (
            Balance,
            ExchangeName,
            LeverageInfo,
            MarginMode,
            PositionMode,
        )

        self._exchange = ExchangeName(str(exchange_name).strip().lower())
        self._balance = Balance(
            exchange=self._exchange,
            asset="USDT",
            total=Decimal(str(balance_available)),
            available=Decimal(str(balance_available)),
        )
        self._positions = list(positions or [])
        self._open_orders = list(open_orders or [])
        self._open_stop_orders = list(open_stop_orders or [])
        self._leverage_info = LeverageInfo(
            exchange=self._exchange,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-PERP",
            leverage=Decimal(str(leverage)) if leverage is not None else None,
            margin_mode=MarginMode(margin_mode) if margin_mode else None,
        )
        self._position_mode = PositionMode(position_mode)
        self._raise_on = dict(raise_on or {})
        self.write_calls: list[tuple[str, object]] = []
        self.fetch_leverage_calls: list[object] = []

    @property
    def exchange(self):
        return self._exchange

    # -- Read methods -------------------------------------------------------

    async def fetch_balance(self, asset: str = "USDT"):
        self._check_raise("fetch_balance")
        return self._balance

    async def fetch_positions(self, symbol=None):
        self._check_raise("fetch_positions")
        return list(self._positions)

    async def fetch_open_orders(self, symbol: str):
        self._check_raise("fetch_open_orders")
        return list(self._open_orders)

    async def fetch_open_stop_orders(self, symbol: str):
        self._check_raise("fetch_open_stop_orders")
        return list(self._open_stop_orders)

    async def fetch_leverage(self, symbol: str, *, margin_mode=None):
        self._check_raise("fetch_leverage")
        self.fetch_leverage_calls.append(margin_mode)
        return self._leverage_info

    async def fetch_position_mode(self):
        self._check_raise("fetch_position_mode")
        return self._position_mode

    # -- Write methods (tracked, must never be called) ----------------------

    async def place_order(self, request):
        self.write_calls.append(("place_order", request))

    async def place_stop_market_order(self, request):
        self.write_calls.append(("place_stop_market_order", request))

    async def cancel_order(self, request):
        self.write_calls.append(("cancel_order", request))

    async def cancel_all_orders(self, symbol):
        self.write_calls.append(("cancel_all_orders", symbol))

    async def cancel_stop_order(self, request):
        self.write_calls.append(("cancel_stop_order", request))

    async def cancel_all_stop_orders(self, symbol):
        self.write_calls.append(("cancel_all_stop_orders", symbol))

    async def set_leverage(self, request):
        self.write_calls.append(("set_leverage", request))

    async def set_margin_mode(self, symbol, margin_mode):
        self.write_calls.append(("set_margin_mode", (symbol, margin_mode)))

    async def set_position_mode(self, mode):
        self.write_calls.append(("set_position_mode", mode))

    async def amend_order(self, request):
        self.write_calls.append(("amend_order", request))

    def _check_raise(self, method: str) -> None:
        exc = self._raise_on.get(method)
        if exc is not None:
            raise exc


def _make_position(exchange_name="okx", symbol="ETH-USDT-PERP", quantity=1.0):
    from decimal import Decimal

    from src.platform.exchanges.models import Position, PositionSide

    return Position(
        exchange=exchange_name,
        symbol=symbol,
        raw_symbol=symbol,
        side=PositionSide.SHORT,
        quantity=Decimal(str(quantity)),
    )


def _make_order(exchange_name="okx", symbol="ETH-USDT-PERP"):
    from src.platform.exchanges.models import Order, OrderStatus

    return Order(
        exchange=exchange_name,
        symbol=symbol,
        raw_symbol=symbol,
        order_id="test-order-1",
        client_order_id=None,
        status=OrderStatus.NEW,
    )


def _make_exchange_read_env(
    tmp_path,
    monkeypatch,
    *,
    expect_real_live: bool = True,
    check_exchange_read: bool = False,
    skip_exchange_read: bool = False,
) -> tuple:
    env_file, _ = _write_env(tmp_path)
    fake_okx = FakeExchangeClient("okx")
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        name = str(exchange).strip().lower()
        if name == "okx":
            return fake_okx
        if name == "binance":
            return fake_binance
        raise ValueError(f"Unexpected exchange: {exchange}")

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client",
        _fake_create,
    )
    report = run_preflight(
        env_file=env_file,
        environ={},
        repo_root=tmp_path,
        expect_real_live=expect_real_live,
        check_exchange_read=check_exchange_read,
        skip_exchange_read=skip_exchange_read,
    )
    return report, fake_okx, fake_binance


# ---------------------------------------------------------------------------
# Exchange read – basic activation logic
# ---------------------------------------------------------------------------


def test_exchange_read_runs_with_expect_real_live(
    tmp_path, monkeypatch
) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    skipped = report.named("EXCHANGE_READ_CHECK_SKIPPED")
    assert len(skipped) == 0
    assert _one(report, "fetch_balance:okx").status == "PASS"
    assert _one(report, "fetch_balance:binance").status == "PASS"


def test_exchange_read_skipped_with_flag(tmp_path, monkeypatch) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=True
    )
    check = _one(report, "EXCHANGE_READ_CHECK_SKIPPED")
    assert check.status == "SKIPPED"
    assert "explicitly skipped" in check.detail


def test_exchange_read_runs_with_check_flag_no_real_live(
    tmp_path, monkeypatch
) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch,
        expect_real_live=False,
        check_exchange_read=True,
        skip_exchange_read=False,
    )
    skipped = report.named("EXCHANGE_READ_CHECK_SKIPPED")
    assert len(skipped) == 0
    assert _one(report, "fetch_balance:okx").status == "PASS"


# ---------------------------------------------------------------------------
# Exchange read – only read methods called
# ---------------------------------------------------------------------------


def test_exchange_read_no_write_methods_called(tmp_path, monkeypatch) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    assert len(okx.write_calls) == 0, f"OKX write calls: {okx.write_calls}"
    assert len(binance.write_calls) == 0, f"Binance write calls: {binance.write_calls}"


# ---------------------------------------------------------------------------
# Exchange read – balance checks
# ---------------------------------------------------------------------------


def test_exchange_read_balance_pass(tmp_path, monkeypatch) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    assert _one(report, "fetch_balance:okx").status == "PASS"
    assert "available=1000" in _one(report, "fetch_balance:okx").detail


def test_exchange_read_balance_zero_or_negative_fails(
    tmp_path, monkeypatch
) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    # We need to set balance to 0 – recreate with custom fake
    env_file, _ = _write_env(tmp_path)
    fake_okx = FakeExchangeClient("okx", balance_available=0)
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report2 = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report2, "fetch_balance:okx").status == "FAIL"
    assert "<= 0" in _one(report2, "fetch_balance:okx").detail


def test_exchange_read_balance_api_error_fails(
    tmp_path, monkeypatch
) -> None:
    env_file, _ = _write_env(tmp_path)
    fake_okx = FakeExchangeClient(
        "okx", raise_on={"fetch_balance": RuntimeError("network down")}
    )
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "fetch_balance:okx").status == "FAIL"


# ---------------------------------------------------------------------------
# Exchange read – positions
# ---------------------------------------------------------------------------


def test_exchange_read_no_positions_pass(tmp_path, monkeypatch) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    assert _one(report, "fetch_positions:okx").status == "PASS"
    assert _one(report, "no_existing_position:okx").status == "PASS"


def test_exchange_read_existing_position_fails(tmp_path, monkeypatch) -> None:
    env_file, _ = _write_env(tmp_path)
    pos = _make_position("okx", quantity=1.0)
    fake_okx = FakeExchangeClient("okx", positions=[pos])
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "no_existing_position:okx").status == "FAIL"
    assert "non-zero" in _one(report, "no_existing_position:okx").detail


def test_exchange_read_short_position_fails(tmp_path, monkeypatch) -> None:
    """Short position with negative quantity must still FAIL no_existing_position."""
    env_file, _ = _write_env(tmp_path)
    pos = _make_position("okx", quantity=-1.0)
    fake_okx = FakeExchangeClient("okx", positions=[pos])
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "no_existing_position:okx").status == "FAIL"
    assert "non-zero" in _one(report, "no_existing_position:okx").detail


def test_exchange_read_zero_position_passes(tmp_path, monkeypatch) -> None:
    """Zero-quantity position must PASS no_existing_position."""
    env_file, _ = _write_env(tmp_path)
    pos = _make_position("okx", quantity=0.0)
    fake_okx = FakeExchangeClient("okx", positions=[pos])
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "no_existing_position:okx").status == "PASS"


def test_exchange_read_positions_api_error_fails(tmp_path, monkeypatch) -> None:
    env_file, _ = _write_env(tmp_path)
    fake_okx = FakeExchangeClient(
        "okx", raise_on={"fetch_positions": RuntimeError("timeout")}
    )
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "fetch_positions:okx").status == "FAIL"


# ---------------------------------------------------------------------------
# Exchange read – open orders
# ---------------------------------------------------------------------------


def test_exchange_read_no_open_orders_pass(tmp_path, monkeypatch) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    assert _one(report, "no_open_orders:okx").status == "PASS"
    assert _one(report, "no_open_orders:binance").status == "PASS"


def test_exchange_read_existing_open_order_fails(tmp_path, monkeypatch) -> None:
    env_file, _ = _write_env(tmp_path)
    order = _make_order("okx")
    fake_okx = FakeExchangeClient("okx", open_orders=[order])
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "no_open_orders:okx").status == "FAIL"


# ---------------------------------------------------------------------------
# Exchange read – stop orders
# ---------------------------------------------------------------------------


def test_exchange_read_no_stop_orders_pass(tmp_path, monkeypatch) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    assert _one(report, "no_open_stop_orders:okx").status == "PASS"


def test_exchange_read_existing_stop_order_fails(tmp_path, monkeypatch) -> None:
    env_file, _ = _write_env(tmp_path)
    order = _make_order("okx")
    fake_okx = FakeExchangeClient("okx", open_stop_orders=[order])
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "no_open_stop_orders:okx").status == "FAIL"


# ---------------------------------------------------------------------------
# Exchange read – leverage and margin mode
# ---------------------------------------------------------------------------


def test_exchange_read_leverage_match_pass(tmp_path, monkeypatch) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    # With default OKX_LEVERAGE=10 and fake leverage=10
    check = _one(report, "leverage_read:okx")
    # Default leverage in BASE_ENV is 10, and default fake leverage is None
    # so this will be WARN (unable to verify). Need to check with explicit leverage.
    # The default fake has leverage=None → WARN "unable to verify"
    pass  # See explicit tests below


def test_exchange_read_leverage_match_explicit(tmp_path, monkeypatch) -> None:
    env_file, _ = _write_env(tmp_path, {"OKX_LEVERAGE": "15", "BINANCE_LEVERAGE": "15"})
    fake_okx = FakeExchangeClient("okx", leverage=15, margin_mode="isolated")
    fake_binance = FakeExchangeClient("binance", leverage=15, margin_mode="isolated")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "leverage_read:okx").status == "PASS"
    assert "15" in _one(report, "leverage_read:okx").detail
    assert _one(report, "leverage_read:binance").status == "PASS"


def test_exchange_read_leverage_mismatch_fails(tmp_path, monkeypatch) -> None:
    env_file, _ = _write_env(tmp_path, {"OKX_LEVERAGE": "10", "BINANCE_LEVERAGE": "10"})
    fake_okx = FakeExchangeClient("okx", leverage=15, margin_mode="isolated")
    fake_binance = FakeExchangeClient("binance", leverage=15, margin_mode="isolated")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "leverage_read:okx").status == "FAIL"
    assert "actual=15" in _one(report, "leverage_read:okx").detail


def test_exchange_read_leverage_unavailable_clean_slate_warns(
    tmp_path, monkeypatch
) -> None:
    env_file, _ = _write_env(tmp_path)
    # leverage=None with clean slate → WARN
    fake_okx = FakeExchangeClient("okx", leverage=None)
    fake_binance = FakeExchangeClient("binance", leverage=None)

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    check = _one(report, "leverage_read:okx")
    assert check.status == "WARN"
    assert "unable to verify leverage from read-only API" in check.detail


def test_exchange_read_leverage_unavailable_with_position_warns(
    tmp_path, monkeypatch
) -> None:
    env_file, _ = _write_env(tmp_path)
    pos = _make_position("binance", symbol="ETHUSDT", quantity=1.0)
    fake_okx = FakeExchangeClient("okx", leverage=None)
    fake_binance = FakeExchangeClient("binance", leverage=None, positions=[pos])

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    check = _one(report, "leverage_read:binance")
    assert check.status == "WARN"
    assert "positions/orders exist" in check.detail


def test_exchange_read_margin_mode_match_pass(tmp_path, monkeypatch) -> None:
    env_file, _ = _write_env(tmp_path)
    fake_okx = FakeExchangeClient("okx", leverage=10, margin_mode="isolated")
    fake_binance = FakeExchangeClient("binance", leverage=10, margin_mode="isolated")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "margin_mode_read:okx").status == "PASS"
    assert "isolated" in _one(report, "margin_mode_read:okx").detail


def test_exchange_read_margin_mode_mismatch_fails(tmp_path, monkeypatch) -> None:
    env_file, _ = _write_env(tmp_path)
    fake_okx = FakeExchangeClient("okx", leverage=10, margin_mode="cross")
    fake_binance = FakeExchangeClient("binance", leverage=10, margin_mode="isolated")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "margin_mode_read:okx").status == "FAIL"


def test_exchange_read_margin_mode_unavailable_warns(
    tmp_path, monkeypatch
) -> None:
    env_file, _ = _write_env(tmp_path)
    fake_okx = FakeExchangeClient("okx", leverage=10, margin_mode=None)
    fake_binance = FakeExchangeClient("binance", leverage=10, margin_mode=None)

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    check = _one(report, "margin_mode_read:okx")
    assert check.status == "WARN"
    assert "unable to verify margin mode" in check.detail


# ---------------------------------------------------------------------------
# Exchange read – fetch_leverage margin_mode parameter
# ---------------------------------------------------------------------------


def test_fetch_leverage_receives_margin_mode_isolated(
    tmp_path, monkeypatch
) -> None:
    """When MARGIN_MODE=isolated, fetch_leverage must receive MarginMode.ISOLATED."""
    from src.platform.exchanges.models import MarginMode

    env_file, _ = _write_env(tmp_path)
    fake_okx = FakeExchangeClient("okx", leverage=10, margin_mode="isolated")
    fake_binance = FakeExchangeClient("binance", leverage=10, margin_mode="isolated")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert len(fake_okx.fetch_leverage_calls) >= 1
    assert fake_okx.fetch_leverage_calls[0] == MarginMode.ISOLATED


def test_fetch_leverage_receives_margin_mode_cross(
    tmp_path, monkeypatch
) -> None:
    """When MARGIN_MODE=cross, fetch_leverage must receive MarginMode.CROSS."""
    from src.platform.exchanges.models import MarginMode

    env_file, _ = _write_env(tmp_path, {"MARGIN_MODE": "cross"})
    fake_okx = FakeExchangeClient("okx", leverage=10, margin_mode="cross")
    fake_binance = FakeExchangeClient("binance", leverage=10, margin_mode="cross")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert len(fake_okx.fetch_leverage_calls) >= 1
    assert fake_okx.fetch_leverage_calls[0] == MarginMode.CROSS


# ---------------------------------------------------------------------------
# Exchange read – position mode
# ---------------------------------------------------------------------------


def test_exchange_read_position_mode_one_way_pass(tmp_path, monkeypatch) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    assert _one(report, "position_mode_read:okx").status == "PASS"
    assert "one_way" in _one(report, "position_mode_read:okx").detail


def test_exchange_read_position_mode_hedge_warns(tmp_path, monkeypatch) -> None:
    env_file, _ = _write_env(tmp_path)
    fake_okx = FakeExchangeClient("okx", position_mode="hedge")
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    assert _one(report, "position_mode_read:okx").status == "PASS"
    warns = [c for c in report.checks if c.name == "position_mode:okx"]
    assert len(warns) == 1
    assert warns[0].status == "WARN"


# ---------------------------------------------------------------------------
# Exchange read – JSON summary / failures / warnings
# ---------------------------------------------------------------------------


def test_exchange_read_json_summary_counts_correct(
    tmp_path, monkeypatch
) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    d = report.to_dict()
    s = d["summary"]
    assert s["pass"] == report.pass_count
    assert s["warn"] == report.warn_count
    assert s["fail"] == report.fail_count
    assert s["skipped"] == report.skipped_count
    # No exchange read checks should be SKIPPED
    assert report.skipped_count == 0


def test_exchange_read_failures_in_json_failures_list(
    tmp_path, monkeypatch
) -> None:
    env_file, _ = _write_env(tmp_path)
    order = _make_order("okx")
    fake_okx = FakeExchangeClient("okx", open_orders=[order])
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    d = report.to_dict()
    failure_names = {f["name"] for f in d["failures"]}
    assert "no_open_orders:okx" in failure_names


def test_exchange_read_warnings_in_json_warnings_list(
    tmp_path, monkeypatch
) -> None:
    env_file, _ = _write_env(tmp_path)
    fake_okx = FakeExchangeClient("okx", leverage=None)
    fake_binance = FakeExchangeClient("binance", leverage=None)

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    d = report.to_dict()
    warning_names = {w["name"] for w in d["warnings"]}
    assert "leverage_read:okx" in warning_names


# ---------------------------------------------------------------------------
# Exchange read – stdout summary
# ---------------------------------------------------------------------------


def test_exchange_read_stdout_summary_counts_correct(
    tmp_path, monkeypatch
) -> None:
    report, okx, binance = _make_exchange_read_env(
        tmp_path, monkeypatch, expect_real_live=True, skip_exchange_read=False
    )
    output = render_report(report)
    assert "SUMMARY:" in output
    assert f"PASS: {report.pass_count}" in output
    assert f"WARN: {report.warn_count}" in output
    assert f"FAIL: {report.fail_count}" in output
    assert f"SKIPPED: {report.skipped_count}" in output


# ---------------------------------------------------------------------------
# Exchange read – secret redaction still applies
# ---------------------------------------------------------------------------


def test_exchange_read_secrets_not_in_report(tmp_path, monkeypatch) -> None:
    secret = "REAL-SECRET-KEY-12345"
    env_file, _ = _write_env(
        tmp_path,
        {"OKX_API_KEY": secret, "OKX_SECRET_KEY": "sekrit"},
    )
    fake_okx = FakeExchangeClient("okx")
    fake_binance = FakeExchangeClient("binance")

    def _fake_create(exchange, config=None, *, http_client=None):
        return fake_okx if str(exchange).strip().lower() == "okx" else fake_binance

    monkeypatch.setattr(
        "tools.preflight_v10a_live.create_exchange_client", _fake_create
    )
    report = run_preflight(
        env_file=env_file, environ={}, repo_root=tmp_path,
        expect_real_live=True, skip_exchange_read=False,
    )
    output = render_report(report)
    assert secret not in output
    assert "sekrit" not in output
    d = report.to_dict()
    raw = json.dumps(d)
    assert secret not in raw
    assert "sekrit" not in raw


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


# ---------------------------------------------------------------------------
# Render report summary / failures / warnings output
# ---------------------------------------------------------------------------


def test_render_report_contains_summary_section(tmp_path: Path) -> None:
    report = _run(tmp_path)
    output = render_report(report)

    assert "SUMMARY:" in output
    assert "PASS:" in output
    assert "WARN:" in output
    assert "FAIL:" in output
    assert "SKIPPED:" in output


def test_render_report_lists_failures_when_fail_exists(tmp_path: Path) -> None:
    report = _run(
        tmp_path, {"AETHER_DRY_RUN": "true", "AETHER_LIVE_TRADING": "false"}
    )
    output = render_report(report)

    assert report.fail_count > 0
    assert "FAILURES:" in output
    assert "none" not in output.split("FAILURES:")[1].split("WARNINGS:")[0]


def test_render_report_lists_warnings_when_warn_exists(tmp_path: Path) -> None:
    report = _run(tmp_path)
    output = render_report(report)

    assert report.warn_count > 0
    assert "WARNINGS:" in output
    assert "none" not in output.split("WARNINGS:")[1].split("FINAL:")[0]


def test_render_report_final_is_last_lines(tmp_path: Path) -> None:
    report = _run(tmp_path)
    output = render_report(report)
    lines = output.splitlines()

    assert lines[-2] == "FINAL:"
    assert lines[-1] == report.final_status


def test_render_report_no_fail_shows_none(tmp_path: Path) -> None:
    report = _run(tmp_path)
    assert report.fail_count == 0

    output = render_report(report)
    # Extract text between FAILURES: and WARNINGS:
    failures_section = output.split("FAILURES:")[1].split("WARNINGS:")[0]
    assert "none" in failures_section


def test_render_report_no_warn_shows_none() -> None:
    report = PreflightReport(expect_real_live=False)
    report.add("ENV", "PASS", "check_a", "")
    report.add("ENV", "PASS", "check_b", "")

    output = render_report(report)
    warnings_section = output.split("WARNINGS:")[1].split("FINAL:")[0]
    assert "none" in warnings_section


# ---------------------------------------------------------------------------
# JSON report summary / failures / warnings fields
# ---------------------------------------------------------------------------


def test_json_report_contains_summary_failures_warnings(tmp_path: Path) -> None:
    env_file, _ = _write_env(tmp_path)
    report = run_preflight(
        env_file=env_file,
        environ={},
        repo_root=tmp_path,
        expect_real_live=True,
    )
    report_path = tmp_path / "report.json"
    write_json_report(report_path, report)
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert "summary" in payload
    assert "failures" in payload
    assert "warnings" in payload
    assert isinstance(payload["summary"], dict)
    assert isinstance(payload["failures"], list)
    assert isinstance(payload["warnings"], list)


def test_json_summary_numbers_match_counts(tmp_path: Path) -> None:
    env_file, _ = _write_env(tmp_path)
    report = run_preflight(
        env_file=env_file,
        environ={},
        repo_root=tmp_path,
        expect_real_live=True,
    )
    report_path = tmp_path / "report.json"
    write_json_report(report_path, report)
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    s = payload["summary"]
    assert s["pass"] == sum(
        1 for c in payload["checks"] if c["status"] == "PASS"
    )
    # Align with existing top-level counts
    assert s["fail"] == payload["fail_count"]
    assert s["warn"] == payload["warn_count"]
    assert s["skipped"] == payload["skipped_count"]


def test_failures_warnings_not_leak_secrets_in_json(tmp_path: Path) -> None:
    secret = "MY-SECRET-DO-NOT-LEAK"
    env_file, values = _write_env(
        tmp_path,
        {"OKX_API_KEY": secret},
    )
    report = run_preflight(
        env_file=env_file,
        environ={},
        repo_root=tmp_path,
        expect_real_live=True,
    )
    # Add checks whose detail fields contain the secret
    report.add("TEST", "WARN", "probe_warn", f"detail-with-{secret}")
    report.add("TEST", "FAIL", "probe_fail", f"another-detail-with-{secret}")

    report_path = tmp_path / "report.json"
    write_json_report(report_path, report)
    raw = report_path.read_text(encoding="utf-8")

    assert secret not in raw

    payload = json.loads(raw)
    for f in payload["failures"]:
        assert secret not in f.get("detail", "")
    for w in payload["warnings"]:
        assert secret not in w.get("detail", "")


def test_new_fields_do_not_change_original_check_results(tmp_path: Path) -> None:
    """Existing preflight checks produce the same per-check statuses."""
    env_file, _ = _write_env(tmp_path)
    report = run_preflight(
        env_file=env_file,
        environ={},
        repo_root=tmp_path,
        expect_real_live=True,
        skip_exchange_read=True,
    )

    # Same assertions as existing test_correct_real_live_env_returns_pass
    assert report.ok is True
    assert report.expect_real_live is True
    assert "PASS_READY_FOR_MANUAL_LIVE_START" in render_report(report)

    # Spot-check a few key checks still pass
    for name in (
        "AETHER_RUNTIME_MODE",
        "AETHER_DRY_RUN",
        "AETHER_LIVE_TRADING",
        "leverage_match",
        "strategy_load",
    ):
        assert _one(report, name).status == "PASS"


# ---------------------------------------------------------------------------
# Status counts – case insensitive
# ---------------------------------------------------------------------------


def test_status_counts_case_insensitive() -> None:
    """fail_count / warn_count / skipped_count / pass_count use .upper()."""
    report = PreflightReport(expect_real_live=False)
    report.add("TEST", "fail", "lower_fail", "")
    report.add("TEST", "warn", "lower_warn", "")
    report.add("TEST", "skipped", "lower_skipped", "")
    report.add("TEST", "pass", "lower_pass", "")
    report.add("TEST", "FAIL", "upper_fail", "")
    report.add("TEST", "WARN", "upper_warn", "")
    report.add("TEST", "SKIPPED", "upper_skipped", "")
    report.add("TEST", "PASS", "upper_pass", "")

    assert report.fail_count == 2
    assert report.warn_count == 2
    assert report.skipped_count == 2
    assert report.pass_count == 2
