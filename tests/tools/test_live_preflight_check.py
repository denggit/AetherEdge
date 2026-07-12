"""Tests for live_preflight_check.py tool functionality."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from src.order_management.position_plan.models import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    PositionPlan,
    PositionPlanStatus,
)
from src.order_management.position_plan.store import SqlitePositionPlanStore
from src.order_management.journal.store import SqliteOrderJournalStore
from src.order_management.reconciliation.validation import is_fake_order_id
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.storage import (
    SqliteKlineStore,
    SqliteRangeBarStore,
)
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.platform import Balance, LeverageInfo, PlatformSnapshot, PositionMode
from src.platform.data.models import MarketDataSource, MarketKline
from src.platform.exchanges.models import ExchangeName
from src.platform.state.sqlite_store import SqliteStateStore
from src.runtime import RuntimeMode
from src.strategy import load_strategy
from tests._support.runtime_manifest import build_manifest


def test_preflight_detects_fake_order_ids_in_position_plan(tmp_path):
    """Verify a PositionPlan with fake order IDs is detected."""
    store_path = tmp_path / "test_plans.sqlite3"
    store = SqlitePositionPlanStore(str(store_path))

    plan = PositionPlan(
        position_id="test-fake-plan",
        strategy_id="test",
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)

    leg = LegPlan(
        position_id="test-fake-plan",
        exchange=ExchangeName.OKX,
        role=LegRole.MASTER,
        target_qty_base=Decimal("0.1"),
        entry_order_id="okx-order-1",
        stop_order_id="okx-stop-1",
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(leg)

    # Scan for fakes
    from src.order_management.reconciliation.service import _detect_fake_order_refs
    fake_refs = _detect_fake_order_refs(plan, store.get_legs("test-fake-plan"))

    assert len(fake_refs) >= 2
    assert any(f.field == "entry_order_id" and f.value == "okx-order-1" for f in fake_refs)
    assert any(f.field == "stop_order_id" and f.value == "okx-stop-1" for f in fake_refs)


def test_preflight_clean_plan_no_fakes(tmp_path):
    """A clean plan with real numeric IDs should have no fake detections."""
    store_path = tmp_path / "test_clean_plans.sqlite3"
    store = SqlitePositionPlanStore(str(store_path))

    plan = PositionPlan(
        position_id="test-clean-plan",
        strategy_id="test",
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.BINANCE,
        master_target_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)

    leg = LegPlan(
        position_id="test-clean-plan",
        exchange=ExchangeName.BINANCE,
        role=LegRole.MASTER,
        target_qty_base=Decimal("0.1"),
        entry_order_id="987654321",
        entry_client_order_id="AEBNOLabc123",
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(leg)

    from src.order_management.reconciliation.service import _detect_fake_order_refs
    fake_refs = _detect_fake_order_refs(plan, store.get_legs("test-clean-plan"))

    assert len(fake_refs) == 0


def test_fake_id_patterns_match_documentation():
    """All fake patterns from the task spec should be detected."""
    specs = [
        "okx-order-1", "okx-1", "okx-stop-1",
        "binance-order-1", "binance-1", "binance-stop-1",
    ]
    for spec in specs:
        assert is_fake_order_id(spec), f"Task spec '{spec}' should be detected as fake"


def test_real_ids_are_not_fake():
    """Real order IDs must not match fake patterns."""
    real = ["1234567890", "987654321", "42", "AEOKOLabc123", "AEBNSPxyz789"]
    for r in real:
        assert not is_fake_order_id(r), f"'{r}' should NOT be fake"


@pytest.mark.asyncio
async def test_default_preflight_reconciles_stable_wal_snapshot_without_source_writes(
    tmp_path,
    monkeypatch,
) -> None:
    import tools.live_preflight_check as preflight

    state_db, journal_db, plan_db = _seed_preflight_databases(tmp_path)
    plan_store = SqlitePositionPlanStore(plan_db)
    plan_store.upsert_position(_plan("wal-plan", strategy_id="before-wal"))
    gc.collect()
    writer = sqlite3.connect(plan_db)
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute(
            "UPDATE position_plans SET strategy_id='latest-from-wal' WHERE position_id='wal-plan'"
        )
        writer.commit()
        assert Path(f"{plan_db}-wal").exists()
        args = _preflight_args(
            tmp_path,
            state_db=state_db,
            journal_db=journal_db,
            plan_db=plan_db,
        )
        before = _directory_manifest(tmp_path)
        _install_generic_preflight(monkeypatch, preflight, args)
        real_check = preflight._check_stale_state
        observed = {}

        async def capture_latest(*call_args, **call_kwargs):
            observed["strategy_id"] = call_kwargs["plan_store"].get_position(
                "wal-plan"
            ).strategy_id
            return await real_check(*call_args, **call_kwargs)

        monkeypatch.setattr(preflight, "_check_stale_state", capture_latest)

        async def forbidden_apply(*_args, **_kwargs):
            pytest.fail("read-only preflight called reconcile_and_apply")

        monkeypatch.setattr(
            preflight.LiveStateReconciliationService,
            "reconcile_and_apply",
            forbidden_apply,
        )
        exit_code = await preflight.main()
        after = _directory_manifest(tmp_path, exclude={Path(args.report).name})
        before_without_report = {
            key: value for key, value in before.items() if key != Path(args.report).name
        }
    finally:
        writer.close()

    payload = json.loads(Path(args.report).read_text(encoding="utf-8"))
    assert exit_code == preflight.EXIT_FAIL_NEEDS_RECONCILE
    assert observed["strategy_id"] == "latest-from-wal"
    assert payload["reconciliation_mode"] == "read_only"
    assert payload["confirmation_accepted"] is False
    assert after == before_without_report


@pytest.mark.asyncio
async def test_read_only_preflight_reports_missing_databases_without_creating_them(
    tmp_path,
    monkeypatch,
) -> None:
    import tools.live_preflight_check as preflight

    args = _preflight_args(
        tmp_path,
        state_db=tmp_path / "missing-state.sqlite3",
        journal_db=tmp_path / "missing-journal.sqlite3",
        plan_db=tmp_path / "missing-plan.sqlite3",
    )
    _install_generic_preflight(monkeypatch, preflight, args)

    exit_code = await preflight.main()

    assert exit_code == preflight.EXIT_FAIL_CONFIG
    assert not Path(args.env_file).parent.joinpath("missing-state.sqlite3").exists()
    assert not Path(args.env_file).parent.joinpath("missing-journal.sqlite3").exists()
    assert not Path(args.env_file).parent.joinpath("missing-plan.sqlite3").exists()
    payload = json.loads(Path(args.report).read_text(encoding="utf-8"))
    assert payload["reconciliation_mode"] == "read_only"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("token", "plan_confirmation", "journal_confirmation"),
    (
        (None, "actual", "actual"),
        ("WRONG", "actual", "actual"),
        ("APPLY_LIVE_STATE_RECONCILIATION", None, "actual"),
        ("APPLY_LIVE_STATE_RECONCILIATION", "mismatch", "actual"),
        ("APPLY_LIVE_STATE_RECONCILIATION", "relative", "actual"),
    ),
)
async def test_apply_reconcile_rejects_incomplete_or_ambiguous_confirmation_before_write(
    tmp_path,
    monkeypatch,
    token,
    plan_confirmation,
    journal_confirmation,
) -> None:
    import tools.live_preflight_check as preflight

    state_db, journal_db, plan_db = _seed_preflight_databases(tmp_path)
    args = _preflight_args(
        tmp_path,
        state_db=state_db,
        journal_db=journal_db,
        plan_db=plan_db,
        apply=True,
    )
    before = _directory_manifest(tmp_path)
    args.confirm_reconcile_write = token
    args.confirm_position_plan_db = _confirmation_value(
        plan_confirmation, plan_db, tmp_path / "other-plan.sqlite3"
    )
    args.confirm_order_journal_db = _confirmation_value(
        journal_confirmation, journal_db, tmp_path / "other-journal.sqlite3"
    )
    _install_generic_preflight(monkeypatch, preflight, args)
    load_spy = Mock(side_effect=AssertionError("strategy loaded before confirmation"))
    plan_spy = Mock(side_effect=AssertionError("plan store opened before confirmation"))
    journal_spy = Mock(side_effect=AssertionError("journal opened before confirmation"))
    monkeypatch.setattr(preflight, "load_strategy", load_spy)
    monkeypatch.setattr(preflight, "SqlitePositionPlanStore", plan_spy)
    monkeypatch.setattr(preflight, "SqliteOrderJournalStore", journal_spy)

    exit_code = await preflight.main()

    payload = json.loads(Path(args.report).read_text(encoding="utf-8"))
    after = _directory_manifest(tmp_path, exclude={Path(args.report).name})
    assert exit_code == preflight.EXIT_FAIL_CONFIG
    assert payload["confirmation_accepted"] is False
    assert "write_not_applied" in Path(args.report).read_text(encoding="utf-8")
    assert after == before
    load_spy.assert_not_called()
    plan_spy.assert_not_called()
    journal_spy.assert_not_called()


@pytest.mark.asyncio
async def test_apply_reconcile_changes_only_exactly_confirmed_temporary_databases(
    tmp_path,
    monkeypatch,
) -> None:
    import tools.live_preflight_check as preflight

    state_db, journal_db, plan_db = _seed_preflight_databases(tmp_path)
    plans = SqlitePositionPlanStore(plan_db)
    plans.upsert_position(_plan("stale-plan", strategy_id="strategy"))
    plans.upsert_leg(
        LegPlan(
            position_id="stale-plan",
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.1"),
            entry_order_id="okx-order-1",
            stop_order_id="okx-stop-1",
            sync_status=LegSyncStatus.OPEN,
        )
    )
    gc.collect()
    unconfirmed = tmp_path / "unconfirmed.sqlite3"
    with sqlite3.connect(unconfirmed) as connection:
        connection.execute("CREATE TABLE sentinel(value TEXT)")
        connection.execute("INSERT INTO sentinel VALUES ('unchanged')")
    unconfirmed_before = _fingerprint(unconfirmed)
    args = _preflight_args(
        tmp_path,
        state_db=state_db,
        journal_db=journal_db,
        plan_db=plan_db,
        apply=True,
    )
    args.confirm_reconcile_write = preflight.RECONCILE_WRITE_CONFIRMATION
    args.confirm_position_plan_db = str(plan_db.resolve())
    args.confirm_order_journal_db = str(journal_db.resolve())
    _install_generic_preflight(monkeypatch, preflight, args)

    exit_code = await preflight.main()

    persisted = SqlitePositionPlanStore(plan_db).get_position("stale-plan")
    payload = json.loads(Path(args.report).read_text(encoding="utf-8"))
    assert exit_code == preflight.EXIT_PASS
    assert persisted.status == PositionPlanStatus.CLOSED
    assert _fingerprint(unconfirmed) == unconfirmed_before
    assert payload["reconciliation_mode"] == "apply"
    assert payload["confirmation_accepted"] is True
    targets = {item["label"]: item for item in payload["database_paths"]}
    assert targets["state_db"]["write_target"] is False
    assert targets["position_plan_db"]["confirmed_absolute_path"] == str(plan_db.resolve())
    assert targets["order_journal_db"]["confirmed_absolute_path"] == str(journal_db.resolve())


@pytest.mark.asyncio
async def test_direct_live_provider_still_rejects_apply_before_write_confirmation(
    tmp_path,
    monkeypatch,
) -> None:
    import tools.live_preflight_check as preflight
    import tools.live_server_smoke as smoke

    state_db, journal_db, plan_db = _seed_preflight_databases(tmp_path)
    args = _preflight_args(
        tmp_path,
        state_db=state_db,
        journal_db=journal_db,
        plan_db=plan_db,
        apply=True,
    )
    args.strategy = "strategies.eth_portfolio_v1:Strategy"
    _install_generic_preflight(monkeypatch, preflight, args)
    monkeypatch.setattr(
        smoke,
        "run_server_smoke",
        lambda *_args, **_kwargs: pytest.fail("provider smoke must not run"),
    )

    exit_code = await preflight.main()

    report_text = Path(args.report).read_text(encoding="utf-8")
    assert exit_code != preflight.EXIT_PASS
    assert "direct_live_preflight_disallows:--apply-reconcile" in report_text
    assert '"reconciliation_mode": "apply"' in report_text
    assert '"confirmation_accepted": false' in report_text


@pytest.mark.asyncio
async def test_real_portfolio_v1_preflight_uses_five_stable_snapshots_and_reads_wal(
    tmp_path,
    monkeypatch,
) -> None:
    import tools.live_preflight_check as preflight

    sources = _seed_real_portfolio_sources(tmp_path / "source")
    latest_open_ms = 1_700_100_000_000
    writer = sqlite3.connect(sources["mf_feature"])
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute(
            "UPDATE klines SET open_time_ms=?, close_time_ms=?",
            (latest_open_ms, latest_open_ms + 4 * 60 * 60_000 - 1),
        )
        writer.commit()
        assert Path(f"{sources['mf_feature']}-wal").exists()
        args = _real_portfolio_args(tmp_path, sources=sources, apply=False)
        _install_real_portfolio_env(monkeypatch, preflight, args, sources)
        guarded_connect = sqlite3.connect
        forbidden_store_sources = {
            sources["position_plan"].resolve(),
            sources["order_journal"].resolve(),
        }

        def reject_source_store_connections(database, *connect_args, **connect_kwargs):
            from tests._support.runtime_state_guard import resolve_sqlite_target

            path, _read_only = resolve_sqlite_target(
                database,
                uri=bool(connect_kwargs.get("uri", False)),
            )
            if path in forbidden_store_sources:
                raise AssertionError(f"provider opened source store: {path}")
            return guarded_connect(database, *connect_args, **connect_kwargs)

        monkeypatch.setattr(sqlite3, "connect", reject_source_store_connections)
        source_before = _tree_manifest(tmp_path / "source")
        repo_before = _runtime_repo_manifest()

        exit_code = await preflight.main()

        source_after = _tree_manifest(tmp_path / "source")
        repo_after = _runtime_repo_manifest()
    finally:
        writer.close()

    payload = json.loads(Path(args.report).read_text(encoding="utf-8"))
    assert exit_code != preflight.EXIT_FAIL_CONFIG
    assert payload["strategy"] == "eth_portfolio_v1"
    assert payload["report_kind"] == "preflight"
    assert payload["reconciliation_mode"] == "read_only"
    assert (
        payload["lf_data_readiness"]["latest_closed_kline_open_time_ms"]
        == latest_open_ms
    )
    assert source_after == source_before
    assert repo_after == repo_before
    database_paths = payload["database_paths"]
    allowed = Path(os.environ["AETHER_PYTEST_ALLOWED_TEMP_ROOT"]).resolve()
    for name, source_path in sources.items():
        assert database_paths[f"{name}_source"] == str(source_path.resolve())
        snapshot_path = Path(database_paths[name])
        snapshot_path.relative_to(allowed)
        assert not snapshot_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "confirmation_mode",
    ("none", "token", "token_and_paths"),
)
async def test_real_portfolio_v1_apply_is_rejected_before_strategy_construction(
    tmp_path,
    monkeypatch,
    confirmation_mode: str,
) -> None:
    import tools.live_preflight_check as preflight
    from strategies.eth_portfolio_v1.domain.mf_data import MfDataBuffer
    from strategies.eth_portfolio_v1.strategy import Strategy

    sources = _real_portfolio_source_paths(tmp_path / "source")
    args = _real_portfolio_args(tmp_path, sources=sources, apply=True)
    if confirmation_mode in {"token", "token_and_paths"}:
        args.confirm_reconcile_write = preflight.RECONCILE_WRITE_CONFIRMATION
    if confirmation_mode == "token_and_paths":
        args.confirm_position_plan_db = str(sources["position_plan"].resolve())
        args.confirm_order_journal_db = str(sources["order_journal"].resolve())
    _install_real_portfolio_env(monkeypatch, preflight, args, sources)
    load_spy = Mock(wraps=preflight.load_strategy)
    monkeypatch.setattr(preflight, "load_strategy", load_spy)

    with (
        patch.object(Strategy, "__init__", side_effect=AssertionError("Strategy constructed")) as strategy_init,
        patch.object(MfDataBuffer, "__init__", side_effect=AssertionError("MF buffer constructed")) as mf_init,
        patch.object(preflight, "SqlitePositionPlanStore", side_effect=AssertionError("plan store constructed")) as plan_init,
        patch.object(preflight, "SqliteOrderJournalStore", side_effect=AssertionError("journal store constructed")) as journal_init,
    ):
        exit_code = await preflight.main()

    report_text = Path(args.report).read_text(encoding="utf-8")
    assert exit_code != preflight.EXIT_PASS
    assert "direct_live_preflight_disallows:--apply-reconcile" in report_text
    load_spy.assert_not_called()
    strategy_init.assert_not_called()
    mf_init.assert_not_called()
    plan_init.assert_not_called()
    journal_init.assert_not_called()
    assert not (tmp_path / "source").exists()


def test_real_portfolio_strategy_accepts_explicit_mf_snapshot_path(
    tmp_path,
    monkeypatch,
) -> None:
    working = tmp_path / "working"
    working.mkdir()
    monkeypatch.chdir(working)
    snapshot = tmp_path / "snapshot" / "market.sqlite3"

    strategy = load_strategy(
        "strategies.eth_portfolio_v1:Strategy",
        mf_store_path=snapshot,
    )

    assert strategy.mf_data_buffer._store.path.resolve() == snapshot.resolve()
    assert strategy.mf_data_readiness._store.path.resolve() == snapshot.resolve()
    assert not (working / "data/market_data/aether_market_data.sqlite3").exists()


@pytest.mark.asyncio
async def test_preflight_and_server_smoke_default_reports_honor_environment(
    tmp_path,
    monkeypatch,
) -> None:
    import tools.live_preflight_check as preflight
    import tools.live_server_smoke as smoke

    invalid_env = tmp_path / ".env.example"
    invalid_env.write_text("AETHER_MARKET=ETH-USDT-PERP\n", encoding="utf-8")
    preflight_report = tmp_path / "reports" / "preflight.json"
    smoke_report = tmp_path / "reports" / "smoke.json"
    repo_before = _runtime_repo_manifest()

    monkeypatch.setenv("AETHER_LIVE_PREFLIGHT_REPORT", str(preflight_report))
    monkeypatch.setattr(
        sys,
        "argv",
        ["live_preflight_check.py", "--env-file", str(invalid_env)],
    )
    assert await preflight.main() == preflight.EXIT_FAIL_CONFIG

    monkeypatch.setenv("AETHER_LIVE_SMOKE_REPORT", str(smoke_report))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "live_server_smoke.py",
            "--strategy",
            "strategies.eth_portfolio_v1:Strategy",
            "--env-file",
            str(invalid_env),
        ],
    )
    assert await smoke.main() != 0

    assert preflight_report.is_file()
    assert smoke_report.is_file()
    assert _runtime_repo_manifest() == repo_before


def _real_portfolio_source_paths(root: Path) -> dict[str, Path]:
    return {
        "state": root / "state.sqlite3",
        "position_plan": root / "position-plan.sqlite3",
        "order_journal": root / "order-journal.sqlite3",
        "range_checkpoint": root / "range-checkpoint.sqlite3",
        "mf_feature": root / "market-data.sqlite3",
    }


def _seed_real_portfolio_sources(root: Path) -> dict[str, Path]:
    paths = _real_portfolio_source_paths(root)
    root.mkdir(parents=True, exist_ok=True)
    SqliteStateStore(paths["state"])
    SqlitePositionPlanStore(paths["position_plan"])
    SqliteOrderJournalStore(paths["order_journal"])
    SqliteRangeCheckpointStore(paths["range_checkpoint"])
    SqliteRangeBarStore(paths["mf_feature"])
    SqliteTradeFeatureStore(paths["mf_feature"])
    SqliteKlineStore(paths["mf_feature"]).save(
        [
            MarketKline(
                exchange=ExchangeName.OKX,
                symbol="ETH-USDT-PERP",
                raw_symbol="ETH-USDT-SWAP",
                interval="4h",
                open_time_ms=1_700_000_000_000,
                close_time_ms=1_700_000_000_000 + 4 * 60 * 60_000 - 1,
                open=Decimal("2000"),
                high=Decimal("2010"),
                low=Decimal("1990"),
                close=Decimal("2005"),
                volume=Decimal("10"),
                is_closed=True,
                source=MarketDataSource.REST,
            )
        ]
    )
    gc.collect()
    return paths


def _real_portfolio_args(
    tmp_path: Path,
    *,
    sources: dict[str, Path],
    apply: bool,
) -> argparse.Namespace:
    env_file = tmp_path / "portfolio-v1.env"
    env_file.write_text("AETHER_MARKET=ETH-USDT-PERP\n", encoding="utf-8")
    return argparse.Namespace(
        strategy="strategies.eth_portfolio_v1:Strategy",
        defaults=Path(__file__).resolve().parents[2] / "config/aether_defaults.json",
        env_file=env_file,
        report=str(tmp_path / "portfolio-v1-preflight.json"),
        apply_reconcile=apply,
        confirm_reconcile_write=None,
        confirm_position_plan_db=None,
        confirm_order_journal_db=None,
        skip_api=True,
        skip_kline=False,
    )


def _install_real_portfolio_env(
    monkeypatch,
    preflight,
    args: argparse.Namespace,
    sources: dict[str, Path],
) -> None:
    values = {
        "AETHER_RUNTIME_MODE": "live_runtime",
        "AETHER_MARKET": "ETH-USDT-PERP",
        "AETHER_STRATEGY": "strategies.eth_portfolio_v1:Strategy",
        "AETHER_EXCHANGES": "okx,binance",
        "AETHER_DATA_EXCHANGE": "okx",
        "AETHER_MASTER_EXCHANGE": "okx",
        "AETHER_FOLLOWER_EXCHANGES": "binance",
        "AETHER_DRY_RUN": "false",
        "AETHER_LIVE_TRADING": "true",
        "AETHER_STATE_DB": str(sources["state"]),
        "AETHER_POSITION_PLAN_DB": str(sources["position_plan"]),
        "AETHER_ORDER_JOURNAL_DB": str(sources["order_journal"]),
        "AETHER_RANGE_CHECKPOINT_DB": str(sources["range_checkpoint"]),
        "AETHER_MARKET_DATA_DB": str(sources["mf_feature"]),
        "AETHER_RANGE_BACKFILL_ENABLED": "false",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(preflight, "parse_args", lambda: args)


def _tree_manifest(root: Path) -> dict[str, tuple[str, int, int]]:
    return {
        path.relative_to(root).as_posix(): _fingerprint(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _runtime_repo_manifest() -> dict[str, dict[str, int | str]]:
    repo_root = Path(__file__).resolve().parents[2]
    return build_manifest(
        repo_root=repo_root,
        roots=("data/state", "data/market_data", "data/reports", "logs"),
    )


def _plan(position_id: str, *, strategy_id: str) -> PositionPlan:
    return PositionPlan(
        position_id=position_id,
        strategy_id=strategy_id,
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("100"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
    )


def _seed_preflight_databases(tmp_path: Path) -> tuple[Path, Path, Path]:
    state_db = tmp_path / "state.sqlite3"
    journal_db = tmp_path / "journal.sqlite3"
    plan_db = tmp_path / "plans.sqlite3"
    SqliteStateStore(state_db)
    SqliteOrderJournalStore(journal_db)
    SqlitePositionPlanStore(plan_db)
    gc.collect()
    return state_db, journal_db, plan_db


def _preflight_args(
    tmp_path: Path,
    *,
    state_db: Path,
    journal_db: Path,
    plan_db: Path,
    apply: bool = False,
) -> argparse.Namespace:
    env_file = tmp_path / "preflight.env"
    env_file.write_text(
        "\n".join(
            (
                "AETHER_RUNTIME_MODE=live_runtime",
                "AETHER_MARKET=ETH-USDT-PERP",
                "AETHER_EXCHANGES=okx",
                "AETHER_DATA_EXCHANGE=okx",
                "AETHER_STRATEGY=strategies.empty_strategy:Strategy",
                "OKX_API_KEY=test-okx-api-key",
                "OKX_SECRET_KEY=test-okx-api-secret",
                "OKX_PASSPHRASE=test-okx-passphrase",
                f"AETHER_STATE_DB={state_db}",
                f"AETHER_ORDER_JOURNAL_DB={journal_db}",
                f"AETHER_POSITION_PLAN_DB={plan_db}",
            )
        ),
        encoding="utf-8",
    )
    return argparse.Namespace(
        strategy=None,
        defaults=tmp_path / "missing-defaults.json",
        env_file=env_file,
        report=str(tmp_path / "preflight-report.json"),
        apply_reconcile=apply,
        confirm_reconcile_write=None,
        confirm_position_plan_db=None,
        confirm_order_journal_db=None,
        skip_api=False,
        skip_kline=True,
    )


def _install_generic_preflight(monkeypatch, preflight, args) -> None:
    values = {}
    for line in Path(args.env_file).read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(preflight, "parse_args", lambda: args)
    monkeypatch.setattr(
        preflight,
        "load_strategy",
        lambda _path, **_kwargs: SimpleNamespace(
            config=SimpleNamespace(strategy_id="test")
        ),
    )
    monkeypatch.setattr(
        preflight,
        "runtime_mode_from_env",
        lambda: RuntimeMode.LIVE_RUNTIME,
    )

    async def snapshots(*_args, **_kwargs):
        return (_flat_snapshot(),)

    monkeypatch.setattr(preflight, "_fetch_snapshots", snapshots)


def _flat_snapshot() -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(
            exchange=ExchangeName.OKX,
            asset="USDT",
            total=Decimal("1000"),
            available=Decimal("1000"),
        ),
        positions=[],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            leverage=Decimal("15"),
        ),
        position_mode=PositionMode.ONE_WAY,
    )


def _confirmation_value(kind, actual: Path, mismatch: Path):
    if kind is None:
        return None
    if kind == "actual":
        return str(actual.resolve())
    if kind == "mismatch":
        return str(mismatch.resolve())
    if kind == "relative":
        return actual.name
    raise AssertionError(kind)


def _directory_manifest(
    root: Path,
    *,
    exclude: set[str] | None = None,
) -> dict[str, tuple[str, int, int]]:
    excluded = exclude or set()
    return {
        path.name: _fingerprint(path)
        for path in sorted(root.iterdir())
        if path.is_file() and path.name not in excluded
    }


def _fingerprint(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return hashlib.sha256(path.read_bytes()).hexdigest(), stat.st_size, stat.st_mtime_ns


# ── Generic (non-provider) Preflight + Kline warmup tests ──────────────


def _seed_generic_databases(
    state_db: Path,
    journal_db: Path,
    plan_db: Path,
) -> None:
    SqliteStateStore(state_db)
    SqliteOrderJournalStore(journal_db)
    SqlitePositionPlanStore(plan_db)


def _recent_kline_timestamps(*, count: int = 5) -> list[int]:
    """Return *count* closed 4h-bar open_time_ms values ending near now."""
    import time

    interval_ms = 4 * 60 * 60_000
    now_ms = int(time.time() * 1000)
    latest_closed = (now_ms // interval_ms) * interval_ms
    return [latest_closed - (count - i) * interval_ms for i in range(count)]


def _seed_market_data_kline_db(
    path: Path,
    *,
    open_times_ms: list[int] | None = None,
) -> SqliteKlineStore:
    if open_times_ms is None:
        open_times_ms = _recent_kline_timestamps(count=5)
    path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteKlineStore(path)
    klines = [
        MarketKline(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            interval="4h",
            open_time_ms=ot,
            close_time_ms=ot + 4 * 60 * 60_000 - 1,
            open=Decimal(str(2000 + i * 10)),
            high=Decimal(str(2010 + i * 10)),
            low=Decimal(str(1990 + i * 10)),
            close=Decimal(str(2005 + i * 10)),
            volume=Decimal("10"),
            is_closed=True,
            source=MarketDataSource.REST,
        )
        for i, ot in enumerate(open_times_ms)
    ]
    store.save(klines)
    return store


def _source_db_manifest(source_dir: Path) -> dict[str, tuple[str, int, int]]:
    return {
        path.name: _fingerprint(path)
        for path in sorted(source_dir.iterdir())
        if path.is_file()
    }


@pytest.mark.asyncio
async def test_generic_readonly_preflight_kline_warmup_uses_snapshot_and_preserves_source(
    tmp_path,
    monkeypatch,
) -> None:
    """Generic (non-provider) read-only Preflight: Kline warmup reads snapshot."""
    import tools.live_preflight_check as preflight

    # ── Seed source databases ────────────────────────────────────────
    source_dir = tmp_path / "source"
    source_mf = source_dir / "market-data.sqlite3"
    state_db, journal_db, plan_db = _seed_preflight_databases(
        tmp_path / "dbs"
    )
    open_times = _recent_kline_timestamps(count=5)
    _seed_market_data_kline_db(source_mf, open_times_ms=open_times)

    # Pin a connection to prevent WAL checkpoint on writer close (Windows)
    _pin = sqlite3.connect(str(source_mf))
    _pin.execute("PRAGMA journal_mode=WAL")
    # Write one uncheckpointed WAL record (update the last kline)
    latest_open_ms = open_times[-1] + 4 * 60 * 60_000
    writer = sqlite3.connect(str(source_mf))
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute(
            "UPDATE klines SET open_time_ms=?, close_time_ms=? WHERE rowid=1",
            (latest_open_ms, latest_open_ms + 4 * 60 * 60_000 - 1),
        )
        writer.commit()
        wal_path = Path(f"{source_mf}-wal")
        assert wal_path.exists(), "WAL file must exist after uncheckpointed write"
    finally:
        writer.close()

    source_before = _source_db_manifest(source_dir)
    repo_before = _runtime_repo_manifest()

    # ── Guard: reject direct connections to source mf DB ─────────────
    source_mf_resolved = source_mf.resolve()
    original_connect = sqlite3.connect

    def reject_source_mf_connect(database, *args, **kwargs):
        from tests._support.runtime_state_guard import resolve_sqlite_target

        path, read_only = resolve_sqlite_target(
            database, uri=bool(kwargs.get("uri", False)),
        )
        if path is not None and path == source_mf_resolved and not read_only:
            raise AssertionError(
                f"source market-data DB opened for write: {path}"
            )
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", reject_source_mf_connect)
    # Also guard SqliteKlineStore.__init__ path parameter
    original_kline_init = SqliteKlineStore.__init__

    def guarded_kline_init(self, path, *args, **kwargs):
        resolved = Path(path).resolve()
        if resolved == source_mf_resolved:
            raise AssertionError(
                f"SqliteKlineStore constructed with source path: {path}"
            )
        return original_kline_init(self, path, *args, **kwargs)

    monkeypatch.setattr(SqliteKlineStore, "__init__", guarded_kline_init)

    # ── Build args and env ───────────────────────────────────────────
    env_file = tmp_path / "generic-readonly.env"
    env_file.write_text(
        "\n".join(
            (
                "AETHER_RUNTIME_MODE=live_runtime",
                "AETHER_MARKET=ETH-USDT-PERP",
                "AETHER_EXCHANGES=okx",
                "AETHER_DATA_EXCHANGE=okx",
                "AETHER_STRATEGY=tests.fake:Strategy",
                "OKX_API_KEY=test-okx-api-key",
                "OKX_SECRET_KEY=test-okx-api-secret",
                "OKX_PASSPHRASE=test-okx-passphrase",
                f"AETHER_STATE_DB={state_db}",
                f"AETHER_ORDER_JOURNAL_DB={journal_db}",
                f"AETHER_POSITION_PLAN_DB={plan_db}",
                f"AETHER_MARKET_DATA_DB={source_mf}",
            )
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        strategy="tests.fake:Strategy",
        defaults=tmp_path / "missing-defaults.json",
        env_file=env_file,
        report=str(tmp_path / "generic-readonly-report.json"),
        apply_reconcile=False,
        confirm_reconcile_write=None,
        confirm_position_plan_db=None,
        confirm_order_journal_db=None,
        skip_api=True,
        skip_kline=False,
    )

    # ── Install environment ──────────────────────────────────────────
    values = {}
    for line in Path(args.env_file).read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(preflight, "parse_args", lambda: args)
    monkeypatch.setattr(
        preflight,
        "runtime_mode_from_env",
        lambda: RuntimeMode.LIVE_RUNTIME,
    )
    # DO NOT monkeypatch _check_kline_warmup — test the real function
    # DO NOT replace SqliteKlineStore with a fake

    # Mock external exchange API
    async def fake_snapshots(*_args, **_kwargs):
        return (_flat_snapshot(),)

    monkeypatch.setattr(preflight, "_fetch_snapshots", fake_snapshots)

    # ── Run ──────────────────────────────────────────────────────────
    exit_code = await preflight.main()

    # ── Assertions ───────────────────────────────────────────────────
    payload = json.loads(Path(args.report).read_text(encoding="utf-8"))
    kline_checks = [c for c in payload["checks"] if c["name"] == "kline_warmup"]
    assert len(kline_checks) == 1, f"expected kline_warmup check, got {[c['name'] for c in payload['checks']]}"
    kline_check = kline_checks[0]
    assert kline_check["status"] == "ok", f"kline_warmup failed: {kline_check}"
    assert kline_check["detail"]["available"] >= 1

    # Source DB/WAL/SHM completely unchanged
    source_after = _source_db_manifest(source_dir)
    assert source_after == source_before, (
        f"source DB changed:\nbefore={source_before}\nafter={source_after}"
    )

    # Repository runtime manifest unchanged
    repo_after = _runtime_repo_manifest()
    assert repo_after == repo_before, (
        f"repo manifest changed:\nbefore={repo_before}\nafter={repo_after}"
    )

    # Verify kline store path was in a snapshot temp (not source)
    database_paths = {
        item["label"]: item for item in payload.get("database_paths", [])
    }
    mf_entry = database_paths.get("mf_feature_db")
    assert mf_entry is not None, "mf_feature_db missing from database_paths"
    assert mf_entry["write_target"] is False

    _pin.close()


@pytest.mark.asyncio
async def test_generic_apply_preflight_kline_warmup_preserves_market_data_source(
    tmp_path,
    monkeypatch,
) -> None:
    """Generic apply mode: only PositionPlan+Journal modified; mf source untouched."""
    import tools.live_preflight_check as preflight

    # ── Seed source databases ────────────────────────────────────────
    source_dir = tmp_path / "source"
    source_mf = source_dir / "market-data.sqlite3"
    state_db, journal_db, plan_db = _seed_preflight_databases(
        tmp_path / "dbs"
    )
    _seed_market_data_kline_db(source_mf, open_times_ms=_recent_kline_timestamps(count=5))

    # Add a stale plan with fake order IDs
    plans = SqlitePositionPlanStore(plan_db)
    plans.upsert_position(_plan("stale-plan", strategy_id="strategy"))
    plans.upsert_leg(
        LegPlan(
            position_id="stale-plan",
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.1"),
            entry_order_id="okx-order-1",
            stop_order_id="okx-stop-1",
            sync_status=LegSyncStatus.OPEN,
        )
    )
    gc.collect()

    source_before = _source_db_manifest(source_dir)
    source_mf_resolved = source_mf.resolve()
    original_connect = sqlite3.connect

    def reject_source_mf_connect(database, *args, **kwargs):
        from tests._support.runtime_state_guard import resolve_sqlite_target

        path, read_only = resolve_sqlite_target(
            database, uri=bool(kwargs.get("uri", False)),
        )
        if path is not None and path == source_mf_resolved and not read_only:
            raise AssertionError(
                f"source market-data DB opened for write in apply mode: {path}"
            )
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", reject_source_mf_connect)

    # ── Build args ───────────────────────────────────────────────────
    env_file = tmp_path / "generic-apply.env"
    env_file.write_text(
        "\n".join(
            (
                "AETHER_RUNTIME_MODE=live_runtime",
                "AETHER_MARKET=ETH-USDT-PERP",
                "AETHER_EXCHANGES=okx",
                "AETHER_DATA_EXCHANGE=okx",
                "AETHER_STRATEGY=tests.fake:Strategy",
                "OKX_API_KEY=test-okx-api-key",
                "OKX_SECRET_KEY=test-okx-api-secret",
                "OKX_PASSPHRASE=test-okx-passphrase",
                f"AETHER_STATE_DB={state_db}",
                f"AETHER_ORDER_JOURNAL_DB={journal_db}",
                f"AETHER_POSITION_PLAN_DB={plan_db}",
                f"AETHER_MARKET_DATA_DB={source_mf}",
            )
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        strategy="tests.fake:Strategy",
        defaults=tmp_path / "missing-defaults.json",
        env_file=env_file,
        report=str(tmp_path / "generic-apply-report.json"),
        apply_reconcile=True,
        confirm_reconcile_write=preflight.RECONCILE_WRITE_CONFIRMATION,
        confirm_position_plan_db=str(plan_db.resolve()),
        confirm_order_journal_db=str(journal_db.resolve()),
        skip_api=False,
        skip_kline=False,
    )

    values = {}
    for line in Path(args.env_file).read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(preflight, "parse_args", lambda: args)
    monkeypatch.setattr(
        preflight,
        "runtime_mode_from_env",
        lambda: RuntimeMode.LIVE_RUNTIME,
    )

    async def fake_snapshots(*_args, **_kwargs):
        return (_flat_snapshot(),)

    monkeypatch.setattr(preflight, "_fetch_snapshots", fake_snapshots)

    # ── Run ──────────────────────────────────────────────────────────
    exit_code = await preflight.main()

    # ── Assertions ───────────────────────────────────────────────────
    payload = json.loads(Path(args.report).read_text(encoding="utf-8"))
    # PositionPlan was modified (stale plan closed)
    persisted = SqlitePositionPlanStore(plan_db).get_position("stale-plan")
    assert persisted.status == PositionPlanStatus.CLOSED

    # market-data source DB unchanged
    source_after = _source_db_manifest(source_dir)
    assert source_after == source_before, (
        f"source mf DB changed in apply mode:\nbefore={source_before}\nafter={source_after}"
    )

    # Kline warmup check ran and passed
    kline_checks = [c for c in payload["checks"] if c["name"] == "kline_warmup"]
    assert len(kline_checks) == 1
    assert kline_checks[0]["status"] == "ok"

    # mf_feature write_target is false
    database_paths = {
        item["label"]: item for item in payload.get("database_paths", [])
    }
    mf_entry = database_paths.get("mf_feature_db")
    assert mf_entry is not None
    assert mf_entry["write_target"] is False

    # No new WAL/SHM for source mf
    for suffix in ("-wal", "-shm"):
        assert not Path(f"{source_mf}{suffix}").exists(), (
            f"source mf {suffix} should not exist"
        )

    # PositionPlan and Journal ARE write targets
    plan_entry = database_paths.get("position_plan_db")
    journal_entry = database_paths.get("order_journal_db")
    assert plan_entry["write_target"] is True
    assert journal_entry["write_target"] is True


def test_cli_subprocess_readonly_preflight_uses_pytest_basetemp(
    tmp_path,
    monkeypatch,
) -> None:
    """Real subprocess CLI Preflight: snapshots land in pytest basetemp."""
    import tools.live_preflight_check as preflight

    allowed_root = Path(os.environ["AETHER_PYTEST_ALLOWED_TEMP_ROOT"]).resolve()
    repo_root = Path(os.environ["AETHER_PYTEST_REPO_ROOT"]).resolve()

    # ── Seed databases in tmp_path ───────────────────────────────────
    source_dir = tmp_path / "source"
    source_mf = source_dir / "market-data.sqlite3"
    state_db = tmp_path / "dbs" / "state.sqlite3"
    journal_db = tmp_path / "dbs" / "journal.sqlite3"
    plan_db = tmp_path / "dbs" / "plans.sqlite3"
    _seed_generic_databases(state_db, journal_db, plan_db)
    _seed_market_data_kline_db(source_mf, open_times_ms=_recent_kline_timestamps(count=5))
    gc.collect()

    source_before = _source_db_manifest(source_dir)
    repo_before = _runtime_repo_manifest()
    report_path = tmp_path / "cli-preflight-report.json"

    # CLI subprocess should log to tmp, not the repo
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    env_file = tmp_path / "cli-generic.env"
    env_file.write_text(
        "\n".join(
            (
                "AETHER_RUNTIME_MODE=live_runtime",
                "AETHER_MARKET=ETH-USDT-PERP",
                "AETHER_EXCHANGES=okx",
                "AETHER_DATA_EXCHANGE=okx",
                "AETHER_STRATEGY=tests.fake:Strategy",
                "OKX_API_KEY=test-okx-api-key",
                "OKX_SECRET_KEY=test-okx-api-secret",
                "OKX_PASSPHRASE=test-okx-passphrase",
                f"AETHER_STATE_DB={state_db}",
                f"AETHER_ORDER_JOURNAL_DB={journal_db}",
                f"AETHER_POSITION_PLAN_DB={plan_db}",
                f"AETHER_MARKET_DATA_DB={source_mf}",
                f"AETHER_LIVE_PREFLIGHT_REPORT={report_path}",
                f"LOG_DIR={log_dir}",
            )
        ),
        encoding="utf-8",
    )

    # ── Run CLI as subprocess ────────────────────────────────────────
    script = str(repo_root / "tools" / "live_preflight_check.py")
    # Build a clean env: keep PYTEST guard vars, clear AETHER_* so the
    # env file is the sole source of database paths.
    child_env = {
        key: value
        for key, value in os.environ.items()
        if key.startswith("AETHER_PYTEST_")
        or key in ("PYTHONPATH", "SYSTEMROOT", "SystemRoot", "PATH", "TEMP", "TMP", "TMPDIR",
                   "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "APPDATA", "LOCALAPPDATA")
        or key.startswith("OKX_")
        or key.startswith("BINANCE_")
    }
    # Preserve required PYTEST vars for guard + sitecustomize
    for required in (
        "AETHER_PYTEST_SQLITE_GUARD",
        "AETHER_PYTEST_REPO_ROOT",
        "AETHER_PYTEST_STATE_ROOT",
        "AETHER_PYTEST_ALLOWED_TEMP_ROOT",
        "AETHER_PYTEST_SYSTEM_TEMP_ROOT",
        "AETHER_PYTEST_RUNTIME_HEARTBEAT_DB",
    ):
        if required in os.environ:
            child_env[required] = os.environ[required]
    child_env["LOG_DIR"] = str(log_dir)
    result = subprocess.run(
        [
            sys.executable,
            script,
            "--strategy", "tests.fake:Strategy",
            "--defaults", str(repo_root / "config" / "aether_defaults.json"),
            "--env-file", str(env_file),
            "--report", str(report_path),
            "--skip-api",
        ],
        cwd=str(repo_root),
        text=True,
        capture_output=True,
        check=False,
        env=child_env,
    )

    # ── Assertions ───────────────────────────────────────────────────
    output = result.stdout + result.stderr
    # Exit code 0 or 1 acceptable (0=pass, 1=needs reconcile with stale state)
    assert result.returncode in (0, 1), (
        f"CLI Preflight failed with code {result.returncode}:\n{output}"
    )
    # Must not fail on guard outside-temp rule
    assert "blocked write access outside the temporary directory" not in output, (
        f"guard blocked legitimate snapshot:\n{output}"
    )
    assert "blocked write access to repository runtime database" not in output, (
        f"guard blocked repo path:\n{output}"
    )

    # Report written
    assert report_path.is_file(), f"report not written: {output}"
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    # Kline warmup was not skipped
    kline_checks = [c for c in payload["checks"] if c["name"] == "kline_warmup"]
    assert len(kline_checks) == 1, (
        f"expected kline_warmup check, got {[c['name'] for c in payload['checks']]}"
    )

    # Source database unchanged
    source_after = _source_db_manifest(source_dir)
    assert source_after == source_before, (
        f"source DB changed during subprocess CLI:\n"
        f"before={source_before}\nafter={source_after}"
    )

    # Repo manifest unchanged
    repo_after = _runtime_repo_manifest()
    assert repo_after == repo_before, (
        f"repo manifest changed during subprocess CLI:\n"
        f"before={repo_before}\nafter={repo_after}"
    )
