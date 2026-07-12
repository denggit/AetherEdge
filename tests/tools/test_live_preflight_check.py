"""Tests for live_preflight_check.py tool functionality."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

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
from src.platform import Balance, LeverageInfo, PlatformSnapshot, PositionMode
from src.platform.exchanges.models import ExchangeName
from src.platform.state.sqlite_store import SqliteStateStore
from src.runtime import RuntimeMode


def test_preflight_detects_fake_order_ids_in_position_plan():
    """Verify a PositionPlan with fake order IDs is detected."""
    import tempfile
    store_path = Path(tempfile.mkdtemp()) / "test_plans.sqlite3"
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


def test_preflight_clean_plan_no_fakes():
    """A clean plan with real numeric IDs should have no fake detections."""
    import tempfile
    store_path = Path(tempfile.mkdtemp()) / "test_clean_plans.sqlite3"
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

    exit_code = await preflight.main()

    payload = json.loads(Path(args.report).read_text(encoding="utf-8"))
    after = _directory_manifest(tmp_path, exclude={Path(args.report).name})
    assert exit_code == preflight.EXIT_FAIL_CONFIG
    assert payload["confirmation_accepted"] is False
    assert "write_not_applied" in Path(args.report).read_text(encoding="utf-8")
    assert after == before


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
    _install_generic_preflight(monkeypatch, preflight, args)
    provider_strategy = SimpleNamespace(
        config=SimpleNamespace(strategy_id="portfolio-v1"),
        live_preflight_provider=lambda: None,
    )
    monkeypatch.setattr(preflight, "load_strategy", lambda _path: provider_strategy)
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
                "AETHER_STRATEGY=tests.fake:Strategy",
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
        lambda _path: SimpleNamespace(config=SimpleNamespace(strategy_id="test")),
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
