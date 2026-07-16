#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""AetherEdge unified live preflight check with reconciliation awareness.

Read-only checks before starting live trading. With --apply-reconcile, can
clean up stale PositionPlans and fake order IDs detected during the check.

Exit codes:
  0 = PASS (ready for live)
  1 = FAIL_NEEDS_RECONCILE (stale state detected, run with --apply-reconcile)
  2 = FAIL_UNRESOLVED_FOLLOWER_POSITION (follower needs manual intervention)
  3 = FAIL_CONFIG (configuration issue)

This script does NOT place, cancel, amend, or close orders on exchanges.
It operates on local state stores and read-only exchange REST APIs.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import sys
import time
from contextlib import ExitStack
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.app import AppConfig
from src.app.factory import build_app_context
from src.order_management import (
    FakeOrderRef,
    LiveStateReconciliationReport,
    LiveStateReconciliationService,
    SqliteOrderJournalStore,
    SqlitePositionPlanStore,
    is_fake_order_id,
)
from src.order_management.position_plan.models import LegSyncStatus, PositionPlanStatus
from src.order_management.reconciliation.models import ReconciliationVerdict
from src.platform import ExchangeName
from src.platform.account.factory import create_account_client
from src.platform.config import (
    load_project_env_config,
    set_project_env_config,
)
from src.platform.exchanges.credentials import validate_private_credentials
from src.platform.exchanges.errors import ExchangeConfigError
from src.platform.exchanges.models import ExchangeConfig
from src.platform.execution.factory import create_execution_client
from src.platform.snapshot import PlatformSnapshot, fetch_platform_snapshot
from src.runtime import (
    RuntimeMode,
    live_runtime_config_from_app,
    runtime_mode_from_env,
)
from src.runtime.live_smoke import (
    BootstrapFailureReport,
    strategy_plugin_path,
)
from src.runtime.tasks.scheduler import closed_bar_open_time_ms
from src.strategy import load_strategy
from src.utils.log import get_logger
from tools._sqlite_readonly_snapshot import (
    ReadOnlySnapshotError,
    stable_sqlite_snapshot as _stable_sqlite_snapshot,
)

logger = get_logger(__name__)

EXIT_PASS = 0
EXIT_FAIL_NEEDS_RECONCILE = 1
EXIT_FAIL_UNRESOLVED_FOLLOWER = 2
EXIT_FAIL_CONFIG = 3


@dataclass
class CheckResult:
    name: str
    status: str  # "ok", "warn", "fail"
    detail: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class PreflightReport:
    started_time_ms: int
    symbol: str | None = None
    strategy: str | None = None
    runtime_mode: str | None = None
    checks: list[CheckResult] = field(default_factory=list)
    reconciliation: LiveStateReconciliationReport | None = None
    reconciliation_mode: str = "read_only"
    confirmation_accepted: bool = False
    database_paths: list[dict[str, Any]] = field(default_factory=list)
    verdict: str = "pass"

    @property
    def ok(self) -> bool:
        return self.verdict in {"pass", "pass_with_cleanup"}

    def add(
        self,
        name: str,
        status: str,
        *,
        detail: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        self.checks.append(
            CheckResult(name=name, status=status, detail=detail or {}, error=error)
        )
        prefix = {"ok": "[ok]", "warn": "[warn]", "fail": "[fail]"}.get(status, "[info]")
        msg = f"{prefix} {name}"
        if error:
            msg += f": {error}"
        if status == "fail":
            logger.error(msg)
        elif status == "warn":
            logger.warning(msg)
        else:
            logger.info(msg)

    def to_json(self) -> str:
        payload: dict[str, Any] = {
            "started_time_ms": self.started_time_ms,
            "symbol": self.symbol,
            "strategy": self.strategy,
            "runtime_mode": self.runtime_mode,
            "reconciliation_mode": self.reconciliation_mode,
            "confirmation_accepted": self.confirmation_accepted,
            "database_paths": self.database_paths,
            "verdict": self.verdict,
            "ok": self.ok,
            "checks": [asdict(c) for c in self.checks],
        }
        if self.reconciliation is not None:
            payload["reconciliation"] = {
                "ok": self.reconciliation.ok,
                "verdict": self.reconciliation.verdict.value,
                "stale_plans_closed": self.reconciliation.stale_plans_closed,
                "fake_order_refs_found": len(self.reconciliation.fake_order_refs_found),
                "unresolved_follower_positions": self.reconciliation.unresolved_follower_positions,
                "active_position_after": self.reconciliation.active_position_after,
                "exchange_positions": self.reconciliation.exchange_positions,
                "exchange_open_orders": self.reconciliation.exchange_open_orders,
                "exchange_open_stops": self.reconciliation.exchange_open_stops,
                "issues": self.reconciliation.issues,
            }
        return json.dumps(payload, indent=2, ensure_ascii=False, default=str)


async def main() -> int:
    args = parse_args()
    report = PreflightReport(
        started_time_ms=_now_ms(),
        reconciliation_mode=("apply" if args.apply_reconcile else "read_only"),
    )
    logger.info(
        "Preflight reconciliation mode | reconciliation_mode=%s",
        report.reconciliation_mode,
    )
    try:
        project_env = load_project_env_config(
            env_file=args.env_file,
        )
    except (OSError, RuntimeError, UnicodeError, ValueError) as exc:
        report.add(
            "load_project_env_config",
            "fail",
            error=f"config_load_failed:{type(exc).__name__}",
        )
        report.verdict = "fail_config"
        _maybe_write_report(args.report, report)
        return EXIT_FAIL_CONFIG
    set_project_env_config(project_env)

    # ── 1. Load config ──
    try:
        app_config = AppConfig.from_env(
            defaults_path=args.defaults,
            environ=project_env.values,
        )
        if args.strategy:
            strategy_path = strategy_plugin_path(args.strategy)
            app_config = replace(app_config, strategy=strategy_path)
    except Exception as exc:
        report.add("load_app_config", "fail", error=str(exc))
        report.verdict = "fail_config"
        _maybe_write_report(args.report, report)
        return EXIT_FAIL_CONFIG

    report.symbol = app_config.symbol
    report.strategy = app_config.strategy

    try:
        database_sources = _resolve_database_source_paths(
            app_config=app_config,
            project_env=project_env,
            defaults_path=args.defaults,
        )
        provider_declared = _strategy_class_declares_provider(
            app_config.strategy,
            "live_preflight_provider",
        )
    except Exception as exc:
        report.add(
            "strategy_provider_inspection",
            "fail",
            error=f"provider_inspection_failed:{type(exc).__name__}",
        )
        report.verdict = "fail_config"
        _maybe_write_report(args.report, report)
        return EXIT_FAIL_CONFIG

    state_db = database_sources["state"]
    journal_db = database_sources["order_journal"]
    plan_db = database_sources["position_plan"]
    mf_db = database_sources["mf_feature"]
    database_targets = tuple(
        (
            f"{name}_db",
            path,
            bool(
                args.apply_reconcile
                and name in {"position_plan", "order_journal"}
            ),
        )
        for name, path in database_sources.items()
    )
    generic_database_targets = tuple(
        target
        for target in database_targets
        if target[0]
        in {"state_db", "position_plan_db", "order_journal_db"}
    )
    report.database_paths = [
        _database_path_detail(label, path, write_target=write_target)
        for label, path, write_target in database_targets
    ]
    for detail in report.database_paths:
        logger.warning(
            "Preflight database target | reconciliation_mode=%s label=%s "
            "absolute_path=%s exists=%s size_bytes=%s write_target=%s",
            report.reconciliation_mode,
            detail["label"],
            detail["absolute_path"],
            detail["exists"],
            detail["size_bytes"],
            detail["write_target"],
        )

    if provider_declared and args.apply_reconcile:
        final_report = BootstrapFailureReport(
            verdict="fail_config",
            exit_code=1,
            issues=["direct_live_preflight_disallows:--apply-reconcile"],
        )
        _write_provider_preflight_report(
            args.report,
            final_report,
            reconciliation_mode=report.reconciliation_mode,
            confirmation_accepted=False,
        )
        return int(final_report.exit_code)

    if args.apply_reconcile:
        if not _accept_reconcile_write_confirmation(
            args,
            report,
            position_plan_db=plan_db,
            order_journal_db=journal_db,
        ) or not _check_database_paths(
            report,
            database_targets=generic_database_targets,
            apply_reconcile=True,
        ):
            report.verdict = "fail_config"
            _maybe_write_report(args.report, report)
            return EXIT_FAIL_CONFIG

    if not args.skip_api:
        try:
            for exchange in app_config.exchanges:
                validate_private_credentials(
                    exchange,
                    ExchangeConfig.from_env(
                        exchange,
                        env=project_env.values,
                    ),
                )
        except ExchangeConfigError as exc:
            report.add(
                "private_credentials",
                "fail",
                error=str(exc),
            )
            report.verdict = "fail_config"
            _maybe_write_report(args.report, report)
            return EXIT_FAIL_CONFIG

    if provider_declared:
        from tools.live_server_smoke import run_server_smoke

        final_report = await run_server_smoke(
            defaults_path=args.defaults,
            env_file=args.env_file,
            strategy_name=args.strategy or app_config.strategy,
            provider_hook="live_preflight_provider",
            provider_kwargs={
                "skip_api": args.skip_api,
                "skip_kline": args.skip_kline,
            },
            read_only_state=True,
            database_source_paths=database_sources,
        )
        _write_provider_preflight_report(
            args.report,
            final_report,
            reconciliation_mode=report.reconciliation_mode,
            confirmation_accepted=False,
        )
        return int(final_report.exit_code)

    try:
        strategy = load_strategy(app_config.strategy)
    except Exception as exc:
        report.add("load_strategy", "fail", error=str(exc))
        report.verdict = "fail_config"
        _maybe_write_report(args.report, report)
        return EXIT_FAIL_CONFIG

    runtime_mode = runtime_mode_from_env()
    report.runtime_mode = runtime_mode.value

    # ── 2. Runtime mode check ──
    if runtime_mode != RuntimeMode.LIVE_RUNTIME:
        report.add(
            "runtime_mode_check",
            "fail",
            error=f"Expected LIVE_RUNTIME, got {runtime_mode.value}",
        )
        report.verdict = "fail_config"
        _maybe_write_report(args.report, report)
        return EXIT_FAIL_CONFIG
    report.add("runtime_mode_check", "ok", detail={"mode": runtime_mode.value})

    # ── 3. Strategy identity check ──
    identity_provider = getattr(strategy, "strategy_identity", None)
    strategy_id = identity_provider() if callable(identity_provider) else None
    if not strategy_id:
        report.add("strategy_identity", "fail", error="Strategy has no strategy_id")
        report.verdict = "fail_config"
        _maybe_write_report(args.report, report)
        return EXIT_FAIL_CONFIG
    report.add("strategy_identity", "ok", detail={"strategy_id": strategy_id})

    # ── 4. Local DB writability ──
    if not args.apply_reconcile and not _check_database_paths(
        report,
        database_targets=generic_database_targets,
        apply_reconcile=False,
    ):
        report.verdict = "fail_config"
        _maybe_write_report(args.report, report)
        return EXIT_FAIL_CONFIG

    # ── 5. Read exchange snapshots ──
    if not args.skip_api:
        snapshots = await _fetch_snapshots(app_config, report)
        if snapshots is None:
            _maybe_write_report(args.report, report)
            return EXIT_FAIL_CONFIG
    else:
        snapshots = ()
        report.add("exchange_snapshots", "warn", detail={"skipped": True})

    # ── 6. Check local PositionPlan store for stale state ──
    try:
        if args.apply_reconcile:
            plan_store = SqlitePositionPlanStore(str(plan_db.resolve()))
            journal = SqliteOrderJournalStore(str(journal_db.resolve()))
            await _check_stale_state(
                report,
                plan_store=plan_store,
                order_journal=journal,
                snapshots=snapshots,
                apply_reconcile=True,
            )
            _check_fake_order_ids(report, plan_store)
            # ── 8. Kline warmup check (mf snapshot even in apply mode) ──
            if not args.skip_kline:
                with _stable_sqlite_snapshot(mf_db) as mf_snapshot:
                    await _check_kline_warmup(
                        app_config=app_config,
                        strategy=strategy,
                        report=report,
                        market_data_db_path=mf_snapshot,
                    )
        else:
            with ExitStack() as stack:
                snapshot_plan = stack.enter_context(_stable_sqlite_snapshot(plan_db))
                snapshot_journal = stack.enter_context(
                    _stable_sqlite_snapshot(journal_db)
                )
                if not args.skip_kline:
                    snapshot_mf = stack.enter_context(
                        _stable_sqlite_snapshot(mf_db)
                    )
                plan_store = _SnapshotPositionPlanStore(snapshot_plan)
                journal = _SnapshotOrderJournalStore(snapshot_journal)
                try:
                    await _check_stale_state(
                        report,
                        plan_store=plan_store,
                        order_journal=journal,
                        snapshots=snapshots,
                        apply_reconcile=False,
                    )
                    _check_fake_order_ids(report, plan_store)
                    # ── 8. Kline warmup check ──
                    if not args.skip_kline:
                        await _check_kline_warmup(
                            app_config=app_config,
                            strategy=strategy,
                            report=report,
                            market_data_db_path=snapshot_mf,
                        )
                finally:
                    journal.close_snapshot_connections()
                    plan_store.close_snapshot_connections()
    except ReadOnlySnapshotError as exc:
        report.add(
            "read_only_reconciliation_snapshot",
            "fail",
            error=f"read_only_snapshot_unstable:{exc}",
        )
        report.verdict = "fail_config"
        _maybe_write_report(args.report, report)
        return EXIT_FAIL_CONFIG

    # ── Determine final verdict ──
    _finalize_verdict(report)

    _maybe_write_report(args.report, report)

    # Print summary
    ok_count = sum(1 for c in report.checks if c.status == "ok")
    warn_count = sum(1 for c in report.checks if c.status == "warn")
    fail_count = sum(1 for c in report.checks if c.status == "fail")
    logger.info(
        "Preflight complete | verdict=%s ok=%s/%s checks_passed=%s warn=%s fail=%s",
        report.verdict,
        report.ok,
        len(report.checks),
        ok_count,
        warn_count,
        fail_count,
    )

    exit_map = {
        "pass": EXIT_PASS,
        "pass_with_cleanup": EXIT_PASS,
        "fail_needs_reconcile": EXIT_FAIL_NEEDS_RECONCILE,
        "fail_unresolved_follower_position": EXIT_FAIL_UNRESOLVED_FOLLOWER,
        "fail_config": EXIT_FAIL_CONFIG,
    }
    return exit_map.get(report.verdict, EXIT_FAIL_CONFIG)


RECONCILE_WRITE_CONFIRMATION = "APPLY_LIVE_STATE_RECONCILIATION"


def _strategy_class_declares_provider(
    strategy_path: str,
    provider_name: str,
) -> bool:
    if ":" not in strategy_path:
        raise ValueError("strategy path must be 'module:attribute'")
    module_name, attribute_name = strategy_path.split(":", 1)
    module = importlib.import_module(module_name)
    strategy_attribute = getattr(module, attribute_name)
    return callable(getattr(strategy_attribute, provider_name, None))


def _resolve_database_source_paths(
    *,
    app_config: AppConfig,
    project_env,
    defaults_path: str | Path,
) -> dict[str, Path]:
    runtime_config = live_runtime_config_from_app(
        app_config,
        defaults_path=defaults_path,
        environ=project_env.values,
    )
    return {
        "state": Path(app_config.state_db_path).expanduser().resolve(strict=False),
        "position_plan": Path(
            project_env.get(
                "AETHER_POSITION_PLAN_DB",
                "data/state/aether_position_plan.sqlite3",
            )
        ).expanduser().resolve(strict=False),
        "order_journal": Path(
            project_env.get(
                "AETHER_ORDER_JOURNAL_DB",
                "data/state/aether_order_journal.sqlite3",
            )
        ).expanduser().resolve(strict=False),
        "range_checkpoint": Path(
            runtime_config.range_checkpoint_db_path
        ).expanduser().resolve(strict=False),
        "mf_feature": Path(
            runtime_config.market_data_db_path
        ).expanduser().resolve(strict=False),
    }


class _SnapshotPositionPlanStore(SqlitePositionPlanStore):
    def __init__(self, path: str | Path) -> None:
        self._snapshot_connections: list[Any] = []
        super().__init__(path)

    def _connect(self):
        connection = super()._connect()
        self._snapshot_connections.append(connection)
        return connection

    def close_snapshot_connections(self) -> None:
        for connection in reversed(self._snapshot_connections):
            connection.close()
        self._snapshot_connections.clear()


class _SnapshotOrderJournalStore(SqliteOrderJournalStore):
    def __init__(self, path: str | Path) -> None:
        self._snapshot_connections: list[Any] = []
        super().__init__(path)

    def _connect(self):
        connection = super()._connect()
        self._snapshot_connections.append(connection)
        return connection

    def close_snapshot_connections(self) -> None:
        for connection in reversed(self._snapshot_connections):
            connection.close()
        self._snapshot_connections.clear()


def _database_path_detail(
    label: str,
    path: Path,
    *,
    write_target: bool,
) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=False)
    exists = resolved.exists()
    return {
        "label": label,
        "absolute_path": str(resolved),
        "exists": exists,
        "size_bytes": resolved.stat().st_size if exists else None,
        "write_target": write_target,
        "confirmed_absolute_path": None,
    }


def _accept_reconcile_write_confirmation(
    args: argparse.Namespace,
    report: PreflightReport,
    *,
    position_plan_db: Path,
    order_journal_db: Path,
) -> bool:
    token = getattr(args, "confirm_reconcile_write", None)
    supplied_plan = getattr(args, "confirm_position_plan_db", None)
    supplied_journal = getattr(args, "confirm_order_journal_db", None)
    actual_plan = position_plan_db.expanduser().resolve(strict=False)
    actual_journal = order_journal_db.expanduser().resolve(strict=False)

    reason: str | None = None
    confirmed_plan: Path | None = None
    confirmed_journal: Path | None = None
    if token != RECONCILE_WRITE_CONFIRMATION:
        reason = "confirmation_token_mismatch"
    elif not supplied_plan or not supplied_journal:
        reason = "confirmed_database_path_missing"
    else:
        raw_plan = Path(supplied_plan).expanduser()
        raw_journal = Path(supplied_journal).expanduser()
        if not raw_plan.is_absolute() or not raw_journal.is_absolute():
            reason = "confirmed_database_path_must_be_absolute"
        else:
            confirmed_plan = raw_plan.resolve(strict=False)
            confirmed_journal = raw_journal.resolve(strict=False)
            if actual_plan == actual_journal or confirmed_plan == confirmed_journal:
                reason = "confirmed_database_paths_are_ambiguous"
            elif confirmed_plan != actual_plan or confirmed_journal != actual_journal:
                reason = "confirmed_database_path_mismatch"

    if reason is not None:
        report.confirmation_accepted = False
        report.add(
            "reconciliation_write_confirmation",
            "fail",
            detail={"confirmation_accepted": False, "reason": reason},
            error="write_not_applied",
        )
        return False

    report.confirmation_accepted = True
    for detail in report.database_paths:
        if detail["label"] == "position_plan_db":
            detail["confirmed_absolute_path"] = str(confirmed_plan)
        elif detail["label"] == "order_journal_db":
            detail["confirmed_absolute_path"] = str(confirmed_journal)
    report.add(
        "reconciliation_write_confirmation",
        "ok",
        detail={
            "confirmation_accepted": True,
            "position_plan_db": str(confirmed_plan),
            "order_journal_db": str(confirmed_journal),
        },
    )
    logger.warning(
        "Reconciliation write confirmed | confirmation_accepted=true "
        "position_plan_db=%s order_journal_db=%s",
        confirmed_plan,
        confirmed_journal,
    )
    return True


def _check_database_paths(
    report: PreflightReport,
    *,
    database_targets: tuple[tuple[str, Path, bool], ...],
    apply_reconcile: bool,
) -> bool:
    ok = True
    for label, path, write_target in database_targets:
        resolved = path.expanduser().resolve(strict=False)
        parent = resolved.parent
        if apply_reconcile:
            if not write_target:
                report.add(
                    f"{label}_not_write_target",
                    "ok",
                    detail={
                        "absolute_path": str(resolved),
                        "write_target": False,
                        "probe_created": False,
                    },
                )
                continue
            parent_exists = parent.exists()
            writable = parent_exists and os.access(parent, os.W_OK)
            status = "ok" if writable else "fail"
            report.add(
                f"{label}_writable",
                status,
                detail={
                    "absolute_path": str(resolved),
                    "parent_writable": writable,
                    "write_target": write_target,
                    "probe_created": False,
                },
                error=None if writable else "database_parent_not_writable",
            )
            ok = ok and writable
            continue

        exists = resolved.is_file()
        status = "ok" if exists else ("warn" if label == "state_db" else "fail")
        report.add(
            f"{label}_readable",
            status,
            detail={
                "absolute_path": str(resolved),
                "exists": exists,
                "parent_writable": parent.exists() and os.access(parent, os.W_OK),
                "probe_created": False,
            },
            error=None if exists else "database_missing",
        )
        if label != "state_db":
            ok = ok and exists
    return ok


# ── Checks ──────────────────────────────────────────────────────────────


async def _fetch_snapshots(
    app_config: AppConfig, report: PreflightReport
) -> tuple[PlatformSnapshot, ...] | None:
    """Fetch read-only exchange snapshots for all configured exchanges."""
    snapshots: list[PlatformSnapshot] = []
    for exchange in app_config.exchanges:
        try:
            config = ExchangeConfig.from_env(exchange)
            validate_private_credentials(exchange, config)
            account = create_account_client(exchange, symbol=app_config.symbol, config=config)
            execution = create_execution_client(exchange, symbol=app_config.symbol, config=config)
            snapshot = await fetch_platform_snapshot(account=account, execution=execution)
            snapshots.append(snapshot)
            pos_count = sum(1 for p in snapshot.positions if p.quantity != Decimal("0"))
            report.add(
                f"exchange_snapshot_{exchange.value}",
                "ok" if pos_count == 0 else "warn",
                detail={
                    "exchange": exchange.value,
                    "symbol": app_config.symbol,
                    "positions": pos_count,
                    "open_orders": len(snapshot.open_orders),
                    "open_stop_orders": len(snapshot.open_stop_orders),
                    "balance_available": str(snapshot.balance.available),
                },
            )
        except Exception as exc:
            report.add(
                f"exchange_snapshot_{exchange.value}",
                "fail",
                error=str(exc),
            )
            report.verdict = "fail_config"
            return None
    return tuple(snapshots)


async def _check_stale_state(
    report: PreflightReport,
    *,
    plan_store: SqlitePositionPlanStore,
    order_journal: SqliteOrderJournalStore,
    snapshots: tuple[PlatformSnapshot, ...],
    apply_reconcile: bool,
) -> None:
    """Run reconciliation check (and optionally apply) to detect stale state."""
    if not snapshots:
        report.add("stale_state_check", "warn", detail={"skipped": "no snapshots"})
        return

    service = LiveStateReconciliationService(
        position_plan_store=plan_store,
        order_journal=order_journal,
        state_store=None,  # Preflight is read-only for state_store
        alert_sink=None,
    )

    active_plans = plan_store.list_active_positions()
    fake_count = 0
    for plan in active_plans:
        for leg in plan_store.get_legs(plan.position_id):
            if leg.entry_order_id and is_fake_order_id(leg.entry_order_id):
                fake_count += 1
            if leg.stop_order_id and is_fake_order_id(leg.stop_order_id):
                fake_count += 1

    if apply_reconcile:
        recon_report = await service.reconcile_and_apply(snapshots)
        report.reconciliation = recon_report
        if recon_report.stale_plans_closed > 0 or recon_report.fake_order_refs_found:
            report.add(
                "stale_state_reconciled",
                "ok",
                detail={
                    "stale_plans_closed": recon_report.stale_plans_closed,
                    "fake_refs_cleaned": len(recon_report.fake_order_refs_found),
                    "applied": True,
                },
            )
        else:
            report.add(
                "stale_state_check",
                "ok",
                detail={"stale_plans": 0, "fake_refs": 0, "applied": False},
            )
    else:
        recon_report = await service.reconcile(snapshots)
        report.reconciliation = recon_report
        if recon_report.stale_plans_closed > 0 or recon_report.fake_order_refs_found:
            report.add(
                "stale_state_detected",
                "fail",
                detail={
                    "stale_plans": recon_report.stale_plans_closed,
                    "fake_refs": len(recon_report.fake_order_refs_found),
                    "action": "Run with --apply-reconcile to clean up",
                    "fake_ref_details": [
                        {"position_id": f.position_id, "exchange": f.exchange, "field": f.field, "value": f.value}
                        for f in recon_report.fake_order_refs_found
                    ],
                },
            )
        else:
            report.add(
                "stale_state_check",
                "ok",
                detail={"stale_plans": 0, "fake_refs": 0},
            )

    if recon_report.unresolved_follower_positions > 0:
        report.add(
            "unresolved_follower_positions",
            "fail",
            detail={
                "count": recon_report.unresolved_follower_positions,
                "issues": recon_report.issues,
            },
        )


def _check_fake_order_ids(
    report: PreflightReport, plan_store: SqlitePositionPlanStore
) -> None:
    """Check for fake/test order IDs in position plans."""
    fake_refs: list[dict[str, str]] = []
    for plan in plan_store.list_active_positions():
        for leg in plan_store.get_legs(plan.position_id):
            if leg.entry_order_id and is_fake_order_id(leg.entry_order_id):
                fake_refs.append({
                    "position_id": plan.position_id,
                    "exchange": leg.exchange.value,
                    "role": leg.role.value if hasattr(leg.role, "value") else str(leg.role),
                    "field": "entry_order_id",
                    "value": leg.entry_order_id,
                })
            if leg.stop_order_id and is_fake_order_id(leg.stop_order_id):
                fake_refs.append({
                    "position_id": plan.position_id,
                    "exchange": leg.exchange.value,
                    "role": leg.role.value if hasattr(leg.role, "value") else str(leg.role),
                    "field": "stop_order_id",
                    "value": leg.stop_order_id,
                })
    if fake_refs:
        report.add(
            "fake_order_id_detection",
            "fail",
            detail={
                "count": len(fake_refs),
                "refs": fake_refs,
                "action": "Run with --apply-reconcile to clean up",
            },
        )
    else:
        report.add("fake_order_id_detection", "ok", detail={"count": 0})


async def _check_kline_warmup(
    *,
    app_config: AppConfig,
    strategy: object,
    report: PreflightReport,
    market_data_db_path: str | Path,
) -> None:
    """Verify closed-kline warmup data is available using a snapshot path."""
    import gc

    try:
        from src.market_data.storage import SqliteKlineStore
        from src.market_data.warmup.gap_detector import interval_to_ms
        from src.runtime.requirements import resolve_strategy_runtime_requirements

        requirements = resolve_strategy_runtime_requirements(
            strategy, fallback_data_streams=app_config.data_streams
        )
        if not requirements.closed_kline.enabled:
            report.add("kline_warmup", "ok", detail={"skipped": "not required"})
            return

        interval = requirements.closed_kline.interval
        interval_ms = interval_to_ms(interval)
        end_open = closed_bar_open_time_ms(int(time.time() * 1000), interval_ms=interval_ms, close_buffer_ms=60000)
        start_open = max(0, end_open - int(requirements.closed_kline.warmup_days) * 24 * 60 * 60_000)

        store = SqliteKlineStore(market_data_db_path)
        try:
            from src.market_data.models import TimeRange

            rows = store.load(
                symbol=app_config.symbol,
                interval=interval,
                time_range=TimeRange(start_open, end_open),
            )
            available = sum(1 for r in rows if r.is_closed)
            min_records = max(1, int(requirements.closed_kline.min_records or 1))

            if available >= min_records:
                report.add(
                    "kline_warmup",
                    "ok",
                    detail={"available": available, "min_records": min_records, "interval": interval},
                )
            else:
                report.add(
                    "kline_warmup",
                    "fail",
                    detail={
                        "available": available,
                        "min_records": min_records,
                        "interval": interval,
                        "action": "Run warmup before starting live trading",
                    },
                )
        finally:
            del store
            gc.collect()
    except Exception as exc:
        report.add("kline_warmup", "fail", error=str(exc))


# ── Helpers ─────────────────────────────────────────────────────────────


def _finalize_verdict(report: PreflightReport) -> None:
    """Determine the overall preflight verdict from check results."""
    fail_checks = [c for c in report.checks if c.status == "fail"]
    if not fail_checks:
        if report.verdict not in {"fail_config", "fail_unresolved_follower_position"}:
            report.verdict = "pass"
        return

    # Check if we already have a specific verdict
    if report.verdict in {"fail_config", "fail_unresolved_follower_position"}:
        return

    fail_names = {c.name for c in fail_checks}
    if "stale_state_detected" in fail_names or "fake_order_id_detection" in fail_names:
        report.verdict = "fail_needs_reconcile"
    elif "unresolved_follower_positions" in fail_names:
        report.verdict = "fail_unresolved_follower_position"
    elif any("config" in name or "identity" in name for name in fail_names):
        report.verdict = "fail_config"
    else:
        report.verdict = "fail_needs_reconcile"


def _maybe_write_report(path: str, report: PreflightReport) -> None:
    """Write JSON report to disk if path is provided."""
    if not path:
        return
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(report.to_json(), encoding="utf-8")
        logger.info("Preflight report written | path=%s", path)
    except Exception as exc:
        logger.error("Failed to write preflight report | path=%s error=%s", path, exc)


def _write_provider_preflight_report(
    path: str | Path,
    report: Any,
    *,
    reconciliation_mode: str,
    confirmation_accepted: bool,
) -> None:
    payload = json.loads(report.to_json())
    payload["reconciliation_mode"] = reconciliation_mode
    payload["confirmation_accepted"] = confirmation_accepted
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


# ── CLI ─────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="AetherEdge unified live preflight check",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Exit codes:
  0 = PASS
  1 = FAIL_NEEDS_RECONCILE (run with --apply-reconcile to fix)
  2 = FAIL_UNRESOLVED_FOLLOWER_POSITION
  3 = FAIL_CONFIG""",
    )
    parser.add_argument(
        "--strategy",
        default=None,
        help="Strategy alias or module:attribute path",
    )
    parser.add_argument(
        "--defaults",
        default=str(REPO_ROOT / "config" / "aether_defaults.json"),
        help="Path to defaults JSON (default: config/aether_defaults.json)",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env file path",
    )
    parser.add_argument(
        "--report",
        default=os.environ.get(
            "AETHER_LIVE_PREFLIGHT_REPORT",
            str(
                REPO_ROOT
                / "data"
                / "reports"
                / "preflight"
                / "portfolio_v1_preflight.json"
            ),
        ),
        help="Output report path",
    )
    parser.add_argument(
        "--apply-reconcile",
        action="store_true",
        help="Apply reconciliation to clean up stale state and fake order IDs",
    )
    parser.add_argument(
        "--confirm-reconcile-write",
        default=None,
        help=(
            "Required with --apply-reconcile; must equal "
            f"{RECONCILE_WRITE_CONFIRMATION}"
        ),
    )
    parser.add_argument(
        "--confirm-position-plan-db",
        default=None,
        help="Required absolute PositionPlan database path for apply mode",
    )
    parser.add_argument(
        "--confirm-order-journal-db",
        default=None,
        help="Required absolute order-journal database path for apply mode",
    )
    parser.add_argument(
        "--skip-api",
        action="store_true",
        help="Skip exchange REST API checks",
    )
    parser.add_argument(
        "--skip-kline",
        action="store_true",
        help="Skip kline warmup check",
    )
    return parser.parse_args()


if __name__ == "__main__":
    code = asyncio.run(main())
    sys.exit(code)
