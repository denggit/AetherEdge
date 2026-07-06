from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.app import AppConfig
from src.order_management.quantity import NativeQuantityConverter
from src.order_management.reconciliation.service import (
    LiveStateReconciliationService,
)
from src.order_management.safety import (
    RecoveryExitOrderValidator,
    order_matches_position_scope,
)
from src.platform import ExchangeName, PositionMode, PositionSide
from src.platform.markets import get_market_profile
from src.platform.snapshot import PlatformSnapshot, fetch_platform_snapshot
from src.runtime.config import LiveRuntimeConfig
from src.runtime.no_mutation import (
    MutationAttemptError,
    NoMutationExecutionClient,
)
from strategies.eth_portfolio_v1.preflight.readiness import (
    PortfolioV1ReadinessInspector,
)
from strategies.eth_portfolio_v1.domain.recovery import (
    audit_portfolio_v1_plans,
    plan_sleeve_id,
)
from strategies.eth_portfolio_v1.domain.sleeves import (
    LF_SLEEVE_ID,
    MF_RESERVED_SLEEVE_ID,
)


EXIT_PASS = 0
EXIT_FAIL_CONFIG = 1
EXIT_FAIL_API = 2
EXIT_FAIL_STATE = 3
EXIT_FAIL_MARKET_DATA = 4
EXIT_FAIL_RECOVERY = 5
EXIT_FAIL_MUTATION_ATTEMPT = 6
EXIT_FAIL_UNKNOWN = 7

_SENSITIVE_KEY_PARTS = (
    "api_key",
    "api_secret",
    "passphrase",
    "email_password",
    "secret",
    "password",
)


@dataclass(frozen=True)
class LiveGateCheck:
    name: str
    status: str
    detail: Mapping[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class PortfolioV1LiveGateReport:
    generated_at_ms: int = field(
        default_factory=lambda: int(time.time() * 1000)
    )
    report_kind: str = "smoke"
    strategy: str = "eth_portfolio_v1"
    symbol: str = ""
    runtime_mode: str = ""
    exchanges: list[str] = field(default_factory=list)
    data_exchange: str = ""
    hedge_mode: dict[str, Any] = field(default_factory=dict)
    account_snapshot_summary: dict[str, Any] = field(default_factory=dict)
    position_plan_summary: dict[str, Any] = field(default_factory=dict)
    recovery_audit_summary: dict[str, Any] = field(default_factory=dict)
    lf_data_readiness: dict[str, Any] = field(default_factory=dict)
    mf_data_readiness: dict[str, Any] = field(default_factory=dict)
    causal_audit: dict[str, Any] = field(default_factory=dict)
    startup_gate_results: list[LiveGateCheck] = field(default_factory=list)
    database_paths: dict[str, str] = field(default_factory=dict)
    mutation_attempted: bool = False
    mutation_attempts: list[str] = field(default_factory=list)
    ok: bool = False
    verdict: str = "fail_unknown"
    exit_code: int = EXIT_FAIL_UNKNOWN
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    _sensitive_values: tuple[str, ...] = field(
        default=(), repr=False
    )

    def add(
        self,
        name: str,
        *,
        ok: bool,
        detail: Mapping[str, Any] | None = None,
        error: str | None = None,
        status: str | None = None,
    ) -> None:
        self.startup_gate_results.append(
            LiveGateCheck(
                name=name,
                status=status or ("ok" if ok else "fail"),
                detail=dict(detail or {}),
                error=error,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return _sanitize(
            {
                "strategy": self.strategy,
                "generated_at_ms": self.generated_at_ms,
                "report_kind": self.report_kind,
                "symbol": self.symbol,
                "runtime_mode": self.runtime_mode,
                "exchanges": list(self.exchanges),
                "data_exchange": self.data_exchange,
                "hedge_mode": dict(self.hedge_mode),
                "account_snapshot_summary": dict(
                    self.account_snapshot_summary
                ),
                "position_plan_summary": dict(
                    self.position_plan_summary
                ),
                "recovery_audit_summary": dict(
                    self.recovery_audit_summary
                ),
                "lf_data_readiness": dict(self.lf_data_readiness),
                "mf_data_readiness": dict(self.mf_data_readiness),
                "causal_audit": dict(self.causal_audit),
                "startup_gate_results": [
                    asdict(check) for check in self.startup_gate_results
                ],
                "database_paths": dict(self.database_paths),
                "mutation_attempted": self.mutation_attempted,
                "mutation_attempts": list(self.mutation_attempts),
                "ok": self.ok,
                "verdict": self.verdict,
                "exit_code": self.exit_code,
                "issues": list(self.issues),
                "warnings": list(self.warnings),
            },
            sensitive_values=self._sensitive_values,
        )

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            indent=2,
            ensure_ascii=False,
            default=str,
        )


class PortfolioV1LiveGate:
    """Run the real direct-live startup evidence checks without mutations."""

    def __init__(
        self,
        *,
        app_config: AppConfig,
        runtime_config: LiveRuntimeConfig,
        strategy: object,
        account_clients: Sequence[object],
        execution_clients: Sequence[object],
        position_plan_store: object,
        order_journal: object,
        readiness_inspector: PortfolioV1ReadinessInspector,
        database_paths: Mapping[str, str | Path],
        repo_root: str | Path,
        required_master_exchange: ExchangeName,
        required_follower_exchange: ExchangeName,
        call_strategy_on_start: bool = True,
        report_kind: str = "smoke",
        startup_feature_backfill_enabled: bool = True,
        sensitive_values: Sequence[str] = (),
    ) -> None:
        self.app_config = app_config
        self.runtime_config = runtime_config
        self.strategy = strategy
        self.account_clients = tuple(
            NoMutationExecutionClient(client)
            for client in account_clients
        )
        self.execution_clients = tuple(
            NoMutationExecutionClient(client)
            for client in execution_clients
        )
        self.position_plan_store = position_plan_store
        self.order_journal = order_journal
        self.readiness_inspector = readiness_inspector
        self.database_paths = {
            str(name): Path(path)
            for name, path in database_paths.items()
        }
        self.repo_root = Path(repo_root)
        self.required_master_exchange = required_master_exchange
        self.required_follower_exchange = required_follower_exchange
        self.call_strategy_on_start = call_strategy_on_start
        self.report_kind = str(report_kind)
        self.startup_feature_backfill_enabled = bool(
            startup_feature_backfill_enabled
        )
        self.sensitive_values = tuple(
            value for value in sensitive_values if value
        )

    async def run(self) -> PortfolioV1LiveGateReport:
        report = PortfolioV1LiveGateReport(
            report_kind=self.report_kind,
            symbol=self.app_config.symbol,
            runtime_mode=self.runtime_config.mode.value,
            exchanges=[
                exchange.value for exchange in self.app_config.exchanges
            ],
            data_exchange=self.app_config.data_exchange.value,
            database_paths={
                name: str(path)
                for name, path in self.database_paths.items()
            },
            _sensitive_values=self.sensitive_values,
        )
        try:
            config_issues = self._config_issues()
            report.add(
                "direct_live_config",
                ok=not config_issues,
                detail=self._config_audit(),
                error="; ".join(config_issues) or None,
            )
            if config_issues:
                return self._fail(
                    report,
                    verdict="fail_config",
                    exit_code=EXIT_FAIL_CONFIG,
                    issues=config_issues,
                )

            forbidden_hits = _forbidden_source_hits(self.repo_root)
            report.add(
                "forbidden_source_scan",
                ok=not forbidden_hits,
                detail={"match_count": len(forbidden_hits)},
                error=(
                    None
                    if not forbidden_hits
                    else "forbidden source token found"
                ),
            )
            if forbidden_hits:
                return self._fail(
                    report,
                    verdict="fail_config",
                    exit_code=EXIT_FAIL_CONFIG,
                    issues=["forbidden_source_token_found"],
                )

            db_issues = self._probe_databases()
            report.add(
                "database_read_write_probe",
                ok=not db_issues,
                detail={"paths": report.database_paths},
                error="; ".join(db_issues) or None,
            )
            if db_issues:
                return self._fail(
                    report,
                    verdict="fail_state",
                    exit_code=EXIT_FAIL_STATE,
                    issues=db_issues,
                )

            snapshots = await self._fetch_snapshots(report)
            if snapshots is None:
                return self._fail(
                    report,
                    verdict="fail_api",
                    exit_code=EXIT_FAIL_API,
                    issues=["account_snapshot_read_failed"],
                )

            hedge_issues = self._hedge_mode_issues(snapshots)
            report.hedge_mode = dict(
                getattr(self, "_last_hedge_audit", {})
            )
            report.add(
                "hedge_mode",
                ok=not hedge_issues,
                detail=report.hedge_mode,
                error="; ".join(hedge_issues) or None,
            )
            if hedge_issues:
                return self._fail(
                    report,
                    verdict="fail_config",
                    exit_code=EXIT_FAIL_CONFIG,
                    issues=hedge_issues,
                )

            recovery_issues = await self._recovery_audit(
                snapshots, report
            )
            report.add(
                "portfolio_v1_recovery_audit",
                ok=not recovery_issues,
                detail=report.recovery_audit_summary,
                error="; ".join(recovery_issues) or None,
            )
            if recovery_issues:
                return self._fail(
                    report,
                    verdict="fail_recovery",
                    exit_code=EXIT_FAIL_RECOVERY,
                    issues=recovery_issues,
                )

            readiness = self.readiness_inspector.inspect()
            readiness_audit = readiness.audit()
            report.lf_data_readiness = readiness_audit[
                "lf_data_readiness"
            ]
            report.mf_data_readiness = readiness_audit[
                "mf_data_readiness"
            ]
            report.causal_audit = readiness_audit["causal_audit"]
            readiness_issues = tuple(
                str(issue) for issue in readiness.issues
            )
            lf_ready = bool(report.lf_data_readiness.get("ok"))
            mf_ready = bool(report.mf_data_readiness.get("ok"))
            mf_enabled = self._mf_enabled()
            mf_blocking = bool(
                mf_enabled
                and not mf_ready
                and not self.startup_feature_backfill_enabled
            )
            mf_degraded = bool(
                mf_enabled
                and not mf_ready
                and self.startup_feature_backfill_enabled
            )
            mf_issues = [
                issue
                for issue in readiness_issues
                if issue.startswith("mf_")
            ]
            report.lf_data_readiness.update(
                {
                    "blocking": not lf_ready,
                    "readiness_scope": "lf_primary_strategy",
                    "issues": [
                        issue
                        for issue in readiness_issues
                        if issue.startswith("lf_")
                    ],
                }
            )
            report.mf_data_readiness.update(
                {
                    "blocking": mf_blocking,
                    "sleeve_ready": mf_ready,
                    "signals_enabled": bool(mf_enabled and mf_ready),
                    "background_prebuild_required": bool(
                        mf_enabled and not mf_ready
                    ),
                    "readiness_scope": "mf_sleeve",
                    "issues": mf_issues,
                }
            )
            causal_blocking = self._causal_failure_blocks(readiness)
            report.causal_audit["blocking"] = causal_blocking
            report.add(
                "lf_data_readiness",
                ok=lf_ready,
                detail=report.lf_data_readiness,
            )
            report.add(
                "mf_data_readiness",
                ok=mf_ready,
                detail=report.mf_data_readiness,
                status="warn" if mf_degraded else None,
            )
            report.add(
                "causal_no_future_audit",
                ok=not causal_blocking,
                detail=report.causal_audit,
                status=(
                    "warn"
                    if not causal_blocking
                    and not bool(report.causal_audit.get("ok"))
                    else None
                ),
            )
            if mf_degraded:
                report.warnings.append(
                    "mf_data_not_ready_sleeve_disabled_until_ready"
                )

            hard_readiness_issues: list[str] = []
            for issue in readiness_issues:
                if not lf_ready and issue.startswith("lf_"):
                    hard_readiness_issues.append(issue)
                elif mf_blocking and issue.startswith("mf_"):
                    hard_readiness_issues.append(issue)
                elif causal_blocking and (
                    issue == "causal_future_violation"
                    or "future" in issue
                ):
                    hard_readiness_issues.append(issue)
            if not lf_ready and not any(
                issue.startswith("lf_")
                for issue in hard_readiness_issues
            ):
                hard_readiness_issues.append("lf_data_not_ready")
            if mf_blocking and not any(
                issue.startswith("mf_")
                for issue in hard_readiness_issues
            ):
                hard_readiness_issues.append("mf_data_not_ready")
            if causal_blocking and not any(
                issue == "causal_future_violation"
                for issue in hard_readiness_issues
            ):
                hard_readiness_issues.append("causal_future_violation")
            if hard_readiness_issues:
                return self._fail(
                    report,
                    verdict="fail_market_data",
                    exit_code=EXIT_FAIL_MARKET_DATA,
                    issues=hard_readiness_issues,
                )

            if self.call_strategy_on_start:
                on_start_issues = await self._call_on_start_read_only(
                    snapshots,
                    report,
                    allow_mf_not_ready=mf_degraded,
                )
                if on_start_issues:
                    return self._fail(
                        report,
                        verdict="fail_recovery",
                        exit_code=EXIT_FAIL_RECOVERY,
                        issues=on_start_issues,
                    )

            mutation_issues = self._mutation_issues()
            if mutation_issues:
                return self._fail(
                    report,
                    verdict="fail_mutation_attempt",
                    exit_code=EXIT_FAIL_MUTATION_ATTEMPT,
                    issues=mutation_issues,
                )

            report.ok = True
            report.verdict = "pass"
            report.exit_code = EXIT_PASS
            report.add(
                "direct_live_startup_gates",
                ok=True,
                detail={
                    "producers_started": False,
                    "signals_executed": False,
                },
            )
            return report
        except MutationAttemptError as exc:
            return self._fail(
                report,
                verdict="fail_mutation_attempt",
                exit_code=EXIT_FAIL_MUTATION_ATTEMPT,
                issues=[str(exc)],
            )
        except Exception as exc:
            return self._fail(
                report,
                verdict="fail_unknown",
                exit_code=EXIT_FAIL_UNKNOWN,
                issues=[f"{type(exc).__name__}:{exc}"],
            )
        finally:
            self._copy_mutation_audit(report)

    def _config_issues(self) -> list[str]:
        issues: list[str] = []
        strategy_config = getattr(self.strategy, "config", None)
        strategy_id = str(
            getattr(strategy_config, "strategy_id", "")
        ).strip()
        strategy_symbol = str(
            getattr(strategy_config, "symbol", "")
        ).strip()
        mf_config = getattr(strategy_config, "mf", None)
        policy = self.runtime_config.master_follower_policy
        exchanges = set(self.app_config.exchanges)
        if self.runtime_config.mode.value != "live_runtime":
            issues.append("runtime_mode_not_live_runtime")
        if strategy_id != "eth_portfolio_v1":
            issues.append("strategy_not_eth_portfolio_v1")
        if strategy_symbol != self.app_config.symbol:
            issues.append("strategy_symbol_mismatch")
        if not {
            self.required_master_exchange,
            self.required_follower_exchange,
        }.issubset(exchanges):
            issues.append("required_master_and_follower_exchanges_missing")
        if (
            self.app_config.data_exchange
            is not self.required_master_exchange
        ):
            issues.append("data_exchange_must_match_required_master")
        if policy is None:
            issues.append("master_follower_policy_missing")
        else:
            if (
                policy.master_exchange
                is not self.required_master_exchange
            ):
                issues.append("master_exchange_role_mismatch")
            if (
                self.required_follower_exchange
                not in policy.follower_exchanges
            ):
                issues.append("required_follower_role_missing")
        if mf_config is None or not bool(
            getattr(mf_config, "enabled", False)
        ):
            issues.append("mf_must_be_enabled")
        if str(getattr(mf_config, "exit_variant", "")) != "time48":
            issues.append("mf_exit_variant_must_be_time48")
        if self.app_config.dry_run:
            issues.append("direct_live_config_cannot_be_dry_run")
        for client in self.execution_clients:
            sandbox = getattr(client._client, "_sandbox", None)
            live_enabled = getattr(
                client._client, "_live_trading_enabled", None
            )
            if sandbox is True:
                issues.append(
                    f"{client.exchange.value}_sandbox_not_direct_live"
                )
            if live_enabled is False:
                issues.append(
                    f"{client.exchange.value}_live_trading_not_enabled"
                )
        return issues

    def _config_audit(self) -> dict[str, Any]:
        strategy_config = getattr(self.strategy, "config", None)
        mf_config = getattr(strategy_config, "mf", None)
        policy = self.runtime_config.master_follower_policy
        return {
            "strategy": getattr(strategy_config, "strategy_id", None),
            "symbol": self.app_config.symbol,
            "exchanges": [
                exchange.value for exchange in self.app_config.exchanges
            ],
            "data_exchange": self.app_config.data_exchange.value,
            "required_master_exchange": (
                self.required_master_exchange.value
            ),
            "required_follower_exchange": (
                self.required_follower_exchange.value
            ),
            "master_exchange": (
                None if policy is None else policy.master_exchange.value
            ),
            "follower_exchanges": (
                []
                if policy is None
                else [
                    exchange.value
                    for exchange in policy.follower_exchanges
                ]
            ),
            "mf_enabled": bool(
                getattr(mf_config, "enabled", False)
            ),
            "mf_feature_backfill_enabled": (
                self.startup_feature_backfill_enabled
            ),
            "mf_exit_variant": getattr(
                mf_config, "exit_variant", None
            ),
            "dry_run": self.app_config.dry_run,
            "execution_client_direct_live": {
                client.exchange.value: {
                    "sandbox": getattr(
                        client._client, "_sandbox", None
                    ),
                    "live_trading_enabled": getattr(
                        client._client,
                        "_live_trading_enabled",
                        None,
                    ),
                }
                for client in self.execution_clients
            },
        }

    def _probe_databases(self) -> list[str]:
        issues: list[str] = []
        for name, path in self.database_paths.items():
            if not path.is_file():
                issues.append(f"{name}_db_missing")
                continue
            try:
                with sqlite3.connect(path) as conn:
                    conn.execute("SELECT 1")
                    conn.execute("BEGIN IMMEDIATE")
                    conn.rollback()
            except (OSError, sqlite3.Error) as exc:
                issues.append(f"{name}_db_probe_failed:{exc}")
        return issues

    async def _fetch_snapshots(
        self, report: PortfolioV1LiveGateReport
    ) -> tuple[PlatformSnapshot, ...] | None:
        accounts = {
            client.exchange: client for client in self.account_clients
        }
        executions = {
            client.exchange: client for client in self.execution_clients
        }
        snapshots: list[PlatformSnapshot] = []
        try:
            for exchange in self.app_config.exchanges:
                account = accounts.get(exchange)
                execution = executions.get(exchange)
                if account is None or execution is None:
                    raise RuntimeError(
                        f"missing read client for {exchange.value}"
                    )
                snapshots.append(
                    await fetch_platform_snapshot(
                        account=account,
                        execution=execution,
                    )
                )
        except Exception as exc:
            report.add(
                "account_api_read",
                ok=False,
                error=str(exc),
            )
            return None
        report.account_snapshot_summary = {
            snapshot.balance.exchange.value: {
                "position_count": sum(
                    position.quantity != 0
                    for position in snapshot.positions
                ),
                "open_order_count": len(snapshot.open_orders),
                "open_stop_count": len(snapshot.open_stop_orders),
                "position_mode": snapshot.position_mode.value,
                "balance_read": True,
                "leverage_read": True,
            }
            for snapshot in snapshots
        }
        report.add(
            "account_api_read",
            ok=True,
            detail=report.account_snapshot_summary,
        )
        return tuple(snapshots)

    def _hedge_mode_issues(
        self, snapshots: Sequence[PlatformSnapshot]
    ) -> list[str]:
        issues: list[str] = []
        report: dict[str, Any] = {}
        by_exchange = {
            snapshot.balance.exchange: snapshot for snapshot in snapshots
        }
        for exchange in self.app_config.exchanges:
            snapshot = by_exchange.get(exchange)
            mode = None if snapshot is None else snapshot.position_mode
            ok = mode is PositionMode.HEDGE
            report[exchange.value] = {
                "required": "hedge",
                "actual": (
                    "unknown" if mode is None else mode.value
                ),
                "ok": ok,
            }
            if not ok:
                issues.append(f"{exchange.value}_hedge_mode_required")
        return_value = issues
        # Assigned here so report serialization always has per-exchange mode.
        # The caller owns the report object and copies this mapping.
        self._last_hedge_audit = report
        return return_value

    async def _recovery_audit(
        self,
        snapshots: Sequence[PlatformSnapshot],
        report: PortfolioV1LiveGateReport,
    ) -> list[str]:
        payloads = tuple(
            self.position_plan_store.serialize_active_positions()
        )
        audit = audit_portfolio_v1_plans(payloads)
        issues = [str(issue) for issue in audit.get("issues", ())]
        active_positions = sum(
            position.quantity != 0
            for snapshot in snapshots
            for position in snapshot.positions
        )
        active_plans = int(audit["plans"]["active_count"])
        if active_positions and not active_plans:
            issues.append("exchange_position_without_local_plan")
        if active_plans and not active_positions:
            issues.append("local_active_plan_with_exchange_flat")
        stop_issues, stop_audit = self._stop_scope_audit(
            snapshots=snapshots,
            payloads=payloads,
        )
        issues.extend(stop_issues)

        reconciliation = LiveStateReconciliationService(
            position_plan_store=self.position_plan_store,
            order_journal=self.order_journal,
            state_store=None,
            alert_sink=None,
        )
        reconcile_report = await reconciliation.reconcile(
            tuple(snapshots)
        )
        if not reconcile_report.ok:
            issues.extend(
                f"reconciliation:{issue}"
                for issue in reconcile_report.issues
            )
            if not reconcile_report.issues:
                issues.append(
                    "reconciliation:"
                    f"{reconcile_report.verdict.value}"
                )
        report.position_plan_summary = {
            "active_count": active_plans,
            "active_position_ids": audit.get(
                "active_position_ids", []
            ),
            "active_sleeves": audit.get("active_sleeves", []),
        }
        report.recovery_audit_summary = {
            **audit,
            "exchange_active_position_count": active_positions,
            "stop_scope_audit": stop_audit,
            "reconciliation": {
                "ok": reconcile_report.ok,
                "verdict": reconcile_report.verdict.value,
                "issues": list(reconcile_report.issues),
                "unresolved_follower_positions": (
                    reconcile_report.unresolved_follower_positions
                ),
                "read_only": True,
            },
            "issues": list(dict.fromkeys(issues)),
        }
        return list(dict.fromkeys(issues))

    def _stop_scope_audit(
        self,
        *,
        snapshots: Sequence[PlatformSnapshot],
        payloads: Sequence[Mapping[str, Any]],
    ) -> tuple[list[str], dict[str, Any]]:
        issues: list[str] = []
        scopes: list[dict[str, Any]] = []
        market_profile = get_market_profile(self.app_config.symbol)
        converter = NativeQuantityConverter()
        validator = RecoveryExitOrderValidator(
            quantity_converter=converter
        )
        snapshot_by_exchange = {
            snapshot.balance.exchange.value: snapshot
            for snapshot in snapshots
        }
        for payload in payloads:
            position = dict(payload.get("position", {}))
            position_id = str(position.get("position_id") or "")
            sleeve_id = plan_sleeve_id(payload)
            side_text = str(position.get("side") or "").lower()
            position_side = (
                PositionSide.LONG
                if side_text == "long"
                else PositionSide.SHORT
                if side_text == "short"
                else None
            )
            stop_price = _positive_decimal(
                position.get("canonical_stop_price")
            )
            for raw_leg in payload.get("legs", ()):
                leg = dict(raw_leg)
                exchange = str(leg.get("exchange") or "").lower()
                known_ids = (
                    leg.get("stop_order_id"),
                    leg.get("stop_client_order_id"),
                )
                scope = {
                    "exchange": exchange,
                    "position_id": position_id,
                    "sleeve_id": sleeve_id,
                    "known_ids": known_ids,
                }
                scopes.append(scope)
                snapshot = snapshot_by_exchange.get(exchange)
                if snapshot is None:
                    issues.append(f"stop_snapshot_missing:{exchange}")
                    continue
                scoped_orders = tuple(
                    order
                    for order in snapshot.open_stop_orders
                    if order_matches_position_scope(
                        order,
                        position_id=position_id,
                        known_order_ids=known_ids,
                    )
                )
                if sleeve_id == MF_RESERVED_SLEEVE_ID:
                    if scoped_orders:
                        issues.append(
                            f"unexpected_mf_stop:{exchange}:{position_id}"
                        )
                    continue
                if sleeve_id != LF_SLEEVE_ID:
                    continue
                quantity = _positive_decimal(
                    leg.get("filled_qty_base")
                    or leg.get("target_qty_base")
                )
                if (
                    not scoped_orders
                    or stop_price is None
                    or quantity is None
                    or position_side is None
                ):
                    issues.append(
                        f"lf_required_stop_missing:{exchange}:{position_id}"
                    )
                    continue
                native = converter.convert_quantity(
                    exchange=snapshot.balance.exchange,
                    symbol=self.app_config.symbol,
                    base_quantity=quantity,
                    market_profile=market_profile,
                ).native_quantity
                validation = validator.validate_stop_orders(
                    exchange=snapshot.balance.exchange,
                    symbol=self.app_config.symbol,
                    strategy_id="eth_portfolio_v1",
                    position_id=position_id,
                    position_side=position_side,
                    position_mode=snapshot.position_mode,
                    current_position_native_quantity=native,
                    canonical_stop_price=stop_price,
                    open_stop_orders=scoped_orders,
                    open_orders=snapshot.open_orders,
                    market_profile=market_profile,
                )
                if not validation.should_keep_existing_stop:
                    issues.append(
                        "lf_required_stop_invalid:"
                        f"{exchange}:{position_id}:"
                        f"{validation.primary_invalid_reason}"
                    )

        for snapshot in snapshots:
            exchange = snapshot.balance.exchange.value
            for order in snapshot.open_stop_orders:
                assigned = any(
                    scope["exchange"] == exchange
                    and order_matches_position_scope(
                        order,
                        position_id=scope["position_id"],
                        known_order_ids=scope["known_ids"],
                    )
                    for scope in scopes
                )
                if not assigned:
                    issues.append(
                        "unknown_stop_scope:"
                        f"{exchange}:"
                        f"{order.order_id or order.client_order_id or 'unknown'}"
                    )
        return list(dict.fromkeys(issues)), {
            "ok": not issues,
            "scope_count": len(scopes),
            "issues": list(dict.fromkeys(issues)),
            "mf_protective_stop_required": False,
        }

    async def _call_on_start_read_only(
        self,
        snapshots: Sequence[PlatformSnapshot],
        report: PortfolioV1LiveGateReport,
        *,
        allow_mf_not_ready: bool = False,
    ) -> list[str]:
        on_start = getattr(self.strategy, "on_start", None)
        if not callable(on_start):
            return ["strategy_on_start_missing"]
        master = next(
            (
                snapshot
                for snapshot in snapshots
                if snapshot.balance.exchange
                is self.app_config.data_exchange
            ),
            None,
        )
        if master is None:
            return ["strategy_master_snapshot_missing"]
        signals = tuple(await on_start(master) or ())
        mf_audit_value = getattr(
            self.strategy, "last_mf_signal_audit", {}
        )
        mf_audit = (
            dict(mf_audit_value)
            if isinstance(mf_audit_value, Mapping)
            else {}
        )
        mf_ready = bool(mf_audit.get("data_ready", False))
        issues: list[str] = []
        if signals:
            issues.append("strategy_on_start_emitted_signals")
        if not mf_ready and not allow_mf_not_ready:
            issues.append("strategy_on_start_mf_not_ready")
        report.add(
            "strategy_on_start_read_only",
            ok=not issues,
            detail={
                "signal_count": len(signals),
                "signals_executed": False,
                "producers_started": False,
                "mf_data_ready": mf_ready,
                "mf_signals_enabled": mf_ready,
                "mf_not_ready_blocking": bool(
                    not mf_ready and not allow_mf_not_ready
                ),
                "mf_readiness_source": mf_audit.get(
                    "readiness_source"
                ),
            },
            error=(
                None
                if not issues
                else "; ".join(issues)
            ),
        )
        return issues

    def _mf_enabled(self) -> bool:
        strategy_config = getattr(self.strategy, "config", None)
        mf_config = getattr(strategy_config, "mf", None)
        return bool(getattr(mf_config, "enabled", False))

    @staticmethod
    def _causal_failure_blocks(
        readiness: object,
    ) -> bool:
        causal = getattr(readiness, "causal", {})
        if bool(causal.get("ok")):
            return False
        known_checks = (
            "lf_closed_bar_not_future",
            "lf_range_available_not_future",
            "no_future_feature_rows",
        )
        if not any(check in causal for check in known_checks):
            return True
        if not all(bool(causal.get(check)) for check in known_checks):
            return True
        return any(
            issue.startswith(("lf_", "mf_")) and "future" in issue
            for issue in getattr(readiness, "issues", ())
        )

    def _mutation_issues(self) -> list[str]:
        attempts = [
            method
            for client in (
                *self.account_clients,
                *self.execution_clients,
            )
            for method in client.mutation_attempts
        ]
        return [
            f"mutation_attempted:{method}" for method in attempts
        ]

    def _copy_mutation_audit(
        self, report: PortfolioV1LiveGateReport
    ) -> None:
        attempts = [
            method
            for client in (
                *self.account_clients,
                *self.execution_clients,
            )
            for method in client.mutation_attempts
        ]
        report.mutation_attempts = attempts
        report.mutation_attempted = bool(attempts)
        if attempts and report.exit_code != EXIT_FAIL_MUTATION_ATTEMPT:
            report.ok = False
            report.verdict = "fail_mutation_attempt"
            report.exit_code = EXIT_FAIL_MUTATION_ATTEMPT
            report.issues.extend(
                f"mutation_attempted:{method}" for method in attempts
            )

    def _fail(
        self,
        report: PortfolioV1LiveGateReport,
        *,
        verdict: str,
        exit_code: int,
        issues: Sequence[str],
    ) -> PortfolioV1LiveGateReport:
        report.ok = False
        report.verdict = verdict
        report.exit_code = exit_code
        report.issues = list(
            dict.fromkeys([*report.issues, *(str(item) for item in issues)])
        )
        return report


def write_live_gate_report(
    path: str | Path,
    report: PortfolioV1LiveGateReport,
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(report.to_json(), encoding="utf-8")


def _forbidden_source_hits(repo_root: Path) -> list[str]:
    tokens = (
        "mfe_" + "lock",
        "mfe_" + "lock_15_05",
        "mfe_" + "lock_15_05_time48",
    )
    hits: list[str] = []
    for path in repo_root.rglob("*"):
        if not path.is_file():
            continue
        if any(
            part == ".git" or part.startswith(".pytest")
            for part in path.parts
        ):
            continue
        if path.suffix.lower() not in {
            ".py",
            ".json",
            ".md",
            ".toml",
            ".yaml",
            ".yml",
        }:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        if any(token in text for token in tokens):
            hits.append(str(path.relative_to(repo_root)))
    return hits


def _sanitize(
    value: Any,
    *,
    key: str = "",
    sensitive_values: Sequence[str] = (),
) -> Any:
    normalized_key = key.strip().lower()
    if any(part in normalized_key for part in _SENSITIVE_KEY_PARTS):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {
            str(item_key): _sanitize(
                item,
                key=str(item_key),
                sensitive_values=sensitive_values,
            )
            for item_key, item in value.items()
            if not any(
                part in str(item_key).strip().lower()
                for part in _SENSITIVE_KEY_PARTS
            )
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [
            _sanitize(item, sensitive_values=sensitive_values)
            for item in value
        ]
    if isinstance(value, str):
        redacted = value
        for secret in sensitive_values:
            if secret:
                redacted = redacted.replace(secret, "[REDACTED]")
        return redacted
    return value


def _positive_decimal(value: object) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


__all__ = [
    "EXIT_FAIL_API",
    "EXIT_FAIL_CONFIG",
    "EXIT_FAIL_MARKET_DATA",
    "EXIT_FAIL_MUTATION_ATTEMPT",
    "EXIT_FAIL_RECOVERY",
    "EXIT_FAIL_STATE",
    "EXIT_FAIL_UNKNOWN",
    "EXIT_PASS",
    "LiveGateCheck",
    "PortfolioV1LiveGate",
    "PortfolioV1LiveGateReport",
    "write_live_gate_report",
]
