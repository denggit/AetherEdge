from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Mapping

from src.app import AppConfig
from src.order_management import (
    SqliteOrderJournalStore,
    SqlitePositionPlanStore,
)
from src.platform.account.factory import create_account_client
from src.platform.config import (
    ProjectEnvConfig,
    get_project_env_config,
)
from src.platform.exchanges.models import (
    ExchangeConfig,
    ExchangeName,
)
from src.platform.exchanges.credentials import validate_private_credentials
from src.platform.exchanges.errors import ExchangeConfigError
from src.platform.execution.factory import create_execution_client
from src.runtime.account_config import (
    AccountConfigEnv,
    load_account_config_env,
)
from src.runtime.config import live_runtime_config_from_app
from strategies.eth_portfolio_v1.preflight.live_gate import (
    EXIT_FAIL_API,
    EXIT_FAIL_CONFIG,
    EXIT_FAIL_STATE,
    PortfolioV1LiveGate,
    PortfolioV1LiveGateReport,
)
from strategies.eth_portfolio_v1.preflight.readiness import (
    PortfolioV1ReadinessInspector,
)
from strategies.eth_portfolio_v1.preflight.mf_feature_backfill import (
    resolve_mf_feature_backfill_enabled,
)


class PortfolioV1LiveSmokeProvider:
    """Build and run the strategy-owned, read-only direct-live gate."""

    def __init__(
        self,
        *,
        strategy: object,
        strategy_path: str,
        defaults_path: str | Path,
        env_file: str | Path | None,
        repo_root: str | Path,
        project_env: ProjectEnvConfig | None = None,
        report_kind: str = "smoke",
        skip_api: bool = False,
        skip_kline: bool = False,
        database_path_overrides: Mapping[str, str | Path] | None = None,
        database_source_paths: Mapping[str, str | Path] | None = None,
    ) -> None:
        self.strategy = strategy
        self.strategy_path = strategy_path
        self.defaults_path = Path(defaults_path)
        self.env_file = env_file
        self.repo_root = Path(repo_root)
        self.project_env = (
            project_env
            if project_env is not None
            else get_project_env_config()
        )
        self.report_kind = str(report_kind)
        self.skip_api = bool(skip_api)
        self.skip_kline = bool(skip_kline)
        self.database_path_overrides = (
            None
            if database_path_overrides is None
            else dict(database_path_overrides)
        )
        self.database_source_paths = (
            None
            if database_source_paths is None
            else dict(database_source_paths)
        )

    async def run(self) -> PortfolioV1LiveGateReport:
        try:
            env = dict(self.project_env.values)
            app_config = AppConfig.from_env(
                defaults_path=self.defaults_path,
                environ=env,
            )
            app_config = replace(
                app_config,
                strategy=self.strategy_path,
            )
            runtime_config = live_runtime_config_from_app(
                app_config,
                defaults_path=self.defaults_path,
                environ=env,
            )
        except Exception as exc:
            return _bootstrap_failure(
                verdict="fail_config",
                exit_code=EXIT_FAIL_CONFIG,
                issue=(
                    "config_bootstrap_failed:"
                    f"{type(exc).__name__}"
                ),
            )

        exchange_configs = []
        if not self.skip_api:
            try:
                for exchange in app_config.exchanges:
                    exchange_config = ExchangeConfig.from_env(
                        exchange,
                        env=env,
                    )
                    validate_private_credentials(
                        exchange,
                        exchange_config,
                    )
                    exchange_configs.append((exchange, exchange_config))
            except ExchangeConfigError as exc:
                report = _bootstrap_failure(
                    verdict="fail_config",
                    exit_code=EXIT_FAIL_CONFIG,
                    issue=str(exc),
                )
                _add_context(report, app_config, runtime_config)
                return report

        plan_path = Path(
            env.get(
                "AETHER_POSITION_PLAN_DB",
                "data/state/aether_position_plan.sqlite3",
            )
        )
        journal_path = Path(
            env.get(
                "AETHER_ORDER_JOURNAL_DB",
                "data/state/aether_order_journal.sqlite3",
            )
        )
        source_database_paths = {
            "state": Path(app_config.state_db_path),
            "position_plan": plan_path,
            "order_journal": journal_path,
            "range_checkpoint": Path(
                runtime_config.range_checkpoint_db_path
            ),
            "mf_feature": Path(runtime_config.market_data_db_path),
        }
        database_paths, override_issue = _resolve_database_paths(
            report_kind=self.report_kind,
            source_paths=source_database_paths,
            overrides=self.database_path_overrides,
        )
        report_database_paths = _report_database_paths(
            snapshot_paths=database_paths,
            source_paths=(
                source_database_paths
                if self.database_source_paths is None
                else {
                    name: Path(path)
                    for name, path in self.database_source_paths.items()
                }
            ),
        )
        if override_issue is not None:
            report = _bootstrap_failure(
                verdict="fail_config",
                exit_code=EXIT_FAIL_CONFIG,
                issue=override_issue,
            )
            report.database_paths = report_database_paths
            _add_context(report, app_config, runtime_config)
            return report
        missing = [
            name
            for name, path in database_paths.items()
            if not path.is_file()
        ]
        if missing:
            report = _bootstrap_failure(
                verdict="fail_state",
                exit_code=EXIT_FAIL_STATE,
                issue="database_missing:" + ",".join(missing),
            )
            report.database_paths = report_database_paths
            _add_context(report, app_config, runtime_config)
            return report

        try:
            accounts = []
            executions = []
            if not self.skip_api:
                for exchange, exchange_config in exchange_configs:
                    accounts.append(
                        create_account_client(
                            exchange,
                            exchange_config,
                            symbol=app_config.symbol,
                        )
                    )
                    executions.append(
                        create_execution_client(
                            exchange,
                            exchange_config,
                            symbol=app_config.symbol,
                        )
                    )
            plan_store = SqlitePositionPlanStore(
                database_paths["position_plan"]
            )
            journal = SqliteOrderJournalStore(
                database_paths["order_journal"]
            )
        except Exception as exc:
            report = _bootstrap_failure(
                verdict="fail_api",
                exit_code=EXIT_FAIL_API,
                issue=(
                    "client_bootstrap_failed:"
                    f"{type(exc).__name__}"
                ),
            )
            _add_context(report, app_config, runtime_config)
            return report

        config = getattr(self.strategy, "config")
        requirements = config.runtime_requirements
        closed_kline = dict(
            requirements.get("closed_kline", {})
        )
        inspector = PortfolioV1ReadinessInspector(
            symbol=app_config.symbol,
            market_data_db_path=database_paths["mf_feature"],
            range_checkpoint_db_path=(
                database_paths["range_checkpoint"]
            ),
            exchange=app_config.data_exchange.value,
            range_pct=str(config.mf.range_pct),
            price_step=str(config.mf.range_price_step),
            closed_kline_interval=str(
                closed_kline.get("interval", "4h")
            ),
            lf_min_records=int(
                closed_kline.get("min_records", 2000)
            ),
            range_speed_min_periods=(
                config.entry_filters.range_speed_min_periods
            ),
            mf_required_minutes=config.mf.decision_buffer_minutes,
            readiness_mode="historical_preflight",
            large_share_min_samples=(
                config.mf.large_share_min_samples
            ),
            large_share_window_days=(
                config.mf.large_share_window_days
            ),
        )
        # Load account config env for correct margin mode in preflight snapshots.
        account_config_env: AccountConfigEnv | None = None
        try:
            if not self.skip_api:
                account_config_env = load_account_config_env(
                    exchanges=app_config.exchanges,
                    symbol=app_config.symbol,
                    environ=env,
                    require_leverage=False,
                )
        except Exception:
            account_config_env = None

        gate = PortfolioV1LiveGate(
            app_config=app_config,
            runtime_config=runtime_config,
            strategy=self.strategy,
            account_clients=accounts,
            execution_clients=executions,
            position_plan_store=plan_store,
            order_journal=journal,
            readiness_inspector=inspector,
            database_paths=database_paths,
            repo_root=self.repo_root,
            required_master_exchange=ExchangeName.OKX,
            required_follower_exchange=ExchangeName.BINANCE,
            call_strategy_on_start=True,
            report_kind=self.report_kind,
            startup_feature_backfill_enabled=(
                resolve_mf_feature_backfill_enabled(env)
            ),
            skip_api=self.skip_api,
            skip_kline=self.skip_kline,
            sensitive_values=tuple(
                str(value)
                for key, value in env.items()
                if any(
                    marker in key.upper()
                    for marker in (
                        "API_KEY",
                        "SECRET",
                        "PASSPHRASE",
                        "EMAIL_PASSWORD",
                    )
                )
            ),
            account_config_env=account_config_env,
        )
        report = await gate.run()
        report.database_paths = report_database_paths
        return report


_REQUIRED_DATABASE_PATHS = frozenset(
    {
        "state",
        "position_plan",
        "order_journal",
        "range_checkpoint",
        "mf_feature",
    }
)


def _resolve_database_paths(
    *,
    report_kind: str,
    source_paths: Mapping[str, Path],
    overrides: Mapping[str, str | Path] | None,
) -> tuple[dict[str, Path], str | None]:
    if overrides is None:
        if report_kind == "preflight":
            return dict(source_paths), "read_only_database_overrides_required"
        return dict(source_paths), None

    missing = sorted(_REQUIRED_DATABASE_PATHS - set(overrides))
    extra = sorted(set(overrides) - _REQUIRED_DATABASE_PATHS)
    if missing:
        return {}, "database_override_missing:" + ",".join(missing)
    if extra:
        return {}, "database_override_unknown:" + ",".join(extra)
    resolved: dict[str, Path] = {}
    for name in sorted(_REQUIRED_DATABASE_PATHS):
        raw_path = Path(overrides[name]).expanduser()
        if not raw_path.is_absolute():
            return {}, f"database_override_not_absolute:{name}"
        resolved[name] = raw_path.resolve(strict=False)
    return resolved, None


def _report_database_paths(
    *,
    snapshot_paths: Mapping[str, Path],
    source_paths: Mapping[str, Path],
) -> dict[str, str]:
    report_paths = {
        name: str(path.expanduser().resolve(strict=False))
        for name, path in snapshot_paths.items()
    }
    report_paths.update(
        {
            f"{name}_source": str(path.expanduser().resolve(strict=False))
            for name, path in source_paths.items()
        }
    )
    return report_paths


def _bootstrap_failure(
    *,
    verdict: str,
    exit_code: int,
    issue: str,
) -> PortfolioV1LiveGateReport:
    return PortfolioV1LiveGateReport(
        ok=False,
        verdict=verdict,
        exit_code=exit_code,
        issues=[issue],
    )


def _add_context(
    report: PortfolioV1LiveGateReport,
    app_config: AppConfig,
    runtime_config: object,
) -> None:
    report.symbol = app_config.symbol
    report.runtime_mode = runtime_config.mode.value
    report.data_exchange = app_config.data_exchange.value
    report.exchanges = [
        exchange.value for exchange in app_config.exchanges
    ]


__all__ = ["PortfolioV1LiveSmokeProvider"]
