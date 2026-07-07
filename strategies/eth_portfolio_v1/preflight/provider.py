from __future__ import annotations

from dataclasses import replace
from pathlib import Path

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
from src.platform.execution.factory import create_execution_client
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
        database_paths = {
            "state": Path(app_config.state_db_path),
            "position_plan": plan_path,
            "order_journal": journal_path,
            "range_checkpoint": Path(
                runtime_config.range_checkpoint_db_path
            ),
            "mf_feature": Path(runtime_config.market_data_db_path),
        }
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
            report.database_paths = {
                name: str(path)
                for name, path in database_paths.items()
            }
            _add_context(report, app_config, runtime_config)
            return report

        try:
            accounts = []
            executions = []
            if not self.skip_api:
                for exchange in app_config.exchanges:
                    exchange_config = ExchangeConfig.from_env(
                        exchange,
                        env=env,
                    )
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
            plan_store = SqlitePositionPlanStore(plan_path)
            journal = SqliteOrderJournalStore(journal_path)
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
            market_data_db_path=runtime_config.market_data_db_path,
            range_checkpoint_db_path=(
                runtime_config.range_checkpoint_db_path
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
        )
        return await gate.run()


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
