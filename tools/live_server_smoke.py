#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import replace
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.app import AppConfig
from src.order_management import (
    SqliteOrderJournalStore,
    SqlitePositionPlanStore,
)
from src.platform.account.factory import create_account_client
from src.platform.config import load_env_config
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.execution.factory import create_execution_client
from src.runtime.config import live_runtime_config_from_app
from src.runtime.portfolio_v1_live_gate import (
    EXIT_FAIL_CONFIG,
    EXIT_FAIL_STATE,
    PortfolioV1LiveGate,
    PortfolioV1LiveGateReport,
    write_live_gate_report,
)
from src.runtime.portfolio_v1_readiness import (
    PortfolioV1ReadinessInspector,
)
from src.strategy import load_strategy


class PortfolioV1ServerSmoke:
    """Finite smoke runner: execute gates once and never start producers."""

    def __init__(self, gate: PortfolioV1LiveGate) -> None:
        self.gate = gate
        self.producers_started = False

    async def run(self) -> PortfolioV1LiveGateReport:
        return await self.gate.run()


async def run_server_smoke(
    *,
    defaults_path: str | Path,
    env_file: str | Path | None,
    strategy_name: str,
    repo_root: str | Path = REPO_ROOT,
) -> PortfolioV1LiveGateReport:
    strategy_path = _strategy_path(strategy_name)
    try:
        env = load_env_config(env_file)
        app_config = AppConfig.from_env(
            defaults_path=defaults_path,
            env_file=env_file,
        )
        app_config = replace(app_config, strategy=strategy_path)
        runtime_config = live_runtime_config_from_app(
            app_config,
            defaults_path=defaults_path,
            env_file=env_file,
        )
        strategy = load_strategy(strategy_path)
    except Exception as exc:
        return _bootstrap_failure(
            verdict="fail_config",
            exit_code=EXIT_FAIL_CONFIG,
            issue=f"config_bootstrap_failed:{type(exc).__name__}",
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
            name: str(path) for name, path in database_paths.items()
        }
        _add_context(report, app_config, runtime_config)
        return report

    try:
        accounts = []
        executions = []
        for exchange in app_config.exchanges:
            exchange_config = ExchangeConfig.from_env(exchange, env=env)
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
            exit_code=2,
            issue=f"client_bootstrap_failed:{type(exc).__name__}",
        )
        _add_context(report, app_config, runtime_config)
        return report

    requirements = strategy.config.runtime_requirements
    closed_kline = dict(requirements.get("closed_kline", {}))
    inspector = PortfolioV1ReadinessInspector(
        symbol=app_config.symbol,
        market_data_db_path=runtime_config.market_data_db_path,
        range_checkpoint_db_path=runtime_config.range_checkpoint_db_path,
        exchange=app_config.data_exchange.value,
        range_pct=str(strategy.config.mf.range_pct),
        price_step=str(strategy.config.mf.range_price_step),
        closed_kline_interval=str(
            closed_kline.get("interval", "4h")
        ),
        lf_min_records=int(closed_kline.get("min_records", 2000)),
        range_speed_min_periods=(
            strategy.config.entry_filters.range_speed_min_periods
        ),
        mf_required_minutes=strategy.config.mf.decision_buffer_minutes,
        large_share_min_samples=(
            strategy.config.mf.large_share_min_samples
        ),
        large_share_window_days=(
            strategy.config.mf.large_share_window_days
        ),
    )
    gate = PortfolioV1LiveGate(
        app_config=app_config,
        runtime_config=runtime_config,
        strategy=strategy,
        account_clients=accounts,
        execution_clients=executions,
        position_plan_store=plan_store,
        order_journal=journal,
        readiness_inspector=inspector,
        database_paths=database_paths,
        repo_root=repo_root,
        required_master_exchange=ExchangeName.OKX,
        required_follower_exchange=ExchangeName.BINANCE,
        call_strategy_on_start=True,
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
    return await PortfolioV1ServerSmoke(gate).run()


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
    runtime_config,
) -> None:
    report.symbol = app_config.symbol
    report.runtime_mode = runtime_config.mode.value
    report.exchanges = [
        exchange.value for exchange in app_config.exchanges
    ]


def _strategy_path(value: str) -> str:
    normalized = str(value).strip()
    if normalized in {
        "eth_portfolio_v1",
        "strategies.eth_portfolio_v1",
    }:
        return "strategies.eth_portfolio_v1:Strategy"
    return normalized


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="AetherEdge read-only Portfolio V1 server smoke"
    )
    parser.add_argument(
        "--strategy",
        default="eth_portfolio_v1",
    )
    parser.add_argument(
        "--defaults",
        default=str(REPO_ROOT / "config" / "aether_defaults.json"),
    )
    parser.add_argument("--env-file", default=None)
    parser.add_argument(
        "--report",
        default=str(
            REPO_ROOT
            / "data"
            / "reports"
            / "preflight"
            / "portfolio_v1_server_smoke.json"
        ),
    )
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    report = await run_server_smoke(
        defaults_path=args.defaults,
        env_file=args.env_file,
        strategy_name=args.strategy,
    )
    write_live_gate_report(args.report, report)
    return int(report.exit_code)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))


__all__ = [
    "PortfolioV1ServerSmoke",
    "main",
    "run_server_smoke",
]
