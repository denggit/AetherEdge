#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import gc
import os
import sys
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.runtime.live_smoke import (
    BootstrapFailureReport,
    FiniteLiveSmokeRunner,
    strategy_plugin_path,
    write_live_smoke_report,
)
from src.platform.config import (
    load_project_env_config,
    set_project_env_config,
)
from src.app import AppConfig
from src.runtime.config import live_runtime_config_from_app
from src.platform.exchanges.credentials import validate_private_credentials
from src.platform.exchanges.errors import ExchangeConfigError
from src.platform.exchanges.models import ExchangeConfig
from src.strategy import load_strategy
from tools._sqlite_readonly_snapshot import (
    ReadOnlySnapshotError,
    stable_sqlite_snapshots,
)


async def run_server_smoke(
    *,
    defaults_path: str | Path,
    env_file: str | Path | None,
    strategy_name: str,
    repo_root: str | Path = REPO_ROOT,
    provider_hook: str = "live_smoke_provider",
    provider_kwargs: Mapping[str, Any] | None = None,
    strategy_kwargs: Mapping[str, Any] | None = None,
    read_only_state: bool = True,
    database_source_paths: Mapping[str, str | Path] | None = None,
):
    strategy_path = strategy_plugin_path(strategy_name)
    try:
        project_env = load_project_env_config(
            env_file=env_file,
        )
    except (OSError, RuntimeError, UnicodeError, ValueError) as exc:
        return BootstrapFailureReport(
            verdict="fail_config",
            exit_code=1,
            issues=[
                "live_smoke_config_load_failed:"
                f"{type(exc).__name__}"
            ],
        )
    if not bool((provider_kwargs or {}).get("skip_api", False)):
        try:
            app_config = AppConfig.from_env(
                defaults_path=defaults_path,
                environ=project_env.values,
            )
            for exchange in app_config.exchanges:
                validate_private_credentials(
                    exchange,
                    ExchangeConfig.from_env(exchange, env=project_env.values),
                )
        except ExchangeConfigError as exc:
            return BootstrapFailureReport(
                verdict="fail_config",
                exit_code=1,
                issues=[str(exc)],
            )
        except Exception as exc:
            return BootstrapFailureReport(
                verdict="fail_config",
                exit_code=1,
                issues=[
                    "live_smoke_config_bootstrap_failed:"
                    f"{type(exc).__name__}"
                ],
            )
    async def run_provider(
        *,
        database_overrides: Mapping[str, Path] | None = None,
        source_paths: Mapping[str, Path] | None = None,
    ):
        set_project_env_config(project_env)
        resolved_strategy_kwargs = dict(strategy_kwargs or {})
        resolved_provider_kwargs = dict(provider_kwargs or {})
        if database_overrides is not None:
            resolved_strategy_kwargs["mf_store_path"] = database_overrides[
                "mf_feature"
            ]
            resolved_provider_kwargs["database_path_overrides"] = dict(
                database_overrides
            )
            resolved_provider_kwargs["database_source_paths"] = dict(
                source_paths or {}
            )
        strategy = load_strategy(
            strategy_path,
            **resolved_strategy_kwargs,
        )
        provider_factory = getattr(strategy, provider_hook, None)
        if not callable(provider_factory):
            return BootstrapFailureReport(
                verdict="fail_config",
                exit_code=1,
                issues=[
                    "strategy_live_provider_missing:"
                    f"{provider_hook}"
                ],
            )
        provider = provider_factory(
            strategy_path=strategy_path,
            defaults_path=defaults_path,
            env_file=env_file,
            repo_root=repo_root,
            project_env=project_env,
            report_kind=(
                "preflight"
                if provider_hook == "live_preflight_provider"
                else "smoke"
            ),
            **resolved_provider_kwargs,
        )
        return await FiniteLiveSmokeRunner(provider).run()

    try:
        if read_only_state:
            sources = (
                _resolve_database_source_paths(
                    defaults_path=defaults_path,
                    project_env=project_env,
                )
                if database_source_paths is None
                else {
                    str(name): Path(path).expanduser().resolve(strict=False)
                    for name, path in database_source_paths.items()
                }
            )
            with stable_sqlite_snapshots(sources) as snapshots:
                try:
                    result = await run_provider(
                        database_overrides=snapshots,
                        source_paths=sources,
                    )
                finally:
                    # SQLite store context managers do not close connections;
                    # collect only after every source has already been copied.
                    gc.collect()
                return result
        return await run_provider()
    except ReadOnlySnapshotError as exc:
        return BootstrapFailureReport(
            verdict="fail_state",
            exit_code=1,
            issues=[f"read_only_snapshot_failed:{exc}"],
        )
    except Exception as exc:
        return BootstrapFailureReport(
            verdict="fail_unknown",
            exit_code=7,
            issues=[
                "live_smoke_bootstrap_failed:"
                f"{type(exc).__name__}"
            ],
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="AetherEdge read-only strategy server smoke"
    )
    parser.add_argument("--strategy", required=True)
    parser.add_argument(
        "--defaults",
        default=str(
            REPO_ROOT / "config" / "aether_defaults.json"
        ),
    )
    parser.add_argument("--env-file", default=None)
    parser.add_argument(
        "--report",
        default=os.environ.get(
            "AETHER_LIVE_SMOKE_REPORT",
            str(
                REPO_ROOT
                / "data"
                / "reports"
                / "preflight"
                / "portfolio_v1_server_smoke.json"
            ),
        ),
    )
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    report = await run_server_smoke(
        defaults_path=args.defaults,
        env_file=args.env_file,
        strategy_name=args.strategy,
        read_only_state=True,
    )
    write_live_smoke_report(args.report, report)
    return int(report.exit_code)


def _resolve_database_source_paths(
    *,
    defaults_path: str | Path,
    project_env,
) -> dict[str, Path]:
    app_config = AppConfig.from_env(
        defaults_path=defaults_path,
        environ=project_env.values,
    )
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


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))


__all__ = ["main", "run_server_smoke"]
