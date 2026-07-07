#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
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
from src.strategy import load_strategy


async def run_server_smoke(
    *,
    defaults_path: str | Path,
    env_file: str | Path | None,
    strategy_name: str,
    repo_root: str | Path = REPO_ROOT,
    provider_hook: str = "live_smoke_provider",
    provider_kwargs: Mapping[str, Any] | None = None,
):
    strategy_path = strategy_plugin_path(strategy_name)
    try:
        root = Path(repo_root)
        project_env = load_project_env_config(
            env_file=env_file,
            example_file=root / ".env.example",
            include_process_env=False,
        )
        set_project_env_config(project_env)
        strategy = load_strategy(strategy_path)
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
            **dict(provider_kwargs or {}),
        )
        return await FiniteLiveSmokeRunner(provider).run()
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
    write_live_smoke_report(args.report, report)
    return int(report.exit_code)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))


__all__ = ["main", "run_server_smoke"]
