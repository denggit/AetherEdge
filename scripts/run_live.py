#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""AetherEdge live runner entrypoint.

This script keeps the original lightweight app runner as the default. New live
runtime orchestration can be enabled with ``AETHER_RUNTIME_MODE=live_runtime``
without changing the watchdog entrypoint.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.platform.config import ProjectEnvConfig, load_project_env_config, set_project_env_config
from src.platform.exchanges.credentials import validate_private_credentials
from src.platform.exchanges.errors import ExchangeConfigError
from src.platform.exchanges.models import ExchangeConfig


def bootstrap_live_process_config(project_root: Path = PROJECT_ROOT) -> ProjectEnvConfig:
    config = load_project_env_config(
        env_file=project_root / ".env",
    )
    set_project_env_config(config)
    globals()["PROJECT_ENV_CONFIG"] = config
    return config


PROJECT_ENV_CONFIG = bootstrap_live_process_config()

from src.app import AppConfig, AppRunner, build_app_context
from src.runtime import LiveRuntimeRunner, RuntimeMode, live_runtime_config_from_app, runtime_mode_from_env
from src.runtime.runner import LiveRuntimeError, _is_fatal_startup_error
from src.utils.log import get_logger
from scripts.live_launch_gate import (
    live_reports_required,
    strategy_identity,
    validate_live_launch_reports,
)

logger = get_logger(__name__)

FATAL_STARTUP_EXIT_CODE = 78


def _log_live_process_config_loaded(config: ProjectEnvConfig) -> None:
    logger.info(
        "Live process env config loaded | source_files=%s key_count=%s "
        "runtime_mode=%s strategy=%s exchanges=%s follower_exchanges=%s data_exchange=%s "
        "live_trading=%s dry_run=%s margin_mode=%s okx_leverage=%s binance_leverage=%s",
        config.source_files,
        len(config.values),
        config.get("AETHER_RUNTIME_MODE"),
        config.get("AETHER_STRATEGY"),
        config.get("AETHER_EXCHANGES"),
        config.get("AETHER_FOLLOWER_EXCHANGES"),
        config.get("AETHER_DATA_EXCHANGE"),
        config.get("AETHER_LIVE_TRADING"),
        config.get("AETHER_DRY_RUN"),
        config.get("MARGIN_MODE"),
        config.get("OKX_LEVERAGE"),
        config.get("BINANCE_LEVERAGE"),
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run AetherEdge live runner.")
    parser.add_argument("--max-events", type=int, default=None, help="Stop after N market events; useful for smoke runs.")
    parser.add_argument("--defaults", default="config/aether_defaults.json", help="Path to stable defaults JSON.")
    parser.add_argument("--preflight-report", default=None)
    parser.add_argument("--smoke-report", default=None)
    parser.add_argument("--live-gate-max-age-seconds", type=float, default=None)
    args = parser.parse_args()

    _log_live_process_config_loaded(PROJECT_ENV_CONFIG)
    config = AppConfig.from_env(defaults_path=args.defaults)
    runtime_mode = runtime_mode_from_env(defaults_path=args.defaults)
    if runtime_mode is RuntimeMode.LIVE_RUNTIME:
        is_direct_live = PROJECT_ENV_CONFIG.get_bool(
            "AETHER_LIVE_TRADING", False
        ) and not PROJECT_ENV_CONFIG.get_bool("AETHER_DRY_RUN", True)
        required_strategy = PROJECT_ENV_CONFIG.get(
            "AETHER_REQUIRED_LIVE_STRATEGY",
            "",
        )
        if is_direct_live and not required_strategy:
            raise LiveRuntimeError(
                "direct-live trading requires AETHER_REQUIRED_LIVE_STRATEGY "
                "to be set in .env"
            )
        if is_direct_live:
            _validate_direct_live_private_credentials(config)
        if (
            required_strategy
            and strategy_identity(config.strategy)
            != strategy_identity(required_strategy)
        ):
            raise LiveRuntimeError(
                "live strategy does not match required launch target | "
                f"configured={strategy_identity(config.strategy)} "
                f"required={strategy_identity(required_strategy)}"
            )
        require_reports = live_reports_required(
            runtime_mode=runtime_mode,
            strategy=config.strategy,
            configured=PROJECT_ENV_CONFIG.get_bool(
                "AETHER_REQUIRE_LIVE_GATE_REPORTS",
                False,
            ),
            is_direct_live=is_direct_live,
        )
        if require_reports:
            preflight_report = (
                args.preflight_report
                or PROJECT_ENV_CONFIG.get(
                    "AETHER_LIVE_PREFLIGHT_REPORT",
                    (
                        "data/reports/preflight/"
                        "portfolio_v1_preflight.json"
                    ),
                )
            )
            smoke_report = (
                args.smoke_report
                or PROJECT_ENV_CONFIG.get(
                    "AETHER_LIVE_SMOKE_REPORT",
                    (
                        "data/reports/preflight/"
                        "portfolio_v1_server_smoke.json"
                    ),
                )
            )
            max_age_seconds = (
                args.live_gate_max_age_seconds
                if args.live_gate_max_age_seconds is not None
                else PROJECT_ENV_CONFIG.get_float(
                    "AETHER_LIVE_GATE_MAX_AGE_SECONDS",
                    600.0,
                )
            )
            gate = validate_live_launch_reports(
                app_config=config,
                preflight_report_path=preflight_report,
                smoke_report_path=smoke_report,
                max_age_seconds=max_age_seconds,
            )
            if not gate.ok:
                raise LiveRuntimeError(
                    "live preflight/smoke report gate failed | "
                    f"issues={list(gate.issues)}"
                )
    context = build_app_context(config)
    logger.info("Live runner starting | runtime_mode=%s symbol=%s max_events=%s", runtime_mode.value, config.symbol, args.max_events)
    if runtime_mode is RuntimeMode.LIVE_RUNTIME:
        runtime_config = live_runtime_config_from_app(config, defaults_path=args.defaults)
        runner = LiveRuntimeRunner(app_config=config, app_context=context, runtime_config=runtime_config)
        stats = await runner.run(max_market_events=args.max_events)
    else:
        runner = AppRunner(config=config, context=context)
        stats = await runner.run_streams(max_market_events=args.max_events)
    logger.info("Live runner stopped | stats=%s", stats)


def _validate_direct_live_private_credentials(config: AppConfig) -> None:
    """Fail before building strategy clients when live secrets are invalid."""

    for exchange in config.exchanges:
        try:
            validate_private_credentials(
                exchange,
                ExchangeConfig.from_env(exchange),
            )
        except ExchangeConfigError as exc:
            raise LiveRuntimeError(str(exc)) from exc


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except LiveRuntimeError as exc:
        logger.exception("Live runner fatal startup/runtime error")
        if _is_fatal_startup_error(exc):
            raise SystemExit(FATAL_STARTUP_EXIT_CODE)
        raise
