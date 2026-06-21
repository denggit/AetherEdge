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

from src.app import AppConfig, AppRunner, build_app_context
from src.runtime import LiveRuntimeRunner, RuntimeMode, live_runtime_config_from_app, runtime_mode_from_env


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run AetherEdge live runner.")
    parser.add_argument("--max-events", type=int, default=None, help="Stop after N market events; useful for smoke runs.")
    parser.add_argument("--defaults", default="config/aether_defaults.json", help="Path to stable defaults JSON.")
    args = parser.parse_args()

    config = AppConfig.from_env(defaults_path=args.defaults)
    context = build_app_context(config)
    runtime_mode = runtime_mode_from_env(defaults_path=args.defaults)
    if runtime_mode is RuntimeMode.LIVE_RUNTIME:
        runtime_config = live_runtime_config_from_app(config, defaults_path=args.defaults)
        runner = LiveRuntimeRunner(app_config=config, app_context=context, runtime_config=runtime_config)
        stats = await runner.run(max_market_events=args.max_events)
    else:
        runner = AppRunner(config=config, context=context)
        stats = await runner.run_streams(max_market_events=args.max_events)
    print(stats)


if __name__ == "__main__":
    asyncio.run(main())
