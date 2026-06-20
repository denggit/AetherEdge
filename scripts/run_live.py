import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.app import AppConfig, AppRunner, build_app_context


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run AetherEdge lightweight app runner.")
    parser.add_argument("--max-events", type=int, default=None, help="Stop after N market events; useful for smoke runs.")
    parser.add_argument("--defaults", default="config/aether_defaults.json", help="Path to stable defaults JSON.")
    args = parser.parse_args()

    config = AppConfig.from_env(defaults_path=args.defaults)
    context = build_app_context(config)
    runner = AppRunner(config=config, context=context)
    stats = await runner.run_streams(max_market_events=args.max_events)
    print(stats)


if __name__ == "__main__":
    asyncio.run(main())
