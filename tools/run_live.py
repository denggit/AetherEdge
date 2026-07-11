import argparse
import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.app import AppConfig, AppRunner, build_app_context
from src.platform.config import load_project_env_config, set_project_env_config
from src.platform.exchanges.credentials import validate_private_credentials
from src.platform.exchanges.models import ExchangeConfig
from src.utils.log import get_logger

logger = get_logger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run AetherEdge lightweight app runner.")
    parser.add_argument("--max-events", type=int, default=None, help="Stop after N market events; useful for smoke runs.")
    parser.add_argument("--defaults", default="config/aether_defaults.json", help="Path to stable defaults JSON.")
    args = parser.parse_args()

    project_env = load_project_env_config(env_file=REPO_ROOT / ".env")
    set_project_env_config(project_env)
    config = AppConfig.from_env(defaults_path=args.defaults)
    is_direct_live = (
        project_env.get_bool("AETHER_LIVE_TRADING", False)
        and not config.dry_run
    )
    if is_direct_live:
        for exchange in config.exchanges:
            exchange_config = ExchangeConfig.from_env(
                exchange,
                env=project_env.values,
            )
            validate_private_credentials(exchange, exchange_config)

    context = build_app_context(config)
    runner = AppRunner(config=config, context=context)
    logger.info("Lightweight app runner starting | symbol=%s max_events=%s", config.symbol, args.max_events)
    stats = await runner.run_streams(max_market_events=args.max_events)
    logger.info("Lightweight app runner stopped | stats=%s", stats)


if __name__ == "__main__":
    asyncio.run(main())
