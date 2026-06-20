import argparse
import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.app import EmailAlertSink, NoopAlertSink, ProcessWatchdog, WatchdogConfig, build_live_runner_command
from src.platform.config import load_env_config


def _load_defaults() -> dict:
    path = PROJECT_ROOT / "config" / "aether_defaults.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _bool(value) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _optional_int(value):
    if value in (None, "", "none", "None"):
        return None
    return int(value)


async def main() -> int:
    env = load_env_config()
    defaults = _load_defaults()
    parser = argparse.ArgumentParser(description="Watch and restart the AetherEdge live runner if it exits unexpectedly.")
    parser.add_argument("--max-restarts", type=int, default=_optional_int(env.get("AETHER_WATCHDOG_MAX_RESTARTS", defaults.get("watchdog_max_restarts", "10"))))
    parser.add_argument("--restart-delay", type=float, default=float(env.get("AETHER_WATCHDOG_RESTART_DELAY_SECONDS", defaults.get("watchdog_restart_delay_seconds", "5"))))
    parser.add_argument("--no-email", action="store_true", help="Disable watchdog email alerts even if AETHER_ENABLE_EMAIL_ALERT=true.")
    parser.add_argument("child_args", nargs=argparse.REMAINDER, help="Arguments after -- are passed to scripts/run_live.py")
    args = parser.parse_args()

    child_args = list(args.child_args)
    if child_args and child_args[0] == "--":
        child_args = child_args[1:]

    command = build_live_runner_command(project_root=PROJECT_ROOT, child_args=child_args)
    enable_email = _bool(env.get("AETHER_ENABLE_EMAIL_ALERT", "false")) and not args.no_email
    alert_sink = EmailAlertSink() if enable_email else NoopAlertSink()
    config = WatchdogConfig(
        command=command,
        cwd=str(PROJECT_ROOT),
        restart_delay_seconds=args.restart_delay,
        max_restarts=args.max_restarts,
    )
    stats = await ProcessWatchdog(config, alert_sink=alert_sink).run()
    print(stats)
    if stats.last_return_code not in (None, 0):
        return int(stats.last_return_code or 1)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
