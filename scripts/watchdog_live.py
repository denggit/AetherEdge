import argparse
import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.app import (
    EmailAlertSink,
    NoopAlertSink,
    PidFileProcessController,
    ProcessWatchdog,
    WatchdogConfig,
    build_live_runner_command,
)
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


def _str_path(value, fallback: str) -> str:
    return str(value) if value not in (None, "") else fallback


def _parser(env: dict, defaults: dict) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Watch and restart the AetherEdge live runner if it exits unexpectedly.")
    parser.add_argument("action", nargs="?", choices=["run", "start", "stop", "restart", "status"], default="start")
    parser.add_argument("--max-restarts", type=int, default=_optional_int(env.get("AETHER_WATCHDOG_MAX_RESTARTS", defaults.get("watchdog_max_restarts", "10"))))
    parser.add_argument("--restart-delay", type=float, default=float(env.get("AETHER_WATCHDOG_RESTART_DELAY_SECONDS", defaults.get("watchdog_restart_delay_seconds", "5"))))
    parser.add_argument("--pid-file", default=_str_path(env.get("AETHER_WATCHDOG_PID_FILE"), defaults.get("watchdog_pid_file", "data/run/aether_watchdog.pid")))
    parser.add_argument("--log-file", default=_str_path(env.get("AETHER_WATCHDOG_LOG_FILE"), defaults.get("watchdog_log_file", "logs/aether_watchdog.log")))
    parser.add_argument("--stop-timeout", type=float, default=float(env.get("AETHER_WATCHDOG_STOP_TIMEOUT_SECONDS", defaults.get("watchdog_stop_timeout_seconds", "10"))))
    parser.add_argument("--no-email", action="store_true", help="Disable watchdog email alerts even if AETHER_ENABLE_EMAIL_ALERT=true.")
    parser.add_argument("child_args", nargs=argparse.REMAINDER, help="Arguments after -- are passed to scripts/run_live.py")
    return parser


def _clean_child_args(raw_args: list[str]) -> list[str]:
    if raw_args and raw_args[0] == "--":
        return raw_args[1:]
    return raw_args


def _run_command(args) -> tuple[str, ...]:
    command = [sys.executable, str(PROJECT_ROOT / "scripts" / "watchdog_live.py"), "run"]
    if args.max_restarts is not None:
        command.extend(["--max-restarts", str(args.max_restarts)])
    command.extend(["--restart-delay", str(args.restart_delay)])
    if args.no_email:
        command.append("--no-email")
    child_args = _clean_child_args(list(args.child_args))
    if child_args:
        command.append("--")
        command.extend(child_args)
    return tuple(command)


def _controller(args) -> PidFileProcessController:
    return PidFileProcessController(pid_file=PROJECT_ROOT / args.pid_file, log_file=PROJECT_ROOT / args.log_file, cwd=PROJECT_ROOT)


async def _run_watchdog(args, env: dict) -> int:
    child_args = _clean_child_args(list(args.child_args))
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


def main() -> int:
    env = load_env_config()
    defaults = _load_defaults()
    args = _parser(env, defaults).parse_args()
    controller = _controller(args)

    if args.action == "run":
        return asyncio.run(_run_watchdog(args, env))
    if args.action == "start":
        result = controller.start(_run_command(args))
        print(result)
        return 0 if result.ok else 1
    if args.action == "stop":
        result = controller.stop(timeout_seconds=args.stop_timeout)
        print(result)
        return 0 if result.ok else 1
    if args.action == "restart":
        stop_result = controller.stop(timeout_seconds=args.stop_timeout)
        print(stop_result)
        start_result = controller.start(_run_command(args))
        print(start_result)
        return 0 if start_result.ok else 1
    if args.action == "status":
        result = controller.status()
        print(result)
        return 0 if result.ok else 1
    raise ValueError(f"unsupported action: {args.action}")


if __name__ == "__main__":
    raise SystemExit(main())
