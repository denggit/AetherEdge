#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WATCHDOG_SCRIPT="$PROJECT_ROOT/scripts/watchdog_live.py"
WATCHDOG_LOG="${WATCHDOG_LOG_FILE:-$PROJECT_ROOT/logs/aether_watchdog.out}"
WATCHDOG_PID="${WATCHDOG_PID_FILE:-$PROJECT_ROOT/data/run/aether_watchdog.pid}"
LIVE_PID="${LIVE_PID_FILE:-$PROJECT_ROOT/data/run/aether_live.pid}"
LIVE_LOG="${LIVE_LOG_FILE:-$PROJECT_ROOT/logs/aether_live.out}"
BACKFILL_PID="${AETHER_RANGE_BACKFILL_PID_FILE:-$PROJECT_ROOT/data/run/range_backfill_worker.pid}"
BACKFILL_STATUS="${AETHER_RANGE_BACKFILL_STATUS_JSON:-$PROJECT_ROOT/data/reports/range_backfill/status.json}"

cd "$PROJECT_ROOT"
mkdir -p "$(dirname "$WATCHDOG_LOG")" "$(dirname "$WATCHDOG_PID")" "$(dirname "$LIVE_PID")" "$(dirname "$LIVE_LOG")"

start() {
  if [[ -f "$WATCHDOG_PID" ]]; then
    old_pid="$(cat "$WATCHDOG_PID" || true)"
    if [[ -n "${old_pid:-}" ]] && kill -0 "$old_pid" 2>/dev/null; then
      echo "watchdog already running, pid=$old_pid"
      exit 0
    fi
    rm -f "$WATCHDOG_PID"
  fi

  echo "starting watchdog..."
  nohup "${LIVE_PYTHON_BIN:-python}" -u "$WATCHDOG_SCRIPT" >> "$WATCHDOG_LOG" 2>&1 &
  watchdog_pid=$!
  echo "$watchdog_pid" > "$WATCHDOG_PID"
  echo "watchdog started, pid=$watchdog_pid"
  echo "watchdog log: $WATCHDOG_LOG"
  echo "live log: $LIVE_LOG"
}

stop() {
  if [[ ! -f "$WATCHDOG_PID" ]]; then
    echo "watchdog pid file not found; nothing to stop"
    exit 0
  fi

  pid="$(cat "$WATCHDOG_PID" || true)"
  if [[ -z "${pid:-}" ]]; then
    rm -f "$WATCHDOG_PID"
    echo "empty watchdog pid file removed"
    exit 0
  fi

  if kill -0 "$pid" 2>/dev/null; then
    echo "stopping watchdog pid=$pid ..."
    kill "$pid"
    for _ in {1..20}; do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "watchdog did not stop, killing pid=$pid ..."
      kill -9 "$pid" || true
    fi
  else
    echo "watchdog pid=$pid is not running"
  fi

  rm -f "$WATCHDOG_PID"
  echo "watchdog stopped"
}

stop_live() {
  if [[ ! -f "$LIVE_PID" ]]; then
    echo "live child pid file not found; nothing to stop"
    return 0
  fi
  live_pid="$(cat "$LIVE_PID" || true)"
  if [[ -n "${live_pid:-}" ]] && kill -0 "$live_pid" 2>/dev/null; then
    echo "stopping live child pid=$live_pid ..."
    kill "$live_pid" || true
    for _ in {1..20}; do
      if ! kill -0 "$live_pid" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$live_pid" 2>/dev/null; then
      kill -9 "$live_pid" || true
    fi
  else
    echo "live child pid=$live_pid is not running"
  fi
  rm -f "$LIVE_PID"
}

stop_backfill() {
  if [[ ! -f "$BACKFILL_PID" ]]; then
    echo "range backfill worker: no pid file"
    return 0
  fi
  backfill_pid="$(cat "$BACKFILL_PID" || true)"
  if [[ -n "${backfill_pid:-}" ]] && kill -0 "$backfill_pid" 2>/dev/null; then
    echo "stopping range backfill worker pid=$backfill_pid ..."
    kill "$backfill_pid" || true
    for _ in {1..20}; do
      if ! kill -0 "$backfill_pid" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$backfill_pid" 2>/dev/null; then
      kill -9 "$backfill_pid" || true
    fi
  else
    echo "range backfill worker pid=$backfill_pid is not running"
  fi
  rm -f "$BACKFILL_PID" "$PROJECT_ROOT/data/run/range_backfill_worker.lock"
}

status() {
  if [[ -f "$WATCHDOG_PID" ]]; then
    pid="$(cat "$WATCHDOG_PID" || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "watchdog: running pid=$pid"
    else
      echo "watchdog: pid file exists but process not running"
    fi
  else
    echo "watchdog: not running"
  fi

  if [[ -f "$LIVE_PID" ]]; then
    live_pid="$(cat "$LIVE_PID" || true)"
    if [[ -n "${live_pid:-}" ]] && kill -0 "$live_pid" 2>/dev/null; then
      echo "live child: running pid=$live_pid"
    else
      echo "live child: pid file exists but process not running"
    fi
  else
    echo "live child: no pid file"
  fi

  if [[ -f "$BACKFILL_PID" ]]; then
    backfill_pid="$(cat "$BACKFILL_PID" || true)"
    if [[ -n "${backfill_pid:-}" ]] && kill -0 "$backfill_pid" 2>/dev/null; then
      echo "range backfill worker: running pid=$backfill_pid"
    else
      echo "range backfill worker: pid file exists but process not running"
    fi
  else
    echo "range backfill worker: no pid file"
  fi

  if [[ -f "$BACKFILL_STATUS" ]]; then
    "${LIVE_PYTHON_BIN:-python}" - "$BACKFILL_STATUS" <<'PY'
import json, sys
path = sys.argv[1]
try:
    data = json.load(open(path, encoding="utf-8"))
    print(
        "range backfill status: "
        f"range_speed_ready={data.get('range_speed_ready')} "
        f"missing_bucket_count={data.get('missing_bucket_count')}"
    )
except Exception as exc:
    print(f"range backfill status: unreadable error={exc}")
PY
  else
    echo "range backfill status: no status json"
  fi
}

logs() {
  tail -f "$WATCHDOG_LOG" "$LIVE_LOG"
}

case "${1:-start}" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop || true
    start
    ;;
  stop-live)
    stop_live
    ;;
  stop-backfill)
    stop_backfill
    ;;
  stop-all)
    stop || true
    stop_backfill || true
    ;;
  status)
    status
    ;;
  logs)
    logs
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs|stop-live|stop-backfill|stop-all}"
    exit 1
    ;;
esac
