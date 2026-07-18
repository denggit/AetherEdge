from __future__ import annotations

from dataclasses import dataclass
from src.runtime.strategy_capabilities import (
    StrategyCapabilityError,
    StrategyContractError,
    ValidatedStrategyCapabilities,
    validate_dynamic_strategy_capabilities,
    validate_strategy_capabilities,
)
from src.strategy.ports import (
    RangeSpeedHistoryProvider,
    StrategyDecisionAuditProvider,
    StrategyPendingWorkProvider,
    StrategyRecoveryStatus,
    StrategyRecoveryStatusProvider,
    StrategyStartupPreviewProvider,
    StrategyStopAdoptionProvider,
)
from src.utils.log import get_logger

logger = get_logger("src.runtime.runner")

@dataclass
class LiveRuntimeStats:
    market_events_seen: int = 0
    account_events_seen: int = 0
    feature_events_seen: int = 0
    signals_seen: int = 0
    dry_run_actions: int = 0
    order_intents_created: int = 0
    order_results_seen: int = 0
    submitted_intents: int = 0
    partial_failures: int = 0
    failed_intents: int = 0
    range_bars_closed: int = 0
    range_aggregates_created: int = 0
    closed_klines_seen: int = 0
    warmup_runs: int = 0
    recovery_runs: int = 0
    on_start_called: bool = False
    producer_failures: int = 0
    producer_stale: int = 0
    errors: int = 0
    market_events_dropped: int = 0

@dataclass(frozen=True)
class MarketQueueDrainResult:
    processed: int
    deferred: int
    examined: int
    queue_size_before: int
    queue_size_after: int
    duration_ms: int
    hit_event_limit: bool
    hit_time_limit: bool

class LiveRuntimeError(RuntimeError):
    pass

FATAL_STARTUP_ERROR_MARKERS = (
    "closed-kline warmup loaded insufficient records",
    "closed-kline warmup did not catch up",
    "startup snapshot is required before live trading",
    "startup reconciliation missing exchange snapshots",
    "startup reconciliation failed",
    "runtime recovery failed",
    "strategy position mode requirement failed",
    "live preflight/smoke report gate failed",
    "direct-live trading requires aether_required_live_strategy",
    "live strategy does not match required launch target",
    "unsupported runtime mode",
    "private_credentials",
)

def _is_fatal_startup_error(exc: BaseException) -> bool:
    """Return True when the error should cause a fatal exit (code 78)."""
    if isinstance(exc, StrategyCapabilityError):
        return True
    text = str(exc).lower()
    return any(marker in text for marker in FATAL_STARTUP_ERROR_MARKERS)

@dataclass
class StartupPreviewState:
    """Snapshot of strategy mutable state captured before a startup catch-up
    preview so it can be rolled back when the previewed signal is ultimately
    NOT executed (e.g. price guard failure or journal dedupe)."""

    provider: StrategyStartupPreviewProvider | None
    state: object | None
