from __future__ import annotations

import hashlib

from src.platform.exchanges.models import ExchangeName
from src.signals.models import TradeSignal

_ACTION_CODES = {
    "open_long": "ol",
    "open_short": "os",
    "close_long": "cl",
    "close_short": "cs",
    "reduce_long": "rl",
    "reduce_short": "rs",
    "place_stop_loss_long": "sl",
    "place_stop_loss_short": "ss",
    "cancel_all_orders": "ca",
    "cancel_all_stop_orders": "cs",
}


class DeterministicClientOrderIdFactory:
    """Create compact deterministic client order IDs for idempotency."""

    def __init__(self, *, prefix: str = "AE") -> None:
        self.prefix = _clean(prefix)[:4] or "AE"

    def create(self, *, strategy_id: str, signal: TradeSignal, exchange: ExchangeName, sequence: int = 0) -> str:
        action_code = _ACTION_CODES.get(signal.action.value, "xx")
        digest = hashlib.blake2b(
            f"{strategy_id}|{signal.symbol}|{signal.action.value}|{signal.created_time_ms}|{exchange.value}|{sequence}".encode("utf-8"),
            digest_size=8,
        ).hexdigest().upper()
        exchange_code = exchange.value[:2].upper()
        return f"{self.prefix}{exchange_code}{action_code.upper()}{digest}"[:32]


def _clean(value: str) -> str:
    return "".join(ch for ch in str(value).upper() if ch.isalnum())
