from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from src.platform.account.ports import AccountClient
from src.platform.exchanges.models import ExchangeName, PositionMode


@dataclass(frozen=True)
class PositionModeRequirement:
    """A strategy-declared, exchange-scoped startup requirement."""

    required_mode: PositionMode
    exchanges: tuple[ExchangeName, ...]
    source: str


@dataclass(frozen=True)
class PositionModeStatus:
    exchange: ExchangeName
    symbol: str
    mode: str
    hedge_mode: bool
    source: str
    error: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)

    def audit(
        self,
        required_mode: PositionMode = PositionMode.HEDGE,
    ) -> dict[str, Any]:
        return {
            "exchange": self.exchange.value,
            "symbol": self.symbol,
            "required_mode": required_mode.value,
            "actual_mode": self.mode,
            "position_mode_ok": self.mode == required_mode.value,
            # Kept as a data-level compatibility field for existing reports.
            "hedge_mode_ok": self.hedge_mode,
            "source": self.source,
            "error": self.error,
        }


def resolve_position_mode_requirements(
    strategy: object,
) -> tuple[PositionModeRequirement, ...]:
    """Resolve optional startup requirements without knowing the plugin."""

    provider = getattr(strategy, "runtime_startup_requirements", None)
    if not callable(provider):
        return ()
    values = provider()
    if values is None:
        return ()
    resolved = tuple(values)
    invalid = tuple(
        type(value).__name__
        for value in resolved
        if not isinstance(value, PositionModeRequirement)
    )
    if invalid:
        raise TypeError(
            "invalid position mode requirement types: "
            f"{invalid}"
        )
    return resolved


def position_mode_status(
    *,
    exchange: ExchangeName,
    symbol: str,
    value: object,
    source: str,
    error: str | None = None,
) -> PositionModeStatus:
    raw = dict(value) if isinstance(value, Mapping) else {}
    candidate = _position_mode_candidate(value)
    mode = _normalized_mode(candidate)
    return PositionModeStatus(
        exchange=exchange,
        symbol=symbol,
        mode=mode,
        hedge_mode=mode == PositionMode.HEDGE.value,
        source=source,
        error=error,
        raw=raw,
    )


async def fetch_position_mode_statuses(
    *,
    exchanges: Sequence[ExchangeName],
    symbol: str,
    account_clients: Sequence[AccountClient],
    source: str,
) -> tuple[PositionModeStatus, ...]:
    clients: dict[ExchangeName, AccountClient] = {}
    for client in account_clients:
        try:
            exchange = client.exchange
            exchange_name = (
                exchange
                if isinstance(exchange, ExchangeName)
                else ExchangeName(str(exchange).strip().lower())
            )
        except (AttributeError, TypeError, ValueError):
            continue
        clients[exchange_name] = client

    statuses: list[PositionModeStatus] = []
    for exchange in exchanges:
        client = clients.get(exchange)
        if client is None:
            statuses.append(
                position_mode_status(
                    exchange=exchange,
                    symbol=symbol,
                    value=None,
                    source=source,
                    error="account_client_missing",
                )
            )
            continue
        try:
            mode = await client.fetch_position_mode()
            statuses.append(
                position_mode_status(
                    exchange=exchange,
                    symbol=symbol,
                    value=mode,
                    source=source,
                )
            )
        except Exception as exc:
            statuses.append(
                position_mode_status(
                    exchange=exchange,
                    symbol=symbol,
                    value=None,
                    source=source,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
    return tuple(statuses)


def _position_mode_candidate(value: object) -> object:
    if not isinstance(value, Mapping):
        return value
    if "posMode" in value:
        return value.get("posMode")
    if "dualSidePosition" in value:
        dual_side = value.get("dualSidePosition")
        if isinstance(dual_side, bool):
            return PositionMode.HEDGE if dual_side else PositionMode.ONE_WAY
        normalized = str(dual_side or "").strip().lower()
        if normalized == "true":
            return PositionMode.HEDGE
        if normalized == "false":
            return PositionMode.ONE_WAY
        return None
    return value.get("mode")


def _normalized_mode(value: object) -> str:
    if isinstance(value, PositionMode):
        return value.value
    normalized = str(value or "").strip().lower()
    if normalized in {
        PositionMode.HEDGE.value,
        "long_short_mode",
        "long_short",
        "dual_side",
        "dual_side_position",
    }:
        return PositionMode.HEDGE.value
    if normalized in {
        PositionMode.ONE_WAY.value,
        "net_mode",
        "net",
        "oneway",
        "single",
    }:
        return PositionMode.ONE_WAY.value
    return "unknown"


__all__ = [
    "PositionModeRequirement",
    "PositionModeStatus",
    "fetch_position_mode_statuses",
    "position_mode_status",
    "resolve_position_mode_requirements",
]
