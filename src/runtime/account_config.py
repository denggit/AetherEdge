from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.platform.account.ports import AccountClient
from src.platform.config import load_env_config
from src.platform.exchanges.models import ExchangeName, LeverageInfo, MarginMode, Order, Position
from src.platform.execution.ports import ExecutionClient
from src.utils.log import get_logger

logger = get_logger(__name__)


LEVERAGE_ENV_KEYS: Mapping[str, str] = {
    "okx": "OKX_LEVERAGE",
    "binance": "BINANCE_LEVERAGE",
}


class AccountConfigBootstrapError(RuntimeError):
    pass


@dataclass(frozen=True)
class AccountConfigTarget:
    exchange: ExchangeName
    symbol: str
    margin_mode: MarginMode
    leverage: Decimal


@dataclass(frozen=True)
class AccountConfigEnv:
    margin_mode: MarginMode
    targets: tuple[AccountConfigTarget, ...]
    missing_leverage: tuple[ExchangeName, ...] = ()

    def target_for(self, exchange: ExchangeName) -> AccountConfigTarget | None:
        return next((target for target in self.targets if target.exchange == exchange), None)


@dataclass(frozen=True)
class AccountConfigBootstrapResult:
    exchange: ExchangeName
    symbol: str
    expected_margin_mode: MarginMode
    expected_leverage: Decimal
    before_margin_mode: MarginMode | None
    before_leverage: Decimal | None
    after_margin_mode: MarginMode | None
    after_leverage: Decimal | None
    active_positions: tuple[Position, ...] = ()
    open_orders: tuple[Order, ...] = ()
    open_stop_orders: tuple[Order, ...] = ()
    applied: bool = False
    verified: bool = False
    skipped_write: bool = False
    reason: str | None = None
    error: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def has_blockers(self) -> bool:
        return bool(self.active_positions or self.open_orders or self.open_stop_orders)

    @property
    def ok(self) -> bool:
        return self.verified and self.error is None

    def detail(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange.value,
            "symbol": self.symbol,
            "expected_margin_mode": self.expected_margin_mode.value,
            "expected_leverage": str(self.expected_leverage),
            "before_margin_mode": None if self.before_margin_mode is None else self.before_margin_mode.value,
            "before_leverage": None if self.before_leverage is None else str(self.before_leverage),
            "after_margin_mode": None if self.after_margin_mode is None else self.after_margin_mode.value,
            "after_leverage": None if self.after_leverage is None else str(self.after_leverage),
            "active_positions": len(self.active_positions),
            "open_orders": len(self.open_orders),
            "open_stop_orders": len(self.open_stop_orders),
            "applied": self.applied,
            "verified": self.verified,
            "skipped_write": self.skipped_write,
            "reason": self.reason,
        }


def load_account_config_env(
    *,
    exchanges: Sequence[ExchangeName],
    symbol: str,
    env_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
    require_leverage: bool = False,
) -> AccountConfigEnv:
    values = _load_env(env_file=env_file, environ=environ)
    margin_mode = _parse_margin_mode(values.get("MARGIN_MODE", "isolated"))
    targets: list[AccountConfigTarget] = []
    missing: list[ExchangeName] = []

    for exchange in exchanges:
        key = LEVERAGE_ENV_KEYS.get(exchange.value)
        if key is None:
            continue
        raw = values.get(key)
        if raw in (None, ""):
            missing.append(exchange)
            continue
        targets.append(
            AccountConfigTarget(
                exchange=exchange,
                symbol=symbol,
                margin_mode=margin_mode,
                leverage=_parse_leverage(raw, key=key),
            )
        )

    if require_leverage and missing:
        missing_text = ", ".join(LEVERAGE_ENV_KEYS[exchange.value] for exchange in missing)
        raise AccountConfigBootstrapError(f"missing exchange leverage env: {missing_text}")

    return AccountConfigEnv(margin_mode=margin_mode, targets=tuple(targets), missing_leverage=tuple(missing))


async def bootstrap_account_config(
    *,
    targets: Sequence[AccountConfigTarget],
    account_clients: Sequence[AccountClient],
    execution_clients: Sequence[ExecutionClient],
    apply: bool,
    dry_run: bool,
    fail_on_error: bool = True,
) -> tuple[AccountConfigBootstrapResult, ...]:
    account_by_exchange = {client.exchange: client for client in account_clients}
    execution_by_exchange = {client.exchange: client for client in execution_clients}
    results: list[AccountConfigBootstrapResult] = []

    for target in targets:
        account = account_by_exchange.get(target.exchange)
        execution = execution_by_exchange.get(target.exchange)
        if account is None or execution is None:
            message = f"account config clients missing for {target.exchange.value}"
            if fail_on_error:
                raise AccountConfigBootstrapError(message)
            results.append(_error_result(target, message))
            continue

        try:
            results.append(await _bootstrap_one(target=target, account=account, execution=execution, apply=apply, dry_run=dry_run))
        except Exception as exc:
            if fail_on_error:
                raise
            results.append(_error_result(target, str(exc)))

    return tuple(results)


async def _bootstrap_one(
    *,
    target: AccountConfigTarget,
    account: AccountClient,
    execution: ExecutionClient,
    apply: bool,
    dry_run: bool,
) -> AccountConfigBootstrapResult:
    positions = tuple(await account.fetch_positions())
    open_orders = tuple(await execution.fetch_open_orders())
    open_stop_orders = tuple(await execution.fetch_open_stop_orders())
    before = await account.fetch_leverage(margin_mode=target.margin_mode)

    before_margin = _normalize_margin_mode(before.margin_mode)
    before_leverage = before.leverage
    already_verified = _matches(before, target)
    if already_verified:
        logger.info(
            "Account config verified | exchange=%s symbol=%s margin_mode=%s leverage=%s",
            target.exchange.value,
            target.symbol,
            target.margin_mode.value,
            target.leverage,
        )
        return AccountConfigBootstrapResult(
            exchange=target.exchange,
            symbol=target.symbol,
            expected_margin_mode=target.margin_mode,
            expected_leverage=target.leverage,
            before_margin_mode=before_margin,
            before_leverage=before_leverage,
            after_margin_mode=before_margin,
            after_leverage=before_leverage,
            active_positions=positions,
            open_orders=open_orders,
            open_stop_orders=open_stop_orders,
            verified=True,
            reason="already_configured",
        )

    if positions or open_orders or open_stop_orders:
        message = "account has active positions or open orders; refusing to change margin/leverage"
        return AccountConfigBootstrapResult(
            exchange=target.exchange,
            symbol=target.symbol,
            expected_margin_mode=target.margin_mode,
            expected_leverage=target.leverage,
            before_margin_mode=before_margin,
            before_leverage=before_leverage,
            after_margin_mode=before_margin,
            after_leverage=before_leverage,
            active_positions=positions,
            open_orders=open_orders,
            open_stop_orders=open_stop_orders,
            verified=False,
            reason="blocked_by_existing_position_or_order",
            error=message,
        )

    if dry_run or not apply:
        return AccountConfigBootstrapResult(
            exchange=target.exchange,
            symbol=target.symbol,
            expected_margin_mode=target.margin_mode,
            expected_leverage=target.leverage,
            before_margin_mode=before_margin,
            before_leverage=before_leverage,
            after_margin_mode=before_margin,
            after_leverage=before_leverage,
            active_positions=positions,
            open_orders=open_orders,
            open_stop_orders=open_stop_orders,
            verified=False,
            skipped_write=True,
            reason="dry_run" if dry_run else "read_only",
            error="account config does not match target and writes are disabled",
        )

    logger.info(
        "Applying account config | exchange=%s symbol=%s margin_mode=%s leverage=%s",
        target.exchange.value,
        target.symbol,
        target.margin_mode.value,
        target.leverage,
    )
    margin_result = await account.set_margin_mode(target.margin_mode)
    leverage_result = await account.set_leverage(target.leverage, margin_mode=target.margin_mode)
    after = await account.fetch_leverage(margin_mode=target.margin_mode)
    after_margin = _normalize_margin_mode(after.margin_mode)
    verified = _matches(after, target)
    error = None if verified else "account config verification mismatch after apply"

    return AccountConfigBootstrapResult(
        exchange=target.exchange,
        symbol=target.symbol,
        expected_margin_mode=target.margin_mode,
        expected_leverage=target.leverage,
        before_margin_mode=before_margin,
        before_leverage=before_leverage,
        after_margin_mode=after_margin,
        after_leverage=after.leverage,
        active_positions=positions,
        open_orders=open_orders,
        open_stop_orders=open_stop_orders,
        applied=True,
        verified=verified,
        reason="applied" if verified else "verification_mismatch",
        error=error,
        raw={"set_margin_mode": margin_result, "set_leverage": leverage_result.raw, "fetch_leverage": after.raw},
    )


def raise_on_failed_account_config(results: Sequence[AccountConfigBootstrapResult]) -> None:
    failures = [result for result in results if not result.ok]
    if not failures:
        return
    detail = "; ".join(
        f"{result.exchange.value}: {result.error or result.reason or 'not verified'}"
        for result in failures
    )
    raise AccountConfigBootstrapError(f"account config bootstrap failed: {detail}")


def _matches(info: LeverageInfo, target: AccountConfigTarget) -> bool:
    return info.leverage == target.leverage and _normalize_margin_mode(info.margin_mode) is target.margin_mode


def _normalize_margin_mode(value: MarginMode | str | None) -> MarginMode | None:
    if value in (None, ""):
        return None
    if isinstance(value, MarginMode):
        return value
    return MarginMode(str(value).strip().lower())


def _parse_margin_mode(value: str) -> MarginMode:
    try:
        return MarginMode(str(value or "isolated").strip().lower())
    except ValueError as exc:
        raise AccountConfigBootstrapError(f"invalid MARGIN_MODE: {value!r}") from exc


def _parse_leverage(value: str, *, key: str) -> Decimal:
    try:
        leverage = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise AccountConfigBootstrapError(f"invalid {key}: {value!r}") from exc
    if leverage <= 0:
        raise AccountConfigBootstrapError(f"{key} must be > 0")
    return leverage


def _load_env(*, env_file: str | Path | None, environ: Mapping[str, str] | None) -> dict[str, str]:
    values = dict(load_env_config(env_file, environ=environ))
    if environ is not None and env_file is None:
        allowed = {str(key) for key in environ.keys()}
        values = {key: value for key, value in values.items() if key in allowed}
    return values


def _error_result(target: AccountConfigTarget, message: str) -> AccountConfigBootstrapResult:
    return AccountConfigBootstrapResult(
        exchange=target.exchange,
        symbol=target.symbol,
        expected_margin_mode=target.margin_mode,
        expected_leverage=target.leverage,
        before_margin_mode=None,
        before_leverage=None,
        after_margin_mode=None,
        after_leverage=None,
        reason="error",
        error=message,
    )
