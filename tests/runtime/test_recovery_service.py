from __future__ import annotations

import asyncio
from decimal import Decimal

from src.order_management import OrderIntent, SqliteOrderJournalStore
from src.order_management.position_plan.models import LegPlan, LegRole, LegSyncStatus, PositionPlan, PositionPlanStatus
from src.order_management.position_plan.store import SqlitePositionPlanStore
from src.platform import Balance, ExchangeName, LeverageInfo, Order, OrderSide, OrderStatus, OrderType, Position, PositionMode, PositionSide
from src.platform.state import SqliteStateStore
from src.runtime.recovery import RecoveryExchangeContext, RuntimeRecoveryService
from src.signals import SignalAction, TradeSignal
from strategies.eth_lf_portfolio_v8.strategy import Strategy


class FakeAccount:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    async def fetch_balance(self, asset="USDT"):
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("100"), available=Decimal("90"))

    async def fetch_positions(self, symbol=None):
        return [
            Position(
                exchange=self.exchange,
                symbol=self.symbol,
                raw_symbol="ETH-USDT-SWAP",
                side=PositionSide.BOTH,
                quantity=Decimal("0"),
                raw={"instId": "ETH-USDT-SWAP", "posSide": "both", "pos": "0"},
            )
        ]

    async def fetch_leverage(self):
        return LeverageInfo(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", leverage=Decimal("3"))

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY


class FakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    async def fetch_open_orders(self):
        return [Order(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", order_id="ord-1", client_order_id="cid-1", status=OrderStatus.NEW)]

    async def fetch_open_stop_orders(self):
        return []


class ConfigurableFakeAccount:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self, positions=None):
        self._positions = positions if positions is not None else []

    async def fetch_balance(self, asset="USDT"):
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("100"), available=Decimal("90"))

    async def fetch_positions(self, symbol=None):
        return list(self._positions)

    async def fetch_leverage(self):
        return LeverageInfo(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", leverage=Decimal("3"))

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY


class ConfigurableFakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self, *, open_orders=None, open_stop_orders=None):
        self._open_orders = open_orders if open_orders is not None else []
        self._open_stop_orders = open_stop_orders if open_stop_orders is not None else []

    async def fetch_open_orders(self):
        return list(self._open_orders)

    async def fetch_open_stop_orders(self):
        return list(self._open_stop_orders)


class RecoverableStrategy:
    def __init__(self):
        self.contexts = []

    async def recover(self, context):
        self.contexts.append(context)
        return [TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CANCEL_ALL_ORDERS)]


def test_runtime_recovery_collects_snapshot_reconciles_loads_intents_and_calls_strategy(tmp_path):
    state_store = SqliteStateStore(tmp_path / "state.sqlite3")
    journal = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CANCEL_ALL_ORDERS, created_time_ms=1)
    intent = OrderIntent(intent_id="intent-1", strategy_id="v8", signal=signal, target_exchanges=(ExchangeName.OKX,))
    assert journal.claim_intent(intent) is True
    strategy = RecoverableStrategy()
    service = RuntimeRecoveryService(
        exchange_contexts=(RecoveryExchangeContext(account=FakeAccount(), execution=FakeExecution(), state_store=state_store),),
        order_journal=journal,
        intent_ids=("intent-1",),
    )

    report = asyncio.run(service.recover(strategy=strategy))

    assert report.ok is True
    assert len(report.snapshots) == 1
    assert len(report.reconcile_reports) == 1
    assert len(report.order_intents) == 1
    assert len(report.strategy_signals) == 1
    assert strategy.contexts[0].order_intent_ids == ("intent-1",)
    assert state_store.load_latest_account_snapshot(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP") is not None


def test_runtime_recovery_without_recoverable_strategy_is_noop_for_strategy_hook(tmp_path):
    state_store = SqliteStateStore(tmp_path / "state.sqlite3")
    service = RuntimeRecoveryService(
        exchange_contexts=(RecoveryExchangeContext(account=FakeAccount(), execution=FakeExecution(), state_store=state_store),)
    )

    report = asyncio.run(service.recover(strategy=object()))

    assert report.strategy_signals == ()
    assert report.ok is True


def test_startup_recovery_marks_stale_local_stop_closed_and_continues(tmp_path):
    state_store = SqliteStateStore(tmp_path / "state.sqlite3")
    plan_store = _active_short_plan_store(tmp_path / "plans.sqlite3")
    stale_stop = _stop_order(order_id="3681380310358618112", quantity=Decimal("2.82"))
    state_store.save_order(stale_stop, is_stop_order=True)
    strategy = Strategy()
    service = RuntimeRecoveryService(
        exchange_contexts=(
            RecoveryExchangeContext(
                account=ConfigurableFakeAccount(positions=[_short_okx_position()]),
                execution=ConfigurableFakeExecution(open_stop_orders=[]),
                state_store=state_store,
            ),
        ),
        position_plan_store=plan_store,
    )

    report = asyncio.run(service.recover(strategy=strategy))

    loaded = state_store.get_order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", order_id="3681380310358618112")
    assert loaded is not None
    assert loaded.status is OrderStatus.CANCELED
    assert loaded.raw["local_reconcile_reason"] == "startup_recovery_missing_from_exchange_open_stop_orders"
    assert report.ok is True
    assert report.issues == ()
    assert report.strategy_signals
    place = next(signal for signal in report.strategy_signals if signal.action is SignalAction.PLACE_STOP_LOSS_SHORT)
    assert place.quantity == Decimal("0.282")
    assert place.trigger_price == Decimal("1719.40")


def test_startup_recovery_marks_stale_local_regular_order_closed(tmp_path):
    state_store = SqliteStateStore(tmp_path / "state.sqlite3")
    state_store.save_order(
        Order(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            order_id="stale-entry",
            client_order_id="stale-entry-cid",
            status=OrderStatus.NEW,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("2000"),
            quantity=Decimal("0.1"),
        ),
        is_stop_order=False,
    )
    service = RuntimeRecoveryService(
        exchange_contexts=(
            RecoveryExchangeContext(
                account=ConfigurableFakeAccount(),
                execution=ConfigurableFakeExecution(open_orders=[], open_stop_orders=[]),
                state_store=state_store,
            ),
        )
    )

    report = asyncio.run(service.recover(strategy=RecoverableStrategy()))

    loaded = state_store.get_order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", order_id="stale-entry")
    assert loaded is not None
    assert loaded.status is OrderStatus.CANCELED
    assert loaded.raw["local_reconcile_reason"] == "startup_recovery_missing_from_exchange_open_orders"
    assert report.ok is True
    assert report.issues == ()


def test_startup_recovery_saves_exchange_stop_missing_locally_and_continues(tmp_path):
    state_store = SqliteStateStore(tmp_path / "state.sqlite3")
    plan_store = _active_short_plan_store(tmp_path / "plans.sqlite3")
    live_stop = _stop_order(order_id="live-stop", client_order_id="pos-1-stop", quantity=Decimal("2.82"))
    strategy = Strategy()
    service = RuntimeRecoveryService(
        exchange_contexts=(
            RecoveryExchangeContext(
                account=ConfigurableFakeAccount(positions=[_short_okx_position()]),
                execution=ConfigurableFakeExecution(open_stop_orders=[live_stop]),
                state_store=state_store,
            ),
        ),
        position_plan_store=plan_store,
    )

    report = asyncio.run(service.recover(strategy=strategy))

    loaded = state_store.get_order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", order_id="live-stop")
    assert loaded is not None
    assert loaded.status is OrderStatus.NEW
    assert loaded.is_stop_order is True
    assert report.ok is True
    assert report.issues == ()
    assert not any(signal.action is SignalAction.PLACE_STOP_LOSS_SHORT for signal in report.strategy_signals)


def _short_okx_position() -> Position:
    return Position(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        side=PositionSide.BOTH,
        quantity=Decimal("-2.82"),
        entry_price=Decimal("1700"),
        raw={"instId": "ETH-USDT-SWAP", "posSide": "both", "pos": "-2.82"},
    )


def _stop_order(*, order_id: str, quantity: Decimal, client_order_id: str = "pos-1-stop") -> Order:
    return Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id=order_id,
        client_order_id=client_order_id,
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        price=Decimal("1719.40"),
        quantity=quantity,
        raw={"reduceOnly": "true"},
    )


# ══════════════════════════════════════════════════════════════════════════════
# Runtime recovery postcondition tests (8.3–8.6)
# ══════════════════════════════════════════════════════════════════════════════

import pytest
from unittest.mock import MagicMock, patch

from src.app import AppContext
from src.order_management.safety import RecoveryExitOrderValidator
from src.platform.exchanges.models import OrderSide, PositionMode
from src.platform.markets import get_market_profile
from src.platform.snapshot import PlatformSnapshot
from src.runtime.runner import LiveRuntimeError, LiveRuntimeRunner, _first_active_position, _position_side_from_quantity, _position_side_label
from src.runtime.recovery.models import RecoveryReport
from src.strategy import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
    StrategyRecoveryStatus,
)


def _minimal_runner(
    *,
    strategy=None,
    symbol: str = "ETH-USDT-PERP",
    data_exchange=ExchangeName.OKX,
) -> LiveRuntimeRunner:
    """Build a LiveRuntimeRunner with just enough state for postcondition checks.

    Uses aggressive patching to bypass the heavy __init__ path — only
    ``app_config`` and ``context.strategy`` are real; everything else is
    mocked.
    """
    from src.app import AppConfig

    app_config = AppConfig(
        symbol=symbol,
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=data_exchange,
        strategy="strategies.eth_lf_portfolio_v8:Strategy",
        data_streams=("trades",),
        state_db_path=":memory:",
        market_queue_maxsize=100,
        signal_queue_maxsize=100,
        alert_queue_maxsize=100,
        dry_run=True,
        enable_email_alerts=False,
    )

    # Bypass the entire __init__ and only set the attributes the
    # postcondition method actually reads.
    runner = LiveRuntimeRunner.__new__(LiveRuntimeRunner)
    runner.app_config = app_config
    runner.context = MagicMock()
    runner.context.strategy = strategy if strategy is not None else _FakeStrategy()
    runner.stats = MagicMock()
    runner.services = {}
    runner._market_queue = MagicMock()
    runner._stop_event = MagicMock()
    runner._producer_tasks = []
    runner._sync_tasks = []
    runner._health = MagicMock()
    runner._heartbeat_service = MagicMock()
    runner._producer_monitor = MagicMock()
    runner._producer_supervisor = MagicMock()
    runner._closed_bar_scheduler = MagicMock()
    runner._closed_bar_interval = "4h"
    runner._closed_bar_interval_ms = 14400000
    runner._closed_bar_buffer_ms = 5000
    runner._range_pct = Decimal("0.002")
    runner._range_aggregate_interval = "4h"
    runner._last_snapshot = None
    runner._last_snapshots = ()
    runner._execution_clients = None
    runner._account_clients = None
    runner._order_journal = None
    runner._position_plan_store = None
    runner._order_coordinator = None
    runner._account_sync_service = None
    runner._order_sync_service = None
    runner._request_sync_throttle = MagicMock()
    runner._recovery_service = "__default__"
    runner._reconciliation_service = "__default__"
    runner._range_bar_store = None
    runner._range_bar_builder = None
    runner._range_bar_aggregator = None
    runner._intent_factory = MagicMock()
    runner.requirements = MagicMock()
    runner.requirements.closed_kline = MagicMock()
    runner.requirements.closed_kline.enabled = True
    runner.requirements.closed_kline.interval = "4h"
    runner.requirements.closed_kline.warmup_days = 0
    runner.requirements.trades = MagicMock()
    runner.requirements.trades.enabled = False
    runner.requirements.range_bars = MagicMock()
    runner.requirements.range_bars.enabled = False
    runner.requirements.order_book = MagicMock()
    runner.requirements.order_book.enabled = False
    runner.runtime_config = MagicMock()
    runner.runtime_config.warmup_enabled = False
    runner.runtime_config.mode = MagicMock()
    runner.runtime_config.mode.value = "live_runtime"
    runner.runtime_config.closed_bar_interval = "4h"
    runner.runtime_config.closed_bar_buffer_ms = 5000
    runner.runtime_config.range_pct = Decimal("0.002")
    runner.runtime_config.scheduler_poll_seconds = 1
    runner.runtime_config.master_follower_policy = None
    runner.runtime_config.startup_catchup = MagicMock()
    runner.runtime_config.startup_catchup.enabled = False
    runner.runtime_config.producer_stale_timeout_ms = 60000
    return runner


class _FakeStrategy:
    """Minimal strategy stub for postcondition tests."""

    def __init__(self) -> None:
        self.recovery_blocking_manual_required = False
        self.recovery_alerts: list[str] = []
        self.config = MagicMock(strategy_id="test_strategy")
        self.position = MagicMock(
            in_pos=False,
            position_id=None,
            stop_price=None,
            open_legs={},
        )

    def recovery_status(self) -> StrategyRecoveryStatus:
        return StrategyRecoveryStatus(
            blocking_manual_required=self.recovery_blocking_manual_required,
            alerts=tuple(self.recovery_alerts),
        )

    def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
        if not self.position.in_pos or not self.position.position_id:
            return ()
        return (
            StrategyPositionSnapshot(
                strategy_id="test_strategy",
                position_id=self.position.position_id,
                symbol="ETH-USDT-PERP",
                side=StrategyPositionSide.SHORT,
                status=StrategyPositionStatus.ACTIVE,
                base_quantity=Decimal("2.82"),
                stop_price=self.position.stop_price,
                metadata={"active_exchanges": tuple(self.position.open_legs)},
            ),
        )


def _recovery_report(*, snapshots=(), strategy_signals=()) -> RecoveryReport:
    return RecoveryReport(
        ok=True,
        snapshots=tuple(snapshots),
        strategy_signals=tuple(strategy_signals),
    )


def _position(exchange: ExchangeName, qty: str) -> Position:
    return Position(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP" if exchange is ExchangeName.OKX else "ETHUSDT",
        side=PositionSide.BOTH,
        quantity=Decimal(qty),
        entry_price=Decimal("2000"),
    )


class TestRecoveryProtectionPostcondition:
    """8.4, 8.5, 8.6: Runtime recovery protection postcondition validation."""

    def test_active_position_without_stop_or_signal_raises(self):
        """8.4: active position + no stop + no signal → LiveRuntimeError."""
        strategy = _FakeStrategy()
        strategy.recovery_blocking_manual_required = False
        strategy.config = MagicMock()
        strategy.config.strategy_id = "test_strategy"
        strategy.position = MagicMock()
        strategy.position.in_pos = True
        strategy.position.position_id = "pos-1"
        strategy.position.stop_price = Decimal("1719.40")
        strategy.position.open_legs = {"okx": MagicMock()}

        snapshot = _custom_snapshot(
            ExchangeName.OKX,
            positions=[_position(ExchangeName.OKX, "-2.82")],
            open_stop_orders=[],
        )
        report = _recovery_report(snapshots=(snapshot,), strategy_signals=())

        runner = _minimal_runner(strategy=strategy)

        with pytest.raises(LiveRuntimeError, match="recovery protection postcondition failed"):
            runner._validate_recovery_protection_postcondition(report)

    def test_active_position_with_valid_bot_stop_passes_without_signal(self):
        """8.5: active position + valid bot stop → no signal needed, postcondition passes."""
        strategy = _FakeStrategy()
        strategy.recovery_blocking_manual_required = False
        strategy.config = MagicMock()
        strategy.config.strategy_id = "test_strategy"
        strategy.position = MagicMock()
        strategy.position.in_pos = True
        strategy.position.position_id = "pos-1"
        strategy.position.stop_price = Decimal("1719.40")
        strategy.position.open_legs = {"okx": MagicMock()}

        valid_stop = Order(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            order_id="okx-valid-stop",
            client_order_id="pos-1-stop",
            status=OrderStatus.NEW,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("1719.40"),
            quantity=Decimal("2.82"),
            raw={"reduceOnly": "true"},
        )
        snapshot = _custom_snapshot(
            ExchangeName.OKX,
            positions=[_position(ExchangeName.OKX, "-2.82")],
            open_stop_orders=[valid_stop],
        )
        report = _recovery_report(snapshots=(snapshot,), strategy_signals=())

        runner = _minimal_runner(strategy=strategy)
        # Should not raise
        runner._validate_recovery_protection_postcondition(report)

    def test_manual_stop_does_not_satisfy_postcondition_without_signal(self):
        """8.6: manual stop only → not bot-owned → postcondition fails."""
        strategy = _FakeStrategy()
        strategy.recovery_blocking_manual_required = False
        strategy.config = MagicMock()
        strategy.config.strategy_id = "test_strategy"
        strategy.position = MagicMock()
        strategy.position.in_pos = True
        strategy.position.position_id = "pos-1"
        strategy.position.stop_price = Decimal("1719.40")
        strategy.position.open_legs = {"okx": MagicMock()}

        manual_stop = Order(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            order_id="manual-1",
            client_order_id="user-manual-stop",
            status=OrderStatus.NEW,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("1719.40"),
            quantity=Decimal("2.0"),
            raw={"reduceOnly": "true"},
        )
        snapshot = _custom_snapshot(
            ExchangeName.OKX,
            positions=[_position(ExchangeName.OKX, "-2.82")],
            open_stop_orders=[manual_stop],
        )
        report = _recovery_report(snapshots=(snapshot,), strategy_signals=())

        runner = _minimal_runner(strategy=strategy)

        with pytest.raises(LiveRuntimeError, match="recovery protection postcondition failed"):
            runner._validate_recovery_protection_postcondition(report)

    def test_manual_stop_with_place_stop_signal_satisfies_postcondition(self):
        """Manual stop + recovery PLACE_STOP_LOSS signal → postcondition passes."""
        strategy = _FakeStrategy()
        strategy.recovery_blocking_manual_required = False
        strategy.config = MagicMock()
        strategy.config.strategy_id = "test_strategy"
        strategy.position = MagicMock()
        strategy.position.in_pos = True
        strategy.position.position_id = "pos-1"
        strategy.position.stop_price = Decimal("1719.40")
        strategy.position.open_legs = {"okx": MagicMock()}

        manual_stop = Order(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            order_id="manual-1",
            client_order_id="user-manual-stop",
            status=OrderStatus.NEW,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("1719.40"),
            quantity=Decimal("2.0"),
            raw={"reduceOnly": "true"},
        )
        snapshot = _custom_snapshot(
            ExchangeName.OKX,
            positions=[_position(ExchangeName.OKX, "-2.82")],
            open_stop_orders=[manual_stop],
        )
        place_signal = TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.PLACE_STOP_LOSS_SHORT,
            quantity=Decimal("0.282"),
            trigger_price=Decimal("1719.40"),
            metadata={"target_exchanges": ["okx"]},
        )
        report = _recovery_report(snapshots=(snapshot,), strategy_signals=(place_signal,))

        runner = _minimal_runner(strategy=strategy)
        # Should not raise — place_stop signal satisfies postcondition
        runner._validate_recovery_protection_postcondition(report)

    def test_blocking_flag_skips_postcondition(self):
        """When recovery_blocking_manual_required is True, postcondition is skipped."""
        strategy = _FakeStrategy()
        strategy.recovery_blocking_manual_required = True  # blocking

        snapshot = _custom_snapshot(
            ExchangeName.OKX,
            positions=[_position(ExchangeName.OKX, "-2.82")],
            open_stop_orders=[],
        )
        report = _recovery_report(snapshots=(snapshot,), strategy_signals=())

        runner = _minimal_runner(strategy=strategy)
        # Should not raise — blocking flag bypasses postcondition
        runner._validate_recovery_protection_postcondition(report)

    def test_flat_snapshot_passes_postcondition(self):
        """No active position → postcondition passes."""
        strategy = _FakeStrategy()
        strategy.recovery_blocking_manual_required = False

        snapshot = _custom_snapshot(
            ExchangeName.OKX,
            positions=[_position(ExchangeName.OKX, "0")],
            open_stop_orders=[],
        )
        report = _recovery_report(snapshots=(snapshot,), strategy_signals=())

        runner = _minimal_runner(strategy=strategy)
        # Should not raise
        runner._validate_recovery_protection_postcondition(report)

    def test_postcondition_error_message_contains_diagnostics(self):
        """Error message includes exchange, symbol, position_side, qty, stop info."""
        strategy = _FakeStrategy()
        strategy.recovery_blocking_manual_required = False
        strategy.config = MagicMock()
        strategy.config.strategy_id = "test_strategy"
        strategy.position = MagicMock()
        strategy.position.in_pos = True
        strategy.position.position_id = "pos-1"
        strategy.position.stop_price = Decimal("1719.40")
        strategy.position.open_legs = {"okx": MagicMock()}

        snapshot = _custom_snapshot(
            ExchangeName.OKX,
            positions=[_position(ExchangeName.OKX, "-2.82")],
            open_stop_orders=[],
        )
        report = _recovery_report(snapshots=(snapshot,), strategy_signals=())

        runner = _minimal_runner(strategy=strategy)

        with pytest.raises(LiveRuntimeError) as exc_info:
            runner._validate_recovery_protection_postcondition(report)

        msg = str(exc_info.value)
        assert "recovery protection postcondition failed" in msg
        assert "okx" in msg
        assert "ETH-USDT-PERP" in msg
        assert "bot_owned_valid_stop=false" in msg
        assert "place_stop_signal=false" in msg


class TestPostExecutionStopProtection:
    """Acceptance criteria 1–5: post-execution stop protection validation."""

    @pytest.fixture
    def runner_with_mock_exchanges(self):
        """Runner whose exchange clients can be controlled per test."""
        strategy = _FakeStrategy()
        strategy.config = MagicMock()
        strategy.config.strategy_id = "test_strategy"
        strategy.position = MagicMock()
        strategy.position.in_pos = True
        strategy.position.position_id = "pos-1"
        strategy.position.stop_price = Decimal("1719.40")
        strategy.position.open_legs = {"okx": MagicMock()}
        runner = _minimal_runner(strategy=strategy)
        return runner

    def _set_mock_clients(self, runner, *, positions=None, open_stop_orders=None,
                          position_mode=PositionMode.ONE_WAY,
                          fetch_position_mode_ok=True):
        """Install mock exchange clients that return the given data."""
        mock_exec = MagicMock()
        mock_exec.exchange = ExchangeName.OKX
        mock_exec.fetch_open_stop_orders = _async_return(open_stop_orders or [])
        mock_acct = MagicMock()
        mock_acct.exchange = ExchangeName.OKX
        mock_acct.fetch_positions = _async_return(positions or [])
        if fetch_position_mode_ok:
            mock_acct.fetch_position_mode = _async_return(position_mode)
        else:
            mock_acct.fetch_position_mode = _async_raise(RuntimeError("unsupported"))
        runner._execution_clients = (mock_exec,)
        runner._account_clients = (mock_acct,)

    # ── Acceptance criteria tests ───────────────────────────────────────

    @pytest.mark.asyncio
    async def test_stop_submit_success_post_validation_passes(self, runner_with_mock_exchanges):
        """1. active position + stop submit success → startup passes."""
        runner = runner_with_mock_exchanges
        valid_stop = Order(
            exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP", order_id="new-stop",
            client_order_id="pos-1-stop", status=OrderStatus.NEW,
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            price=Decimal("1719.40"), quantity=Decimal("2.82"),
            raw={"reduceOnly": "true"},
        )
        self._set_mock_clients(
            runner,
            positions=[_position(ExchangeName.OKX, "-2.82")],
            open_stop_orders=[valid_stop],
        )
        # Should not raise
        await runner._validate_post_execution_stop_protection()

    @pytest.mark.asyncio
    async def test_stop_submit_fails_post_validation_raises(self, runner_with_mock_exchanges):
        """2. active position + stop submit fails (no stop on exchange) → fatal."""
        runner = runner_with_mock_exchanges
        self._set_mock_clients(
            runner,
            positions=[_position(ExchangeName.OKX, "-2.82")],
            open_stop_orders=[],  # stop was not placed
        )
        with pytest.raises(LiveRuntimeError, match="post-execution stop validation failed"):
            await runner._validate_post_execution_stop_protection()

    @pytest.mark.asyncio
    async def test_manual_stop_only_stop_submit_success_passes(self, runner_with_mock_exchanges):
        """4. manual stop only + active plan + stop submit success → passes."""
        runner = runner_with_mock_exchanges
        manual_stop = Order(
            exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP", order_id="manual-1",
            client_order_id="user-manual", status=OrderStatus.NEW,
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            price=Decimal("1719.40"), quantity=Decimal("2.0"),
            raw={"reduceOnly": "true"},
        )
        bot_stop = Order(
            exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP", order_id="bot-stop",
            client_order_id="pos-1-stop", status=OrderStatus.NEW,
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            price=Decimal("1719.40"), quantity=Decimal("2.82"),
            raw={"reduceOnly": "true"},
        )
        self._set_mock_clients(
            runner,
            positions=[_position(ExchangeName.OKX, "-2.82")],
            open_stop_orders=[manual_stop, bot_stop],
        )
        # Should not raise — bot stop now exists alongside manual stop
        await runner._validate_post_execution_stop_protection()

    @pytest.mark.asyncio
    async def test_manual_stop_only_stop_submit_fails_raises(self, runner_with_mock_exchanges):
        """5. manual stop only + active plan + stop submit fails → fatal."""
        runner = runner_with_mock_exchanges
        manual_stop = Order(
            exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP", order_id="manual-1",
            client_order_id="user-manual", status=OrderStatus.NEW,
            side=OrderSide.BUY, order_type=OrderType.MARKET,
            price=Decimal("1719.40"), quantity=Decimal("2.0"),
            raw={"reduceOnly": "true"},
        )
        self._set_mock_clients(
            runner,
            positions=[_position(ExchangeName.OKX, "-2.82")],
            open_stop_orders=[manual_stop],  # only manual, no bot stop
        )
        with pytest.raises(LiveRuntimeError, match="post-execution stop validation failed"):
            await runner._validate_post_execution_stop_protection()

    @pytest.mark.asyncio
    async def test_no_canonical_stop_price_raises(self, runner_with_mock_exchanges):
        """Missing canonical_stop_price → fatal."""
        runner = runner_with_mock_exchanges
        runner.context.strategy.position.stop_price = None
        with pytest.raises(LiveRuntimeError, match="no canonical stop price"):
            await runner._validate_post_execution_stop_protection()

    @pytest.mark.asyncio
    async def test_position_gone_after_placement_passes(self, runner_with_mock_exchanges):
        """If the position was closed between pre and post check, it's ok."""
        runner = runner_with_mock_exchanges
        self._set_mock_clients(
            runner,
            positions=[_position(ExchangeName.OKX, "0")],  # flat
            open_stop_orders=[],
        )
        # Should not raise — no position to protect
        await runner._validate_post_execution_stop_protection()

    @pytest.mark.asyncio
    async def test_fetch_failure_raises(self, runner_with_mock_exchanges):
        """Cannot fetch exchange state → fatal."""
        runner = runner_with_mock_exchanges
        mock_exec = MagicMock()
        mock_exec.exchange = ExchangeName.OKX
        mock_exec.fetch_open_stop_orders = _async_raise(RuntimeError("network error"))
        mock_acct = MagicMock()
        mock_acct.exchange = ExchangeName.OKX
        mock_acct.fetch_positions = _async_return([_position(ExchangeName.OKX, "-2.82")])
        mock_acct.fetch_position_mode = _async_return(PositionMode.ONE_WAY)
        runner._execution_clients = (mock_exec,)
        runner._account_clients = (mock_acct,)

        with pytest.raises(LiveRuntimeError, match="cannot fetch exchange state"):
            await runner._validate_post_execution_stop_protection()


def _async_return(value):
    async def _fn(*args, **kwargs):
        return value
    return _fn


def _async_raise(exc):
    async def _fn(*args, **kwargs):
        raise exc
    return _fn


def _custom_snapshot(
    exchange: ExchangeName,
    *,
    positions: list[Position],
    open_stop_orders: list[Order] | None = None,
    open_orders: list[Order] | None = None,
    position_mode: PositionMode = PositionMode.ONE_WAY,
) -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=exchange, asset="USDT", total=Decimal("10000"), available=Decimal("10000")),
        positions=positions,
        open_orders=open_orders or [],
        open_stop_orders=open_stop_orders or [],
        leverage=LeverageInfo(exchange=exchange, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP" if exchange is ExchangeName.OKX else "ETHUSDT", leverage=Decimal("3")),
        position_mode=position_mode,
    )


def _active_short_plan_store(path) -> SqlitePositionPlanStore:
    store = SqlitePositionPlanStore(path)
    store.upsert_position(
        PositionPlan(
            position_id="pos-1",
            strategy_id="eth_lf_portfolio_v9c_reclaim_priority",
            entry_engine="BULL_RECLAIM_V2",
            side="short",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1719.40"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.282"),
            master_filled_qty_base=Decimal("0.282"),
            created_time_ms=123,
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id="pos-1",
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.282"),
            filled_qty_base=Decimal("0.282"),
            sync_status=LegSyncStatus.OPEN,
        )
    )
    return store
