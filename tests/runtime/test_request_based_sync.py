from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

import src.runtime.account_sync.service as sync_service
from src.app.alerts import AppAlert
from src.platform import Balance, ExchangeName, LeverageInfo, Order, OrderQuery, OrderSide, OrderStatus, Position, PositionMode, PositionSide, StopOrderQuery
from src.runtime.account_sync import AccountStateSyncService, KnownOrderRef, OrderStateSyncService, SyncExchangeContext
from src.runtime.requirements import AccountStateRequirement, OrderStateRequirement


class MemoryAlerts:
    def __init__(self) -> None:
        self.items: list[AppAlert] = []

    def emit(self, alert: AppAlert) -> None:
        self.items.append(alert)


class MemoryState:
    def __init__(self) -> None:
        self.snapshots = []
        self.orders = []
        self.closed_snapshots = []

    def save_snapshot(self, snapshot):
        self.snapshots.append(snapshot)

    def save_order(self, order, *, is_stop_order=False):
        self.orders.append((order, is_stop_order))

    def mark_missing_open_orders_closed(self, *, exchange, symbol, live_order_keys, is_stop_order, missing_status=OrderStatus.CANCELED, reason="missing_from_exchange_open_orders"):
        self.closed_snapshots.append(
            {
                "exchange": exchange,
                "symbol": symbol,
                "live_order_keys": live_order_keys,
                "is_stop_order": is_stop_order,
                "missing_status": missing_status,
                "reason": reason,
            }
        )
        return 1


class FakeAccount:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []

    async def fetch_balance(self, asset="USDT"):
        self.calls.append("fetch_balance")
        if self.fail:
            raise RuntimeError("account unavailable")
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("100"), available=Decimal("90"))

    async def fetch_positions(self, symbol=None):
        self.calls.append("fetch_positions")
        return [Position(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol, side=PositionSide.LONG, quantity=Decimal("1"))]

    async def fetch_leverage(self, *, margin_mode=None):
        self.calls.append("fetch_leverage")
        return LeverageInfo(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol, leverage=Decimal("10"))

    async def fetch_position_mode(self):
        self.calls.append("fetch_position_mode")
        return PositionMode.ONE_WAY


class FakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self, *, open_orders=None, open_stop_orders=None) -> None:
        self.calls: list[str] = []
        self.open_orders = open_orders
        self.open_stop_orders = open_stop_orders

    async def fetch_open_orders(self):
        self.calls.append("fetch_open_orders")
        if self.open_orders is not None:
            return list(self.open_orders)
        return [Order(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol, order_id="o1", client_order_id="c1", status=OrderStatus.NEW, side=OrderSide.BUY)]

    async def fetch_open_stop_orders(self):
        self.calls.append("fetch_open_stop_orders")
        if self.open_stop_orders is not None:
            return list(self.open_stop_orders)
        return [Order(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol, order_id="s1", client_order_id="sc1", status=OrderStatus.NEW, side=OrderSide.SELL)]


@pytest.mark.asyncio
async def test_account_sync_fetches_snapshot_and_persists_state():
    state = MemoryState()
    account = FakeAccount()
    service = AccountStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=FakeExecution(), state_store=state),),
        config=AccountStateRequirement(poll_interval_seconds=300),
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert account.calls == ["fetch_balance", "fetch_positions", "fetch_leverage", "fetch_position_mode"]
    assert len(state.snapshots) == 1
    assert state.snapshots[0].balance.available == Decimal("90")


@pytest.mark.asyncio
async def test_account_sync_notifies_snapshot_callback_after_success():
    state = MemoryState()
    seen: list[tuple[Any, str]] = []
    service = AccountStateSyncService(
        contexts=(SyncExchangeContext(account=FakeAccount(), execution=FakeExecution(), state_store=state),),
        config=AccountStateRequirement(poll_interval_seconds=300),
        snapshot_callback=lambda snapshot, sync_type: seen.append((snapshot, sync_type)),
    )

    results = await service.sync_once(sync_type="account_periodic")

    assert results[0].success is True
    assert len(seen) == 1
    assert seen[0][0].balance.available == Decimal("90")
    assert seen[0][1] == "account_periodic"


@pytest.mark.asyncio
async def test_account_sync_failure_does_not_raise_and_alerts_after_threshold():
    alerts = MemoryAlerts()
    account = FakeAccount(fail=True)
    service = AccountStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=FakeExecution(), state_store=MemoryState()),),
        config=AccountStateRequirement(consecutive_failure_alert_threshold=2),
        alert_sink=alerts,
    )

    first = await service.sync_once()
    second = await service.sync_once()

    assert first[0].success is False
    assert second[0].success is False
    assert len(alerts.items) == 1


@pytest.mark.asyncio
async def test_order_sync_fetches_positions_open_orders_and_open_stop_orders_when_active():
    state = MemoryState()
    account = FakeAccount()
    execution = FakeExecution()
    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(poll_interval_seconds=20),
        active_check=lambda: True,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert "fetch_positions" in account.calls
    assert execution.calls == ["fetch_open_orders", "fetch_open_stop_orders"]
    assert [(order.order_id, is_stop) for order, is_stop in state.orders] == [("o1", False), ("s1", True)]


@pytest.mark.asyncio
async def test_order_sync_inactive_logs_debug_not_info(caplog, monkeypatch):
    async def no_sleep(stop_event, interval_seconds):
        return None

    monkeypatch.setattr(sync_service, "_sleep_with_jitter", no_sleep)
    stop_event = asyncio.Event()

    def inactive_once() -> bool:
        stop_event.set()
        return False

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=FakeAccount(), execution=FakeExecution(open_orders=[], open_stop_orders=[]), state_store=MemoryState()),),
        config=OrderStateRequirement(poll_interval_seconds=0),
        active_check=inactive_once,
    )

    caplog.set_level(logging.DEBUG)
    await service.run_periodic(stop_event)

    info_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.INFO]
    debug_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.DEBUG]
    assert not any("Order state sync inactive" in message for message in info_messages)
    assert not any("Order state sync still inactive" in message for message in info_messages)
    assert any("Order state sync inactive" in message for message in debug_messages)


@pytest.mark.asyncio
async def test_order_sync_still_inactive_never_logs_info(caplog, monkeypatch):
    async def no_sleep(stop_event, interval_seconds):
        return None

    monkeypatch.setattr(sync_service, "_sleep_with_jitter", no_sleep)
    stop_event = asyncio.Event()
    ticks = 0

    def inactive_then_stop() -> bool:
        nonlocal ticks
        ticks += 1
        if ticks >= 3:
            stop_event.set()
        return False

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=FakeAccount(), execution=FakeExecution(open_orders=[], open_stop_orders=[]), state_store=MemoryState()),),
        config=OrderStateRequirement(poll_interval_seconds=0),
        active_check=inactive_then_stop,
    )
    monkeypatch.setattr(service._inactive_skip_summary, "should_emit_summary", lambda *args, **kwargs: True)

    caplog.set_level(logging.DEBUG)
    await service.run_periodic(stop_event)

    info_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.INFO]
    debug_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.DEBUG]
    assert not any("still inactive" in message for message in info_messages)
    assert not any("skipped_ticks" in message for message in info_messages)
    assert any("Order state sync still inactive" in message for message in debug_messages)


@pytest.mark.asyncio
async def test_order_sync_reconciles_missing_regular_open_orders_as_closed():
    state = MemoryState()
    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=FakeAccount(), execution=FakeExecution(open_orders=[], open_stop_orders=[]), state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=True, sync_open_stop_orders=False),
        active_check=lambda: True,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert state.closed_snapshots == [
        {
            "exchange": ExchangeName.OKX,
            "symbol": "ETH-USDT-PERP",
            "live_order_keys": set(),
            "is_stop_order": False,
            "missing_status": OrderStatus.CANCELED,
            "reason": "missing_from_exchange_open_orders",
        }
    ]


@pytest.mark.asyncio
async def test_order_sync_reconciles_missing_stop_orders_as_closed():
    state = MemoryState()
    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=FakeAccount(), execution=FakeExecution(open_orders=[], open_stop_orders=[]), state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=True),
        active_check=lambda: True,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert state.closed_snapshots[0]["is_stop_order"] is True
    assert state.closed_snapshots[0]["live_order_keys"] == set()


# ────────────────────────────────────────────────────────────────────────────
# Extended fakes for known-order-ref tests
# ────────────────────────────────────────────────────────────────────────────


class TrackingExecution(FakeExecution):
    """Execution client that records every fetch_order_status / fetch_stop_order_status query."""

    def __init__(self, *, open_orders=None, open_stop_orders=None, fail_on: str | None = None) -> None:
        super().__init__(open_orders=open_orders, open_stop_orders=open_stop_orders)
        self.order_queries: list[OrderQuery] = []
        self.stop_order_queries: list[StopOrderQuery] = []
        self._fail_on = fail_on

    async def fetch_order_status(self, query: OrderQuery) -> Order:
        self.order_queries.append(query)
        if self._fail_on and self._fail_on in (query.order_id or ""):
            raise RuntimeError(f"simulated fetch failure for {query.order_id}")
        return Order(
            exchange=self.exchange,
            symbol=query.symbol,
            raw_symbol=query.symbol,
            order_id=query.order_id,
            client_order_id=query.client_order_id,
            status=OrderStatus.FILLED,
            quantity=Decimal("0.5"),
            filled_quantity=Decimal("0.5"),
        )

    async def fetch_stop_order_status(self, query: StopOrderQuery) -> Order:
        self.stop_order_queries.append(query)
        if self._fail_on and self._fail_on in (query.stop_order_id or ""):
            raise RuntimeError(f"simulated fetch failure for {query.stop_order_id}")
        return Order(
            exchange=self.exchange,
            symbol=query.symbol,
            raw_symbol=query.symbol,
            order_id=query.stop_order_id,
            client_order_id=query.client_order_id,
            status=OrderStatus.NEW,
        )


@dataclass
class FakeLeg:
    """Minimal leg with just the fields _known_ids reads."""
    position_id: str = "p1"
    exchange: ExchangeName = ExchangeName.OKX
    entry_order_id: str | None = None
    entry_client_order_id: str | None = None
    stop_order_id: str | None = None
    stop_client_order_id: str | None = None


@dataclass
class FakePlan:
    position_id: str = "p1"


class FakePositionPlanStore:
    """Minimal store that returns pre-configured legs."""

    def __init__(self, legs: list[FakeLeg] | None = None) -> None:
        self._legs = legs or []

    def list_active_positions(self) -> list[FakePlan]:
        if not self._legs:
            return []
        return [FakePlan(position_id=self._legs[0].position_id)]

    def get_legs(self, position_id: str) -> list[FakeLeg]:
        return [leg for leg in self._legs if leg.position_id == position_id]


# ────────────────────────────────────────────────────────────────────────────
# Known-order-ref tests
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_order_sync_skips_invalid_known_order_ids():
    """Invalid order_id / client_order_id values never reach the exchange."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": ["", "None", "null", "nan", "N/A", "undefined", None, "   "],  # type: ignore[list-item]
                "stop_orders": ["", "None", "null"],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert len(execution.order_queries) == 0, f"expected 0 order queries, got {len(execution.order_queries)}"
    assert len(execution.stop_order_queries) == 0, f"expected 0 stop order queries, got {len(execution.stop_order_queries)}"


@pytest.mark.asyncio
async def test_order_sync_skips_invalid_known_order_ids_from_position_plan_store():
    """Position plan legs with all-invalid refs are skipped."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()
    store = FakePositionPlanStore(
        legs=[
            FakeLeg(entry_order_id=None, entry_client_order_id="", stop_order_id="None", stop_client_order_id="null"),
        ]
    )

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        position_plan_store=store,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert len(execution.order_queries) == 0
    assert len(execution.stop_order_queries) == 0


@pytest.mark.asyncio
async def test_order_sync_fetches_known_order_by_client_order_id_when_order_id_missing():
    """When order_id is None but client_order_id is valid, use client_order_id."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": [{"order_id": None, "client_order_id": "client-abc"}],
                "stop_orders": [{"order_id": None, "client_order_id": "stop-client-xyz"}],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert len(execution.order_queries) == 1
    assert execution.order_queries[0].client_order_id == "client-abc"
    assert execution.order_queries[0].order_id is None
    assert len(execution.stop_order_queries) == 1
    assert execution.stop_order_queries[0].client_order_id == "stop-client-xyz"
    assert execution.stop_order_queries[0].stop_order_id is None


@pytest.mark.asyncio
async def test_order_sync_fetches_known_order_by_client_order_id_from_position_plan():
    """Position plan leg with only client_order_id set still queries."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()
    store = FakePositionPlanStore(
        legs=[
            FakeLeg(
                entry_order_id=None,
                entry_client_order_id="entry-client-1",
                stop_order_id=None,
                stop_client_order_id="stop-client-1",
            ),
        ]
    )

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        position_plan_store=store,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert len(execution.order_queries) == 1
    assert execution.order_queries[0].client_order_id == "entry-client-1"
    assert execution.order_queries[0].order_id is None
    assert len(execution.stop_order_queries) == 1
    assert execution.stop_order_queries[0].client_order_id == "stop-client-1"
    assert execution.stop_order_queries[0].stop_order_id is None


@pytest.mark.asyncio
async def test_order_sync_known_order_status_failure_does_not_abort_exchange_sync():
    """A single known order fetch failure is recorded but does not abort the entire exchange sync."""
    state = MemoryState()
    execution = TrackingExecution(fail_on="11111")
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": ["11111", "22222"],
                "stop_orders": ["33333"],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=True, sync_open_orders=True, sync_open_stop_orders=True),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    # The overall result should be unsuccessful because a known fetch failed
    assert results[0].success is False
    # But the sync did NOT raise — it returned a result
    assert "known_order_status_failures" in results[0].metadata
    assert any("11111" in f for f in results[0].metadata["known_order_status_failures"])
    # 22222 was still attempted (request_count includes it)
    assert len(execution.order_queries) == 2
    # stop order was still attempted
    assert len(execution.stop_order_queries) == 1
    # open orders and positions synced normally
    assert "fetch_open_orders" in execution.calls
    assert "fetch_open_stop_orders" in execution.calls
    assert "fetch_positions" in account.calls


@pytest.mark.asyncio
async def test_order_sync_success_when_all_known_refs_fetch_ok():
    """When all known refs are valid and fetch successfully, result.success is True."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": ["11111", "22222"],
                "stop_orders": ["33333"],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert len(execution.order_queries) == 2
    assert len(execution.stop_order_queries) == 1
    assert "known_order_status_failures" not in results[0].metadata


@pytest.mark.asyncio
async def test_order_sync_skips_cleans_and_fetches_mixed_valid_invalid_known_ids():
    """Mixed bag of valid, invalid, and sentinel IDs — valid ones fetch, invalid ones skip."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": ["11111", "", "None", "22222"],
                "stop_orders": [None, "33333", "null"],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    assert results[0].success is True
    order_ids_queried = [q.order_id for q in execution.order_queries]
    assert order_ids_queried == ["11111", "22222"]
    stop_ids_queried = [q.stop_order_id for q in execution.stop_order_queries]
    assert stop_ids_queried == ["33333"]
    # Invalid legacy items ("", "None", None, "null") are filtered out
    # inside _known_ids() — they never produce KnownOrderRef objects,
    # so skipped_invalid_order_refs in sync_context may be 0.
    # The important thing: only valid refs reached the exchange.
    assert len(execution.order_queries) == 2
    assert len(execution.stop_order_queries) == 1


@pytest.mark.asyncio
async def test_known_ids_deduplicates_order_refs():
    """Duplicate order refs from the callback are deduplicated."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": ["11111", "11111", "22222"],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert len(execution.order_queries) == 2
    order_ids_queried = [q.order_id for q in execution.order_queries]
    assert sorted(order_ids_queried) == ["11111", "22222"]


@pytest.mark.asyncio
async def test_known_ids_legacy_tuple_pair_format():
    """Legacy (order_id, client_order_id) tuple pair format is supported."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": [("11111", "cid-1")],
                "stop_orders": [("22222", "scid-1")],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert len(execution.order_queries) == 1
    assert execution.order_queries[0].order_id == "11111"
    assert execution.order_queries[0].client_order_id == "cid-1"


@pytest.mark.asyncio
async def test_clean_order_id_helper():
    """Unit-test _clean_order_id directly."""
    from src.runtime.account_sync.service import _clean_order_id

    assert _clean_order_id(None) is None
    assert _clean_order_id("") is None
    assert _clean_order_id("   ") is None
    assert _clean_order_id("None") is None
    assert _clean_order_id("null") is None
    assert _clean_order_id("N/A") is None
    assert _clean_order_id("nan") is None
    assert _clean_order_id("undefined") is None
    assert _clean_order_id("Na") is None
    assert _clean_order_id("  None  ") is None
    assert _clean_order_id("real-id") == "real-id"
    assert _clean_order_id("  12345  ") == "12345"
    assert _clean_order_id(12345) == "12345"


@pytest.mark.asyncio
async def test_known_order_ref_dataclass():
    """KnownOrderRef is a frozen dataclass with optional fields."""
    ref = KnownOrderRef(order_id="abc", client_order_id="xyz")
    assert ref.order_id == "abc"
    assert ref.client_order_id == "xyz"

    ref2 = KnownOrderRef()
    assert ref2.order_id is None
    assert ref2.client_order_id is None


# ────────────────────────────────────────────────────────────────────────────
# Known order status failure counter and alert tests
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_known_order_status_failures_increment_failure_counter_and_alert():
    """Consecutive known order status fetch failures accumulate and alert at threshold."""
    state = MemoryState()
    alerts = MemoryAlerts()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": ["11111"],
                "stop_orders": [],
            }
        }

    # First sync — known order fetch fails
    execution1 = TrackingExecution(fail_on="11111")
    service1 = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution1, state_store=state),),
        config=OrderStateRequirement(
            sync_position=False,
            sync_open_orders=False,
            sync_open_stop_orders=False,
            consecutive_failure_alert_threshold=2,
        ),
        active_check=lambda: True,
        known_order_ids=known,
        alert_sink=alerts,
    )
    result1 = await service1.sync_once()

    assert result1[0].success is False
    assert "known_order_status_failures" in result1[0].metadata
    assert result1[0].metadata["consecutive_failures"] == 1
    # No alert yet (threshold=2)
    assert len(alerts.items) == 0

    # Second sync — known order fetch fails again
    state2 = MemoryState()
    execution2 = TrackingExecution(fail_on="11111")
    service2 = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution2, state_store=state2),),
        config=OrderStateRequirement(
            sync_position=False,
            sync_open_orders=False,
            sync_open_stop_orders=False,
            consecutive_failure_alert_threshold=2,
        ),
        active_check=lambda: True,
        known_order_ids=known,
        alert_sink=alerts,
    )
    # Copy the failure counter from service1
    service2._failures = dict(service1._failures)

    result2 = await service2.sync_once()

    assert result2[0].success is False
    assert result2[0].metadata["consecutive_failures"] == 2
    # Alert emitted at threshold=2
    assert len(alerts.items) == 1
    alert = alerts.items[0]
    assert alert.subject == "AetherEdge order sync known order status failures"
    assert alert.severity == "error"
    assert "exchange=okx" in alert.content.lower() or "okx" in alert.content
    assert "2" in alert.content  # consecutive_failures


@pytest.mark.asyncio
async def test_invalid_known_order_refs_do_not_increment_failure_counter():
    """Invalid order refs are skipped and do NOT increase the failure counter."""
    state = MemoryState()
    alerts = MemoryAlerts()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": ["", "None", "null"],
                "stop_orders": ["", "None", "null"],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(
            sync_position=False,
            sync_open_orders=False,
            sync_open_stop_orders=False,
            consecutive_failure_alert_threshold=2,
        ),
        active_check=lambda: True,
        known_order_ids=known,
        alert_sink=alerts,
    )

    # Sync twice — all refs are invalid, no exchange calls, no failures
    await service.sync_once()
    await service.sync_once()

    assert len(execution.order_queries) == 0
    assert len(execution.stop_order_queries) == 0
    # No alerts because no failures
    assert len(alerts.items) == 0
    # Counter should not have been incremented
    assert service._failures.get("okx", 0) == 0


# ────────────────────────────────────────────────────────────────────────────
# Known order ref validation with exchange-specific ID checks (AE-V9C-LIVE-BOOTSTRAP-012)
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_known_ids_filters_fake_exchange_order_ids_from_position_plan():
    """Position plan legs with fake order IDs (okx-order-1) are marked INVALID_FORMAT."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    # Leg with fake OKX order ID
    store = FakePositionPlanStore(
        legs=[
            FakeLeg(
                position_id="p1",
                exchange=ExchangeName.OKX,
                entry_order_id="okx-order-1",
                entry_client_order_id=None,
                stop_order_id="okx-stop-1",
                stop_client_order_id=None,
            ),
        ]
    )

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        position_plan_store=store,
    )

    results = await service.sync_once()

    # Fake IDs should be skipped — no exchange calls
    assert results[0].success is True
    assert len(execution.order_queries) == 0, (
        f"Expected 0 order queries (fake IDs skipped), got {len(execution.order_queries)}"
    )
    assert len(execution.stop_order_queries) == 0, (
        f"Expected 0 stop order queries (fake IDs skipped), got {len(execution.stop_order_queries)}"
    )


@pytest.mark.asyncio
async def test_known_ids_uses_client_order_id_when_exchange_id_is_fake():
    """When exchange order_id is fake but client_order_id is valid, use client only."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    store = FakePositionPlanStore(
        legs=[
            FakeLeg(
                position_id="p1",
                exchange=ExchangeName.OKX,
                entry_order_id="okx-order-1",
                entry_client_order_id="AEOKOLabc123",
                stop_order_id="okx-stop-1",
                stop_client_order_id="AEOKSPxyz789",
            ),
        ]
    )

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        position_plan_store=store,
    )

    results = await service.sync_once()

    assert results[0].success is True
    # Should query using client_order_id only (order_id is None)
    assert len(execution.order_queries) == 1
    assert execution.order_queries[0].order_id is None
    assert execution.order_queries[0].client_order_id == "AEOKOLabc123"
    assert len(execution.stop_order_queries) == 1
    assert execution.stop_order_queries[0].stop_order_id is None
    assert execution.stop_order_queries[0].client_order_id == "AEOKSPxyz789"


@pytest.mark.asyncio
async def test_known_ids_skips_binance_fake_order_ids():
    """Binance fake order IDs (binance-order-1) are filtered out."""
    state = MemoryState()
    execution = TrackingExecution()
    execution.exchange = ExchangeName.BINANCE  # override for Binance
    account = FakeAccount()
    account.exchange = ExchangeName.BINANCE  # override for Binance test

    store = FakePositionPlanStore(
        legs=[
            FakeLeg(
                position_id="p1",
                exchange=ExchangeName.BINANCE,
                entry_order_id="binance-order-1",
                entry_client_order_id=None,
                stop_order_id="binance-stop-1",
                stop_client_order_id=None,
            ),
        ]
    )

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        position_plan_store=store,
    )

    results = await service.sync_once()
    assert len(execution.order_queries) == 0
    assert len(execution.stop_order_queries) == 0


@pytest.mark.asyncio
async def test_known_ids_detects_okx_1_as_fake():
    """okx-1 is a fake pattern and should be filtered."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    store = FakePositionPlanStore(
        legs=[
            FakeLeg(
                position_id="p1",
                exchange=ExchangeName.OKX,
                entry_order_id="okx-1",
                entry_client_order_id=None,
            ),
        ]
    )

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        position_plan_store=store,
    )

    results = await service.sync_once()
    assert len(execution.order_queries) == 0, (
        f"Expected 0 queries for fake ID 'okx-1', got {len(execution.order_queries)}"
    )


@pytest.mark.asyncio
async def test_known_ids_allows_real_numeric_order_ids():
    """Real numeric exchange order IDs pass validation and are queried."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    store = FakePositionPlanStore(
        legs=[
            FakeLeg(
                position_id="p1",
                exchange=ExchangeName.OKX,
                entry_order_id="1234567890",
                entry_client_order_id="AEOKOLabc123",
            ),
        ]
    )

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        position_plan_store=store,
    )

    results = await service.sync_once()
    assert len(execution.order_queries) == 1
    # Should use the numeric order_id
    assert execution.order_queries[0].order_id == "1234567890"
    assert execution.order_queries[0].client_order_id == "AEOKOLabc123"


# ── Callback path exchange-specific validation (AE-V9C-LIVE-BOOTSTRAP-013) ──


@pytest.mark.asyncio
async def test_callback_known_ids_fake_order_id_with_valid_client():
    """Callback returns fake order_id + valid client_order_id → use client only."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": [{"order_id": "okx-order-1", "client_order_id": "AEOKOLabc123"}],
                "stop_orders": [{"order_id": "okx-stop-1", "client_order_id": "AEOKSPxyz789"}],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert len(execution.order_queries) == 1
    # order_id must be None (fake filtered out), client_order_id used
    assert execution.order_queries[0].order_id is None, (
        f"order_id should be None (fake filtered), got {execution.order_queries[0].order_id}"
    )
    assert execution.order_queries[0].client_order_id == "AEOKOLabc123"
    assert len(execution.stop_order_queries) == 1
    assert execution.stop_order_queries[0].stop_order_id is None
    assert execution.stop_order_queries[0].client_order_id == "AEOKSPxyz789"


@pytest.mark.asyncio
async def test_callback_known_ids_fake_order_id_without_client_skipped():
    """Callback returns fake order_id + no client → marked INVALID_FORMAT, no query."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": [{"order_id": "okx-order-1", "client_order_id": None}],
                "stop_orders": [{"order_id": None, "client_order_id": None}],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    assert results[0].success is True
    # No exchange calls — refs were skipped as INVALID_FORMAT
    assert len(execution.order_queries) == 0, (
        f"Expected 0 order queries (fake ID + no client), got {len(execution.order_queries)}"
    )
    assert len(execution.stop_order_queries) == 0
    # Verify skipped_invalid is tracked
    assert results[0].metadata.get("skipped_invalid_order_refs", 0) >= 1


@pytest.mark.asyncio
async def test_callback_known_ids_legacy_plain_string_fake_skipped():
    """Legacy plain string callback: fake order_id is filtered out."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": ["okx-order-1", "1234567890"],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    assert results[0].success is True
    # Only the numeric ID should be queried
    assert len(execution.order_queries) == 1, (
        f"Expected 1 order query (fake filtered, only numeric), got {len(execution.order_queries)}"
    )
    assert execution.order_queries[0].order_id == "1234567890"


@pytest.mark.asyncio
async def test_callback_known_ids_binance_fake_with_client():
    """Binance callback: fake orderId + valid clientAlgoId → use client only."""
    state = MemoryState()
    execution = TrackingExecution()
    execution.exchange = ExchangeName.BINANCE
    account = FakeAccount()
    account.exchange = ExchangeName.BINANCE

    def known() -> dict[str, Any]:
        return {
            "binance": {
                "orders": [{"order_id": "binance-order-1", "client_order_id": "AEBIOLabc123"}],
            }
        }

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=False, sync_open_stop_orders=False),
        active_check=lambda: True,
        known_order_ids=known,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert len(execution.order_queries) == 1
    assert execution.order_queries[0].order_id is None
    assert execution.order_queries[0].client_order_id == "AEBIOLabc123"


# ────────────────────────────────────────────────────────────────────────────
# Log noise reduction tests (AE-V9C-LIVE-LOGGING-014)
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_order_sync_inactive_summary_not_emitted_on_second_tick():
    """First inactive logs INFO; second tick (20s later) must NOT emit summary.
    The summary should only fire after the full 600s interval."""
    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=FakeAccount(), execution=TrackingExecution(), state_store=MemoryState()),),
        config=OrderStateRequirement(poll_interval_seconds=20),
        active_check=lambda: False,
    )

    # ── Tick 1: first inactive (state transition None → False) ──
    assert not service.active_check()
    service._inactive_skip_summary.record_skip("inactive")
    t1 = 100.0
    # This is the first-inactive path: state-change INFO + mark_emitted
    assert service._last_order_sync_active is not False  # None is not False
    service._last_order_sync_active = False
    service._inactive_skip_summary.mark_emitted("inactive", now=t1)
    assert service._inactive_skip_summary.count("inactive") == 1

    # ── Tick 2: 20 seconds later — within 600s window ──
    assert not service.active_check()
    service._inactive_skip_summary.record_skip("inactive")
    t2 = 120.0
    # should NOT emit summary (mark_emitted seeded timer at t1=100)
    assert service._inactive_skip_summary.should_emit_summary(
        "inactive", interval_seconds=600.0, now=t2
    ) is False
    assert service._inactive_skip_summary.count("inactive") == 2

    # ── Tick 3: another 20s later — still within window ──
    assert not service.active_check()
    service._inactive_skip_summary.record_skip("inactive")
    t3 = 140.0
    assert service._inactive_skip_summary.should_emit_summary(
        "inactive", interval_seconds=600.0, now=t3
    ) is False
    assert service._inactive_skip_summary.count("inactive") == 3

    # ── After 10 minutes + 1s — summary SHOULD fire ──
    t4 = 701.0
    assert service._inactive_skip_summary.should_emit_summary(
        "inactive", interval_seconds=600.0, now=t4
    ) is True

    # ── State tracker is correct ──
    assert service._last_order_sync_active is False


@pytest.mark.asyncio
async def test_order_sync_logs_active_transition(caplog):
    """Inactive -> active transition should log INFO."""
    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=FakeAccount(), execution=TrackingExecution(), state_store=MemoryState()),),
        config=OrderStateRequirement(poll_interval_seconds=20),
        active_check=lambda: True,
    )

    # Initially None → True on first active check
    assert service._last_order_sync_active is None
    assert service.active_check() is True
    service._last_order_sync_active = True
    # Transition detected: None -> True
    assert service._last_order_sync_active is True

    # Subsequent active check: no change
    assert service.active_check() is True
    # Still True, no state transition
    assert service._last_order_sync_active is True


@pytest.mark.asyncio
async def test_order_sync_logs_active_to_inactive_transition():
    """Active -> inactive transition should be detectable."""
    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=FakeAccount(), execution=TrackingExecution(), state_store=MemoryState()),),
        config=OrderStateRequirement(poll_interval_seconds=20),
        active_check=lambda: False,
    )

    # First set it active
    service._last_order_sync_active = True

    # Now simulate inactive check
    assert not service.active_check()
    # State transition: True -> False
    assert service._last_order_sync_active is True  # still True until we update it
    service._last_order_sync_active = False
    # Transition detected
    assert service._last_order_sync_active is False


@pytest.mark.asyncio
async def test_order_sync_logs_open_order_count_only_on_change(caplog):
    """Open orders count-change detection: same count → no INFO, change → INFO."""
    import logging

    caplog.set_level(logging.DEBUG, logger="src.runtime.account_sync.service")

    state = MemoryState()
    account = FakeAccount()
    # Execution with 1 open order (the default FakeExecution returns 1)
    execution = TrackingExecution()

    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=True, sync_open_stop_orders=False),
        active_check=lambda: True,
    )

    # First sync: prev_count=-1 (initial) → count=1 → change detected → INFO
    await service.sync_once()
    assert service._last_open_order_count.get("okx") == 1

    # Second sync: pre-set to same count → no change → DEBUG
    execution2 = TrackingExecution()
    state2 = MemoryState()
    service2 = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=FakeAccount(), execution=execution2, state_store=state2),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=True, sync_open_stop_orders=False),
        active_check=lambda: True,
    )
    service2._last_open_order_count["okx"] = 1  # pre-set to same value
    await service2.sync_once()
    # Should be unchanged
    assert service2._last_open_order_count.get("okx") == 1

    # Third sync: count changes 1 -> 0 (empty orders)
    execution3 = TrackingExecution(open_orders=[])
    state3 = MemoryState()
    service3 = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=FakeAccount(), execution=execution3, state_store=state3),),
        config=OrderStateRequirement(sync_position=False, sync_open_orders=True, sync_open_stop_orders=False),
        active_check=lambda: True,
    )
    service3._last_open_order_count["okx"] = 1
    await service3.sync_once()
    # Should be updated to 0
    assert service3._last_open_order_count.get("okx") == 0


@pytest.mark.asyncio
async def test_account_sync_logs_info_only_when_account_fingerprint_changes(caplog):
    """Account sync: same fingerprint → no INFO 'Account state changed'."""
    import logging

    caplog.set_level(logging.DEBUG, logger="src.runtime.account_sync.service")

    account = FakeAccount()
    state = MemoryState()
    service = AccountStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=FakeExecution(), state_store=state),),
        config=AccountStateRequirement(poll_interval_seconds=300),
    )

    # First sync: fingerprint is None → should log INFO
    results = await service.sync_once()
    assert results[0].success is True
    assert "okx" in service._last_account_fingerprint

    fp1 = service._last_account_fingerprint["okx"]

    # Second sync: same balance/positions → same fingerprint → should log DEBUG
    account2 = FakeAccount()
    state2 = MemoryState()
    service2 = AccountStateSyncService(
        contexts=(SyncExchangeContext(account=account2, execution=FakeExecution(), state_store=state2),),
        config=AccountStateRequirement(poll_interval_seconds=300),
    )
    service2._last_account_fingerprint["okx"] = fp1  # pre-set to same fingerprint
    results2 = await service2.sync_once()
    assert results2[0].success is True
    fp2 = service2._last_account_fingerprint.get("okx")
    assert fp1 == fp2  # Fingerprints match → no change logged at INFO


@pytest.mark.asyncio
async def test_account_sync_logs_balance_change(caplog):
    """Account balance change → fingerprint differs → INFO logged."""
    import logging

    caplog.set_level(logging.INFO, logger="src.runtime.account_sync.service")

    account1 = FakeAccount()
    service = AccountStateSyncService(
        contexts=(SyncExchangeContext(account=account1, execution=FakeExecution(), state_store=MemoryState()),),
        config=AccountStateRequirement(poll_interval_seconds=300),
    )
    await service.sync_once()
    fp1 = service._last_account_fingerprint.get("okx")

    # Change balance
    class ChangedAccount(FakeAccount):
        async def fetch_balance(self, asset="USDT"):
            self.calls.append("fetch_balance")
            return Balance(exchange=self.exchange, asset=asset, total=Decimal("90"), available=Decimal("80"))

    account2 = ChangedAccount()
    service2 = AccountStateSyncService(
        contexts=(SyncExchangeContext(account=account2, execution=FakeExecution(), state_store=MemoryState()),),
        config=AccountStateRequirement(poll_interval_seconds=300),
    )
    service2._last_account_fingerprint["okx"] = fp1
    await service2.sync_once()
    fp2 = service2._last_account_fingerprint.get("okx")
    # Fingerprints should differ (balance changed 100→90, 90→80)
    assert fp1 != fp2


@pytest.mark.asyncio
async def test_known_order_failure_still_warning_and_alert(caplog):
    """Known order status fetch failure must still log WARNING and emit alert."""
    import logging

    caplog.set_level(logging.WARNING, logger="src.runtime.account_sync.service")

    alerts = MemoryAlerts()
    state = MemoryState()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {"okx": {"orders": ["11111"], "stop_orders": []}}

    execution = TrackingExecution(fail_on="11111")
    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(
            sync_position=False, sync_open_orders=False, sync_open_stop_orders=False,
            consecutive_failure_alert_threshold=1,
        ),
        active_check=lambda: True,
        known_order_ids=known,
        alert_sink=alerts,
    )

    result = await service.sync_once()
    assert result[0].success is False
    assert "known_order_status_failures" in result[0].metadata
    # Alert emitted at threshold=1
    assert len(alerts.items) == 1
    assert alerts.items[0].severity == "error"


@pytest.mark.asyncio
async def test_account_sync_failure_still_warning(caplog):
    """Account sync failure must still log WARNING."""
    import logging

    caplog.set_level(logging.WARNING, logger="src.runtime.account_sync.service")

    account = FakeAccount(fail=True)
    service = AccountStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=FakeExecution(), state_store=MemoryState()),),
        config=AccountStateRequirement(poll_interval_seconds=300),
    )

    result = await service.sync_once()
    assert result[0].success is False
    assert result[0].error is not None
