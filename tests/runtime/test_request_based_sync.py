from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

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
    execution = TrackingExecution(fail_on="bad-order")
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": ["bad-order", "good-order"],
                "stop_orders": ["stop-1"],
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
    assert any("bad-order" in f for f in results[0].metadata["known_order_status_failures"])
    # good-order was still attempted (request_count includes it)
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
                "orders": ["o1", "o2"],
                "stop_orders": ["s1"],
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
                "orders": ["real-order-1", "", "None", "real-order-2"],
                "stop_orders": [None, "real-stop-1", "null"],
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
    assert order_ids_queried == ["real-order-1", "real-order-2"]
    stop_ids_queried = [q.stop_order_id for q in execution.stop_order_queries]
    assert stop_ids_queried == ["real-stop-1"]
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
                "orders": ["dup-1", "dup-1", "dup-2"],
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
    assert sorted(order_ids_queried) == ["dup-1", "dup-2"]


@pytest.mark.asyncio
async def test_known_ids_legacy_tuple_pair_format():
    """Legacy (order_id, client_order_id) tuple pair format is supported."""
    state = MemoryState()
    execution = TrackingExecution()
    account = FakeAccount()

    def known() -> dict[str, Any]:
        return {
            "okx": {
                "orders": [("oid-1", "cid-1")],
                "stop_orders": [("soid-1", "scid-1")],
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
    assert execution.order_queries[0].order_id == "oid-1"
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
                "orders": ["bad-order"],
                "stop_orders": [],
            }
        }

    # First sync — known order fetch fails
    execution1 = TrackingExecution(fail_on="bad-order")
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
    execution2 = TrackingExecution(fail_on="bad-order")
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
