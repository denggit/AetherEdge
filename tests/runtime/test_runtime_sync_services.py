from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.app import AppConfig
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime import runner as runner_module
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeRunner
from src.runtime.sync_services import RuntimeSyncServiceRegistry


def _runner(*, services: dict | None = None) -> LiveRuntimeRunner:
    config = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="tests.fake:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )
    injected = {
        "project_env_config": ProjectEnvConfig(
            values={},
            source_files=(),
            env_file=Path(".env"),
            example_file=None,
        ),
        "runtime_requirements": StrategyRuntimeRequirements.from_mapping({}),
    }
    injected.update(services or {})
    return LiveRuntimeRunner(
        app_config=config,
        app_context=SimpleNamespace(strategy=object()),
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services=injected,
    )


def test_registry_initial_state_and_owned_fields() -> None:
    registry = RuntimeSyncServiceRegistry()

    assert vars(registry) == {
        "_account_service": None,
        "_order_service": None,
    }


def test_registry_properties_expose_current_slots_without_side_effects() -> None:
    account = Mock()
    order = Mock()
    registry = RuntimeSyncServiceRegistry(
        account_service=account,
        order_service=order,
    )

    assert registry.account_service is account
    assert registry.order_service is order
    assert vars(registry) == {
        "_account_service": account,
        "_order_service": order,
    }
    account.assert_not_called()
    order.assert_not_called()


def test_account_and_order_factories_are_independent_and_called_once() -> None:
    registry = RuntimeSyncServiceRegistry()
    account = object()
    order = object()
    account_factory = Mock(return_value=account)
    order_factory = Mock(return_value=order)

    assert registry.get_account(account_factory) is account
    assert registry.get_account(account_factory) is account
    account_factory.assert_called_once_with()
    order_factory.assert_not_called()

    assert registry.get_order(order_factory) is order
    assert registry.get_order(order_factory) is order
    order_factory.assert_called_once_with()
    assert registry.get_account(account_factory) is account


def test_injected_services_bypass_factories_without_business_calls() -> None:
    account = SimpleNamespace(run_periodic=Mock(), sync_once=Mock())
    order = SimpleNamespace(run_periodic=Mock(), sync_once=Mock())
    registry = RuntimeSyncServiceRegistry(
        account_service=account,
        order_service=order,
    )
    account_factory = Mock()
    order_factory = Mock()

    assert registry.get_account(account_factory) is account
    assert registry.get_order(order_factory) is order

    account_factory.assert_not_called()
    order_factory.assert_not_called()
    account.run_periodic.assert_not_called()
    account.sync_once.assert_not_called()
    order.run_periodic.assert_not_called()
    order.sync_once.assert_not_called()


@pytest.mark.parametrize("slot", ("account", "order"))
def test_factory_error_is_preserved_and_slot_can_retry(slot: str) -> None:
    registry = RuntimeSyncServiceRegistry()
    error = RuntimeError(f"{slot} factory failed")
    service = object()
    factory = Mock(side_effect=(error, service))
    getter = getattr(registry, f"get_{slot}")

    with pytest.raises(RuntimeError) as raised:
        getter(factory)

    assert raised.value is error
    assert vars(registry)[f"_{slot}_service"] is None
    assert getter(factory) is service
    assert getter(factory) is service
    assert factory.call_count == 2


def test_complete_registry_injection_overrides_legacy_services() -> None:
    registry_account = object()
    registry_order = object()
    registry = RuntimeSyncServiceRegistry(
        account_service=registry_account,
        order_service=registry_order,
    )
    legacy_account = object()
    legacy_order = object()

    runner = _runner(
        services={
            "sync_service_registry": registry,
            "account_sync_service": legacy_account,
            "order_sync_service": legacy_order,
        }
    )

    assert runner._sync_service_registry is registry
    assert runner.services["sync_service_registry"] is registry
    assert runner._account_sync_service is registry_account
    assert runner._order_sync_service is registry_order
    assert runner._account_sync_service is not legacy_account
    assert runner._order_sync_service is not legacy_order
    assert runner.services["account_sync_service"] is legacy_account
    assert runner.services["order_sync_service"] is legacy_order
    assert runner._get_account_sync_service() is registry_account
    assert runner._get_order_sync_service() is registry_order
    assert runner._account_sync_service is registry_account
    assert runner._order_sync_service is registry_order


def test_empty_registry_overrides_legacy_compatibility_fields() -> None:
    registry = RuntimeSyncServiceRegistry()
    legacy_account = object()
    legacy_order = object()

    runner = _runner(
        services={
            "sync_service_registry": registry,
            "account_sync_service": legacy_account,
            "order_sync_service": legacy_order,
        }
    )

    assert runner._account_sync_service is None
    assert runner._order_sync_service is None
    assert runner.services["account_sync_service"] is legacy_account
    assert runner.services["order_sync_service"] is legacy_order


@pytest.mark.parametrize(
    ("registry_account", "registry_order"),
    ((object(), None), (None, object())),
)
def test_partial_registry_immediately_controls_compatibility_fields(
    registry_account: object | None,
    registry_order: object | None,
) -> None:
    legacy_account = object()
    legacy_order = object()
    registry = RuntimeSyncServiceRegistry(
        account_service=registry_account,
        order_service=registry_order,
    )

    runner = _runner(
        services={
            "sync_service_registry": registry,
            "account_sync_service": legacy_account,
            "order_sync_service": legacy_order,
        }
    )

    assert runner._account_sync_service is registry_account
    assert runner._order_sync_service is registry_order
    assert runner.services["account_sync_service"] is legacy_account
    assert runner.services["order_sync_service"] is legacy_order


def test_empty_registry_construction_keeps_sync_dependencies_lazy(
    monkeypatch,
) -> None:
    account_factory = Mock(return_value=object())
    order_factory = Mock(return_value=object())
    get_contexts = Mock(return_value=(object(),))
    get_position_plan_store = Mock(return_value=object())
    monkeypatch.setattr(
        "src.runtime.components.account.AccountStateSyncService",
        account_factory,
    )
    monkeypatch.setattr(
        "src.runtime.components.account.OrderStateSyncService",
        order_factory,
    )
    monkeypatch.setattr(
        LiveRuntimeRunner,
        "_get_sync_contexts",
        get_contexts,
    )
    monkeypatch.setattr(
        LiveRuntimeRunner,
        "_get_position_plan_store",
        get_position_plan_store,
    )

    runner = _runner(
        services={"sync_service_registry": RuntimeSyncServiceRegistry()}
    )

    assert runner._account_sync_service is None
    assert runner._order_sync_service is None
    account_factory.assert_not_called()
    order_factory.assert_not_called()
    get_contexts.assert_not_called()
    get_position_plan_store.assert_not_called()


def test_complete_registry_injection_does_not_create_default(
    monkeypatch,
) -> None:
    registry = RuntimeSyncServiceRegistry()
    default_factory = Mock()
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeSyncServiceRegistry",
        default_factory,
    )

    runner = _runner(services={"sync_service_registry": registry})

    default_factory.assert_not_called()
    assert runner._sync_service_registry is registry
    assert runner.services["sync_service_registry"] is registry


def test_runner_creates_one_default_registry(monkeypatch) -> None:
    registry = object()
    factory = Mock(return_value=registry)
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeSyncServiceRegistry",
        factory,
    )

    runner = _runner()

    factory.assert_called_once_with(
        account_service=None,
        order_service=None,
    )
    assert runner._sync_service_registry is registry
    assert runner.services["sync_service_registry"] is registry


@pytest.mark.parametrize(
    ("account_injected", "order_injected"),
    ((False, False), (True, False), (False, True), (True, True)),
)
def test_default_registry_preserves_all_legacy_injection_combinations(
    account_injected: bool,
    order_injected: bool,
) -> None:
    account = object() if account_injected else None
    order = object() if order_injected else None
    services = {}
    if account_injected:
        services["account_sync_service"] = account
    if order_injected:
        services["order_sync_service"] = order

    runner = _runner(services=services)

    assert vars(runner._sync_service_registry) == {
        "_account_service": account,
        "_order_service": order,
    }
    assert runner._account_sync_service is account
    assert runner._order_sync_service is order
    if account_injected:
        assert runner._get_account_sync_service() is account
    if order_injected:
        assert runner._get_order_sync_service() is order


def test_account_builder_preserves_all_dependencies_and_identity(
    monkeypatch,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    contexts = (object(), object())
    config = object()
    alerts = object()
    throttle = object()
    snapshot_callback = Mock()
    service = object()
    factory = Mock(return_value=service)
    get_contexts = Mock(return_value=contexts)
    monkeypatch.setattr(
        "src.runtime.components.account.AccountStateSyncService",
        factory,
    )
    runner._get_sync_contexts = get_contexts
    runner.requirements = SimpleNamespace(account_state=config)
    runner.context = SimpleNamespace(alerts=alerts)
    runner._request_sync_throttle = throttle
    runner._on_account_snapshot_synced = snapshot_callback

    result = runner._build_account_sync_service()

    assert result is service
    get_contexts.assert_called_once_with()
    factory.assert_called_once_with(
        contexts=contexts,
        config=config,
        alert_sink=alerts,
        throttle=throttle,
        snapshot_callback=snapshot_callback,
    )


def test_order_builder_preserves_all_dependencies_and_lazy_position_store(
    monkeypatch,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    contexts = (object(), object())
    config = object()
    alerts = object()
    throttle = object()
    active_check = Mock()
    position_plan_store = object()
    service = object()
    factory = Mock(return_value=service)
    get_contexts = Mock(return_value=contexts)
    get_position_plan_store = Mock(return_value=position_plan_store)
    monkeypatch.setattr(
        "src.runtime.components.account.OrderStateSyncService",
        factory,
    )
    runner._get_sync_contexts = get_contexts
    runner._get_position_plan_store = get_position_plan_store
    runner.requirements = SimpleNamespace(order_state=config)
    runner.context = SimpleNamespace(alerts=alerts)
    runner._request_sync_throttle = throttle
    runner._order_sync_active = active_check

    get_contexts.assert_not_called()
    get_position_plan_store.assert_not_called()
    result = runner._build_order_sync_service()

    assert result is service
    get_contexts.assert_called_once_with()
    get_position_plan_store.assert_called_once_with()
    factory.assert_called_once_with(
        contexts=contexts,
        config=config,
        alert_sink=alerts,
        throttle=throttle,
        active_check=active_check,
        position_plan_store=position_plan_store,
    )


def test_default_getters_build_lazily_once_and_keep_legacy_fields() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    registry = RuntimeSyncServiceRegistry()
    account = object()
    order = object()
    account_builder = Mock(return_value=account)
    order_builder = Mock(return_value=order)
    runner.services = {"sync_service_registry": registry}
    runner._sync_service_registry = registry
    runner._account_sync_service = None
    runner._order_sync_service = None
    runner._build_account_sync_service = account_builder
    runner._build_order_sync_service = order_builder

    account_builder.assert_not_called()
    order_builder.assert_not_called()
    assert runner._get_account_sync_service() is account
    assert runner._get_account_sync_service() is account
    assert runner._get_order_sync_service() is order
    assert runner._get_order_sync_service() is order

    account_builder.assert_called_once_with()
    order_builder.assert_called_once_with()
    assert runner._account_sync_service is account
    assert runner._order_sync_service is order
    assert "account_sync_service" not in runner.services
    assert "order_sync_service" not in runner.services


def test_account_and_order_builders_resolve_contexts_independently(
    monkeypatch,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    account_service = object()
    order_service = object()
    get_contexts = Mock(side_effect=((object(),), (object(),)))
    get_position_plan_store = Mock(return_value=object())
    runner._sync_service_registry = RuntimeSyncServiceRegistry()
    runner._account_sync_service = None
    runner._order_sync_service = None
    runner._get_sync_contexts = get_contexts
    runner._get_position_plan_store = get_position_plan_store
    runner.requirements = SimpleNamespace(
        account_state=object(),
        order_state=object(),
    )
    runner.context = SimpleNamespace(alerts=object())
    runner._request_sync_throttle = object()
    runner._on_account_snapshot_synced = Mock()
    runner._order_sync_active = Mock()

    account_factory = Mock(return_value=account_service)
    order_factory = Mock(return_value=order_service)
    monkeypatch.setattr(
        "src.runtime.components.account.AccountStateSyncService",
        account_factory,
    )
    monkeypatch.setattr(
        "src.runtime.components.account.OrderStateSyncService",
        order_factory,
    )

    assert runner._get_account_sync_service() is account_service
    assert runner._get_order_sync_service() is order_service

    assert get_contexts.call_count == 2
    get_position_plan_store.assert_called_once_with()


def test_periodic_and_immediate_getters_share_service_instances() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    stop_event = object()
    account_periodic: list[object] = []
    order_periodic: list[object] = []

    class Service:
        def __init__(self, calls: list[object]) -> None:
            self.calls = calls

        def run_periodic(self, event):
            self.calls.append(event)
            return object()

    account = Service(account_periodic)
    order = Service(order_periodic)
    registry = RuntimeSyncServiceRegistry(
        account_service=account,
        order_service=order,
    )

    class Lifecycle:
        def start(self, factories):
            return [factory() for factory in factories]

    runner._sync_service_registry = registry
    runner._account_sync_service = None
    runner._order_sync_service = None
    runner._sync_lifecycle = Lifecycle()
    runner._sync_tasks = []
    runner._stop_event = stop_event
    runner.requirements = SimpleNamespace(
        account_state=SimpleNamespace(poll_enabled=True),
        order_state=SimpleNamespace(poll_when_position_enabled=True),
    )
    runner._periodic_follower_close_check = lambda event: object()
    runner._heartbeat_service = SimpleNamespace(
        run_periodic=lambda event: object()
    )
    runner._get_startup_feature_backfill_providers = lambda: ()

    runner._start_sync_tasks()

    assert account_periodic == [stop_event]
    assert order_periodic == [stop_event]
    assert runner._get_account_sync_service() is account
    assert runner._get_order_sync_service() is order
    assert runner._account_sync_service is account
    assert runner._order_sync_service is order
