from __future__ import annotations

from dataclasses import fields

import pytest

from src.app import AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.planner import ExecutionPlanner
from src.runtime import runner as runner_module
from src.runtime.components import (
    AccountComponent,
    MarketEventsComponent,
    WiringComponent,
)
from src.runtime.context import RuntimeContext
from src.runtime.ports import AccountPorts, MarketEventPorts
from src.runtime.runner import LiveRuntimeRunner
from src.runtime.config import LiveRuntimeConfig
from src.runtime.models import RuntimeMode
from src.runtime.services import (
    AccountRuntimeServices,
    ExecutionRuntimeServices,
    MarketRuntimeServices,
)
from tests.runtime.test_live_runtime_runner import (
    FakeData,
    FakeExecution,
    FakeStateStore,
    FakeStrategy,
    _app_config,
)


def _runner() -> LiveRuntimeRunner:
    config = _app_config()
    return LiveRuntimeRunner(
        app_config=config,
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        app_context=AppContext(
            data=FakeData(),
            execution=FakeExecution(),
            state_store=FakeStateStore(),
            strategy=FakeStrategy(),
            planner=ExecutionPlanner(),
            alerts=AsyncAlertDispatcher(NoopAlertSink()),
        ),
    )


def test_runner_has_no_dynamic_component_method_scanner() -> None:
    assert not hasattr(runner_module, "_compatibility_component_methods")
    assert not hasattr(runner_module, "_COMPATIBILITY_COMPONENT_METHODS")
    assert not hasattr(runner_module.LiveRuntimeRunner, "_bind_component_ports")


def test_runner_context_and_components_have_isolated_namespaces() -> None:
    runner = _runner()

    assert runner.market_events.__dict__ is not runner.account_runtime.__dict__
    assert runner.market_events.__dict__ is not runner.__dict__
    assert runner.account_runtime.__dict__ is not runner.__dict__
    assert not hasattr(runner.context, "__dict__")


def test_runner_is_composition_only() -> None:
    runner = _runner()

    assert not isinstance(runner, AccountComponent)
    assert not isinstance(runner, MarketEventsComponent)
    assert not isinstance(runner, WiringComponent)


def test_context_rejects_unknown_runtime_fields() -> None:
    context = RuntimeContext()

    with pytest.raises(AttributeError):
        context.dynamic_runtime_dependency = object()


def test_components_only_receive_declared_domain_dependencies() -> None:
    runner = _runner()

    assert not hasattr(runner.market_events, "_order_coordinator")
    assert not hasattr(runner.market_events, "_reconciliation_service")
    assert not hasattr(runner.account_runtime, "_market_queue")
    assert isinstance(runner.market_events.ports, MarketEventPorts)
    assert isinstance(runner.account_runtime.ports, AccountPorts)
    assert runner.market_events.ports is not runner.account_runtime.ports


def test_service_groups_are_independent_values() -> None:
    market = MarketRuntimeServices()
    account = AccountRuntimeServices()
    execution = ExecutionRuntimeServices()

    assert "_source" not in {item.name for item in fields(type(market))}
    assert "_source" not in {item.name for item in fields(type(account))}
    assert "_source" not in {item.name for item in fields(type(execution))}
    marker = object()
    market.kline_store = marker
    assert account.clients is None
    assert execution.clients is None
