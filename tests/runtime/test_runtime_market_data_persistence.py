from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.runtime.market_data_persistence import RuntimeMarketDataPersistence
from src.runtime.runner import LiveRuntimeRunner
from src.runtime import runner as runner_module


class _CapturingPersistenceService:
    def __init__(self, *, accepted: bool = True) -> None:
        self.accepted = accepted
        self.submissions: list[dict[str, object]] = []
        self.metrics_calls = 0

    def submit(self, **kwargs) -> bool:
        self.submissions.append(kwargs)
        return self.accepted

    def metrics(self):
        self.metrics_calls += 1
        return SimpleNamespace(pending_count=3, dropped=2)


class _SaveStore:
    def __init__(self) -> None:
        self.saved: list[list[object]] = []

    def save(self, rows) -> None:
        self.saved.append(rows)


class _AggregateStore:
    def __init__(self) -> None:
        self.saved: list[dict[str, object]] = []

    def save_completed_aggregate(self, **kwargs) -> None:
        self.saved.append(kwargs)


def _gateway(
    service: _CapturingPersistenceService,
    *,
    kline_provider,
    range_bar_provider,
    aggregate_provider,
    clock_ms=lambda: 123,
) -> RuntimeMarketDataPersistence:
    return RuntimeMarketDataPersistence(
        persistence_service=service,  # type: ignore[arg-type]
        kline_store_provider=kline_provider,
        range_bar_store_provider=range_bar_provider,
        completed_aggregate_store_provider=aggregate_provider,
        exchange="okx",
        clock_ms=clock_ms,
    )


def test_gateway_holds_only_explicit_dependencies() -> None:
    service = _CapturingPersistenceService()
    kline_provider = Mock()
    range_bar_provider = Mock()
    aggregate_provider = Mock()
    clock_ms = Mock()
    gateway = _gateway(
        service,
        kline_provider=kline_provider,
        range_bar_provider=range_bar_provider,
        aggregate_provider=aggregate_provider,
        clock_ms=clock_ms,
    )

    assert vars(gateway) == {
        "_persistence_service": service,
        "_kline_store_provider": kline_provider,
        "_range_bar_store_provider": range_bar_provider,
        "_completed_aggregate_store_provider": aggregate_provider,
        "_exchange": "okx",
        "_clock_ms": clock_ms,
    }


@pytest.mark.parametrize(
    ("method_name", "description", "provider_name"),
    (
        ("persist_closed_kline", "closed_kline", "kline"),
        ("persist_range_bar", "range_bar", "range_bar"),
    ),
)
@pytest.mark.parametrize("accepted", (True, False))
def test_simple_writes_are_lazy_and_preserve_identity(
    method_name: str,
    description: str,
    provider_name: str,
    accepted: bool,
) -> None:
    service = _CapturingPersistenceService(accepted=accepted)
    stores = {"kline": _SaveStore(), "range_bar": _SaveStore()}
    provider_calls = {"kline": 0, "range_bar": 0, "aggregate": 0}

    def provider(name):
        def get_store():
            provider_calls[name] += 1
            return stores[name]

        return get_store

    gateway = _gateway(
        service,
        kline_provider=provider("kline"),
        range_bar_provider=provider("range_bar"),
        aggregate_provider=provider("aggregate"),
    )
    model = object()
    on_error = Mock()

    result = getattr(gateway, method_name)(model, on_error=on_error)

    assert result is accepted
    assert provider_calls == {"kline": 0, "range_bar": 0, "aggregate": 0}
    assert len(service.submissions) == 1
    submission = service.submissions[0]
    assert submission["description"] == description
    assert submission["on_error"] is on_error

    submission["write"]()  # type: ignore[operator]

    assert provider_calls[provider_name] == 1
    assert stores[provider_name].saved == [[model]]
    assert stores[provider_name].saved[0][0] is model
    assert isinstance(stores[provider_name].saved[0], list)


@pytest.mark.parametrize("accepted", (True, False))
def test_completed_aggregate_write_is_lazy_and_uses_execution_time_clock(
    accepted: bool,
) -> None:
    service = _CapturingPersistenceService(accepted=accepted)
    store = _AggregateStore()
    provider = Mock(return_value=store)
    clock_ms = Mock(return_value=987654321)
    gateway = _gateway(
        service,
        kline_provider=Mock(),
        range_bar_provider=Mock(),
        aggregate_provider=provider,
        clock_ms=clock_ms,
    )
    aggregate = object()
    on_error = Mock()

    result = gateway.persist_completed_range_aggregate(
        aggregate,  # type: ignore[arg-type]
        coverage_status="partial",
        missing_gap_ms=456,
        on_error=on_error,
    )

    assert result is accepted
    provider.assert_not_called()
    clock_ms.assert_not_called()
    submission = service.submissions[0]
    assert submission["description"] == "completed_range_aggregate"
    assert submission["on_error"] is on_error

    submission["write"]()  # type: ignore[operator]

    provider.assert_called_once_with()
    clock_ms.assert_called_once_with()
    assert store.saved == [
        {
            "exchange": "okx",
            "aggregate": aggregate,
            "coverage_status": "partial",
            "missing_gap_ms": 456,
            "completed_at_ms": 987654321,
        }
    ]
    assert store.saved[0]["aggregate"] is aggregate


@pytest.mark.parametrize(
    ("method_name", "description"),
    (
        ("persist_closed_kline", "closed_kline"),
        ("persist_range_bar", "range_bar"),
        (
            "persist_completed_range_aggregate",
            "completed_range_aggregate",
        ),
    ),
)
@pytest.mark.parametrize("accepted", (True, False))
def test_gateway_reports_rejection_once_without_triggering_lazy_dependencies(
    method_name: str,
    description: str,
    accepted: bool,
) -> None:
    service = _CapturingPersistenceService(accepted=accepted)
    kline_provider = Mock()
    range_bar_provider = Mock()
    aggregate_provider = Mock()
    clock_ms = Mock()
    gateway = _gateway(
        service,
        kline_provider=kline_provider,
        range_bar_provider=range_bar_provider,
        aggregate_provider=aggregate_provider,
        clock_ms=clock_ms,
    )
    on_rejected = Mock()
    kwargs = {"on_error": Mock(), "on_rejected": on_rejected}
    if method_name == "persist_completed_range_aggregate":
        kwargs.update(coverage_status="complete", missing_gap_ms=0)

    result = getattr(gateway, method_name)(object(), **kwargs)

    assert result is accepted
    if accepted:
        on_rejected.assert_not_called()
    else:
        on_rejected.assert_called_once_with(description)
        assert (
            on_rejected.call_args.args[0]
            is service.submissions[0]["description"]
        )
    kline_provider.assert_not_called()
    range_bar_provider.assert_not_called()
    aggregate_provider.assert_not_called()
    clock_ms.assert_not_called()


def test_gateway_does_not_swallow_rejection_callback_errors() -> None:
    service = _CapturingPersistenceService(accepted=False)
    provider = Mock()
    clock_ms = Mock()
    gateway = _gateway(
        service,
        kline_provider=provider,
        range_bar_provider=provider,
        aggregate_provider=provider,
        clock_ms=clock_ms,
    )
    error = RuntimeError("rejection handler failed")

    def fail_on_rejected(description: str) -> None:
        raise error

    with pytest.raises(RuntimeError) as raised:
        gateway.persist_closed_kline(
            object(),  # type: ignore[arg-type]
            on_error=Mock(),
            on_rejected=fail_on_rejected,
        )

    assert raised.value is error
    provider.assert_not_called()
    clock_ms.assert_not_called()


@pytest.mark.parametrize(
    "method_name",
    (
        "persist_closed_kline",
        "persist_range_bar",
        "persist_completed_range_aggregate",
    ),
)
def test_provider_errors_are_deferred_and_propagate_from_write(
    method_name: str,
) -> None:
    service = _CapturingPersistenceService()
    error = RuntimeError("store unavailable")

    def failing_provider():
        raise error

    gateway = _gateway(
        service,
        kline_provider=failing_provider,
        range_bar_provider=failing_provider,
        aggregate_provider=failing_provider,
    )
    kwargs = {"on_error": Mock()}
    if method_name == "persist_completed_range_aggregate":
        kwargs.update(coverage_status="missing", missing_gap_ms=1)

    getattr(gateway, method_name)(object(), **kwargs)

    with pytest.raises(RuntimeError) as raised:
        service.submissions[0]["write"]()  # type: ignore[operator]
    assert raised.value is error


class _InjectedGateway:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    def persist_closed_kline(self, *args, **kwargs) -> bool:
        self.calls.append(("closed", args, kwargs))
        return False

    def persist_range_bar(self, *args, **kwargs) -> bool:
        self.calls.append(("range", args, kwargs))
        return False

    def persist_completed_range_aggregate(self, *args, **kwargs) -> bool:
        self.calls.append(("aggregate", args, kwargs))
        return False


def test_runner_wrappers_use_one_injected_gateway_and_keep_none_return() -> None:
    gateway = _InjectedGateway()
    runner = object.__new__(LiveRuntimeRunner)
    runner.services = {"market_data_persistence": gateway}
    runner._market_data_persistence = None
    handled: list[tuple[str, object, BaseException]] = []
    runner._on_closed_kline_persist_error = (
        lambda model, exc: handled.append(("closed", model, exc))
    )
    runner._on_range_bar_persist_error = (
        lambda model, exc: handled.append(("range", model, exc))
    )
    runner._on_completed_range_aggregate_persist_error = (
        lambda model, exc: handled.append(("aggregate", model, exc))
    )
    rejected: list[str] = []
    runner._on_live_persistence_write_rejected = rejected.append
    kline = object()
    bar = object()
    aggregate = object()

    assert runner._persist_closed_kline(kline) is None  # type: ignore[arg-type]
    assert runner._persist_range_bar(bar) is None  # type: ignore[arg-type]
    assert (
        runner._persist_completed_range_aggregate(  # type: ignore[arg-type]
            aggregate,
            coverage_status="complete",
            missing_gap_ms=789,
        )
        is None
    )

    assert [call[0] for call in gateway.calls] == ["closed", "range", "aggregate"]
    assert [call[1][0] for call in gateway.calls] == [kline, bar, aggregate]
    assert gateway.calls[2][2]["coverage_status"] == "complete"
    assert gateway.calls[2][2]["missing_gap_ms"] == 789
    assert all(
        call[2]["on_rejected"] is runner._on_live_persistence_write_rejected
        for call in gateway.calls
    )
    error = RuntimeError("write failed")
    for _, args, kwargs in gateway.calls:
        kwargs["on_error"](error)  # type: ignore[operator]
        assert handled[-1][1] is args[0]
        assert handled[-1][2] is error


def test_runner_logs_one_exact_warning_for_each_gateway_rejection(
    monkeypatch,
) -> None:
    service = _CapturingPersistenceService(accepted=False)
    kline_provider = Mock()
    range_bar_provider = Mock()
    aggregate_provider = Mock()
    clock_ms = Mock()
    gateway = _gateway(
        service,
        kline_provider=kline_provider,
        range_bar_provider=range_bar_provider,
        aggregate_provider=aggregate_provider,
        clock_ms=clock_ms,
    )
    runner = object.__new__(LiveRuntimeRunner)
    runner.services = {"runtime_persistence_service": service}
    runner._runtime_persistence_service = service
    runner._market_data_persistence = gateway
    rendered_warnings: list[str] = []

    def capture_warning(message: str, *args) -> None:
        rendered_warnings.append(message % args)

    monkeypatch.setattr(runner_module.logger, "warning", capture_warning)

    assert runner._persist_closed_kline(object()) is None  # type: ignore[arg-type]
    assert runner._persist_range_bar(object()) is None  # type: ignore[arg-type]
    assert (
        runner._persist_completed_range_aggregate(  # type: ignore[arg-type]
            object(),
            coverage_status="partial",
            missing_gap_ms=12,
        )
        is None
    )

    assert rendered_warnings == [
        "Live persistence write dropped | description=closed_kline pending=3 dropped=2",
        "Live persistence write dropped | description=range_bar pending=3 dropped=2",
        "Live persistence write dropped | description=completed_range_aggregate pending=3 dropped=2",
    ]
    assert service.metrics_calls == 3
    kline_provider.assert_not_called()
    range_bar_provider.assert_not_called()
    aggregate_provider.assert_not_called()
    clock_ms.assert_not_called()


@pytest.mark.asyncio
async def test_runner_gateway_delegate_keeps_alert_loop_for_worker_errors() -> None:
    gateway = _InjectedGateway()
    runner = object.__new__(LiveRuntimeRunner)
    runner.services = {"market_data_persistence": gateway}
    runner._market_data_persistence = gateway
    runner._on_closed_kline_persist_error = lambda model, exc: None

    runner._persist_closed_kline(object())  # type: ignore[arg-type]

    assert runner._persistence_alert_loop is asyncio.get_running_loop()


def test_runner_default_gateway_is_created_and_cached_once() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    service = _CapturingPersistenceService()
    runner.services = {}
    runner._runtime_persistence_service = service
    runner._market_data_persistence = None
    runner.app_config = SimpleNamespace(
        data_exchange=SimpleNamespace(value="okx")
    )

    first = runner._get_market_data_persistence()
    second = runner._get_market_data_persistence()

    assert first is second
    assert runner.services["market_data_persistence"] is first
    assert vars(first)["_persistence_service"] is service


def test_runner_live_kline_store_prefers_injected_store() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    store = object()
    runner.services = {"kline_store": store}

    assert runner._get_live_kline_store() is store
    assert runner._get_live_kline_store() is store


def test_runner_live_kline_store_uses_configured_path_and_caches(
    monkeypatch,
    tmp_path,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    path = tmp_path / "market.sqlite3"
    store = object()
    factory = Mock(return_value=store)
    monkeypatch.setattr(
        "src.runtime.components.range_runtime.SqliteKlineStore",
        factory,
    )
    runner.services = {}
    runner.range_config = SimpleNamespace(market_data_db_path=path)

    assert runner._get_live_kline_store() is store
    assert runner._get_live_kline_store() is store
    factory.assert_called_once_with(path)
    assert runner.services["kline_store"] is store


def test_runner_persistence_error_handlers_keep_messages_and_severity(
    monkeypatch,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    alerts = []
    runner._emit_alert_threadsafe = alerts.append
    exception_log = Mock()
    warning_log = Mock()
    monkeypatch.setattr(runner_module.logger, "exception", exception_log)
    monkeypatch.setattr(runner_module.logger, "warning", warning_log)
    error = RuntimeError("disk unavailable")
    kline = SimpleNamespace(
        symbol="ETH-USDT-PERP",
        interval="4h",
        open_time_ms=100,
        close_time_ms=200,
    )
    bar = SimpleNamespace(
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bar_id=3,
        start_time_ms=100,
        end_time_ms=200,
    )
    aggregate = SimpleNamespace(
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=100,
    )

    runner._on_closed_kline_persist_error(kline, error)  # type: ignore[arg-type]
    runner._on_range_bar_persist_error(bar, error)  # type: ignore[arg-type]
    runner._on_completed_range_aggregate_persist_error(  # type: ignore[arg-type]
        aggregate,
        error,
    )

    assert [(alert.subject, alert.severity) for alert in alerts] == [
        ("AetherEdge closed kline persistence failed", "error"),
        ("AetherEdge range bar persistence failed", "warning"),
    ]
    assert "error=RuntimeError:disk unavailable" in alerts[0].content
    assert "error=RuntimeError:disk unavailable" in alerts[1].content
    assert exception_log.call_count == 2
    warning_log.assert_called_once_with(
        "Failed to persist completed range aggregate | symbol=%s range_pct=%s bucket_start_ms=%s error=%s",
        "ETH-USDT-PERP",
        "0.002",
        100,
        error,
    )
