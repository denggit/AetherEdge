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
from src.runtime.health_state import RuntimeHealthState
from src.runtime.models import RuntimeHealth, RuntimePhase
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeRunner


def _runner(
    *,
    services: dict | None = None,
    warmup_enabled: bool = True,
) -> LiveRuntimeRunner:
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
            warmup_enabled=warmup_enabled,
        ),
        services=injected,
    )


def test_initial_snapshot_identity_and_owned_field_are_preserved() -> None:
    initial = RuntimeHealth(
        phase=RuntimePhase.CREATED,
        metadata={"source": "initial"},
    )

    state = RuntimeHealthState(initial)

    assert state.current is initial
    assert vars(state) == {"_current": initial}


def test_update_with_none_preserves_values_and_copies_metadata() -> None:
    initial_metadata = {"source": "initial"}
    initial = RuntimeHealth(
        phase=RuntimePhase.ERROR,
        healthy=False,
        warmup_complete=True,
        caught_up=True,
        last_market_event_time_ms=123,
        error="existing error",
        metadata=initial_metadata,
    )
    state = RuntimeHealthState(initial)

    updated = state.update(RuntimePhase.RUNNING)

    assert updated is state.current
    assert updated is not initial
    assert updated == RuntimeHealth(
        phase=RuntimePhase.RUNNING,
        healthy=False,
        warmup_complete=True,
        caught_up=True,
        last_market_event_time_ms=123,
        error="existing error",
        metadata={"source": "initial"},
    )
    assert updated.metadata is not initial.metadata
    assert initial.metadata is initial_metadata
    assert initial.phase is RuntimePhase.ERROR
    assert initial.error == "existing error"


def test_update_replaces_all_values_and_metadata_without_merging() -> None:
    initial = RuntimeHealth(
        phase=RuntimePhase.CREATED,
        healthy=True,
        metadata={"old": "value", "shared": "old"},
    )
    replacement_metadata = {"shared": "new", "new": "value"}
    state = RuntimeHealthState(initial)

    updated = state.update(
        RuntimePhase.CATCHING_UP,
        healthy=False,
        warmup_complete=True,
        caught_up=True,
        last_market_event_time_ms=456,
        error="new error",
        metadata=replacement_metadata,
    )

    assert updated is state.current
    assert updated is not initial
    assert updated == RuntimeHealth(
        phase=RuntimePhase.CATCHING_UP,
        healthy=False,
        warmup_complete=True,
        caught_up=True,
        last_market_event_time_ms=456,
        error="new error",
        metadata={"shared": "new", "new": "value"},
    )
    assert "old" not in updated.metadata
    assert updated.metadata is not replacement_metadata

    replacement_metadata["new"] = "mutated"
    assert updated.metadata["new"] == "value"
    assert initial == RuntimeHealth(
        phase=RuntimePhase.CREATED,
        healthy=True,
        metadata={"old": "value", "shared": "old"},
    )


@pytest.mark.parametrize("warmup_enabled", (True, False))
def test_runner_creates_one_default_state_with_exact_initial_snapshot(
    monkeypatch,
    warmup_enabled: bool,
) -> None:
    current = object()
    state = SimpleNamespace(current=current, update=Mock())
    state_factory = Mock(return_value=state)
    heartbeat = Mock()
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeHealthState",
        state_factory,
    )
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeHeartbeatService",
        Mock(return_value=heartbeat),
    )

    runner = _runner(warmup_enabled=warmup_enabled)

    state_factory.assert_called_once_with(
        RuntimeHealth(
            phase=RuntimePhase.CREATED,
            warmup_complete=not warmup_enabled,
            caught_up=not warmup_enabled,
            metadata={
                "runtime_mode": RuntimeMode.LIVE_RUNTIME.value,
                "strategy": "tests.fake:Strategy",
            },
        )
    )
    assert runner._runtime_health_state is state
    assert runner.services["runtime_health_state"] is state
    assert runner._health is current
    state.update.assert_not_called()
    heartbeat.start.assert_not_called()


def test_injected_state_has_priority_and_is_not_updated_during_construction(
    monkeypatch,
) -> None:
    current = RuntimeHealth(
        phase=RuntimePhase.ERROR,
        healthy=False,
        error="injected",
    )
    state = SimpleNamespace(current=current, update=Mock())
    default_factory = Mock()
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeHealthState",
        default_factory,
    )

    runner = _runner(services={"runtime_health_state": state})

    default_factory.assert_not_called()
    state.update.assert_not_called()
    assert runner._runtime_health_state is state
    assert runner.services["runtime_health_state"] is state
    assert runner._health is state.current


def test_set_health_delegates_once_and_synchronizes_compatibility_field() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    previous = RuntimeHealth(phase=RuntimePhase.CREATED)
    updated = RuntimeHealth(
        phase=RuntimePhase.RUNNING,
        healthy=False,
        warmup_complete=True,
        caught_up=True,
        last_market_event_time_ms=789,
        error="delegated",
        metadata={"source": "delegate"},
    )
    state = SimpleNamespace(update=Mock(return_value=updated))
    metadata = {"source": "delegate"}
    runner._runtime_health_state = state
    runner._health = previous

    result = runner._set_health(
        RuntimePhase.RUNNING,
        healthy=False,
        warmup_complete=True,
        caught_up=True,
        last_market_event_time_ms=789,
        error="delegated",
        metadata=metadata,
    )

    assert result is None
    state.update.assert_called_once_with(
        RuntimePhase.RUNNING,
        healthy=False,
        warmup_complete=True,
        caught_up=True,
        last_market_event_time_ms=789,
        error="delegated",
        metadata=metadata,
    )
    assert runner._health is updated


@pytest.mark.asyncio
async def test_health_returns_current_compatibility_field() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    snapshot = RuntimeHealth(phase=RuntimePhase.CATCHING_UP)
    runner._health = snapshot

    assert await runner.health() is snapshot


@pytest.mark.asyncio
async def test_start_and_stop_return_the_final_compatibility_snapshots() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner._health = RuntimeHealth(phase=RuntimePhase.CREATED)
    runner._stop_event = SimpleNamespace(set=Mock())

    def update(phase, **kwargs):
        return RuntimeHealth(
            phase=phase,
            healthy=kwargs.get("healthy", True),
            warmup_complete=kwargs.get("warmup_complete", False),
            caught_up=kwargs.get("caught_up", False),
        )

    runner._runtime_health_state = SimpleNamespace(update=Mock(side_effect=update))

    async def no_op() -> None:
        return None

    runner._stop_market_data_modules = no_op
    runner._stop_producers = no_op
    runner._stop_live_persistence_writer = no_op

    running = await runner.start()
    stopped = await runner.stop()

    assert running is not stopped
    assert running.phase is RuntimePhase.RUNNING
    assert running.warmup_complete is True
    assert running.caught_up is True
    assert stopped is runner._health
    assert stopped.phase is RuntimePhase.STOPPED
    assert stopped.healthy is True
    runner._stop_event.set.assert_called_once_with()
