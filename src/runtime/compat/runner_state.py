from __future__ import annotations

from src.runtime.context import RuntimeContext


class LegacyRunnerStateFacade:
    """Explicit state bridges retained for pre-composition integrations."""

    @property
    def runtime_state(self):
        return self.context.state

    def _component_state(self, name: str, fallback: str):
        component = self.__dict__.get(name)
        return component, self.__dict__.get(fallback)

    @property
    def _health(self):
        component, fallback = self._component_state("lifecycle", "_legacy_health")
        return component._health if component is not None else fallback

    @_health.setter
    def _health(self, value) -> None:
        component, _ = self._component_state("lifecycle", "_legacy_health")
        if component is None:
            self.__dict__["_legacy_health"] = value
        else:
            component._health = value

    @property
    def _account_config_new_entries_blocked(self) -> bool:
        component, fallback = self._component_state(
            "startup", "_legacy_account_config_new_entries_blocked"
        )
        return bool(
            component._account_config_new_entries_blocked
            if component is not None
            else fallback
        )

    @_account_config_new_entries_blocked.setter
    def _account_config_new_entries_blocked(self, value: bool) -> None:
        component, _ = self._component_state(
            "startup", "_legacy_account_config_new_entries_blocked"
        )
        if component is None:
            self.__dict__["_legacy_account_config_new_entries_blocked"] = bool(
                value
            )
        else:
            component._account_config_new_entries_blocked = bool(value)

    @property
    def _prefer_latest_state_event(self) -> bool:
        component, fallback = self._component_state(
            "market_events", "_legacy_prefer_latest_state_event"
        )
        return bool(
            component._prefer_latest_state_event
            if component is not None
            else fallback
        )

    @_prefer_latest_state_event.setter
    def _prefer_latest_state_event(self, value: bool) -> None:
        component, _ = self._component_state(
            "market_events", "_legacy_prefer_latest_state_event"
        )
        if component is None:
            self.__dict__["_legacy_prefer_latest_state_event"] = bool(value)
        else:
            component._prefer_latest_state_event = bool(value)

    @property
    def _market_queue_drain_batch_size(self) -> int:
        component, fallback = self._component_state(
            "market_events", "_legacy_market_queue_drain_batch_size"
        )
        raw = (
            component._market_queue_drain_batch_size
            if component is not None
            else fallback
        )
        return int(raw or 1)

    @_market_queue_drain_batch_size.setter
    def _market_queue_drain_batch_size(self, value: int) -> None:
        component, _ = self._component_state(
            "market_events", "_legacy_market_queue_drain_batch_size"
        )
        if component is None:
            self.__dict__["_legacy_market_queue_drain_batch_size"] = int(value)
        else:
            component._market_queue_drain_batch_size = int(value)

    @property
    def _range_module(self):
        component, fallback = self._component_state(
            "market_data_lifecycle", "_legacy_range_module"
        )
        return component._range_module if component is not None else fallback

    @_range_module.setter
    def _range_module(self, value) -> None:
        component, _ = self._component_state(
            "market_data_lifecycle", "_legacy_range_module"
        )
        if component is None:
            self.__dict__["_legacy_range_module"] = value
        else:
            component._range_module = value

    @property
    def _range_repair_journal(self):
        component, fallback = self._component_state(
            "market_events", "_legacy_range_repair_journal"
        )
        return (
            component._range_repair_journal
            if component is not None
            else fallback
        )

    @_range_repair_journal.setter
    def _range_repair_journal(self, value) -> None:
        component, _ = self._component_state(
            "market_events", "_legacy_range_repair_journal"
        )
        if component is None:
            self.__dict__["_legacy_range_repair_journal"] = value
        else:
            component._range_repair_journal = value

    @property
    def _market_data_runtime(self):
        context = self.__dict__.get("context")
        if isinstance(context, RuntimeContext):
            return context.state.market.runtime
        return self.__dict__.get("_legacy_market_data_runtime")

    @_market_data_runtime.setter
    def _market_data_runtime(self, value) -> None:
        context = self.__dict__.get("context")
        if isinstance(context, RuntimeContext):
            context.state.market.runtime = value
            for component_name in ("market_events", "market_data_lifecycle"):
                component = self.__dict__.get(component_name)
                if component is not None:
                    component._market_data_runtime = value
        else:
            self.__dict__["_legacy_market_data_runtime"] = value

    @property
    def _recovery_service(self):
        component, fallback = self._component_state(
            "recovery", "_legacy_recovery_service"
        )
        return component._recovery_service if component is not None else fallback

    @_recovery_service.setter
    def _recovery_service(self, value) -> None:
        component, _ = self._component_state(
            "recovery", "_legacy_recovery_service"
        )
        if component is None:
            self.__dict__["_legacy_recovery_service"] = value
        else:
            component._recovery_service = value

    def _shared_lifecycle_value(self, name: str, fallback: str):
        context = self.__dict__.get("context")
        if isinstance(context, RuntimeContext):
            return getattr(context.resources.lifecycle, name)
        return self.__dict__.get(fallback)

    def _set_shared_lifecycle_value(
        self,
        name: str,
        fallback: str,
        value,
    ) -> None:
        context = self.__dict__.get("context")
        if isinstance(context, RuntimeContext):
            setattr(context.resources.lifecycle, name, value)
        else:
            self.__dict__[fallback] = value

    @property
    def _last_snapshot(self):
        return self._shared_lifecycle_value(
            "last_snapshot", "_legacy_last_snapshot"
        )

    @_last_snapshot.setter
    def _last_snapshot(self, value) -> None:
        self._set_shared_lifecycle_value(
            "last_snapshot", "_legacy_last_snapshot", value
        )

    @property
    def _last_snapshots(self):
        return self._shared_lifecycle_value(
            "last_snapshots", "_legacy_last_snapshots"
        ) or ()

    @_last_snapshots.setter
    def _last_snapshots(self, value) -> None:
        self._set_shared_lifecycle_value(
            "last_snapshots", "_legacy_last_snapshots", tuple(value)
        )

    @property
    def _producer_tasks(self):
        component, fallback = self._component_state(
            "lifecycle", "_legacy_producer_tasks"
        )
        return component._producer_tasks if component is not None else fallback or []

    @_producer_tasks.setter
    def _producer_tasks(self, value) -> None:
        component, _ = self._component_state(
            "lifecycle", "_legacy_producer_tasks"
        )
        if component is None:
            self.__dict__["_legacy_producer_tasks"] = value
        else:
            component._producer_tasks = value

    @property
    def _sync_tasks(self):
        component, fallback = self._component_state(
            "lifecycle", "_legacy_sync_tasks"
        )
        return (
            component.__dict__.get("_sync_tasks", [])
            if component is not None
            else fallback or []
        )

    @_sync_tasks.setter
    def _sync_tasks(self, value) -> None:
        component, _ = self._component_state("lifecycle", "_legacy_sync_tasks")
        if component is None:
            self.__dict__["_legacy_sync_tasks"] = value
        else:
            component._sync_tasks = value


__all__ = ["LegacyRunnerStateFacade"]
