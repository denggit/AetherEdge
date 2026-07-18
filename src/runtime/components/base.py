from __future__ import annotations

from src.runtime.services import RuntimeServices


class RuntimeSharedState:
    """Explicit state context shared only where legacy components require it."""


class RuntimeComponent:
    """Domain component backed by an explicit state context, not Runner state."""

    def __init__(self, owner: object) -> None:
        object.__setattr__(self, "_owner", owner)
        object.__setattr__(self, "_state", owner._ensure_runtime_state())

    def __getattribute__(self, name: str):
        if name in {"_owner", "_state", "__class__", "__dict__"}:
            return object.__getattribute__(self, name)
        owner = object.__getattribute__(self, "_owner")
        override_reader = getattr(owner, "_runtime_component_override", None)
        if callable(override_reader):
            found, value = override_reader(name)
            if found:
                return value
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            state = object.__getattribute__(self, "_state")
            state_values = object.__getattribute__(state, "__dict__")
            if name in state_values:
                return state_values[name]
            return getattr(owner, name)

    def __getattr__(self, name: str):
        state = object.__getattribute__(self, "_state")
        state_values = object.__getattribute__(state, "__dict__")
        if name in state_values:
            return state_values[name]
        return getattr(object.__getattribute__(self, "_owner"), name)

    def __setattr__(self, name: str, value: object) -> None:
        if name in {"_owner", "_state"}:
            object.__setattr__(self, name, value)
            return
        descriptor = type(self).__dict__.get(name)
        if isinstance(descriptor, property) and descriptor.fset is not None:
            object.__setattr__(self, name, value)
            return
        setattr(object.__getattribute__(self, "_state"), name, value)

    def service_dependencies(self) -> RuntimeServices:
        state = object.__getattribute__(self, "_state")
        services = state.__dict__.get("runtime_services")
        if not isinstance(services, RuntimeServices):
            services = RuntimeServices.coerce(state.__dict__.get("services"))
            object.__setattr__(state, "runtime_services", services)
            object.__setattr__(state, "services", services)
        return services


__all__ = ["RuntimeComponent", "RuntimeSharedState"]
