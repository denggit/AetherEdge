from __future__ import annotations

from src.runtime.services import RuntimeServices


class RuntimeComponent:
    """Domain component whose mutable state is owned by the orchestrator."""

    def __init__(self, owner: object) -> None:
        object.__setattr__(self, "_owner", owner)

    def __getattribute__(self, name: str):
        if name in {"_owner", "__class__"}:
            return object.__getattribute__(self, name)
        owner = object.__getattribute__(self, "_owner")
        if name == "__dict__":
            return object.__getattribute__(owner, "__dict__")
        owner_state = object.__getattribute__(owner, "__dict__")
        if name in owner_state:
            return owner_state[name]
        return object.__getattribute__(self, name)

    def __getattr__(self, name: str):
        return getattr(object.__getattribute__(self, "_owner"), name)

    def __setattr__(self, name: str, value: object) -> None:
        if name == "_owner":
            object.__setattr__(self, name, value)
            return
        descriptor = type(self).__dict__.get(name)
        if isinstance(descriptor, property) and descriptor.fset is not None:
            object.__setattr__(self, name, value)
            return
        setattr(object.__getattribute__(self, "_owner"), name, value)

    def service_dependencies(self) -> RuntimeServices:
        owner = object.__getattribute__(self, "_owner")
        services = owner.__dict__.get("runtime_services")
        if not isinstance(services, RuntimeServices):
            services = RuntimeServices.coerce(owner.__dict__.get("services"))
            object.__setattr__(owner, "runtime_services", services)
            object.__setattr__(owner, "services", services)
        return services
