from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from src.runtime.module import CapabilityId, RuntimeModule


ModuleFactory = Callable[[], RuntimeModule]


@dataclass(frozen=True)
class ModuleDefinition:
    module_id: str
    provides: frozenset[CapabilityId]
    requires: frozenset[CapabilityId]
    factory: ModuleFactory

    def __post_init__(self) -> None:
        module_id = self.module_id.strip().lower()
        if not module_id:
            raise ValueError("module id must be non-empty")
        if not self.provides:
            raise ValueError(f"module must provide a capability: {module_id}")
        object.__setattr__(self, "module_id", module_id)


@dataclass(frozen=True)
class RuntimePlan:
    requested: frozenset[CapabilityId]
    resolved: frozenset[CapabilityId]
    module_ids: tuple[str, ...]
    skipped_module_ids: tuple[str, ...]
    shared_capabilities: frozenset[CapabilityId]


class CapabilityResolutionError(RuntimeError):
    pass


class CapabilityRegistry:
    """Map each public capability to exactly one provider module."""

    def __init__(self) -> None:
        self._providers: dict[CapabilityId, str] = {}

    def register(self, definition: ModuleDefinition) -> None:
        for capability in definition.provides:
            existing = self._providers.get(capability)
            if existing is not None and existing != definition.module_id:
                raise CapabilityResolutionError(
                    "duplicate capability provider | "
                    f"capability={capability} | "
                    f"providers={existing},{definition.module_id}"
                )
            self._providers[capability] = definition.module_id

    def provider_for(self, capability: CapabilityId) -> str:
        try:
            return self._providers[capability]
        except KeyError as exc:
            raise CapabilityResolutionError(
                f"no module provides capability: {capability}"
            ) from exc

    @property
    def capabilities(self) -> frozenset[CapabilityId]:
        return frozenset(self._providers)


class ModuleRegistry:
    """Store lazy definitions and instantiate only the resolved plan."""

    def __init__(self, capabilities: CapabilityRegistry | None = None) -> None:
        self.capabilities = capabilities or CapabilityRegistry()
        self._definitions: dict[str, ModuleDefinition] = {}

    def register(self, definition: ModuleDefinition) -> None:
        if definition.module_id in self._definitions:
            raise CapabilityResolutionError(
                f"duplicate module id: {definition.module_id}"
            )
        self.capabilities.register(definition)
        self._definitions[definition.module_id] = definition

    def definition(self, module_id: str) -> ModuleDefinition:
        try:
            return self._definitions[module_id]
        except KeyError as exc:
            raise CapabilityResolutionError(
                f"unknown module id: {module_id}"
            ) from exc

    @property
    def module_ids(self) -> tuple[str, ...]:
        return tuple(self._definitions)

    def instantiate(self, plan: RuntimePlan) -> tuple[RuntimeModule, ...]:
        instances: list[RuntimeModule] = []
        identities: set[int] = set()
        for module_id in plan.module_ids:
            definition = self.definition(module_id)
            module = definition.factory()
            if id(module) in identities:
                raise CapabilityResolutionError(
                    f"module factory reused an instance: {module_id}"
                )
            identities.add(id(module))
            if module.module_id != module_id:
                raise CapabilityResolutionError(
                    "module factory identity mismatch | "
                    f"registered={module_id} actual={module.module_id}"
                )
            if module.provides != definition.provides:
                raise CapabilityResolutionError(
                    f"module provides mismatch: {module_id}"
                )
            if module.requires != definition.requires:
                raise CapabilityResolutionError(
                    f"module requirements mismatch: {module_id}"
                )
            instances.append(module)
        return tuple(instances)


class DependencyResolver:
    def __init__(self, registry: ModuleRegistry) -> None:
        self.registry = registry

    def resolve(
        self,
        requested: Iterable[CapabilityId | str],
    ) -> RuntimePlan:
        requested_ids = frozenset(_capability(value) for value in requested)
        ordered_modules: list[str] = []
        resolved_capabilities: set[CapabilityId] = set()
        visiting: list[str] = []
        visited: set[str] = set()
        dependency_use: Counter[CapabilityId] = Counter()

        def visit_capability(capability: CapabilityId) -> None:
            resolved_capabilities.add(capability)
            visit_module(self.registry.capabilities.provider_for(capability))

        def visit_module(module_id: str) -> None:
            if module_id in visited:
                return
            if module_id in visiting:
                cycle = " -> ".join((*visiting, module_id))
                raise CapabilityResolutionError(
                    f"module dependency cycle: {cycle}"
                )
            visiting.append(module_id)
            definition = self.registry.definition(module_id)
            for dependency in sorted(definition.requires):
                dependency_use[dependency] += 1
                visit_capability(dependency)
            visiting.pop()
            visited.add(module_id)
            resolved_capabilities.update(definition.provides)
            ordered_modules.append(module_id)

        for capability in sorted(requested_ids):
            dependency_use[capability] += 1
            visit_capability(capability)

        skipped = tuple(
            module_id
            for module_id in self.registry.module_ids
            if module_id not in visited
        )
        shared = frozenset(
            capability
            for capability, uses in dependency_use.items()
            if uses > 1
        )
        return RuntimePlan(
            requested=requested_ids,
            resolved=frozenset(resolved_capabilities),
            module_ids=tuple(ordered_modules),
            skipped_module_ids=skipped,
            shared_capabilities=shared,
        )


def _capability(value: CapabilityId | str) -> CapabilityId:
    return value if isinstance(value, CapabilityId) else CapabilityId(value)


__all__ = [
    "CapabilityRegistry",
    "CapabilityResolutionError",
    "DependencyResolver",
    "ModuleDefinition",
    "ModuleFactory",
    "ModuleRegistry",
    "RuntimePlan",
]
