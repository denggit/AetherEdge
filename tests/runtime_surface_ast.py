from __future__ import annotations

import ast
import copy
from pathlib import Path


def runtime_component_paths(source_root: Path) -> tuple[Path, ...]:
    runtime_root = source_root / "runtime"
    return (
        runtime_root / "runner.py",
        *sorted((runtime_root / "components").glob("*.py")),
    )


def runtime_surface_class(source_root: Path) -> ast.ClassDef:
    """Return the logical Runner surface assembled from its owning components."""

    paths = runtime_component_paths(source_root)
    runner_tree = _tree(paths[0])
    runner = copy.deepcopy(
        next(
            node
            for node in runner_tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "LiveRuntimeRunner"
        )
    )
    methods = {
        node.name: node
        for node in runner.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    for path in paths[1:]:
        for class_node in (
            node for node in _tree(path).body if isinstance(node, ast.ClassDef)
        ):
            if not class_node.name.endswith("Component"):
                continue
            wiring_initializer_helpers: set[str] = set()
            if class_node.name == "WiringComponent":
                initialize = next(
                    method
                    for method in class_node.body
                    if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and method.name == "initialize"
                )
                wiring_initializer_helpers = {
                    call.func.attr
                    for call in ast.walk(initialize)
                    if isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute)
                    and isinstance(call.func.value, ast.Name)
                    and call.func.value.id == "self"
                }
            for method in class_node.body:
                if not isinstance(
                    method, (ast.FunctionDef, ast.AsyncFunctionDef)
                ):
                    continue
                if method.name in wiring_initializer_helpers:
                    # These implementation helpers are already flattened into
                    # the logical Runner constructor below.  Keeping them as
                    # additional logical methods would double-count factories.
                    continue
                copied = copy.deepcopy(method)
                if class_node.name == "WiringComponent" and copied.name == "initialize":
                    initializer = methods["__init__"]
                    wiring_methods = {
                        candidate.name: candidate
                        for candidate in class_node.body
                        if isinstance(
                            candidate,
                            (ast.FunctionDef, ast.AsyncFunctionDef),
                        )
                    }
                    for statement in copied.body:
                        call = (
                            statement.value
                            if isinstance(statement, ast.Expr)
                            and isinstance(statement.value, ast.Call)
                            else None
                        )
                        helper_name = (
                            call.func.attr
                            if isinstance(call, ast.Call)
                            and isinstance(call.func, ast.Attribute)
                            and isinstance(call.func.value, ast.Name)
                            and call.func.value.id == "self"
                            else None
                        )
                        if helper_name in wiring_methods:
                            initializer.body.extend(
                                copy.deepcopy(wiring_methods[helper_name].body)
                            )
                        else:
                            initializer.body.append(statement)
                    continue
                methods.setdefault(copied.name, copied)
    startup_catchup = methods["_evaluate_startup_catchup_once"]
    for helper_name in (
        "_startup_catchup_window",
        "_complete_startup_catchup",
    ):
        startup_catchup.body.extend(
            copy.deepcopy(methods.pop(helper_name).body)
        )
    for owner_name, helper_name in {
        "_validate_recovery_protection_postcondition": (
            "_validate_snapshot_recovery_protection"
        ),
        "_validate_post_execution_stop_protection": (
            "_validate_exchange_post_execution_protection"
        ),
        "_verify_stop_order_results": "_resolve_stop_check_position",
    }.items():
        # Architecture characterization treats these component-owned helpers
        # as part of the public safety path they implement.
        methods[owner_name].body.extend(
            copy.deepcopy(methods[helper_name].body)
        )
    for public_name, implementation_name in {
        "process_market_event": "_process_market_event",
        "process_market_feature": "_process_market_feature_event",
        "_startup": "_run_startup_sequence",
    }.items():
        implementation = copy.deepcopy(methods[implementation_name])
        implementation.name = public_name
        methods[public_name] = implementation
        methods.pop(implementation_name, None)
    runner.body = [
        *(
            node
            for node in runner.body
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ),
        *methods.values(),
    ]
    ast.fix_missing_locations(runner)
    return runner


def runtime_component_method_owners(source_root: Path) -> dict[str, Path]:
    owners: dict[str, Path] = {}
    for path in runtime_component_paths(source_root):
        for node in ast.walk(_tree(path)):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                owners.setdefault(node.name, path)
    return owners


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


__all__ = [
    "runtime_component_method_owners",
    "runtime_component_paths",
    "runtime_surface_class",
]
