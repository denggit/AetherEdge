from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "src" / "runtime"
RUNNER = RUNTIME / "runner.py"
COMPONENTS = RUNTIME / "components"
ORDER_COORDINATOR = ROOT / "src" / "order_management" / "coordinator"


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def test_runtime_orchestrator_meets_size_and_range_boundaries() -> None:
    source = RUNNER.read_text(encoding="utf-8")
    tree = _tree(RUNNER)
    runner = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "LiveRuntimeRunner"
    )

    assert len(source.splitlines()) <= 500
    assert runner.end_lineno - runner.lineno + 1 <= 250
    assert "range_" not in source.lower()
    assert "RangeBar" not in source
    assert "RangeBuilder" not in source


def test_runtime_components_are_focused_and_below_file_limit() -> None:
    oversized = {
        path.relative_to(ROOT).as_posix(): len(
            path.read_text(encoding="utf-8").splitlines()
        )
        for path in COMPONENTS.glob("*.py")
        if len(path.read_text(encoding="utf-8").splitlines()) > 800
    }
    assert oversized == {}


def test_core_runtime_uses_typed_services_not_string_locator() -> None:
    core_paths = (
        RUNNER,
        RUNTIME / "composition.py",
        *COMPONENTS.glob("*.py"),
    )
    violations = {}
    for path in core_paths:
        source = path.read_text(encoding="utf-8")
        found = [
            token
            for token in (
                "services.get(",
                "self.services[",
            )
            if token in source
        ]
        if found:
            violations[path.relative_to(ROOT).as_posix()] = found
    assert violations == {}
    wiring = (COMPONENTS / "wiring.py").read_text(encoding="utf-8")
    assert "services: RuntimeServicesInput" in wiring


def test_formal_entry_uses_only_live_composition_root() -> None:
    source = (ROOT / "scripts" / "run_live.py").read_text(encoding="utf-8")
    assert "compose_live_runtime(" in source
    assert "LiveRuntimeRunner(" not in source
    assert "build_app_context(" not in source


def test_order_coordinator_is_an_orchestrator_over_separate_responsibilities() -> None:
    service = ORDER_COORDINATOR / "service.py"
    tree = _tree(service)
    coordinator = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "MultiExchangeOrderCoordinator"
    )
    bases = {ast.unparse(base) for base in coordinator.bases}
    assert bases == {
        "OrderIntentPlanner",
        "MasterFollowerExecutor",
        "MultiExchangeExecutor",
        "OrderSafetyValidator",
        "ExecutionResultRecorder",
        "PositionPlanUpdater",
    }
    assert coordinator.end_lineno - coordinator.lineno + 1 <= 250
    assert {
        "OrderIntentPlanner": "intent_planner.py",
        "MasterFollowerExecutor": "master_follower_executor.py",
        "MultiExchangeExecutor": "multi_exchange_executor.py",
        "OrderSafetyValidator": "safety_validator.py",
        "ExecutionResultRecorder": "result_recorder.py",
        "PositionPlanUpdater": "position_plan_updater.py",
    } == {
        class_node.name: path.name
        for path in ORDER_COORDINATOR.glob("*.py")
        for class_node in _tree(path).body
        if isinstance(class_node, ast.ClassDef)
        and class_node.name
        in {
            "OrderIntentPlanner",
            "MasterFollowerExecutor",
            "MultiExchangeExecutor",
            "OrderSafetyValidator",
            "ExecutionResultRecorder",
            "PositionPlanUpdater",
        }
    }


def test_no_refactor_production_file_exceeds_eight_hundred_lines() -> None:
    paths = (
        RUNNER,
        *COMPONENTS.glob("*.py"),
        *(RUNTIME / "market_data").glob("*.py"),
        *ORDER_COORDINATOR.glob("*.py"),
    )
    assert {
        path.relative_to(ROOT).as_posix(): len(
            path.read_text(encoding="utf-8").splitlines()
        )
        for path in paths
        if len(path.read_text(encoding="utf-8").splitlines()) > 800
    } == {}
