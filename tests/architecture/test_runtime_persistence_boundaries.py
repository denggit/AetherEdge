from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
PERSISTENCE = SOURCE_ROOT / "runtime" / "persistence.py"
RUNNER = SOURCE_ROOT / "runtime" / "runner.py"


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _imports(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_background_write_classes_have_one_definition() -> None:
    definitions = {"BackgroundWriteItem": [], "BackgroundWriteQueue": []}
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.ClassDef) and node.name in definitions:
                definitions[node.name].append(path.relative_to(PROJECT_ROOT).as_posix())

    assert definitions == {
        "BackgroundWriteItem": ["src/runtime/persistence.py"],
        "BackgroundWriteQueue": ["src/runtime/persistence.py"],
    }


def test_runner_keeps_aliases_without_private_class_definitions() -> None:
    tree = _tree(RUNNER)
    aliases = {
        target.id: node.value.id
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Name)
        for target in node.targets
        if target.id in {"_BackgroundWriteItem", "_BackgroundWriteQueue"}
    }

    assert aliases == {
        "_BackgroundWriteItem": "BackgroundWriteItem",
        "_BackgroundWriteQueue": "BackgroundWriteQueue",
    }
    assert not any(
        isinstance(node, ast.ClassDef)
        and node.name in {"_BackgroundWriteItem", "_BackgroundWriteQueue"}
        for node in ast.walk(tree)
    )


def test_persistence_module_has_only_generic_dependencies() -> None:
    imports = _imports(PERSISTENCE)
    allowed = {
        "__future__",
        "queue",
        "threading",
        "dataclasses",
        "typing",
        "collections.abc",
        "src.utils.log",
    }

    assert imports <= allowed
    assert "asyncio" not in imports


def test_persistence_module_has_no_runtime_or_business_vocabulary() -> None:
    tree = _tree(PERSISTENCE)
    forbidden_names = {
        "AppAlert",
        "AppContext",
        "RangeCheckpointWriter",
        "RangeRepairJournalWriter",
        "SQLite",
        "Sqlite",
        "_execute_signals",
        "on_start",
        "on_kline",
        "on_ticker",
        "on_trade",
        "on_order_book",
        "on_account_event",
        "on_account_snapshot",
        "on_order_results",
        "on_market_feature",
        "recover",
    }
    used_names = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id in forbidden_names
    }
    used_names.update(
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr in forbidden_names
    )
    forbidden_text = (
        "database path",
        "exchange",
        "order journal",
        "position plan",
        "strategy callback",
    )
    string_violations = {
        text
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        for text in forbidden_text
        if text in node.value.lower()
    }

    assert used_names == set()
    assert string_violations == set()


def test_runtime_persistence_wrappers_remain_in_runner() -> None:
    runner_class = next(
        node
        for node in _tree(RUNNER).body
        if isinstance(node, ast.ClassDef) and node.name == "LiveRuntimeRunner"
    )
    methods = {
        node.name
        for node in runner_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    assert {
        "_get_live_persistence_writer",
        "_submit_live_persistence_write",
        "_stop_live_persistence_writer",
        "_emit_alert_threadsafe",
        "_maybe_log_live_data_path_stats",
    } <= methods


def test_independent_range_writers_were_not_moved() -> None:
    persistence_names = {
        node.id for node in ast.walk(_tree(PERSISTENCE)) if isinstance(node, ast.Name)
    }
    runner_names = {
        node.id for node in ast.walk(_tree(RUNNER)) if isinstance(node, ast.Name)
    }

    assert "RangeCheckpointWriter" not in persistence_names
    assert "RangeRepairJournalWriter" not in persistence_names
    assert "RangeCheckpointWriter" in runner_names
    assert "RangeRepairJournalWriter" in runner_names
