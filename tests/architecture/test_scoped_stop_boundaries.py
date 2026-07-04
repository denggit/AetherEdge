from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STOPS_ROOT = PROJECT_ROOT / "src" / "order_management" / "stops"
FORBIDDEN_IMPORT_PREFIXES = (
    "strategies",
    "src.runtime",
    "src.platform.exchanges.okx",
    "src.platform.exchanges.binance",
)
FORBIDDEN_ENDPOINT_FRAGMENTS = (
    "/api/v5",
    "fapi",
    "dapi",
    "api/v3",
)


def test_stop_scope_has_no_strategy_runtime_or_raw_adapter_dependency() -> None:
    imports = _imports(STOPS_ROOT / "stop_scope.py")

    assert not any(
        imported == prefix or imported.startswith(f"{prefix}.")
        for imported in imports
        for prefix in FORBIDDEN_IMPORT_PREFIXES
    )


def test_order_management_stops_has_no_exchange_endpoint_strings() -> None:
    sources = "\n".join(
        path.read_text(encoding="utf-8").lower()
        for path in STOPS_ROOT.rglob("*.py")
    )

    for fragment in FORBIDDEN_ENDPOINT_FRAGMENTS:
        assert fragment not in sources


def test_scoped_stop_builder_stays_outside_strategy_and_runtime_layers() -> None:
    imports = _imports(STOPS_ROOT / "stop_replace.py")

    assert "src.order_management.stops.stop_scope" in imports
    assert not any(
        imported == prefix or imported.startswith(f"{prefix}.")
        for imported in imports
        for prefix in FORBIDDEN_IMPORT_PREFIXES
    )


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports
