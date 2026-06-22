from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_tool():
    path = Path("tools/exchange_connectivity_smoke.py")
    spec = importlib.util.spec_from_file_location("exchange_connectivity_smoke", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_smoke_tool_only_treats_expected_noop_errors_as_soft_ok():
    tool = _load_tool()

    assert tool._is_expected_noop_error(Exception("HTTP 400 from exchange API: {'code': -4059, 'msg': 'No need to change position side.'}"))
    assert tool._is_expected_noop_error(Exception("HTTP 400 from exchange API: {'code': -4046, 'msg': 'No need to change margin type.'}"))
    assert not tool._is_expected_noop_error(Exception("HTTP 401 from exchange API: {'msg': 'Invalid Sign', 'code': '50113'}"))
