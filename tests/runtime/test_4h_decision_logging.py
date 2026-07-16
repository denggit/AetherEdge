from __future__ import annotations

import ast
import inspect
import logging
from decimal import Decimal
from types import SimpleNamespace

import src.runtime.runner as runner_module
import src.runtime.strategy_diagnostics as diagnostics_module
from src.runtime.runner import LiveRuntimeRunner


def _runner(audit):
    class AuditStrategy:
        def decision_audit(self):
            return audit

    runner = object.__new__(LiveRuntimeRunner)
    runner.context = SimpleNamespace(strategy=AuditStrategy())
    runner.app_config = SimpleNamespace(symbol="ETH-USDT-PERP")
    runner._closed_bar_interval = "4h"
    runner._closed_bar_buffer_ms = 60_000
    return runner


def _kline():
    return SimpleNamespace(
        open_time_ms=100,
        close_time_ms=200,
        close=Decimal("2000"),
    )


def _audit(**overrides):
    audit = {
        "bar_open_time_ms": 100,
        "bar_close_time_ms": 200,
        "actions": [],
        "reason": "flat_route",
        "selected_engine": "NONE",
        "selected_side": "flat",
    }
    audit.update(overrides)
    return audit


def _messages(caplog):
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.INFO
    ]


def test_audit_without_engine_diag_text_only_prints_summary(caplog) -> None:
    caplog.set_level(logging.INFO)

    _runner(_audit())._log_4h_decision_summary(
        open_time_ms=100, closed_kline=_kline()
    )

    messages = _messages(caplog)
    assert sum("4H decision completed" in message for message in messages) == 1
    assert not any("4H engine diagnostics" in message for message in messages)


def test_audit_with_engine_diag_text_prints_one_multiline_detail(caplog) -> None:
    caplog.set_level(logging.INFO)
    text = "engine_diag:\n  momentum:\n    signal=0"

    _runner(_audit(engine_diag_text=text))._log_4h_decision_summary(
        open_time_ms=100, closed_kline=_kline()
    )

    messages = _messages(caplog)
    details = [
        message for message in messages if "4H engine diagnostics" in message
    ]
    assert len(details) == 1
    assert "symbol=ETH-USDT-PERP interval=4h" in details[0]
    assert text in details[0]


def test_no_audit_does_not_print_engine_diagnostics(caplog) -> None:
    caplog.set_level(logging.INFO)

    _runner(None)._log_4h_decision_summary(
        open_time_ms=100, closed_kline=_kline()
    )

    messages = _messages(caplog)
    assert any("decision=no_audit" in message for message in messages)
    assert not any("4H engine diagnostics" in message for message in messages)


def test_empty_engine_diag_text_does_not_print_detail(caplog) -> None:
    caplog.set_level(logging.INFO)

    _runner(_audit(engine_diag_text="  "))._log_4h_decision_summary(
        open_time_ms=100, closed_kline=_kline()
    )

    assert not any(
        "4H engine diagnostics" in message for message in _messages(caplog)
    )


def test_runtime_has_generic_hook_without_portfolio_v1_import() -> None:
    source = inspect.getsource(runner_module)
    tree = ast.parse(source)
    imported_modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }

    assert not any(
        module.startswith("strategies.eth_portfolio_v1")
        for module in imported_modules
    )
    diagnostics_source = inspect.getsource(diagnostics_module)
    assert source.count("4H engine diagnostics") == 0
    assert diagnostics_source.count("4H engine diagnostics") == 1
    assert "log_closed_bar_decision" in inspect.getsource(
        LiveRuntimeRunner._log_4h_decision_summary
    )
