from __future__ import annotations

import asyncio
import csv
import json
from pathlib import Path

from tools.preflight_check_v10b import (
    EXPECTED_STRATEGY,
    PreflightReport,
    run_preflight,
    scan_plugin_boundary,
    structural_stop_self_test,
    write_json_report,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_default_without_bars_csv_passes_with_warnings() -> None:
    report = asyncio.run(
        run_preflight(
            environ={"AETHER_STRATEGY": EXPECTED_STRATEGY},
            repo_root=REPO_ROOT,
        )
    )

    assert report.result in {"PASS", "PASS_WITH_WARNINGS"}
    assert report.failures == []
    assert _check(report, "bars_csv_not_provided_skip_local_bar_check").status == "WARN"
    assert _check(report, "api_position_check_skipped").status == "WARN"


def test_aether_strategy_pointing_to_v10a_fails() -> None:
    report = asyncio.run(
        run_preflight(
            environ={
                "AETHER_STRATEGY": "strategies.eth_lf_portfolio_v10a:Strategy",
            },
            repo_root=REPO_ROOT,
        )
    )

    assert report.result == "FAIL"
    check = _check(report, "aether_strategy_env")
    assert check.status == "FAIL"
    assert "v10a" in check.message.lower()


def test_structural_stop_self_test_passes() -> None:
    assert structural_stop_self_test() == []


def test_twenty_bar_csv_fails(tmp_path: Path) -> None:
    bars = _write_bars_csv(tmp_path / "bars20.csv", count=20)

    report = asyncio.run(
        run_preflight(
            bars_csv=bars,
            environ={"AETHER_STRATEGY": EXPECTED_STRATEGY},
            repo_root=REPO_ROOT,
        )
    )

    check = _check(report, "local_closed_bars")
    assert check.status == "FAIL"
    assert "closed_bar_rows=20" in check.message
    assert report.result == "FAIL"


def test_twenty_one_bar_csv_passes_local_check(tmp_path: Path) -> None:
    bars = _write_bars_csv(tmp_path / "bars21.csv", count=21)

    report = asyncio.run(
        run_preflight(
            bars_csv=bars,
            environ={"AETHER_STRATEGY": EXPECTED_STRATEGY},
            repo_root=REPO_ROOT,
        )
    )

    check = _check(report, "local_closed_bars")
    assert check.status == "PASS"
    assert "closed_bar_rows=21" in check.message
    assert report.result != "FAIL"


def test_plugin_boundary_scanner_detects_forbidden_marker(tmp_path: Path) -> None:
    plugin = tmp_path / "strategies" / "eth_lf_portfolio_v10b"
    plugin.mkdir(parents=True)
    (plugin / "safe.py").write_text("VALUE = 'safe'\n", encoding="utf-8")
    (plugin / "bad.py").write_text("from backtest import forbidden\n", encoding="utf-8")

    violations = scan_plugin_boundary(plugin)

    assert len(violations) == 1
    assert "from backtest" in violations[0]
    assert "bad.py" in violations[0]


def test_json_output_is_written_with_required_fields(tmp_path: Path) -> None:
    report = asyncio.run(
        run_preflight(
            environ={"AETHER_STRATEGY": EXPECTED_STRATEGY},
            repo_root=REPO_ROOT,
        )
    )
    output = tmp_path / "nested" / "v10b_preflight.json"

    result = write_json_report(output, report)

    assert result == output
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["result"] == report.result
    assert payload["strategy_id"] == "eth_lf_portfolio_v10b_all_swing_structural_stop"
    assert payload["strategy_version"] == "V10B"
    assert payload["structural_stop"]["lookback_bars"] == 21
    assert isinstance(payload["checks"], list)
    assert isinstance(payload["warnings"], list)
    assert isinstance(payload["failures"], list)


def _write_bars_csv(path: Path, *, count: int) -> Path:
    rows = [
        {
            "close_time_ms": str(index * 4 * 60 * 60 * 1000),
            "high": "105",
            "low": "95",
            "close": "100",
            "timeframe": "4h",
            "exchange": "okx",
        }
        for index in range(1, count + 1)
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return path


def _check(report: PreflightReport, name: str):
    matches = [check for check in report.checks if check.name == name]
    assert len(matches) == 1
    return matches[0]
