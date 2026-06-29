from __future__ import annotations

import argparse
import csv
from pathlib import Path

from tools.check_v10b_structural_stop_parity import run


def test_parity_tool_writes_required_structural_columns(tmp_path: Path) -> None:
    bars_path = tmp_path / "bars.csv"
    output_path = tmp_path / "structural_stop_parity.csv"
    rows = [
        {
            "bar_close_time": str(index),
            "low": "95",
            "high": "110",
            "close": "100",
        }
        for index in range(1, 22)
    ]
    with bars_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    result = run(
        argparse.Namespace(
            bars_csv=str(bars_path),
            reference_csv=None,
            output=str(output_path),
            side="long",
            initial_stop="90",
            price_tick=None,
        )
    )

    assert result == output_path
    with output_path.open("r", encoding="utf-8", newline="") as handle:
        output_rows = list(csv.DictReader(handle))
    assert len(output_rows) == 21
    assert output_rows[-1]["accepted"] == "true"
    assert output_rows[-1]["raw_candidate"] == "95.0"
    assert output_rows[-1]["final_stop"] == "95.0"
    assert set(output_rows[-1]) == {
        "bar_close_time",
        "side",
        "old_stop",
        "swing_low_21",
        "swing_high_21",
        "raw_candidate",
        "rounded_candidate",
        "accepted",
        "final_stop",
        "mismatch_reason",
    }
