from __future__ import annotations

import csv
from pathlib import Path
from typing import Mapping, Any


class CsvAuditStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, row: Mapping[str, Any]) -> None:
        exists = self.path.exists()
        keys = list(row.keys())
        with self.path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            if not exists:
                writer.writeheader()
            writer.writerow(dict(row))
