from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


class LiveSmokeReport(Protocol):
    ok: bool
    verdict: str
    exit_code: int

    def to_json(self) -> str: ...


class LiveSmokeProvider(Protocol):
    async def run(self) -> LiveSmokeReport: ...


class FiniteLiveSmokeRunner:
    """Execute one provider pass without starting runtime producers."""

    def __init__(self, provider: LiveSmokeProvider) -> None:
        self.provider = provider
        self.producers_started = False

    async def run(self) -> LiveSmokeReport:
        return await self.provider.run()


@dataclass
class BootstrapFailureReport:
    verdict: str
    exit_code: int
    issues: list[str] = field(default_factory=list)
    ok: bool = False

    def to_json(self) -> str:
        return json.dumps(
            {
                "ok": self.ok,
                "verdict": self.verdict,
                "exit_code": self.exit_code,
                "issues": list(self.issues),
            },
            indent=2,
            ensure_ascii=False,
        )


def write_live_smoke_report(
    path: str | Path,
    report: LiveSmokeReport,
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(report.to_json(), encoding="utf-8")


def strategy_plugin_path(value: str) -> str:
    """Normalize a strategy id/module/path without a strategy registry."""

    normalized = str(value).strip()
    if ":" in normalized:
        return normalized
    if not normalized.startswith("strategies."):
        normalized = f"strategies.{normalized}"
    return f"{normalized}:Strategy"


__all__ = [
    "BootstrapFailureReport",
    "FiniteLiveSmokeRunner",
    "LiveSmokeProvider",
    "LiveSmokeReport",
    "strategy_plugin_path",
    "write_live_smoke_report",
]
