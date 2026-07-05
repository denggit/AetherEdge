from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.runtime.no_mutation import (
    MutationAttemptError,
    NoMutationExecutionClient,
)
from strategies.eth_portfolio_v1.preflight.live_gate import (
    EXIT_FAIL_CONFIG,
    EXIT_FAIL_MARKET_DATA,
    EXIT_FAIL_MUTATION_ATTEMPT,
    EXIT_FAIL_RECOVERY,
    EXIT_PASS,
    PortfolioV1LiveGateReport,
    write_live_gate_report,
)
from src.runtime.live_smoke import FiniteLiveSmokeRunner


class _Gate:
    def __init__(self, report: PortfolioV1LiveGateReport) -> None:
        self.report = report
        self.calls = 0

    async def run(self):
        self.calls += 1
        return self.report


def _report(*, exit_code: int, verdict: str) -> PortfolioV1LiveGateReport:
    report = PortfolioV1LiveGateReport(
        symbol="ETH-USDT-PERP",
        runtime_mode="live_runtime",
        exchanges=["okx", "binance"],
        ok=exit_code == EXIT_PASS,
        exit_code=exit_code,
        verdict=verdict,
    )
    report.add(
        "direct_live_startup_gates",
        ok=exit_code == EXIT_PASS,
        detail={
            "producers_started": False,
            "signals_executed": False,
        },
    )
    return report


@pytest.mark.asyncio
async def test_all_smoke_gates_pass_returns_zero() -> None:
    gate = _Gate(_report(exit_code=EXIT_PASS, verdict="pass"))
    smoke = FiniteLiveSmokeRunner(gate)

    result = await smoke.run()

    assert result.exit_code == 0
    assert result.ok is True
    assert gate.calls == 1


@pytest.mark.parametrize(
    ("exit_code", "verdict"),
    (
        (EXIT_FAIL_CONFIG, "fail_config"),
        (EXIT_FAIL_RECOVERY, "fail_recovery"),
        (EXIT_FAIL_MARKET_DATA, "fail_mf_readiness"),
        (EXIT_FAIL_MARKET_DATA, "fail_lf_readiness"),
        (EXIT_FAIL_MUTATION_ATTEMPT, "fail_mutation_attempt"),
    ),
)
@pytest.mark.asyncio
async def test_failed_smoke_gate_is_nonzero(
    exit_code: int,
    verdict: str,
) -> None:
    smoke = FiniteLiveSmokeRunner(
        _Gate(_report(exit_code=exit_code, verdict=verdict))
    )

    result = await smoke.run()

    assert result.exit_code != 0
    assert result.ok is False


class _MutationClient:
    exchange = SimpleNamespace(value="okx")
    symbol = "ETH-USDT-PERP"
    market_profile = SimpleNamespace(symbol=symbol)


@pytest.mark.parametrize("method", ("place_order", "cancel_order"))
@pytest.mark.asyncio
async def test_mutation_attempt_in_smoke_wrapper_fails(
    method: str,
) -> None:
    client = NoMutationExecutionClient(_MutationClient())

    with pytest.raises(MutationAttemptError):
        await getattr(client, method)(object())

    assert client.mutation_attempted is True


@pytest.mark.asyncio
async def test_smoke_does_not_start_long_running_producers() -> None:
    smoke = FiniteLiveSmokeRunner(
        _Gate(_report(exit_code=EXIT_PASS, verdict="pass"))
    )

    await smoke.run()

    assert smoke.producers_started is False


def test_smoke_report_contains_startup_gate_results() -> None:
    report = _report(exit_code=EXIT_PASS, verdict="pass")

    payload = report.to_dict()

    assert payload["startup_gate_results"]
    assert payload["startup_gate_results"][0]["name"] == (
        "direct_live_startup_gates"
    )


def test_smoke_report_creates_parent_and_writes_utf8_json(tmp_path) -> None:
    report = _report(exit_code=EXIT_PASS, verdict="pass")
    target = tmp_path / "nested" / "服务器-smoke.json"

    write_live_gate_report(target, report)

    assert target.is_file()
    assert '"ok": true' in target.read_text(encoding="utf-8")
