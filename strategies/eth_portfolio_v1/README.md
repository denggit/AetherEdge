# ETH LF Portfolio V10B AetherEdge Plugin

V10B keeps the V10A portfolio, entry filters, routing, sizing, add-on, and
range-exit behavior unchanged and adds one all-engine swing structural stop:

```text
struct_stop_all_swing_n21_buf0p0_trig0p0_h0
```

Plugin path:

```text
strategies.eth_lf_portfolio_v10b:Strategy
```

Strategy identity:

```text
strategy_id: eth_lf_portfolio_v10b_all_swing_structural_stop
strategy_version: V10B
```

## Structural stop

On every completed 4H strategy bar, after all current-bar exit decisions:

- long uses the lowest low of the latest 21 completed 4H bars;
- short uses the highest high of the latest 21 completed 4H bars;
- no candidate exists before a full 21-bar window;
- the candidate must tighten the confirmed stop and beat the V10A stop;
- the candidate must remain on the protective side of the completed close;
- rounding is followed by the same direction and close checks;
- an accepted stop affects only subsequent bars and order-management rounds.

The canonical candidate is calculated from OKX master strategy state. The
standard stop-sync signal carries that one canonical price to the open master
and follower legs; Binance does not calculate a separate structural level.

If structural evaluation, rounding, or validation fails, the strategy keeps or
updates the existing V10A stop. It never cancels the V10A stop merely because
the V10B candidate failed.

## Runtime boundary

The plugin consumes normalized closed-kline/range feature events and emits
standard `TradeSignal` stop-sync intents. It does not call exchange adapters.
Startup hydration uses the existing 365-day/2000-record closed-kline warmup,
which is more than the 21 bars required for the structural window.

## Start

From PowerShell at the repository root:

```powershell
$env:PYTHONPATH="."
$env:AETHER_STRATEGY="strategies.eth_lf_portfolio_v10b:Strategy"
python scripts/run_live.py
```
