# ETH Portfolio V1 AetherEdge Plugin

ETH Portfolio V1 is an independent portfolio strategy plugin. It is no longer
modeled as a long-term single-LF strategy, although its only active sleeve in
R005 is still the existing LF implementation. The LF alpha rules, router,
sizing, range exit, structural stop, risk scaling, and execution behavior
remain equivalent to the pre-R005 V1 baseline.

Plugin path and identity:

```text
strategies.eth_portfolio_v1:Strategy
strategy_id: eth_portfolio_v1
strategy_version: V1
```

## Current scope

- The LF sleeve is active and owns the existing position and signal behavior.
- The MF sleeve is a disabled state placeholder. It receives no market events,
  emits no signals, places no orders, and contributes no active position.
- Low Sweep signal/data migration is not part of R005; it remains reserved for
  R007/R008.
- The plugin owns its copied domain, engine, execution, feature, and
  persistence modules and does not import another concrete strategy plugin.
- V1 regular stop replacement never uses global `cancel_all_stop_orders`.

## Logical position provider

`Strategy.position_snapshots()` is the standard V1 provider used to expose
logical positions to the generic runtime. In R005 it returns active logical
positions only: an active LF position is adapted from the existing LF state,
while flat LF and disabled MF sleeves return no snapshot.

The LF adapter preserves the existing `position_id` exactly and maps the
existing base-asset `qty`, average entry, confirmed stop, side, engine, entry
time, and active exchanges into `StrategyPositionSnapshot`. This provider does
not change stop ownership, recovery scope, or scoped stop replacement.

## Scoped stop replacement

V1 stop replacement is confirmation-gated:

1. `_replace_stop_signals()` emits only the new LF-quantity, `reduce_only`
   stop. Old-stop identifiers are carried in that signal's metadata.
2. Order-result feedback verifies that the new stop succeeded on every target
   exchange.
3. Only successful feedback emits scoped cancels for the old LF stops, using
   their existing exchange order IDs and/or client order IDs.

Old-stop cancels are never placed in the initial signal list. If any target
exchange fails to confirm the new stop, feedback emits no cancel and the old
stop remains in place. A successful scoped cancel includes the V1 strategy,
LF sleeve, position, side, symbol, target exchange, and old stop identifiers.

If an old stop identifier is unavailable, V1 places the new protective stop
but does not fall back to a global cancel. The new-stop metadata marks manual
cleanup as required. Before live rollout, order events and recovery data must
reliably populate old stop exchange/client IDs; otherwise obsolete stops cannot
be cleaned up automatically.

## Sleeve direction

V1 now owns explicit LF and MF state boundaries. LF signals carry
`strategy_id=eth_portfolio_v1` and `sleeve_id=lf`; position-scoped signals keep
their existing `position_id`. MF remains disabled and inert until a later
milestone migrates its signal and data behavior. Automatic hedge-mode
switching is also outside R005.

## Runtime boundary

The plugin consumes normalized closed-kline/range feature events and emits
standard `TradeSignal` intents. It does not call exchange adapters. Runtime
requirements and all behavior parameters other than plugin identity remain
aligned with V10B.

## Start

From PowerShell at the repository root:

```powershell
$env:PYTHONPATH="."
$env:AETHER_STRATEGY="strategies.eth_portfolio_v1:Strategy"
python scripts/run_live.py
```
