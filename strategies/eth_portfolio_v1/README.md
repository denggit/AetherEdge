# ETH Portfolio V1 AetherEdge Plugin

ETH Portfolio V1 is an independent portfolio strategy plugin with separate LF
and MF logical sleeves. The LF alpha rules, router, sizing, range exit,
structural stop, risk scaling, and execution behavior remain equivalent to the
pre-R005 V1 baseline. The MF Low Sweep sleeve is enabled for the promoted
time48 live variant.

Plugin path and identity:

```text
strategies.eth_portfolio_v1:Strategy
strategy_id: eth_portfolio_v1
strategy_version: V1
```

## Current scope

- V1 composes its sleeves through a plugin-private `SleeveRegistry`.
- The registry contains the active `lf` sleeve and the active `mf` Low Sweep
  sleeve.
- MF Low Sweep uses OKX as data/master and Binance as follower when both
  exchanges are configured.
- MF sizing uses per-exchange margin intent: `mf.margin_fraction` is multiplied
  by each exchange's configured leverage from runtime/account config
  (`OKX_LEVERAGE`, `BINANCE_LEVERAGE`, `MARGIN_MODE`). Each exchange sizes from
  its own account equity and available balance, with `available_margin_buffer`
  applied to the available-margin cap.
- **Live sizing (1× equity):** `margin_fraction=0.0666666667` at 15× leverage
  → target notional ≈ 1.0× equity. This is a conservative reduction from the
  CoinBacktest 1.5× exposure, to improve survivability during deep MAE paths
  (e.g. 2025-02-03 style extreme moves) under isolated-margin execution.
- **MF hard stop:** after a successful MF open, a 5.0% hard stop loss is
  placed as an exchange stop market order on each filled exchange, calculated
  from the master average fill price. The stop is reduce-only and scoped to
  the MF sleeve only.
- **Hard stop cooldown:** when the MF hard stop is filled, the MF sleeve is
  cleared and a 12-hour cooldown is activated. During cooldown, new MF entries
  are blocked but LF signals and time48 exits on an active sleeve are not
  affected.
- **Time48 exit cancels stop:** a normal time48 close automatically cancels
  the associated MF hard stop orders (scoped cancel, not global).
- The plugin owns its copied domain, engine, execution, feature, and
  persistence modules and does not import another concrete strategy plugin.
- V1 regular stop replacement never uses global `cancel_all_stop_orders`.

## Logical position provider

`Strategy.position_snapshots()` delegates to the sleeve registry, which
aggregates snapshots from enabled sleeves in registration order. In R005-fix
it returns active logical positions only: the LF sleeve adapts its existing
position state, while flat LF and disabled MF sleeves return no snapshot.

The LF sleeve composes its existing snapshot adapter. The adapter preserves the
existing `position_id` exactly and maps base-asset `qty`, average entry,
confirmed stop, side, engine, entry time, and active exchanges into
`StrategyPositionSnapshot`. The MF sleeve snapshots include
`exchange_quantities_base` so recovery and preflight can distinguish master and
follower leg quantities. This provider does not change stop ownership, recovery
scope, or scoped stop replacement.

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

The registry depends only on the plugin-private `PortfolioSleeve` protocol and
unique string IDs; it has no fixed LF/MF field set. Future sleeves such as
`mf_low_sweep` or `hf_range` can be registered without changing the public
`src` architecture. LF signals continue to carry
`strategy_id=eth_portfolio_v1` and `sleeve_id=lf`, and position-scoped signals
keep their existing `position_id`. MF signals carry `sleeve_id=mf`,
`close_scope=mf_sleeve_only`, and per-exchange `exchange_quantities_base` so
OKX and Binance open and close their own sized legs. Automatic hedge-mode
switching is outside the strategy plugin and remains a runtime/account config
responsibility.

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
