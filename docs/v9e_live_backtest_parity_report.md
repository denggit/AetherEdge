# V9E Live / Backtest Parity Report

CoinBacktest canonical file:

- `D:\Code_Project\CoinBacktest\backtest\lf\eth_lf_portfolio_v9e_range_exit_overlay_backtest.py`

AetherEdge live implementation files:

- `strategies/eth_lf_portfolio_v8/config.json`
- `strategies/eth_lf_portfolio_v8/strategy.py`
- `strategies/eth_lf_portfolio_v8/execution/range_exit.py`
- `strategies/eth_lf_portfolio_v8/domain/position_state.py`
- `tools/v8_live_preflight_check.py`

## Scope

V9E is V9C frozen baseline plus one range/footprint protective exit overlay.

This pass did not change entry signals, engine priority, sizing formulas,
`max_total_notional_mult`, initial stop, protected stop, trailing stop, add-on
logic, CoinBacktest, exchange adapters, leverage bootstrap, or
`config/env_loader.py`.

## Range Exit Semantics

The live overlay follows the V9E `_range_exit_signal(...)` formulas for long and
short positions:

- Requires enabled range exit, valid side, positive `risk_per_coin`, minimum
  hold bars, minimum peak MFE in R, configured giveback fraction, and current
  closed 4H range/footprint context.
- With `require_reversal=true`, requires hostile range/footprint reversal.
- Long hostile reversal: `rf_imbalance <= -contra_imbalance` or
  `rf_close_pos <= bad_close_pos`.
- Short hostile reversal: `rf_imbalance >= contra_imbalance` or
  `rf_close_pos >= 1 - bad_close_pos`.
- Exit reason is `RANGE_EXIT_NEXT_OPEN`.

Live intentionally does not implement `delay_bars` or pending delayed range
exit. User testing selected delay `0`; non-zero live delay config fails at
strategy config load.

## Risk-Per-Coin Parity

CoinBacktest V9E initializes `risk_per_coin` from the first entry initial stop
distance: `abs(first_entry - initial_sl)`.

Add-on fills change `avg_entry`, `qty`, and `units`, but they do not change
`risk_per_coin`. AetherEdge live preserves this behavior. Master position
reconciliation may update `avg_entry` and `qty` from exchange truth, but it must
not recompute `risk_per_coin` from the reconciled `avg_entry`. If
`risk_per_coin` is missing, live repairs it only from `first_entry` and
`initial_sl`.

## Execution Ordering

CoinBacktest V9E order:

1. touched stop
2. channel exit
3. opposite exit
4. range exit
5. max-hold exit
6. stop update
7. add

Live has no strategy-level touched-stop close because exchange stop orders cover
that path. Active live close ordering is:

1. channel exit
2. opposite exit
3. range exit
4. max-hold exit
5. stop update
6. add

When range exit triggers, live emits a normal reduce-only market close signal
through the existing master/follower close flow. It clears any pending deferred
add and does not create a same-bar add.

## Intentional Execution Differences

| Area | Backtest | Live |
| --- | --- | --- |
| Range exit fill | next open plus slippage | market close fill |
| Close propagation | single simulated position | master close then follower close |
| Stop touch | bar-level touched-stop close | exchange stop order and recovery checks |
| Order recovery | not modeled | live stop/order recovery remains active |

The live closed-bar decision point maps to the backtest next-open exit intent:
the decision is made only after the current 4H bucket is complete, then a market
close is emitted without using future range bars.
