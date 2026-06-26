# V9C Live / Backtest Strategy Parity Report

CoinBacktest is canonical for strategy semantics.
AetherEdge live should match CoinBacktest unless CoinBacktest has a proven bug.

## Scope

This report compares CoinBacktest V9C strategy semantics from:

- `D:\Code_Project\CoinBacktest\backtest\lf\eth_lf_portfolio_v9c_reclaim_priority_backtest.py`
- Shared CoinBacktest executor formulas imported there from `eth_1d_4h_trend_rider_v8_position_lock_backtest.py`

Against AetherEdge live V9C semantics in:

- `strategies/eth_lf_portfolio_v8/strategy.py`
- `strategies/eth_lf_portfolio_v8/execution/*`
- `strategies/eth_lf_portfolio_v8/domain/*`
- `src/runtime/runner.py`

The AetherEdge directory is named `eth_lf_portfolio_v8`, but the active config id is `eth_lf_portfolio_v9c_reclaim_priority`.

## Test Methodology Note

The parity tests in `tests/parity/` use a minimal canonical snapshot helper extracted from CoinBacktest V9C formulas.

They do not directly import or execute the CoinBacktest runtime. This is intentional because AetherEdge and CoinBacktest are separate projects and direct cross-project imports would make the live repository tests brittle.

Implication:

- These tests protect AetherEdge live from drifting away from the currently extracted CoinBacktest V9C canonical semantics.
- If CoinBacktest V9C strategy logic changes in the future, the canonical helper and this report must be reviewed and updated intentionally.
- Any CoinBacktest strategy logic change still requires a historical backtest rerun before being accepted as the new canonical baseline.

## CoinBacktest V9C Canonical Strategy Logic

- Engine routing: V9C uses `reclaim_first` priority: `BULL_RECLAIM_V2`, then `MOMENTUM_V3`, then `BEAR_V3_ONLY`.
- A 4H bar close confirms the strategy decision; entry/add/channel/opposite/max-hold market execution is modeled at the next 4H open.
- Current-bar stop touch is evaluated against the active stop from before the current bar's close update.
- If no exit occurs, current-bar close calculates `next_stop` from ATR trailing and protected stop; that updated stop becomes the active stop for later bars.
- The same bar then still evaluates add eligibility. Therefore same-bar `stop_update + add` is allowed by canonical strategy semantics.
- Initial entry risk multiplier is `risk_mult * quality_mult * micro_entry_risk_scale * global_risk_scale`.
- Add sizing uses `risk_mult * quality_mult * global_risk_scale`; add does not apply `micro_entry_risk_scale`.
- Add trigger uses current `units`: long `high >= first_entry + units * add_every_r * risk_per_coin`, short `low <= first_entry - units * add_every_r * risk_per_coin`.
- Initial stop is `entry - initial_atr_mult * atr` for long and `entry + initial_atr_mult * atr` for short.
- Protected stop and trailing stop select the better stop for the position side.
- Exit reasons compared here: entry-engine channel exit, opposite signal exit, and max-hold exit.

## AetherEdge Live Current Corresponding Logic

- Router already uses reclaim-first priority and portfolio risk/quality multiplier scaling.
- Live entry sizing uses base-asset quantity and the same effective risk formula.
- Live add sizing uses `micro_entry_risk_scale=1`, matching canonical add semantics.
- Live stop formulas use the same initial stop, protected stop, trailing stop, and candidate selection rules.
- Live close signals map channel/opposite/max-hold into market close intents.
- Live stop touch is not generated as a strategy close signal; it is represented by exchange stop orders. This is an execution-model difference, not a strategy mismatch.

## Differences

| Area | Classification | Finding | Resolution |
| --- | --- | --- | --- |
| Same-bar stop update plus add | live_bug_mismatch | CoinBacktest allows a bar to update stop and still evaluate add. Live previously returned immediately after stop update, so no same-bar add plan was preserved. | Fixed live with deferred add-after-stop-confirmed flow. |
| Stop touch exit | intentional_execution_difference | Backtest marks stop touch in the bar model. Live uses real exchange stop orders and post-check. | Not changed. |
| Entry/add fill price | intentional_execution_difference | Backtest uses next open plus slippage. Live uses current close for planning and real fill for position state. | Not changed; parity tests compare base quantity/formula, not fill price. |
| Fees/slippage | intentional_execution_difference | Backtest has explicit `fee_rate` and `slippage_pct`; live fee comes from exchange fills/accounting. | Not changed; historical fee changes would require a retest. |
| OKX native contracts | intentional_execution_difference | Backtest quantities are ETH base qty; OKX live orders are converted to contracts. | Not changed; conversion audit test added. |

No `possible_backtest_bug_needs_retest` item was found in this pass.

## Fixes Made

- Added `PendingAddAfterStopUpdatePlan` to live strategy state.
- Split add calculation into deterministic plan creation and signal emission.
- When a closed bar produces both stop update and add eligibility, live now:
  1. stores the pending add plan,
  2. emits only stop replace signals,
  3. waits for stop order results and post-check confirmation,
  4. emits the deferred add only after stop confirmation,
  5. uses existing add-fill stop replacement flow to cover the new total position.
- Stop update failure, metadata rejection, close decision, entry failure, stale bar, or position change clears the deferred add.
- Added `micro_entry_risk_scale` metadata to live open/add signals for parity audit.

## Not Changed

- CoinBacktest strategy logic was not modified.
- Strategy parameters were not modified.
- Sizing formulas were not modified.
- OKX/Binance adapters were not modified.
- Stop post-check, retry, journal, master reconcile, follower repair, and follower quantity behavior were not weakened.

## Backtest Changes Requiring Historical Retest

None were made.

Any future change to CoinBacktest routing, sizing, stop formulas, fee/slippage configuration, or execution timing should be marked as requiring a historical backtest rerun before being accepted.

## Deterministic Parity Tests Added

- `test_v9c_parity_same_bar_stop_update_add_policy_matches_backtest`
- `test_live_defers_add_until_stop_update_confirmed_when_same_bar_backtest_allows_both`
- `test_live_does_not_execute_deferred_add_when_stop_update_fails`
- `test_live_add_after_stop_confirmed_replaces_stop_for_new_total_position`
- `test_v9c_parity_open_signal_matches_backtest_canonical`
- `test_v9c_parity_initial_entry_sizing_matches_backtest`
- `test_v9c_parity_initial_stop_formula_matches_backtest`
- `test_v9c_parity_protected_stop_formula_matches_backtest`
- `test_v9c_parity_trailing_stop_formula_matches_backtest`
- `test_v9c_parity_stop_candidate_selection_matches_backtest`
- `test_v9c_parity_add_trigger_matches_backtest`
- `test_v9c_parity_units_definition_for_add_matches_backtest`
- `test_v9c_parity_add_sizing_micro_scale_policy_matches_backtest`
- `test_v9c_parity_exit_channel_matches_backtest`
- `test_v9c_parity_opposite_signal_exit_matches_backtest`
- `test_v9c_parity_max_hold_exit_matches_backtest`
- `test_v9c_parity_okx_contract_conversion_audit`

## Canonical Same-Bar Conclusion

CoinBacktest V9C canonical semantics are B: the same bar can update stop and still check add.

AetherEdge live now preserves that strategy semantics while enforcing live-safe execution ordering: stop replace first, stop post-check confirmed second, deferred add third, add-fill stop coverage fourth.
