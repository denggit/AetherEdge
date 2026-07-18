# AetherEdge runtime architecture audit

This is the Phase 0 working record for the demand-driven runtime refactor. It
describes the current workspace only. Git history and older design claims were
not used.

## Inventory and verification scope

- Production Python: 211 files under `src/`, plus 2 configuration helpers.
- Strategy plugins (read-only for this refactor): 83 Python files and their
  JSON manifests.
- Tests: 275 Python files, 66,763 physical lines, 2,805 collected tests.
- Tooling: 19 Python files. Live shell/Python entrypoints were read in full.
- `src/platform/data/**` is hidden by the repository ignore pattern for
  `data/`; it exists in the workspace and was included by filesystem inventory
  rather than relying on `rg --files` alone.

## Formal startup chain

The current production chain is:

```text
scripts/start_live_watchdog.sh
  -> scripts/watchdog_live.py
  -> src.app.watchdog.run_live_watchdog
  -> scripts/run_live.py
  -> bootstrap ProjectEnvConfig
  -> AppConfig.from_env
  -> build_app_context
  -> live_runtime_config_from_app
  -> LiveRuntimeRunner.run
```

`scripts/run_live.sh` is a direct compatibility launcher and
`scripts/watchdog_live.sh` delegates to `start_live_watchdog.sh`. The watchdog
core is already centralized in `src/app/watchdog.py`.

## Current runtime classes and paths

- `src/runtime/runner.py::LiveRuntimeRunner`: 5,307-line class (5,943-line
  module). It owns composition, startup, shutdown, market producers, queues,
  Range construction/repair/checkpoints, feature derivation, strategy
  dispatch, account/order sync, recovery, reconciliation, execution, health,
  persistence, and diagnostics.
- `src/runtime/context.py::LiveRuntimeContext`: wraps `AppContext` plus
  `Mapping[str, Any]`; it is not the formal composition root.
- `src/app/factory.py::build_app_context`: eagerly constructs the combined
  market-data feed, execution clients, state store, strategy, planner, and
  alerts.
- `src/platform/data/factory.py::create_market_data_feed`: constructs REST,
  Trade WebSocket, OrderBook WebSocket and optional store as one feed object.
- Callback-only coordinators in `src/runtime/*_coordinator.py` reduce local
  method size but leave business ownership and state in `LiveRuntimeRunner`.
- `src/runtime/sync_services.py` is a two-slot lazy object registry, while the
  runner still uses a string-keyed `services` dictionary for the rest of the
  graph.

There is one formal production runner today, but dependency construction is
duplicated between `src/app/factory.py`, platform factories, and runner lazy
getters. The refactor must leave one composition root and one runtime path.

## Raw data sources

Current reusable adapters are:

- Trade stream: OKX and Binance WebSocket feeds.
- Order-book stream: OKX and Binance WebSocket feeds.
- Closed kline/ticker/historical trade REST access through
  `RestMarketDataFeed` and exchange clients.
- Private account streams: OKX and Binance account WebSockets.

Trade and order-book streams are optional fields of one `RestMarketDataFeed`;
they are not independently registered lifecycle modules. App composition uses
the factory defaults and therefore constructs both stream adapters regardless
of the resolved strategy requirements. Actual network connection is delayed
until iteration, but disabled capabilities are still represented in the
object graph.

## Derived data capabilities

- Range bars and Range aggregates: builders live in `src/market_data/derived`,
  but live state, buckets, persistence, repair, backfill and checkpoint logic
  are owned by `LiveRuntimeRunner`.
- Fixed-time trade bars, trade footprints and Range footprints are bundled in
  `TradeDerivedFeaturePipeline`.
- `TradeDerivedFeaturePipeline.process_trade` calls
  `strategy.trade_feature_runtime_config()` on every trade and lazily creates
  all three builders as a single all-on/all-off pipeline.
- Closed-kline and normalized feature event conversion lives in
  `src/runtime/features.py`.

## Background work and resources

Runtime-owned work includes:

- market producer tasks and the bounded market queue;
- account/order poll tasks and heartbeat through `RuntimeSyncLifecycle`;
- closed-bar scheduling and polling;
- bounded background persistence writer;
- Range checkpoint writer and repair-journal writer threads;
- Range backfill, micro-repair and Range-speed supervisors;
- strategy feature-readiness refresh and follower-close checks;
- alert dispatcher worker.

Stores opened or lazily created across the graph include state, order journal,
position plan, kline, trade, Range bar, Range checkpoint, Range repair journal,
combined platform market-data, and trade-feature SQLite stores. Creation is
spread across app composition, runner getters, services and strategy-provided
startup helpers.

## Strategy-facing compatibility surface

The existing plugins expose combinations of:

- `runtime_requirements()` and the versioned capability manifest;
- `strategy_identity`, position snapshots, recovery status, pending work,
  startup preview and Range-speed history providers;
- standard market/account/start/order-result callbacks;
- market-feature observer providers;
- legacy `trade_feature_runtime_config()`;
- legacy startup feature-backfill providers and live preflight/smoke providers.

The architecture must parse these once through a generic compatibility adapter.
No file under `strategies/**` may be changed. Framework code must not import,
name, identify or branch on a concrete strategy.

## Order and execution flow

The current externally tested flow is:

```text
TradeSignal
  -> LiveOrderIntentFactory / OrderIntent
  -> ExecutionPlanner / ExecutionPlan
  -> MultiExchangeOrderCoordinator
  -> exchange execution and status synchronization
  -> journal/reconciliation/strategy feedback
  -> PositionPlan update
```

`MultiExchangeOrderCoordinator` is a 1,029-line class in a 1,352-line module.
It combines idempotent claim, intent planning, master/follower sequencing,
per-exchange conversion, exit safety, execution, result recording, and
PositionPlan updates. These behaviors are heavily characterized by tests and
must be separated without changing ordering, retry, reduce-only, hedge-mode,
TP/SL, follower, fill-verification, or recovery semantics.

## Oversized production files

- `src/runtime/runner.py`: 5,943 lines.
- `src/market_data/storage/trade_feature_store.py`: 1,586 lines.
- `src/market_data/range_checkpoint.py`: 1,557 lines.
- `src/order_management/coordinator/service.py`: 1,352 lines.
- `src/market_data/backfill/service.py`: 1,174 lines.
- `src/platform/exchanges/okx/client.py`: 1,131 lines.
- `src/market_data/micro_repair.py`: 900 lines.
- `src/order_management/reconciliation/service.py`: 850 lines.

Strategy files above the target are excluded from modification by the goal.
New production files must stay below 800 lines and have cohesive ownership.

## Strong-coupling and duplication findings

1. `LiveRuntimeRunner.__init__` accepts `Mapping[str, Any]` and performs dozens
   of `services.get("...")` lookups.
2. Runtime imports Range builders, stores, checkpoint types, repair writers,
   supervisors and Range-specific configuration directly.
3. `LiveRuntimeConfig` owns all Range, backfill, repair, checkpoint and
   market-data database fields.
4. Trade feature configuration is read from the strategy on the hot path.
5. Market feature observers are resolved from the strategy again on every
   feature event.
6. App composition constructs a combined market feed before requirements are
   resolved; independent capabilities cannot be omitted from the graph.
7. Exchange/account/execution/store construction occurs both in the app
   factory and runner lazy getters.
8. Generic callback coordinators duplicate lifecycle shape without owning a
   coherent domain boundary.
9. Range persistence/checkpoint/repair/backfill state is split across the
   runner and multiple services, so stop/health ownership is ambiguous.
10. Order execution stages are implemented inside one coordinator rather than
    explicit planner/validator/executor/recorder/updater components.

## Existing test protection

The suite already characterizes startup ordering, recovery, reconciliation,
account configuration, market queue backpressure, Range checkpoint/repair,
feature causality, order safety, master/follower execution, TP/SL, PositionPlan,
watchdog behavior, credentials, shutdown, and strategy parity.

Several architecture tests intentionally freeze the current transitional
ownership (for example asserting that Range builders remain in the runner or
that business methods remain runner-owned). Those tests conflict with the new
goal and must be replaced by stronger target-boundary assertions while their
behavioral characterization tests continue to pass.

Missing requested directory names are `tests/execution` and `tests/live`.
Equivalent coverage is currently in `tests/platform/execution`,
`tests/order_management`, `tests/exchanges`, `tests/app`, `tests/reconcile`,
and the live/runtime/script suites.

## Required migration direction

1. Resolve strategy requirements once and produce an immutable capability plan.
2. Register one provider per capability and resolve transitive dependencies in
   topological order with instance deduplication.
3. Construct only planned modules in the composition root.
4. Give each source/derived/background module its own config, state, tasks,
   connections, stores, warmup/repair support and health.
5. Make the formal runtime orchestrator lifecycle-only and stop in reverse
   order.
6. Keep a thin public `LiveRuntimeRunner` compatibility facade only where
   existing callers need the name; it must not retain algorithms or string
   service location.
7. Separate order intent planning, safety validation, execution, recording and
   PositionPlan updates behind explicit typed dependencies.
8. Remove obsolete factories, contexts, callback coordinators and duplicated
   compatibility code after the production path has migrated.

