from __future__ import annotations

from dataclasses import dataclass

from src.app.alerts import AsyncAlertDispatcher
from src.platform.data import MarketDataFeed
from src.platform.execution import ExecutionClient, MultiExchangeExecutionClient
from src.platform.state import StateStore
from src.planner import ExecutionPlanner
from src.strategy import StrategyPort


ExecutionFacade = ExecutionClient | MultiExchangeExecutionClient


@dataclass(frozen=True)
class AppContext:
    data: MarketDataFeed
    execution: ExecutionFacade
    state_store: StateStore
    strategy: StrategyPort
    planner: ExecutionPlanner
    alerts: AsyncAlertDispatcher
