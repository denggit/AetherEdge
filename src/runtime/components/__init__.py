from src.runtime.components.wiring import WiringComponent
from src.runtime.components.closed_bar import ClosedBarComponent
from src.runtime.components.lifecycle import LifecycleComponent
from src.runtime.components.startup import StartupComponent
from src.runtime.components.recovery import RecoveryComponent
from src.runtime.components.catchup import CatchupComponent
from src.runtime.components.account import AccountComponent
from src.runtime.components.market_events import MarketEventsComponent
from src.runtime.components.signal_execution import SignalExecutionComponent
from src.runtime.components.order_results import OrderResultsComponent
from src.runtime.components.persistence import PersistenceComponent
from src.runtime.components.range_runtime import RangeRuntimeComponent

COMPONENT_TYPES = (
    WiringComponent,
    ClosedBarComponent,
    LifecycleComponent,
    StartupComponent,
    RecoveryComponent,
    CatchupComponent,
    AccountComponent,
    MarketEventsComponent,
    SignalExecutionComponent,
    OrderResultsComponent,
    PersistenceComponent,
    RangeRuntimeComponent,
)
