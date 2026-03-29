from typing import TypedDict, Optional
from datetime import datetime
import uuid


class OpportunityState(TypedDict):
    # Pipeline metadata
    cycle_id: str
    timestamp: str

    # Data from Market Scout
    raw_data: dict                      # {bhavcopy, bulk_deals, insider_trades, ...}

    # Signals from Signal Detector
    signals: list[dict]

    # Enriched signals from Context Builder
    enriched_signals: list[dict]

    # Final ranked opportunities from Strategist
    ranked_opportunities: list[dict]

    # Dashboard updated flag from Publisher
    dashboard_updated: bool

    # Error state
    error: Optional[str]


def new_state() -> OpportunityState:
    """Create a fresh pipeline state."""
    return OpportunityState(
        cycle_id=str(uuid.uuid4())[:8],
        timestamp=datetime.now().isoformat(),
        raw_data={},
        signals=[],
        enriched_signals=[],
        ranked_opportunities=[],
        dashboard_updated=False,
        error=None,
    )
