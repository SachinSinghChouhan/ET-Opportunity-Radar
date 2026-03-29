"""
LangGraph pipeline definition.
Wires all 5 agents into a sequential graph with conditional routing.
"""
from langgraph.graph import StateGraph, END
from loguru import logger

from app.agents.state import OpportunityState
from app.agents.market_scout import market_scout_agent
from app.agents.signal_detector import signal_detector_agent
from app.agents.context_builder import context_builder_agent
from app.agents.strategist import strategist_agent
from app.agents.publisher import publisher_agent


def _route_after_detection(state: OpportunityState) -> str:
    """Skip enrichment if no signals detected."""
    if state.get("signals"):
        return "context_builder"
    logger.info("[{}] No signals detected — skipping to publisher", state["cycle_id"])
    return "publisher"


def build_graph() -> StateGraph:
    graph = StateGraph(OpportunityState)

    # Register agent nodes
    graph.add_node("market_scout", market_scout_agent)
    graph.add_node("signal_detector", signal_detector_agent)
    graph.add_node("context_builder", context_builder_agent)
    graph.add_node("strategist", strategist_agent)
    graph.add_node("publisher", publisher_agent)

    # Pipeline edges
    graph.set_entry_point("market_scout")
    graph.add_edge("market_scout", "signal_detector")

    # Conditional: signals found → enrich → rank → publish
    #              no signals   → publish directly
    graph.add_conditional_edges(
        "signal_detector",
        _route_after_detection,
        {
            "context_builder": "context_builder",
            "publisher": "publisher",
        },
    )
    graph.add_edge("context_builder", "strategist")
    graph.add_edge("strategist", "publisher")
    graph.add_edge("publisher", END)

    return graph.compile()


# Module-level compiled graph — import this to run the pipeline
pipeline = build_graph()
