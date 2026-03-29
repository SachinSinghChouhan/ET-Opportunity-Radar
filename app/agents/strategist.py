"""
Agent 4: Strategist — Ranking & Reasoning
Uses Gemini Pro to rank signals and generate full reasoning chains.
Outputs the top-N opportunities with confidence scores.
"""
from loguru import logger

from app.agents.state import OpportunityState
from app.tools.llm_client import rank_and_reason
from app.config import settings
from app import database as db


async def strategist_agent(state: OpportunityState) -> dict:
    """
    Takes enriched signals, calls LLM to rank and reason, returns top-N opportunities.
    """
    enriched_signals = state.get("enriched_signals", [])
    cycle_id = state["cycle_id"]
    logger.info("[{}] Strategist starting: {} enriched signals", cycle_id, len(enriched_signals))

    if not enriched_signals:
        return {"ranked_opportunities": []}

    # Prepare a clean version of signals for the LLM (remove raw binary data)
    llm_input = [_prepare_for_llm(sig) for sig in enriched_signals]

    try:
        output = await rank_and_reason(llm_input, top_n=settings.top_n_opportunities)

        ranked = []
        for opp in output.opportunities:
            opp_dict = {
                "cycle_id": cycle_id,
                "rank": opp.rank,
                "symbol": opp.symbol.upper(),
                "signal_type": opp.signal_type,
                "confidence": opp.confidence,
                "action": opp.action,
                "reasoning_chain": opp.reasoning_chain,
                "timeframe": opp.timeframe,
                "key_catalysts": opp.key_catalysts,
                "risk_factors": opp.risk_factors,
                "voice_briefing_path": None,
            }
            ranked.append(opp_dict)
            db.insert_opportunity(opp_dict)

        logger.success(
            "[{}] Strategist done: {} ranked opportunities",
            cycle_id, len(ranked),
        )
        for r in ranked:
            logger.info(
                "  #{} {} | {} | conf={:.0%} | {}",
                r["rank"], r["symbol"], r["action"], r["confidence"], r["signal_type"],
            )
        return {"ranked_opportunities": ranked}

    except Exception as e:
        logger.error("[{}] Strategist LLM failed: {}", cycle_id, e)
        # Fallback: return top signals sorted by severity without LLM reasoning
        fallback = _fallback_ranking(enriched_signals, cycle_id)
        return {"ranked_opportunities": fallback}


def _prepare_for_llm(signal: dict) -> dict:
    """Trim signal to minimal fields — keeps LLM prompt small and fast."""
    raw = signal.get("raw_data") or {}
    # Pick only the 3 most useful numeric fields from raw_data
    useful_raw = {k: v for k, v in list(raw.items())[:4]
                  if isinstance(v, (int, float, str)) and k not in ("trades", "deals", "clients")}
    return {
        "symbol": signal.get("symbol"),
        "type": signal.get("signal_type"),
        "severity": signal.get("severity"),
        "metric": signal.get("metric_label"),
        "news": (signal.get("news_context") or "")[:200],  # truncate news
        "data": useful_raw,
    }


def _fallback_ranking(signals: list[dict], cycle_id: str) -> list[dict]:
    """Simple fallback when LLM unavailable — rank by severity score."""
    severity_score = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
    sorted_sigs = sorted(
        signals,
        key=lambda s: severity_score.get(s.get("severity", "LOW"), 0),
        reverse=True,
    )
    ranked = []
    for i, sig in enumerate(sorted_sigs[: settings.top_n_opportunities], 1):
        opp = {
            "cycle_id": cycle_id,
            "rank": i,
            "symbol": sig["symbol"],
            "signal_type": sig["signal_type"],
            "confidence": 0.5,
            "action": "WATCH",
            "reasoning_chain": sig.get("metric_label", "Signal detected. LLM analysis unavailable."),
            "timeframe": "Unknown",
            "key_catalysts": [],
            "risk_factors": ["LLM analysis failed — manual review recommended"],
            "voice_briefing_path": None,
        }
        ranked.append(opp)
        db.insert_opportunity(opp)
    return ranked
