"""
Agent 5: Publisher — Dashboard & Voice Delivery
Generates voice briefing and marks dashboard as ready for refresh.
"""
from loguru import logger

from app.agents.state import OpportunityState
from app.tools.voice import generate_voice_briefing
from app import database as db


async def publisher_agent(state: OpportunityState) -> dict:
    """
    1. Generate voice briefing MP3
    2. Update voice_briefing_path in DB for each opportunity
    3. Signal dashboard to refresh
    """
    cycle_id = state["cycle_id"]
    opportunities = state.get("ranked_opportunities", [])
    logger.info("[{}] Publisher starting: {} opportunities to publish", cycle_id, len(opportunities))

    # Generate voice briefing
    audio_path = generate_voice_briefing(opportunities, cycle_id)

    # Update DB records with audio path
    if audio_path and opportunities:
        with db.db() as conn:
            conn.execute(
                "UPDATE opportunities SET voice_briefing_path = ? WHERE cycle_id = ?",
                (audio_path, cycle_id),
            )
        logger.info("  ✓ Voice briefing: {}", audio_path)

    # Log summary to console for monitoring
    logger.success(
        "[{}] === CYCLE COMPLETE — {} opportunities published ===",
        cycle_id, len(opportunities),
    )
    for opp in opportunities:
        logger.info(
            "  #{rank} {symbol:<12} | {action:<5} | conf={confidence:.0%} | {signal_type}",
            **opp,
        )

    return {"dashboard_updated": True}
