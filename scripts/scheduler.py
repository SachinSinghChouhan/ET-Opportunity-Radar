"""
Production scheduler — runs pipeline automatically every N minutes.
Run: python scripts/scheduler.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from app.database import init_db
from app.agents.graph import pipeline
from app.agents.state import new_state
from app.config import settings


async def run_pipeline_cycle():
    """Single pipeline cycle — called by scheduler."""
    state = new_state()
    logger.info("=== Scheduled cycle starting: {} ===", state["cycle_id"])
    try:
        result = await pipeline.ainvoke(state)
        opps = result.get("ranked_opportunities", [])
        logger.success("Cycle {} complete: {} opportunities", state["cycle_id"], len(opps))
    except Exception as e:
        logger.error("Cycle failed: {}", e)


async def main():
    init_db()
    scheduler = AsyncIOScheduler()

    interval = settings.pipeline_interval_minutes
    scheduler.add_job(
        run_pipeline_cycle,
        trigger="interval",
        minutes=interval,
        id="pipeline_cycle",
        replace_existing=True,
        misfire_grace_time=60,
    )

    logger.info("Scheduler started — running every {} minutes", interval)
    logger.info("Press Ctrl+C to stop")

    scheduler.start()

    # Run one cycle immediately on startup
    await run_pipeline_cycle()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    asyncio.run(main())
