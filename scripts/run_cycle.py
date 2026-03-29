"""
Manual pipeline cycle runner.
Run: python scripts/run_cycle.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
from app.database import init_db
from app.agents.graph import pipeline
from app.agents.state import new_state


async def main():
    logger.info("=== Opportunity Radar — Manual Cycle ===")
    init_db()

    state = new_state()
    logger.info("Starting cycle: {}", state["cycle_id"])

    result = await pipeline.ainvoke(state)

    opps = result.get("ranked_opportunities", [])
    signals = result.get("signals", [])

    print(f"\n{'='*60}")
    print(f"CYCLE {state['cycle_id']} COMPLETE")
    print(f"Signals detected: {len(signals)}")
    print(f"Opportunities ranked: {len(opps)}")
    print(f"{'='*60}")
    for opp in opps:
        print(
            f"  #{opp['rank']} {opp['symbol']:<12} "
            f"{opp['action']:<6} "
            f"conf={opp['confidence']:.0%} "
            f"| {opp['signal_type']}"
        )
        print(f"     {opp['reasoning_chain'][:120]}...")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
