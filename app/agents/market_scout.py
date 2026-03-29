"""
Agent 1: Market Scout — Data Ingestion
Fetches all raw market data from NSE/BSE and stores in SQLite.
Runs at the start of every pipeline cycle.
"""
from datetime import date
from loguru import logger

from app.agents.state import OpportunityState
from app.tools import nse_client, bse_client
from app import database as db


async def market_scout_agent(state: OpportunityState) -> dict:
    """
    Fetches: bhavcopy, bulk deals, insider trades, announcements.
    Falls back to BSE if NSE fails.
    Returns updated raw_data in state.
    """
    logger.info("[{}] Market Scout starting...", state["cycle_id"])
    raw_data = {}

    # 1. Bhavcopy (daily OHLCV for all stocks)
    bhavcopy_rows = await _fetch_bhavcopy_safe()
    if bhavcopy_rows:
        db.upsert_bhavcopy(bhavcopy_rows)
        raw_data["bhavcopy"] = bhavcopy_rows
        logger.info("  ✓ Bhavcopy: {} rows", len(bhavcopy_rows))
    else:
        logger.warning("  ✗ Bhavcopy unavailable — using cached data")
        raw_data["bhavcopy"] = []

    # 2. Bulk / Block deals
    bulk_deals = await _fetch_bulk_deals_safe()
    if bulk_deals:
        db.insert_bulk_deals(bulk_deals)
        raw_data["bulk_deals"] = bulk_deals
        logger.info("  ✓ Bulk deals: {} rows", len(bulk_deals))
    else:
        raw_data["bulk_deals"] = []

    # 3. Insider trades (last 7 days to catch any delayed filings)
    today = date.today()
    from_d = (today.replace(day=max(1, today.day - 7))).strftime("%d-%m-%Y")
    to_d = today.strftime("%d-%m-%Y")
    insider_trades = await nse_client.fetch_insider_trades(from_d, to_d)
    if insider_trades:
        db.insert_insider_trades(insider_trades)
        raw_data["insider_trades"] = insider_trades
        logger.info("  ✓ Insider trades: {} rows", len(insider_trades))
    else:
        raw_data["insider_trades"] = []

    # 4. Corporate announcements (today only)
    announcements = await nse_client.fetch_corporate_announcements(days=1)
    raw_data["announcements"] = announcements
    logger.info("  ✓ Announcements: {} rows", len(announcements))

    # 5. Market status
    market_status = await nse_client.fetch_market_status()
    raw_data["market_status"] = market_status

    logger.success(
        "[{}] Market Scout done: {} price rows, {} bulk deals, {} insider trades",
        state["cycle_id"],
        len(raw_data["bhavcopy"]),
        len(raw_data["bulk_deals"]),
        len(raw_data["insider_trades"]),
    )

    return {"raw_data": raw_data}


async def _fetch_bhavcopy_safe() -> list[dict]:
    """Try today, fall back to yesterday if today unavailable."""
    today = date.today()
    rows = await nse_client.download_bhavcopy(today)
    if not rows:
        from datetime import timedelta
        yesterday = today - timedelta(days=1)
        logger.debug("Today's bhavcopy unavailable, trying yesterday...")
        rows = await nse_client.download_bhavcopy(yesterday)
    return rows


async def _fetch_bulk_deals_safe() -> list[dict]:
    """Try NSE first, fall back to BSE."""
    deals = await nse_client.fetch_bulk_deals()
    if not deals:
        logger.warning("NSE bulk deals failed, trying BSE fallback...")
        deals = await bse_client.fetch_bulk_deals_bse()
    return deals
