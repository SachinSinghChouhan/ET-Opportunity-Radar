"""
Agent 3: Context Builder — News & Historical Enrichment
Enriches each signal with news articles, historical context, and LLM narrative.
"""
import asyncio
from loguru import logger

from app.agents.state import OpportunityState
from app.tools.news_client import fetch_stock_news, format_news_for_llm
from app import database as db


async def context_builder_agent(state: OpportunityState) -> dict:
    """
    For each signal: fetch news, get historical data, generate LLM narrative.
    Runs enrichment concurrently (max 3 at a time to respect rate limits).
    """
    signals = state.get("signals", [])
    cycle_id = state["cycle_id"]
    logger.info("[{}] Context Builder starting: {} signals to enrich", cycle_id, len(signals))

    if not signals:
        return {"enriched_signals": []}

    # Process 3 signals concurrently (Gemini free tier: 15 RPM)
    semaphore = asyncio.Semaphore(3)
    tasks = [_enrich_signal(sig, semaphore) for sig in signals]
    enriched = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out failed enrichments (keep the signal, just without narrative)
    enriched_signals = []
    for i, result in enumerate(enriched):
        if isinstance(result, Exception):
            logger.warning("Failed to enrich signal {}: {}", signals[i]["symbol"], result)
            enriched_signals.append({**signals[i], "narrative": None, "news_articles": []})
        else:
            enriched_signals.append(result)

    logger.success("[{}] Context Builder done: {} enriched signals", cycle_id, len(enriched_signals))
    return {"enriched_signals": enriched_signals}


async def _enrich_signal(signal: dict, semaphore: asyncio.Semaphore) -> dict:
    """Enrich a single signal with news + history + narrative."""
    async with semaphore:
        symbol = signal["symbol"]

        # 1. Fetch recent news
        try:
            news_articles = await fetch_stock_news(symbol, max_articles=5)
        except Exception as e:
            logger.debug("News fetch failed for {}: {}", symbol, e)
            news_articles = []

        # 2. Get historical price data from DB
        historical_data = db.get_bhavcopy_history(symbol, days=30)

        # 3. Skip per-signal LLM calls — Strategist handles all ranking/reasoning in one call
        narrative = None
        historical_context = None
        news_context = format_news_for_llm(news_articles) if news_articles else None

        enriched = {
            **signal,
            "news_articles": news_articles,
            "news_context": news_context,
            "historical_context": historical_context,
            "narrative": narrative,
            "historical_data_points": len(historical_data),
        }
        logger.debug("Enriched: {} | news={} | hist_days={}", symbol, len(news_articles), len(historical_data))
        return enriched
