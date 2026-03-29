"""
Agent 2: Signal Detector — Anomaly Detection
Runs all detectors on ingested data and produces raw signals.
No LLM used here — pure statistical detection.
"""
from loguru import logger

from app.agents.state import OpportunityState
from app.detectors import volume_anomaly, bulk_deal, insider_trade, confluence, price_breakout, result_surprise
from app.config import settings, Severity
from app import database as db


async def signal_detector_agent(state: OpportunityState) -> dict:
    """
    Runs all detectors and collects signals.
    Adds confluence signals for stocks with multiple detections.
    Persists signals to DB.
    """
    cycle_id = state["cycle_id"]
    raw_data = state.get("raw_data", {})
    logger.info("[{}] Signal Detector starting...", cycle_id)

    all_signals = []

    # 1. Volume spike detection (uses bhavcopy + historical DB data)
    bhavcopy = raw_data.get("bhavcopy", [])
    if bhavcopy:
        vol_signals = volume_anomaly.detect(bhavcopy)
        all_signals.extend(vol_signals)
        logger.info("  Volume signals: {}", len(vol_signals))

    # 2. Bulk deal signals
    bulk_deals = raw_data.get("bulk_deals", [])
    if bulk_deals:
        bd_signals = bulk_deal.detect(bulk_deals)
        all_signals.extend(bd_signals)
        logger.info("  Bulk deal signals: {}", len(bd_signals))

    # 3. Insider trade signals
    insider_trades = raw_data.get("insider_trades", [])
    if insider_trades:
        it_signals = insider_trade.detect(insider_trades)
        all_signals.extend(it_signals)
        logger.info("  Insider trade signals: {}", len(it_signals))

    # 4. Price breakout detection
    if bhavcopy:
        pb_signals = price_breakout.detect(bhavcopy)
        all_signals.extend(pb_signals)
        logger.info("  Price breakout signals: {}", len(pb_signals))

    # 5. Result surprise detection
    announcements = raw_data.get("announcements", [])
    if announcements:
        rs_signals = result_surprise.detect(announcements)
        all_signals.extend(rs_signals)
        logger.info("  Result surprise signals: {}", len(rs_signals))

    # 6. Confluence detection (requires signals from steps 1-5)
    if all_signals:
        conf_signals = confluence.detect(all_signals)
        all_signals.extend(conf_signals)
        logger.info("  Confluence signals: {}", len(conf_signals))

    # 5. Filter and cap
    all_signals = _filter_and_rank(all_signals)
    all_signals = all_signals[: settings.max_signals_per_cycle]

    # 6. Persist to DB
    for sig in all_signals:
        db.insert_signal({
            **sig,
            "cycle_id": cycle_id,
            "raw_data": sig.get("raw_data", {}),
            "news_context": None,
            "historical_context": None,
            "narrative": None,
        })

    logger.success(
        "[{}] Signal Detector done: {} total signals",
        cycle_id, len(all_signals),
    )
    return {"signals": all_signals}


def _filter_and_rank(signals: list[dict]) -> list[dict]:
    """
    Remove duplicates (same symbol + same type), sort by severity.
    """
    # Deduplicate: keep highest-severity signal per (symbol, type)
    seen: dict[tuple, dict] = {}
    for sig in signals:
        key = (sig["symbol"], sig["signal_type"])
        existing = seen.get(key)
        if existing is None or _severity_rank(sig["severity"]) > _severity_rank(existing["severity"]):
            seen[key] = sig

    # Sort: HIGH first, then MEDIUM, then LOW
    unique = list(seen.values())
    unique.sort(key=lambda s: _severity_rank(s["severity"]), reverse=True)
    return unique


def _severity_rank(severity: str) -> int:
    return {Severity.HIGH: 3, Severity.MEDIUM: 2, Severity.LOW: 1}.get(severity, 0)
