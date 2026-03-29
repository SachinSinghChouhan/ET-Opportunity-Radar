"""
Insider trade detector — flags SAST/PIT disclosures.
Promoter buys are bullish signals; promoter sells are bearish.
Clustering (multiple insiders buying) raises severity.
"""
from collections import defaultdict
from loguru import logger
from app.config import SignalType, Severity

MIN_VALUE_LAKH = 10.0    # ₹10 lakh minimum
PROMOTER_CATEGORIES = ("promoter", "promoter group", "director", "key managerial")


def detect(insider_trades: list[dict]) -> list[dict]:
    """
    Analyze insider trades, detect promoter clusters, and generate signals.
    """
    if not insider_trades:
        return []

    # Group by symbol + trade_type
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for trade in insider_trades:
        symbol = trade.get("symbol", "").upper().strip()
        trade_type = trade.get("trade_type", "").upper()
        if symbol and trade_type in ("BUY", "SELL"):
            groups[(symbol, trade_type)].append(trade)

    signals = []
    for (symbol, trade_type), trades in groups.items():
        total_value = sum(t.get("value_lakh", 0) for t in trades)
        total_qty = sum(t.get("quantity", 0) for t in trades)

        if total_value < MIN_VALUE_LAKH and total_qty < 1000:
            continue

        # Check if any are promoters/directors
        is_promoter = any(
            any(cat in (t.get("person_category") or "").lower() for cat in PROMOTER_CATEGORIES)
            for t in trades
        )

        signal_type = (
            SignalType.INSIDER_BUY if trade_type == "BUY" else SignalType.INSIDER_SELL
        )

        # Clustering: multiple insiders = higher severity
        severity = _compute_severity(
            len(trades), total_value, is_promoter, trade_type
        )

        persons = [
            f"{t.get('person_name', 'Unknown')} ({t.get('person_category', '')})"
            for t in trades[:5]
        ]
        holding_change = _compute_holding_change(trades)

        action_word = "buying" if trade_type == "BUY" else "selling"
        signals.append({
            "symbol": symbol,
            "signal_type": signal_type,
            "severity": severity,
            "metric_value": round(total_value, 2),
            "metric_label": (
                f"{'Promoter' if is_promoter else 'Insider'} {action_word}: "
                f"₹{total_value:.1f}L | {len(trades)} insider(s)"
            ),
            "raw_data": {
                "total_value_lakh": round(total_value, 2),
                "total_quantity": total_qty,
                "trade_count": len(trades),
                "is_promoter_involved": is_promoter,
                "persons": persons,
                "holding_change_pct": holding_change,
                "trades": [
                    {
                        "person": t.get("person_name"),
                        "category": t.get("person_category"),
                        "quantity": t.get("quantity"),
                        "price": t.get("price"),
                        "date": t.get("date"),
                    }
                    for t in trades[:5]
                ],
            },
        })
        logger.debug(
            "Insider signal: {} | {} | ₹{:.1f}L | promoter={} | count={}",
            symbol, signal_type, total_value, is_promoter, len(trades),
        )

    logger.info("Insider detector: {} groups → {} signals", len(groups), len(signals))
    return signals


def _compute_severity(
    trade_count: int, value_lakh: float, is_promoter: bool, trade_type: str
) -> str:
    score = 0
    if is_promoter:
        score += 3
    if trade_count >= 3:     # Multiple insiders
        score += 2
    elif trade_count >= 2:
        score += 1
    if value_lakh >= 500:    # ₹5Cr+
        score += 2
    elif value_lakh >= 100:  # ₹1Cr+
        score += 1
    # Promoter sell is more alarming than insider sell
    if trade_type == "SELL" and is_promoter:
        score += 1

    if score >= 5:
        return Severity.HIGH
    if score >= 3:
        return Severity.MEDIUM
    return Severity.LOW


def _compute_holding_change(trades: list[dict]) -> float:
    """Estimate total holding % change from trades."""
    changes = []
    for t in trades:
        pre = t.get("holding_pre") or 0
        post = t.get("holding_post") or 0
        if pre and post:
            changes.append(post - pre)
    return round(sum(changes), 4) if changes else 0.0
