"""
Result Surprise Detector — finds stocks with recent quarterly result announcements.
Parses NSE corporate announcements for result-related keywords.
Flags stocks that announced results for potential earnings surprise plays.
"""
import re
from datetime import datetime, timedelta
from loguru import logger

from app.config import SignalType, Severity

RESULT_KEYWORDS = [
    "financial results", "quarterly results", "unaudited results",
    "audited results", "q1 results", "q2 results", "q3 results", "q4 results",
    "half year", "half-year", "annual results", "standalone results",
    "consolidated results", "board meeting.*results", "results.*board",
]

POSITIVE_KEYWORDS = [
    "profit", "revenue growth", "income", "record", "highest", "strong",
    "beat", "above estimate", "growth", "surplus", "positive",
]

NEGATIVE_KEYWORDS = [
    "loss", "decline", "fall", "drop", "below estimate", "weak",
    "negative", "deficit", "concern",
]

LOOKBACK_DAYS = 2   # Announcements from last 2 days


def detect(announcements: list[dict]) -> list[dict]:
    """
    Scan corporate announcements for result-related content.
    Returns RESULT_SURPRISE signals for stocks with recent results.
    """
    if not announcements:
        return []

    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)
    signals = []
    seen_symbols = set()

    for ann in announcements:
        symbol = ann.get("symbol", "")
        subject = ann.get("subject", "") or ann.get("desc", "") or ""
        body = ann.get("body", "") or ann.get("details", "") or ""
        text = (subject + " " + body).lower()
        date_str = ann.get("date", "")

        if not symbol or symbol in seen_symbols:
            continue

        # Check if it's a result announcement
        is_result = any(re.search(kw, text) for kw in RESULT_KEYWORDS)
        if not is_result:
            continue

        # Check recency
        try:
            ann_date = _parse_date(date_str)
            if ann_date and ann_date < cutoff:
                continue
        except Exception:
            pass  # Include if date parsing fails

        seen_symbols.add(symbol)

        # Sentiment from text
        pos_score = sum(1 for kw in POSITIVE_KEYWORDS if kw in text)
        neg_score = sum(1 for kw in NEGATIVE_KEYWORDS if kw in text)

        if pos_score > neg_score:
            signal_type = SignalType.RESULT_SURPRISE
            severity = Severity.HIGH if pos_score >= 2 else Severity.MEDIUM
            direction = "positive"
        elif neg_score > pos_score:
            signal_type = SignalType.RESULT_SURPRISE
            severity = Severity.MEDIUM
            direction = "negative"
        else:
            signal_type = SignalType.RESULT_SURPRISE
            severity = Severity.LOW
            direction = "neutral"

        # Extract quarter from subject
        quarter = _extract_quarter(text)

        signals.append({
            "symbol": symbol,
            "signal_type": signal_type,
            "severity": severity,
            "metric_value": float(pos_score - neg_score),
            "metric_label": f"Result announcement{' — ' + quarter if quarter else ''} ({direction} tone)",
            "raw_data": {
                "subject": subject[:200],
                "date": date_str,
                "quarter": quarter,
                "direction": direction,
                "positive_signals": pos_score,
                "negative_signals": neg_score,
            },
        })
        logger.debug(
            "Result signal: {} | {} | quarter={} | direction={}",
            symbol, subject[:60], quarter, direction,
        )

    logger.info("Result detector: found {} signals from {} announcements", len(signals), len(announcements))
    return signals


def _parse_date(date_str: str) -> datetime | None:
    if not date_str:
        return None
    for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(date_str[:11], fmt)
        except ValueError:
            continue
    return None


def _extract_quarter(text: str) -> str:
    for q in ["q4", "q3", "q2", "q1"]:
        if q in text:
            return q.upper()
    if "annual" in text or "full year" in text:
        return "FY"
    if "half" in text:
        return "H1/H2"
    return ""
