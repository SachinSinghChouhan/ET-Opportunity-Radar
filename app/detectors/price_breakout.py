"""
Price Breakout Detector — finds stocks hitting N-day highs/lows.
Uses bhavcopy history from DB. Signals when price breaks above rolling high.
"""
from loguru import logger

from app.config import SignalType, Severity
from app import database as db

LOOKBACK_DAYS = 30        # N-day high/low window
MIN_HISTORY_DAYS = 5      # Need at least 5 days of data to be useful
BREAKOUT_PCT = 0.995      # Within 0.5% of high counts as breakout


def detect(bhavcopy_today: list[dict]) -> list[dict]:
    """
    For each stock, check if today's close is at/near N-day high.
    Returns PRICE_BREAKOUT signals.
    """
    if not bhavcopy_today:
        return []

    signals = []

    for row in bhavcopy_today:
        symbol = row.get("symbol", "")
        today_close = row.get("close", 0)
        today_high = row.get("high", 0)

        if not symbol or today_close <= 0:
            continue

        history = db.get_bhavcopy_history(symbol, days=LOOKBACK_DAYS + 1)
        if len(history) < MIN_HISTORY_DAYS:
            continue

        closes = [h["close"] for h in history if h.get("close", 0) > 0]
        highs = [h["high"] for h in history if h.get("high", 0) > 0]
        lows = [h["low"] for h in history if h.get("low", 0) > 0]

        if not closes:
            continue

        period_high = max(highs) if highs else max(closes)
        period_low = min(lows) if lows else min(closes)
        prev_close = closes[0] if closes else today_close  # most recent historical close

        # Breakout above N-day high
        if today_close >= period_high * BREAKOUT_PCT and today_close > prev_close:
            pct_from_high = ((today_close - period_high) / period_high) * 100
            days_of_data = len(history)
            severity = Severity.HIGH if today_close >= period_high else Severity.MEDIUM

            signals.append({
                "symbol": symbol,
                "signal_type": SignalType.PRICE_BREAKOUT,
                "severity": severity,
                "metric_value": round(today_close, 2),
                "metric_label": f"{days_of_data}-day high breakout at ₹{today_close:.1f} ({pct_from_high:+.1f}% from high)",
                "raw_data": {
                    "today_close": today_close,
                    "today_high": today_high,
                    "period_high": round(period_high, 2),
                    "period_low": round(period_low, 2),
                    "pct_from_high": round(pct_from_high, 2),
                    "days_of_data": days_of_data,
                    "prev_close": round(prev_close, 2),
                    "change_pct": round(((today_close - prev_close) / prev_close) * 100, 2) if prev_close else 0,
                },
            })
            logger.debug(
                "Price breakout: {} | ₹{:.1f} vs {}-day high ₹{:.1f}",
                symbol, today_close, days_of_data, period_high,
            )

    logger.info("Price breakout detector: found {} signals", len(signals))
    return signals
