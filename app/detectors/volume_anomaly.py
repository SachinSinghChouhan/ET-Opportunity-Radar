"""
Volume anomaly detector — finds stocks with statistically unusual volume.
Uses z-score: if today's volume > mean + 2*std → signal.
"""
import pandas as pd
import numpy as np
from loguru import logger

from app.config import SignalType, Severity
from app import database as db

ZSCORE_THRESHOLD = 2.0       # Flag if volume > mean + 2σ
MIN_AVG_VOLUME = 50_000      # Skip tiny stocks with negligible volume
MIN_HISTORY_DAYS = 10        # Need at least 10 days to compute stats


def detect(bhavcopy_today: list[dict]) -> list[dict]:
    """
    Compare today's volume for each stock against its 20-day history.
    Returns list of signal dicts for anomalous stocks.
    """
    if not bhavcopy_today:
        return []

    signals = []
    checked = 0

    for row in bhavcopy_today:
        symbol = row.get("symbol", "")
        today_vol = row.get("volume", 0)

        if not symbol or today_vol < MIN_AVG_VOLUME:
            continue

        # Get historical data from DB
        history = db.get_bhavcopy_history(symbol, days=21)
        if len(history) < MIN_HISTORY_DAYS:
            continue

        checked += 1
        vols = np.array([h["volume"] for h in history if h["volume"] > 0])
        if len(vols) < MIN_HISTORY_DAYS:
            continue

        mean_vol = vols.mean()
        std_vol = vols.std()

        if std_vol == 0:
            continue

        zscore = (today_vol - mean_vol) / std_vol

        if zscore >= ZSCORE_THRESHOLD:
            pct_above = ((today_vol - mean_vol) / mean_vol) * 100
            severity = _zscore_to_severity(zscore)

            signals.append({
                "symbol": symbol,
                "signal_type": SignalType.VOLUME_SPIKE,
                "severity": severity,
                "metric_value": round(zscore, 2),
                "metric_label": f"Volume {pct_above:.0f}% above 20-day avg ({zscore:.1f}σ)",
                "raw_data": {
                    "today_volume": today_vol,
                    "avg_volume_20d": int(mean_vol),
                    "zscore": round(zscore, 2),
                    "pct_above_avg": round(pct_above, 1),
                    "close_price": row.get("close", 0),
                    "delivery_pct": row.get("delivery_pct", 0),
                },
            })
            logger.debug(
                "Volume spike: {} | vol={:,} | {:.1f}σ | +{:.0f}%",
                symbol, today_vol, zscore, pct_above,
            )

    logger.info(
        "Volume detector: checked {} stocks, found {} signals",
        checked, len(signals),
    )
    return signals


def _zscore_to_severity(z: float) -> str:
    if z >= 4.0:
        return Severity.HIGH
    if z >= 2.5:
        return Severity.MEDIUM
    return Severity.LOW
