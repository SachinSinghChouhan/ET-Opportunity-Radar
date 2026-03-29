"""
Confluence detector — finds stocks with MULTIPLE signal types firing simultaneously.
A stock with insider buy + volume spike + bulk deal buy = very high confidence.
This is the most powerful signal in the system.
"""
from collections import defaultdict
from loguru import logger
from app.config import SignalType, Severity

# Signals that are positive (bullish)
BULLISH_SIGNALS = {SignalType.BULK_DEAL_BUY, SignalType.INSIDER_BUY, SignalType.VOLUME_SPIKE, SignalType.PRICE_BREAKOUT}
BEARISH_SIGNALS = {SignalType.BULK_DEAL_SELL, SignalType.INSIDER_SELL}


def detect(all_signals: list[dict]) -> list[dict]:
    """
    Find stocks where 2+ signals fired. Creates a composite CONFLUENCE signal.
    The original signals remain; this adds an extra high-priority signal.
    """
    # Group existing signals by symbol
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for sig in all_signals:
        by_symbol[sig["symbol"]].append(sig)

    confluence_signals = []
    for symbol, sigs in by_symbol.items():
        if len(sigs) < 2:
            continue

        signal_types = {s["signal_type"] for s in sigs}
        bullish = signal_types & BULLISH_SIGNALS
        bearish = signal_types & BEARISH_SIGNALS

        # Only fire if signals are directionally aligned
        if len(bullish) >= 2 and not bearish:
            direction = "BULLISH"
            severity = _count_to_severity(len(bullish))
        elif len(bearish) >= 2 and not bullish:
            direction = "BEARISH"
            severity = _count_to_severity(len(bearish))
        else:
            continue  # Mixed signals — not a clear confluence

        avg_metric = sum(
            s.get("metric_value", 0) for s in sigs
            if s.get("metric_value")
        ) / len(sigs)

        confluence_signals.append({
            "symbol": symbol,
            "signal_type": SignalType.CONFLUENCE,
            "severity": severity,
            "metric_value": round(avg_metric, 2),
            "metric_label": (
                f"{direction} confluence: {len(sigs)} signals "
                f"({', '.join(sorted(signal_types))})"
            ),
            "raw_data": {
                "direction": direction,
                "signal_count": len(sigs),
                "signal_types": list(signal_types),
                "component_signals": [
                    {
                        "type": s["signal_type"],
                        "severity": s["severity"],
                        "label": s.get("metric_label", ""),
                    }
                    for s in sigs
                ],
            },
        })
        logger.info(
            "Confluence: {} | {} | {} signals | {}",
            symbol, direction, len(sigs), list(signal_types),
        )

    if confluence_signals:
        logger.info("Confluence detector: {} composite signals", len(confluence_signals))
    return confluence_signals


def _count_to_severity(count: int) -> str:
    if count >= 3:
        return Severity.HIGH
    return Severity.MEDIUM
