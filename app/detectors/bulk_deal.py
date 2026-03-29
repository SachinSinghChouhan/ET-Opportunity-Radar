"""
Bulk deal detector — flags significant institutional buy/sell activity.
A deal is significant if: value >= threshold OR it's a known institution.
"""
from loguru import logger
from app.config import SignalType, Severity

MIN_DEAL_VALUE_CR = 5.0      # ₹5 crore minimum to consider
LARGE_DEAL_VALUE_CR = 50.0   # ₹50 crore → HIGH severity

# Known institutional categories in bulk deals
INSTITUTIONAL_KEYWORDS = (
    "mutual fund", "mf", "fii", "fpi", "insurance", "lic", "hdfc", "icici",
    "sbi", "axis", "kotak", "nippon", "uti", "franklin", "dsp", "birla",
    "pension", "fund", "asset management", "amc",
)


def detect(bulk_deals: list[dict]) -> list[dict]:
    """
    Analyze bulk deals and return signals for large/significant deals.
    Groups multiple deals on the same stock for confluence detection.
    """
    if not bulk_deals:
        return []

    # Group by symbol
    by_symbol: dict[str, list[dict]] = {}
    for deal in bulk_deals:
        symbol = deal.get("symbol", "").upper().strip()
        if not symbol:
            continue
        by_symbol.setdefault(symbol, []).append(deal)

    signals = []
    for symbol, deals in by_symbol.items():
        buy_deals = [d for d in deals if d.get("deal_type", "").upper() in ("BUY", "B")]
        sell_deals = [d for d in deals if d.get("deal_type", "").upper() in ("SELL", "S")]

        # Skip block trade arbitrage: both BUY and SELL present with similar value
        # (same-day offsetting trades are not actionable signals)
        buy_val = sum(d.get("value_cr", 0) for d in buy_deals)
        sell_val = sum(d.get("value_cr", 0) for d in sell_deals)
        if buy_val > 0 and sell_val > 0:
            ratio = min(buy_val, sell_val) / max(buy_val, sell_val)
            if ratio > 0.85:  # >85% match = block trade, skip both sides
                logger.debug("Skipping block trade arbitrage: {} (buy={:.1f}Cr sell={:.1f}Cr)", symbol, buy_val, sell_val)
                continue

        for deal_group, signal_type in [
            (buy_deals, SignalType.BULK_DEAL_BUY),
            (sell_deals, SignalType.BULK_DEAL_SELL),
        ]:
            if not deal_group:
                continue

            total_value = sum(d.get("value_cr", 0) for d in deal_group)
            total_qty = sum(d.get("quantity", 0) for d in deal_group)
            avg_price = sum(d.get("price", 0) for d in deal_group) / len(deal_group)
            clients = [d.get("client_name", "") for d in deal_group]
            is_institutional = any(
                any(kw in c.lower() for kw in INSTITUTIONAL_KEYWORDS)
                for c in clients
            )

            # Only signal if meets minimum threshold
            if total_value < MIN_DEAL_VALUE_CR and not is_institutional:
                continue

            severity = _value_to_severity(total_value, is_institutional)
            action_word = "buying" if signal_type == SignalType.BULK_DEAL_BUY else "selling"

            signals.append({
                "symbol": symbol,
                "signal_type": signal_type,
                "severity": severity,
                "metric_value": round(total_value, 2),
                "metric_label": f"₹{total_value:.1f}Cr bulk {action_word} ({len(deal_group)} deals)",
                "raw_data": {
                    "total_value_cr": round(total_value, 2),
                    "total_quantity": total_qty,
                    "avg_price": round(avg_price, 2),
                    "deal_count": len(deal_group),
                    "clients": clients[:5],
                    "is_institutional": is_institutional,
                    "deals": deal_group[:5],
                },
            })
            logger.debug(
                "Bulk deal signal: {} | {} | ₹{:.1f}Cr | institutional={}",
                symbol, signal_type, total_value, is_institutional,
            )

    logger.info("Bulk deal detector: {} symbols → {} signals", len(by_symbol), len(signals))
    return signals


def _value_to_severity(value_cr: float, is_institutional: bool) -> str:
    if value_cr >= LARGE_DEAL_VALUE_CR or (is_institutional and value_cr >= 20):
        return Severity.HIGH
    if value_cr >= 20 or is_institutional:
        return Severity.MEDIUM
    return Severity.LOW
