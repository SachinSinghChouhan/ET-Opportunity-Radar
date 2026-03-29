"""
Backtesting script — checks if past signals predicted price direction.

For each opportunity in DB:
  - Gets the close price on signal date
  - Checks close price 5 and 10 trading days later
  - Marks as HIT if price moved in expected direction (BUY→up, AVOID→down)
  - Prints accuracy stats

Run: python scripts/backtest.py
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
from app.database import init_db, get_connection


def get_price_on_date(conn, symbol: str, date_str: str) -> float | None:
    """Get closing price for a symbol on or after a given date."""
    row = conn.execute(
        """SELECT close, date FROM bhavcopy
           WHERE symbol = ? AND date >= ?
           ORDER BY date ASC LIMIT 1""",
        (symbol, date_str),
    ).fetchone()
    return (row["close"], row["date"]) if row else (None, None)


def get_price_after_days(conn, symbol: str, from_date: str, trading_days: int) -> float | None:
    """Get closing price N trading days after from_date."""
    rows = conn.execute(
        """SELECT close, date FROM bhavcopy
           WHERE symbol = ? AND date > ?
           ORDER BY date ASC LIMIT ?""",
        (symbol, from_date, trading_days),
    ).fetchall()
    if len(rows) >= trading_days:
        return rows[-1]["close"], rows[-1]["date"]
    elif rows:
        return rows[-1]["close"], rows[-1]["date"]  # Best available
    return None, None


def run_backtest():
    init_db()
    conn = get_connection()

    # Get all opportunities
    opps = conn.execute(
        """SELECT o.*, o.created_at as signal_date
           FROM opportunities o
           ORDER BY o.created_at DESC""",
    ).fetchall()

    if not opps:
        print("No opportunities in DB to backtest.")
        return

    print(f"\n{'='*60}")
    print(f"OPPORTUNITY RADAR — BACKTEST REPORT")
    print(f"Analysing {len(opps)} historical opportunities")
    print(f"{'='*60}\n")

    results = []

    for opp in opps:
        symbol = opp["symbol"]
        action = opp["action"]
        signal_date = opp["signal_date"][:10] if opp["signal_date"] else None
        confidence = opp["confidence"] or 0.5
        signal_type = opp["signal_type"]

        if not signal_date or action == "WATCH":
            continue  # Skip WATCH — no clear direction

        # Get entry price
        entry_price, entry_date = get_price_on_date(conn, symbol, signal_date)
        if not entry_price:
            continue

        # Get price after 5 trading days
        price_5d, date_5d = get_price_after_days(conn, symbol, entry_date, 5)
        # Get price after 10 trading days
        price_10d, date_10d = get_price_after_days(conn, symbol, entry_date, 10)

        result = {
            "symbol": symbol,
            "action": action,
            "signal_type": signal_type,
            "confidence": confidence,
            "signal_date": signal_date,
            "entry_price": entry_price,
            "price_5d": price_5d,
            "price_10d": price_10d,
            "ret_5d": ((price_5d - entry_price) / entry_price * 100) if price_5d else None,
            "ret_10d": ((price_10d - entry_price) / entry_price * 100) if price_10d else None,
        }

        # Hit/Miss for BUY signals
        if action == "BUY":
            if result["ret_5d"] is not None:
                result["hit_5d"] = result["ret_5d"] > 0
            if result["ret_10d"] is not None:
                result["hit_10d"] = result["ret_10d"] > 0
        elif action == "AVOID":
            if result["ret_5d"] is not None:
                result["hit_5d"] = result["ret_5d"] < 0
            if result["ret_10d"] is not None:
                result["hit_10d"] = result["ret_10d"] < 0

        results.append(result)

    if not results:
        print("Not enough directional signals (BUY/AVOID) with price history to backtest.")
        print("Tip: Run more cycles to build up history, or seed more data.")
        return

    # Print individual results
    print(f"{'Symbol':<14} {'Action':<6} {'Type':<18} {'Conf':>5} {'Entry':>8} {'5D Ret':>8} {'10D Ret':>8} {'5D':>4} {'10D':>4}")
    print("-" * 80)

    hits_5d = misses_5d = hits_10d = misses_10d = 0
    total_ret_5d = total_ret_10d = 0
    count_5d = count_10d = 0

    for r in results:
        ret5 = f"{r['ret_5d']:+.1f}%" if r.get('ret_5d') is not None else "N/A"
        ret10 = f"{r['ret_10d']:+.1f}%" if r.get('ret_10d') is not None else "N/A"
        hit5 = "✓" if r.get("hit_5d") else ("✗" if "hit_5d" in r else "-")
        hit10 = "✓" if r.get("hit_10d") else ("✗" if "hit_10d" in r else "-")

        print(f"{r['symbol']:<14} {r['action']:<6} {r['signal_type']:<18} {r['confidence']:>5.0%} "
              f"₹{r['entry_price']:>7.1f} {ret5:>8} {ret10:>8} {hit5:>4} {hit10:>4}")

        if "hit_5d" in r:
            if r["hit_5d"]:
                hits_5d += 1
            else:
                misses_5d += 1
            if r.get("ret_5d") is not None:
                total_ret_5d += r["ret_5d"]
                count_5d += 1

        if "hit_10d" in r:
            if r["hit_10d"]:
                hits_10d += 1
            else:
                misses_10d += 1
            if r.get("ret_10d") is not None:
                total_ret_10d += r["ret_10d"]
                count_10d += 1

    # Summary stats
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    total_5d = hits_5d + misses_5d
    total_10d = hits_10d + misses_10d

    if total_5d > 0:
        acc_5d = hits_5d / total_5d * 100
        avg_ret_5d = total_ret_5d / count_5d if count_5d else 0
        print(f"5-day  accuracy : {acc_5d:.0f}%  ({hits_5d}/{total_5d} correct)  avg return: {avg_ret_5d:+.1f}%")
    else:
        print("5-day  accuracy : insufficient data")

    if total_10d > 0:
        acc_10d = hits_10d / total_10d * 100
        avg_ret_10d = total_ret_10d / count_10d if count_10d else 0
        print(f"10-day accuracy : {acc_10d:.0f}%  ({hits_10d}/{total_10d} correct)  avg return: {avg_ret_10d:+.1f}%")
    else:
        print("10-day accuracy : insufficient data")

    # By signal type
    print(f"\nBy signal type:")
    by_type: dict[str, dict] = {}
    for r in results:
        t = r["signal_type"]
        if t not in by_type:
            by_type[t] = {"hits": 0, "total": 0}
        if "hit_5d" in r:
            by_type[t]["total"] += 1
            if r["hit_5d"]:
                by_type[t]["hits"] += 1

    for stype, stats in sorted(by_type.items()):
        if stats["total"] > 0:
            acc = stats["hits"] / stats["total"] * 100
            print(f"  {stype:<22} {acc:.0f}% ({stats['hits']}/{stats['total']})")

    conn.close()
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    run_backtest()
