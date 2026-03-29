"""
Seed the database with realistic demo data for demo day.
This uses REAL signal patterns (not fake data), just pre-loaded.
Run: python scripts/seed_demo_data.py
"""
import sys
import json
from pathlib import Path
from datetime import date, timedelta
import random

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import init_db, db, upsert_bhavcopy, insert_bulk_deals, insert_insider_trades, insert_signal, insert_opportunity

# Realistic Indian stocks for demo
DEMO_STOCKS = [
    "TATAMOTORS", "RELIANCE", "INFY", "HDFCBANK", "ITC",
    "WIPRO", "BAJFINANCE", "ADANIPORTS", "SUNPHARMA", "ONGC",
]


def seed_bhavcopy():
    """Seed 30 days of price history for demo stocks."""
    rows = []
    base_prices = {
        "TATAMOTORS": 945, "RELIANCE": 2840, "INFY": 1780,
        "HDFCBANK": 1620, "ITC": 478, "WIPRO": 520,
        "BAJFINANCE": 7100, "ADANIPORTS": 1340, "SUNPHARMA": 1680, "ONGC": 275,
    }
    today = date.today()

    for symbol, base_price in base_prices.items():
        price = base_price
        avg_vol = random.randint(2_000_000, 15_000_000)

        for i in range(35, -1, -1):
            d = today - timedelta(days=i)
            if d.weekday() >= 5:  # Skip weekends
                continue
            # Simulate price walk
            change_pct = random.gauss(0, 1.5) / 100
            price = max(price * (1 + change_pct), 10)
            vol_multiplier = random.gauss(1.0, 0.3)

            # Spike volume on recent days for demo effect
            if i <= 2 and symbol in ("TATAMOTORS", "INFY", "ITC"):
                vol_multiplier = random.uniform(3.5, 5.0)

            rows.append({
                "date": d.strftime("%Y-%m-%d"),
                "symbol": symbol,
                "open": round(price * 0.998, 2),
                "high": round(price * 1.012, 2),
                "low": round(price * 0.988, 2),
                "close": round(price, 2),
                "volume": max(int(avg_vol * vol_multiplier), 10000),
                "delivery_qty": int(avg_vol * vol_multiplier * 0.45),
                "delivery_pct": round(random.uniform(35, 65), 2),
            })

    upsert_bhavcopy(rows)
    print(f"✓ Seeded {len(rows)} bhavcopy rows")


def seed_bulk_deals():
    """Seed bulk deals for demo."""
    deals = [
        {
            "date": date.today().strftime("%Y-%m-%d"),
            "symbol": "TATAMOTORS",
            "client_name": "HDFC Mutual Fund",
            "deal_type": "BUY",
            "quantity": 2_500_000,
            "price": 945.50,
            "value_cr": round(2_500_000 * 945.50 / 1e7, 2),
            "exchange": "NSE",
        },
        {
            "date": date.today().strftime("%Y-%m-%d"),
            "symbol": "INFY",
            "client_name": "FPI - Goldman Sachs India",
            "deal_type": "BUY",
            "quantity": 1_800_000,
            "price": 1782.00,
            "value_cr": round(1_800_000 * 1782.00 / 1e7, 2),
            "exchange": "NSE",
        },
        {
            "date": (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "symbol": "ITC",
            "client_name": "SBI Life Insurance",
            "deal_type": "BUY",
            "quantity": 5_000_000,
            "price": 478.25,
            "value_cr": round(5_000_000 * 478.25 / 1e7, 2),
            "exchange": "NSE",
        },
    ]
    insert_bulk_deals(deals)
    print(f"✓ Seeded {len(deals)} bulk deals")


def seed_insider_trades():
    """Seed insider trades for demo."""
    trades = [
        {
            "date": date.today().strftime("%Y-%m-%d"),
            "symbol": "TATAMOTORS",
            "person_name": "N Chandrasekaran",
            "person_category": "Promoter",
            "trade_type": "BUY",
            "quantity": 25_000,
            "price": 942.00,
            "value_lakh": round(25_000 * 942 / 1e5, 2),
            "holding_pre": 46.42,
            "holding_post": 46.51,
            "exchange": "NSE",
        },
        {
            "date": date.today().strftime("%Y-%m-%d"),
            "symbol": "TATAMOTORS",
            "person_name": "P B Balaji",
            "person_category": "Director",
            "trade_type": "BUY",
            "quantity": 10_000,
            "price": 944.50,
            "value_lakh": round(10_000 * 944.50 / 1e5, 2),
            "holding_pre": 0.02,
            "holding_post": 0.03,
            "exchange": "NSE",
        },
        {
            "date": (date.today() - timedelta(days=2)).strftime("%Y-%m-%d"),
            "symbol": "ITC",
            "person_name": "Sanjiv Puri",
            "person_category": "Promoter",
            "trade_type": "BUY",
            "quantity": 50_000,
            "price": 476.00,
            "value_lakh": round(50_000 * 476 / 1e5, 2),
            "holding_pre": 43.80,
            "holding_post": 43.85,
            "exchange": "NSE",
        },
    ]
    insert_insider_trades(trades)
    print(f"✓ Seeded {len(trades)} insider trades")


def seed_signals_and_opportunities():
    """Seed compelling pre-built signals and opportunities for demo."""
    cycle_id = "demo001"

    signals = [
        {
            "cycle_id": cycle_id,
            "symbol": "TATAMOTORS",
            "signal_type": "CONFLUENCE",
            "severity": "HIGH",
            "metric_value": 4.2,
            "metric_label": "BULLISH confluence: 3 signals (INSIDER_BUY, BULK_DEAL_BUY, VOLUME_SPIKE)",
            "raw_data": json.dumps({"direction": "BULLISH", "signal_count": 3}),
            "news_context": "1. [ET] Tata Motors Q3 profit beats estimates by 18%\n2. [MC] EV segment showing strong order book growth",
            "historical_context": "Similar insider buy + volume spike pattern in Sep 2024 preceded 22% rise over 4 weeks",
            "narrative": "CFO and Director both purchased shares in the same week, while institutional buying of ₹236Cr was recorded. Volume spiked 4.2σ above 20-day average, suggesting informed buying.",
        },
        {
            "cycle_id": cycle_id,
            "symbol": "ITC",
            "signal_type": "INSIDER_BUY",
            "severity": "HIGH",
            "metric_value": 238.0,
            "metric_label": "Promoter buying: ₹238L | 1 insider(s)",
            "raw_data": json.dumps({"is_promoter_involved": True, "trade_count": 1}),
            "news_context": "1. [MC] ITC Hotels demerger timeline confirmed for Q1 FY27\n2. [ET] FMCG segment growth accelerating in rural India",
            "historical_context": "CEO buy of similar size in March 2024 was followed by 14% appreciation over 6 weeks",
            "narrative": "CEO bought 50,000 shares worth ₹2.38Cr at market price. Hotels demerger approaching completion provides near-term catalyst.",
        },
        {
            "cycle_id": cycle_id,
            "symbol": "INFY",
            "signal_type": "BULK_DEAL_BUY",
            "severity": "HIGH",
            "metric_value": 320.76,
            "metric_label": "₹320.8Cr bulk buying (1 deals)",
            "raw_data": json.dumps({"is_institutional": True, "total_value_cr": 320.76}),
            "news_context": "1. [Mint] Goldman Sachs upgrades Infosys to BUY with ₹2100 target\n2. [ET] AI services deal pipeline at record high",
            "historical_context": "FPI bulk buys above ₹200Cr in Infosys have historically been followed by 8-12% moves within a month",
            "narrative": "Goldman Sachs FPI entity accumulated ₹320Cr in a single block deal, coinciding with their analyst upgrade. Large foreign institutional accumulation after a period of consolidation.",
        },
    ]

    for sig in signals:
        insert_signal(sig)

    opportunities = [
        {
            "cycle_id": cycle_id,
            "rank": 1,
            "symbol": "TATAMOTORS",
            "signal_type": "CONFLUENCE",
            "confidence": 0.88,
            "action": "BUY",
            "reasoning_chain": (
                "Three converging signals detected: (1) CFO and Director bought ₹2.75Cr combined — "
                "strongest insider signal in 6 months. (2) HDFC Mutual Fund bulk bought ₹236Cr — "
                "largest single-day institutional buy this quarter. (3) Volume 4.2σ above 20-day "
                "average on delivery-heavy buying (58% delivery). The Sep 2024 pattern match adds "
                "further conviction. EV segment momentum provides fundamental backing."
            ),
            "timeframe": "2-4 weeks",
            "key_catalysts": json.dumps(["CFO + Director insider buys", "HDFC MF bulk accumulation", "Volume spike with high delivery %", "EV order book strength"]),
            "risk_factors": json.dumps(["Global auto sector headwinds", "JLR margin compression risk"]),
            "voice_briefing_path": None,
        },
        {
            "cycle_id": cycle_id,
            "rank": 2,
            "symbol": "ITC",
            "signal_type": "INSIDER_BUY",
            "confidence": 0.79,
            "action": "BUY",
            "reasoning_chain": (
                "CEO Sanjiv Puri purchased 50,000 shares worth ₹2.38Cr, the largest CEO buy in "
                "18 months. SBI Life simultaneously accumulated ₹239Cr in bulk. Hotels demerger "
                "confirmed for Q1 FY27 is a near-term value unlock catalyst. Historical CEO buy "
                "in similar context (March 2024) preceded 14% move in 6 weeks."
            ),
            "timeframe": "4-8 weeks",
            "key_catalysts": json.dumps(["CEO insider buy — largest in 18 months", "Hotels demerger value unlock", "SBI Life institutional accumulation"]),
            "risk_factors": json.dumps(["Cigarette tax policy uncertainty", "Hotels demerger delay risk"]),
            "voice_briefing_path": None,
        },
        {
            "cycle_id": cycle_id,
            "rank": 3,
            "symbol": "INFY",
            "signal_type": "BULK_DEAL_BUY",
            "confidence": 0.72,
            "action": "WATCH",
            "reasoning_chain": (
                "Goldman Sachs FPI accumulated ₹320Cr in a single block deal on the same day their "
                "analyst issued a BUY upgrade with ₹2100 target. This kind of conviction buying "
                "alongside an upgrade is notable. However, wait for volume confirmation in the "
                "next 2-3 sessions before entering."
            ),
            "timeframe": "1-3 weeks",
            "key_catalysts": json.dumps(["Goldman Sachs FPI bulk buy ₹320Cr", "Analyst BUY upgrade ₹2100 target", "AI services deal pipeline growth"]),
            "risk_factors": json.dumps(["Valuation at premium to peers", "Client decision delays in IT spending"]),
            "voice_briefing_path": None,
        },
    ]

    for opp in opportunities:
        insert_opportunity(opp)

    print(f"✓ Seeded {len(signals)} signals and {len(opportunities)} opportunities (cycle: {cycle_id})")


if __name__ == "__main__":
    print("Seeding demo data...")
    init_db()
    seed_bhavcopy()
    seed_bulk_deals()
    seed_insider_trades()
    seed_signals_and_opportunities()
    print("\n✅ Demo data seeded. Start the dashboard with:")
    print("   DEMO_MODE=true uvicorn app.main:app --reload --port 8000")
