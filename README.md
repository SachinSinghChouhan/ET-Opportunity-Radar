# Opportunity Radar
### AI-powered Indian stock market signal detection | ET AI Hackathon 2026

A multi-agent system that monitors NSE/BSE in real time, detects anomalies (bulk deals, insider trades, volume spikes, promoter holding changes), enriches signals with news and historical context, ranks the top 5 actionable opportunities daily with full reasoning chains, and delivers them via a live dashboard and voice briefing.

---

## Architecture

```
Market Scout → Signal Detector → Context Builder → Strategist → Publisher
     ↓               ↓                  ↓               ↓           ↓
  NSE/BSE          pandas/           Gemini +        Gemini       FastAPI
  APIs           scipy stats       News RSS           Pro       Dashboard
```

**5 LangGraph Agents:**
1. **Market Scout** — Fetches bhavcopy, bulk deals, insider trades, announcements from NSE/BSE
2. **Signal Detector** — Statistical anomaly detection (z-scores, threshold rules, confluence)
3. **Context Builder** — Enriches signals with news + historical context via LLM
4. **Strategist** — Ranks and reasons using Gemini Pro, produces confidence-scored opportunities
5. **Publisher** — Updates dashboard, generates voice briefing MP3

---

## Quick Start

```bash
# 1. Clone and setup
git clone <repo>
cd opportunity-radar
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium

# 2. Configure
cp .env.example .env
# Edit .env: add GEMINI_API_KEY (free at https://aistudio.google.com)

# 3. Initialize database
python -c "from app.database import init_db; init_db()"

# 4a. Run with demo data (for testing)
python scripts/seed_demo_data.py
DEMO_MODE=true uvicorn app.main:app --reload --port 8000

# 4b. Run with live data
python scripts/run_cycle.py        # single cycle
python scripts/scheduler.py       # runs every 5 minutes
uvicorn app.main:app --reload --port 8000
```

Open `http://localhost:8000`

---

## Data Sources

| Data | Source | Method |
|------|--------|--------|
| Daily price (OHLCV) | NSE Bhavcopy Archive | HTTP download |
| Bulk/Block deals | NSE + BSE APIs | Session-based JSON API |
| Insider trades (SAST) | NSE PIT API | Session-based JSON API |
| Corporate announcements | NSE + BSE APIs | Session-based JSON API |
| Shareholding patterns | NSE Shareholding API | Session-based JSON API |
| News | Google News RSS + ET RSS + NewsAPI | RSS / REST API |
| Historical prices | jugaad-data / yfinance | Python library |

All sources are free. No paid subscriptions required.

---

## Tech Stack

- **Agents:** LangGraph + LangChain
- **LLM:** Gemini 1.5 Flash (free, 15 RPM) + Gemini 1.5 Pro for reasoning
- **Data:** httpx, pandas, numpy, scipy, jugaad-data, yfinance
- **Storage:** SQLite (zero setup)
- **Frontend:** FastAPI + Jinja2 + HTMX + Tailwind CSS CDN
- **Voice:** gTTS (Google Text-to-Speech, free)
- **Cost:** $0

---

## Signal Types

| Signal | Detection Method | Significance |
|--------|-----------------|--------------|
| `VOLUME_SPIKE` | Z-score > 2σ vs 20-day average | Unusual market attention |
| `BULK_DEAL_BUY` | Institutional buy > ₹5Cr | Smart money entering |
| `BULK_DEAL_SELL` | Large sell > ₹5Cr | Smart money exiting |
| `INSIDER_BUY` | Promoter/Director purchases | Informed buying |
| `INSIDER_SELL` | Promoter/Director sales | Informed selling |
| `CONFLUENCE` | 2+ bullish signals on same stock | High-conviction signal |

---

## Project Structure

```
opportunity-radar/
├── app/
│   ├── agents/          # 5 LangGraph agents
│   ├── tools/           # NSE/BSE clients, LLM, news, voice
│   ├── detectors/       # Statistical signal detectors
│   └── templates/       # Dashboard HTML
├── scripts/
│   ├── run_cycle.py     # Manual pipeline trigger
│   ├── scheduler.py     # Auto-run every N minutes
│   └── seed_demo_data.py # Demo data seeder
└── tests/
```

---

## Running Tests

```bash
pytest tests/ -v
```

---

*Built for ET AI Hackathon 2026 — PS6: AI for the Indian Investor*
