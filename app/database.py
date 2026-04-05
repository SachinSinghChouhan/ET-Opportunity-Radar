import sqlite3
import json
from pathlib import Path
from contextlib import contextmanager
from loguru import logger
from app.config import settings


def get_db_path() -> str:
    path = Path(settings.db_path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        # Read-only filesystem (e.g. Vercel serverless) — fall back to /tmp
        path = Path("/tmp/opportunity_radar.db")
    return str(path)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables. Safe to call multiple times (CREATE IF NOT EXISTS)."""
    logger.info("Initializing database...")
    with db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS bhavcopy (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                delivery_qty INTEGER,
                delivery_pct REAL,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, symbol)
            );

            CREATE TABLE IF NOT EXISTS bulk_deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                client_name TEXT,
                deal_type TEXT,
                quantity INTEGER,
                price REAL,
                value_cr REAL,
                exchange TEXT DEFAULT 'NSE',
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS insider_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                person_name TEXT,
                person_category TEXT,
                trade_type TEXT,
                quantity INTEGER,
                price REAL,
                value_lakh REAL,
                holding_pre REAL,
                holding_post REAL,
                exchange TEXT DEFAULT 'NSE',
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS shareholding (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quarter TEXT NOT NULL,
                symbol TEXT NOT NULL,
                promoter_pct REAL,
                promoter_pledged_pct REAL,
                fii_pct REAL,
                dii_pct REAL,
                public_pct REAL,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(quarter, symbol)
            );

            CREATE TABLE IF NOT EXISTS announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                subject TEXT,
                description TEXT,
                attachment_url TEXT,
                exchange TEXT DEFAULT 'NSE',
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                symbol TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                severity TEXT DEFAULT 'MEDIUM',
                metric_value REAL,
                metric_label TEXT,
                raw_data TEXT,
                news_context TEXT,
                historical_context TEXT,
                narrative TEXT
            );

            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rank INTEGER,
                symbol TEXT NOT NULL,
                signal_type TEXT,
                confidence REAL,
                action TEXT,
                reasoning_chain TEXT,
                timeframe TEXT,
                key_catalysts TEXT,
                risk_factors TEXT,
                voice_briefing_path TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_bhavcopy_symbol ON bhavcopy(symbol);
            CREATE INDEX IF NOT EXISTS idx_bhavcopy_date ON bhavcopy(date);
            CREATE INDEX IF NOT EXISTS idx_bulk_deals_date ON bulk_deals(date);
            CREATE INDEX IF NOT EXISTS idx_insider_date ON insider_trades(date);
            CREATE INDEX IF NOT EXISTS idx_signals_cycle ON signals(cycle_id);
            CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
            CREATE INDEX IF NOT EXISTS idx_opportunities_cycle ON opportunities(cycle_id);
            CREATE INDEX IF NOT EXISTS idx_opportunities_date ON opportunities(created_at);

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    logger.success("Database initialized at {}", get_db_path())


# ── Query helpers ──────────────────────────────────────────────────────────────

def upsert_bhavcopy(rows: list[dict]):
    if not rows:
        return
    with db() as conn:
        conn.executemany(
            """INSERT OR REPLACE INTO bhavcopy
               (date, symbol, open, high, low, close, volume, delivery_qty, delivery_pct)
               VALUES (:date, :symbol, :open, :high, :low, :close, :volume, :delivery_qty, :delivery_pct)""",
            rows,
        )
    logger.debug("Upserted {} bhavcopy rows", len(rows))


def insert_bulk_deals(rows: list[dict]):
    if not rows:
        return
    with db() as conn:
        conn.executemany(
            """INSERT INTO bulk_deals
               (date, symbol, client_name, deal_type, quantity, price, value_cr, exchange)
               VALUES (:date, :symbol, :client_name, :deal_type, :quantity, :price, :value_cr, :exchange)""",
            rows,
        )
    logger.debug("Inserted {} bulk deal rows", len(rows))


def insert_insider_trades(rows: list[dict]):
    if not rows:
        return
    with db() as conn:
        conn.executemany(
            """INSERT INTO insider_trades
               (date, symbol, person_name, person_category, trade_type,
                quantity, price, value_lakh, holding_pre, holding_post, exchange)
               VALUES (:date, :symbol, :person_name, :person_category, :trade_type,
                       :quantity, :price, :value_lakh, :holding_pre, :holding_post, :exchange)""",
            rows,
        )
    logger.debug("Inserted {} insider trade rows", len(rows))


def upsert_shareholding(rows: list[dict]):
    if not rows:
        return
    with db() as conn:
        conn.executemany(
            """INSERT OR REPLACE INTO shareholding
               (quarter, symbol, promoter_pct, promoter_pledged_pct, fii_pct, dii_pct, public_pct)
               VALUES (:quarter, :symbol, :promoter_pct, :promoter_pledged_pct, :fii_pct, :dii_pct, :public_pct)""",
            rows,
        )


def insert_signal(signal: dict) -> int:
    with db() as conn:
        cur = conn.execute(
            """INSERT INTO signals
               (cycle_id, symbol, signal_type, severity, metric_value, metric_label,
                raw_data, news_context, historical_context, narrative)
               VALUES (:cycle_id, :symbol, :signal_type, :severity, :metric_value,
                       :metric_label, :raw_data, :news_context, :historical_context, :narrative)""",
            {
                **signal,
                "raw_data": json.dumps(signal.get("raw_data", {})),
            },
        )
        return cur.lastrowid


def insert_opportunity(opp: dict) -> int:
    row = dict(opp)
    # Serialize lists to JSON strings for storage
    for field in ("key_catalysts", "risk_factors"):
        val = row.get(field)
        if isinstance(val, list):
            row[field] = json.dumps(val)
        elif val is None:
            row[field] = json.dumps([])
    with db() as conn:
        cur = conn.execute(
            """INSERT INTO opportunities
               (cycle_id, rank, symbol, signal_type, confidence, action,
                reasoning_chain, timeframe, key_catalysts, risk_factors, voice_briefing_path)
               VALUES (:cycle_id, :rank, :symbol, :signal_type, :confidence, :action,
                       :reasoning_chain, :timeframe, :key_catalysts, :risk_factors, :voice_briefing_path)""",
            row,
        )
        return cur.lastrowid


def get_latest_opportunities(limit: int = 5) -> list[dict]:
    with db() as conn:
        rows = conn.execute(
            """SELECT * FROM opportunities
               ORDER BY created_at DESC, rank ASC
               LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_latest_signals(limit: int = 30) -> list[dict]:
    with db() as conn:
        rows = conn.execute(
            """SELECT * FROM signals
               ORDER BY detected_at DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_bhavcopy_history(symbol: str, days: int = 30) -> list[dict]:
    with db() as conn:
        rows = conn.execute(
            """SELECT * FROM bhavcopy
               WHERE symbol = ?
               ORDER BY date DESC
               LIMIT ?""",
            (symbol, days),
        ).fetchall()
    return [dict(r) for r in rows]


def get_recent_bulk_deals(days: int = 7) -> list[dict]:
    with db() as conn:
        rows = conn.execute(
            """SELECT * FROM bulk_deals
               WHERE date >= date('now', ? || ' days')
               ORDER BY date DESC""",
            (f"-{days}",),
        ).fetchall()
    return [dict(r) for r in rows]


def get_recent_insider_trades(days: int = 30) -> list[dict]:
    with db() as conn:
        rows = conn.execute(
            """SELECT * FROM insider_trades
               WHERE date >= date('now', ? || ' days')
               ORDER BY date DESC""",
            (f"-{days}",),
        ).fetchall()
    return [dict(r) for r in rows]


def get_user(username: str) -> dict | None:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,)
        ).fetchone()
    return dict(row) if row else None


def create_user(username: str, password_hash: str):
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (username, password_hash) VALUES (?, ?)",
            (username, password_hash),
        )


def get_opportunity_detail(symbol: str) -> dict | None:
    """Get latest opportunity + all recent signals for a symbol."""
    with db() as conn:
        opp = conn.execute(
            """SELECT * FROM opportunities WHERE symbol = ?
               ORDER BY created_at DESC LIMIT 1""",
            (symbol,),
        ).fetchone()
        if not opp:
            return None

        signals = conn.execute(
            """SELECT * FROM signals WHERE symbol = ?
               ORDER BY detected_at DESC LIMIT 10""",
            (symbol,),
        ).fetchall()

        price_history = conn.execute(
            """SELECT date, open, high, low, close, volume FROM bhavcopy
               WHERE symbol = ?
               ORDER BY date DESC LIMIT 30""",
            (symbol,),
        ).fetchall()

        bulk = conn.execute(
            """SELECT * FROM bulk_deals WHERE symbol = ?
               ORDER BY date DESC LIMIT 10""",
            (symbol,),
        ).fetchall()

        insider = conn.execute(
            """SELECT * FROM insider_trades WHERE symbol = ?
               ORDER BY date DESC LIMIT 10""",
            (symbol,),
        ).fetchall()

    return {
        "opportunity": dict(opp),
        "signals": [dict(r) for r in signals],
        "price_history": [dict(r) for r in price_history],
        "bulk_deals": [dict(r) for r in bulk],
        "insider_trades": [dict(r) for r in insider],
    }
