"""
NSE Client — handles cookie-based session, rate limiting, and all NSE API endpoints.
All NSE endpoints require: visit homepage first to get cookies, then use those cookies.
"""
import asyncio
import httpx
from datetime import datetime, date, timedelta
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.config import NSE_HEADERS, NSE_BASE_URL, settings


class NSERateLimitError(Exception):
    pass


class NSESession:
    """Singleton-style async session with cookie management and rate limiting."""

    def __init__(self):
        self._client: httpx.AsyncClient | None = None
        self._cookies_set = False
        self._lock = asyncio.Lock()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers=NSE_HEADERS,
                follow_redirects=True,
                timeout=httpx.Timeout(30.0),
            )
            self._cookies_set = False
        return self._client

    async def _ensure_cookies(self):
        """Hit NSE homepage to get session cookies."""
        if not self._cookies_set:
            client = await self._get_client()
            logger.debug("Fetching NSE cookies...")
            await client.get(NSE_BASE_URL)
            await asyncio.sleep(1.0)
            self._cookies_set = True
            logger.debug("NSE cookies set")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
    )
    async def get(self, endpoint: str, params: dict | None = None) -> dict | list:
        async with self._lock:
            await self._ensure_cookies()
            await asyncio.sleep(settings.nse_request_delay_seconds)

        client = await self._get_client()
        url = f"{NSE_BASE_URL}{endpoint}"
        logger.debug("NSE GET {}", endpoint)

        resp = await client.get(url, params=params)

        if resp.status_code == 401:
            logger.warning("NSE 401 — refreshing cookies and retrying")
            self._cookies_set = False
            await self._ensure_cookies()
            resp = await client.get(url, params=params)

        if resp.status_code == 429:
            logger.warning("NSE 429 — rate limited, backing off 30s")
            await asyncio.sleep(30)
            raise NSERateLimitError("NSE rate limit hit")

        resp.raise_for_status()
        return resp.json()

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# Module-level singleton
_session = NSESession()


async def get_session() -> NSESession:
    return _session


# ── Data fetch functions ───────────────────────────────────────────────────────

async def fetch_bulk_deals(from_date: str | None = None, to_date: str | None = None) -> list[dict]:
    """
    Fetch bulk/block deals from NSE.
    Dates in format: DD-MM-YYYY
    Returns list of deal dicts.
    """
    session = await get_session()
    try:
        if from_date and to_date:
            data = await session.get(
                "/api/historical/bulk-deals",
                params={"from": from_date, "to": to_date},
            )
        else:
            data = await session.get("/api/snapshot-capital-market-largedeal")

        # NSE returns bulk deals under "BULK_DEALS_DATA" key
        raw = (
            data.get("BULK_DEALS_DATA")
            or data.get("data")
            or data.get("BLOCK")
            or (data if isinstance(data, list) else [])
        )
        results = []
        for item in raw:
            try:
                qty = _parse_int(item.get("qty", item.get("BD_QTY_TRD", item.get("mQty", 0))))
                price = _parse_float(item.get("watp", item.get("BD_TP_WATP", item.get("price", 0))))
                results.append({
                    "date": _normalize_date(item.get("date", item.get("BD_DT_DATE", ""))),
                    "symbol": str(item.get("symbol", item.get("BD_SYMBOL", ""))).strip().upper(),
                    "client_name": str(item.get("clientName", item.get("BD_CLIENT_NAME", ""))).strip(),
                    "deal_type": str(item.get("buySell", item.get("BD_BUY_SELL", ""))).upper(),
                    "quantity": qty,
                    "price": price,
                    "value_cr": round((qty * price) / 1e7, 2),
                    "exchange": "NSE",
                })
            except Exception as e:
                logger.debug("Skipping bulk deal row: {}", e)
        logger.info("Fetched {} bulk deals from NSE", len(results))
        return results
    except Exception as e:
        logger.error("Failed to fetch NSE bulk deals: {}", e)
        return []


async def fetch_insider_trades(from_date: str | None = None, to_date: str | None = None) -> list[dict]:
    """
    Fetch insider/PIT trades from NSE.
    """
    session = await get_session()
    today = date.today()
    fd = from_date or (today - timedelta(days=30)).strftime("%d-%m-%Y")
    td = to_date or today.strftime("%d-%m-%Y")

    try:
        data = await session.get(
            "/api/corporates-pit",
            params={"index": "equities", "from_date": fd, "to_date": td},
        )
        raw = data if isinstance(data, list) else data.get("data", [])
        results = []
        for item in raw:
            try:
                # Real NSE PIT API field names (verified from live response):
                # tdpTransactionType: "Buy" or "Sell"
                # acqName: person name
                # personCategory: "Promoters", "Promoter Group", etc.
                # secAcq: quantity acquired/disposed
                # secVal: transaction value in rupees
                # befAcqSharesPer / afterAcqSharesPer: holding % before/after
                trade_type = _parse_trade_type(
                    item.get("tdpTransactionType", item.get("buyOrSell", ""))
                )
                qty = _parse_int(item.get("secAcq", 0))
                val_rupees = _parse_float(item.get("secVal", 0))
                results.append({
                    "date": _normalize_date(item.get("acqfromDt", item.get("date", ""))),
                    "symbol": str(item.get("symbol", "")).strip().upper(),
                    "person_name": str(item.get("acqName", item.get("personName", ""))).strip(),
                    "person_category": str(item.get("personCategory", item.get("category", ""))).strip(),
                    "trade_type": trade_type,
                    "quantity": qty,
                    "price": round(val_rupees / qty, 2) if qty > 0 else 0.0,
                    "value_lakh": round(val_rupees / 1e5, 2),
                    "holding_pre": _parse_float(item.get("befAcqSharesPer", 0)),
                    "holding_post": _parse_float(item.get("afterAcqSharesPer", 0)),
                    "exchange": "NSE",
                })
            except Exception as e:
                logger.debug("Skipping insider trade row: {}", e)
        logger.info("Fetched {} insider trades from NSE", len(results))
        return results
    except Exception as e:
        logger.error("Failed to fetch NSE insider trades: {}", e)
        return []


async def fetch_corporate_announcements(symbol: str | None = None, days: int = 1) -> list[dict]:
    """Fetch corporate filings and announcements."""
    session = await get_session()
    today = date.today()
    fd = (today - timedelta(days=days)).strftime("%d-%m-%Y")
    td = today.strftime("%d-%m-%Y")

    params = {"index": "equities", "from_date": fd, "to_date": td}
    if symbol:
        params["symbol"] = symbol.upper()

    try:
        data = await session.get("/api/corporate-announcements", params=params)
        raw = data if isinstance(data, list) else data.get("data", [])
        results = []
        for item in raw:
            results.append({
                "date": _normalize_date(item.get("exchdisstime", item.get("date", ""))),
                "symbol": str(item.get("symbol", "")).strip().upper(),
                "subject": str(item.get("subject", item.get("desc", ""))).strip(),
                "description": str(item.get("desc", "")).strip(),
                "attachment_url": str(item.get("attchmntFile", "")).strip(),
                "exchange": "NSE",
            })
        logger.info("Fetched {} announcements from NSE", len(results))
        return results
    except Exception as e:
        logger.error("Failed to fetch NSE announcements: {}", e)
        return []


async def fetch_shareholding(symbol: str) -> dict | None:
    """Fetch latest shareholding pattern for a symbol."""
    session = await get_session()
    try:
        data = await session.get(
            "/api/corporate-shareholding",
            params={"symbol": symbol.upper(), "isseries": "EQUITY"},
        )
        # Extract latest quarter data
        raw = data if isinstance(data, list) else data.get("data", [])
        if not raw:
            return None
        latest = raw[0] if raw else {}
        return {
            "quarter": str(latest.get("quarter", "")),
            "symbol": symbol.upper(),
            "promoter_pct": _parse_float(latest.get("promoter", 0)),
            "promoter_pledged_pct": _parse_float(latest.get("promoterPledged", 0)),
            "fii_pct": _parse_float(latest.get("fii", 0)),
            "dii_pct": _parse_float(latest.get("dii", 0)),
            "public_pct": _parse_float(latest.get("public", 0)),
        }
    except Exception as e:
        logger.error("Failed to fetch shareholding for {}: {}", symbol, e)
        return None


async def fetch_bhavcopy_url(trade_date: date | None = None) -> str:
    """Return bhavcopy download URL for the given date."""
    d = trade_date or date.today()
    # NSE bhavcopy archive URL pattern
    date_str = d.strftime("%d%m%Y")
    return f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"


async def download_bhavcopy(trade_date: date | None = None) -> list[dict]:
    """Download and parse NSE bhavcopy CSV for the given date."""
    import io
    import pandas as pd

    d = trade_date or date.today()
    url = await fetch_bhavcopy_url(d)

    try:
        async with httpx.AsyncClient(headers=NSE_HEADERS, timeout=60.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        df = pd.read_csv(io.BytesIO(resp.content))
        df.columns = [c.strip().upper() for c in df.columns]

        # Normalize column names — verified from live NSE bhavcopy:
        # SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
        # LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS,
        # NO_OF_TRADES, DELIV_QTY, DELIV_PER
        col_map = {
            "SYMBOL": "symbol",
            "OPEN_PRICE": "open", "OPEN": "open",
            "HIGH_PRICE": "high", "HIGH": "high",
            "LOW_PRICE": "low", "LOW": "low",
            "CLOSE_PRICE": "close", "CLOSE": "close",  # prefer CLOSE_PRICE
            "TTL_TRD_QNTY": "volume", "TOTAL_TRADED_QUANTITY": "volume",
            "DELIV_QTY": "delivery_qty", "DELIVERABLE_QTY": "delivery_qty",
            "DELIV_PER": "delivery_pct", "% _DELIVERABLE": "delivery_pct",
        }
        # Drop LAST_PRICE if CLOSE_PRICE already exists to avoid duplicate columns
        if "CLOSE_PRICE" in df.columns and "LAST_PRICE" in df.columns:
            df.drop(columns=["LAST_PRICE"], inplace=True)
        df.rename(columns=col_map, inplace=True)

        required = {"symbol", "open", "high", "low", "close", "volume"}
        available = set(df.columns)
        if not required.issubset(available):
            logger.warning("Bhavcopy missing columns: {}", required - available)

        date_str = d.strftime("%Y-%m-%d")
        rows = []
        for _, row in df.iterrows():
            try:
                rows.append({
                    "date": date_str,
                    "symbol": str(row.get("symbol", "")).strip().upper(),
                    "open": float(row.get("open", 0) or 0),
                    "high": float(row.get("high", 0) or 0),
                    "low": float(row.get("low", 0) or 0),
                    "close": float(row.get("close", 0) or 0),
                    "volume": int(row.get("volume", 0) or 0),
                    "delivery_qty": int(row.get("delivery_qty", 0) or 0),
                    "delivery_pct": float(row.get("delivery_pct", 0) or 0),
                })
            except Exception:
                pass

        logger.info("Downloaded bhavcopy for {}: {} rows", date_str, len(rows))
        return rows

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.warning("Bhavcopy not available for {} (market holiday?)", d)
        else:
            logger.error("Failed to download bhavcopy: {}", e)
        return []
    except Exception as e:
        logger.error("Failed to parse bhavcopy: {}", e)
        return []


async def fetch_market_status() -> dict:
    """Check if market is currently open."""
    session = await get_session()
    try:
        data = await session.get("/api/marketStatus")
        return data
    except Exception:
        return {"marketState": [{"marketStatus": "Unknown"}]}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_float(val) -> float:
    try:
        return float(str(val).replace(",", "").strip() or 0)
    except (ValueError, TypeError):
        return 0.0


def _parse_int(val) -> int:
    try:
        return int(float(str(val).replace(",", "").strip() or 0))
    except (ValueError, TypeError):
        return 0


def _parse_trade_type(val: str) -> str:
    v = str(val).strip().upper()
    if v in ("BUY", "B", "ACQ", "ACQUISITION", "PURCHASE", "MARKET PURCHASE", "OFF MARKET PURCHASE"):
        return "BUY"
    if v in ("SELL", "S", "DISP", "DISPOSAL", "SALE", "MARKET SALE", "OFF MARKET SALE"):
        return "SELL"
    return v


def _normalize_date(val: str) -> str:
    """Normalize various date formats to YYYY-MM-DD."""
    if not val:
        return date.today().strftime("%Y-%m-%d")
    val = str(val).strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%b %d, %Y"):
        try:
            return datetime.strptime(val[:11], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return val[:10]  # best effort
