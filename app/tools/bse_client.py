"""
BSE Client — fallback data source when NSE is unavailable.
BSE API is more lenient with rate limits and doesn't require cookie sessions.
"""
import httpx
from datetime import date, timedelta
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import BSE_BASE_URL

BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
async def _bse_get(endpoint: str, params: dict | None = None) -> dict | list:
    url = f"{BSE_BASE_URL}{endpoint}"
    async with httpx.AsyncClient(headers=BSE_HEADERS, timeout=30.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


async def fetch_bulk_deals_bse(from_date: str | None = None, to_date: str | None = None) -> list[dict]:
    """Fetch bulk/block deals from BSE."""
    today = date.today()
    fd = from_date or (today - timedelta(days=7)).strftime("%Y%m%d")
    td = to_date or today.strftime("%Y%m%d")

    try:
        data = await _bse_get(
            "/BseIndiaAPI/api/DefaultData/w",
            params={
                "Ession": "",
                "Type": "Bulk",
                "strDate": fd,
                "strEndDate": td,
            },
        )
        raw = data if isinstance(data, list) else data.get("Table", [])
        results = []
        for item in raw:
            qty = _parse_int(item.get("mQty", item.get("Qty", 0)))
            price = _parse_float(item.get("Price", 0))
            results.append({
                "date": _normalize_date(item.get("DT_DATE", item.get("Date", ""))),
                "symbol": str(item.get("scrip_cd", item.get("SYMBOL", ""))).strip().upper(),
                "client_name": str(item.get("clientN", item.get("ClientName", ""))).strip(),
                "deal_type": str(item.get("buySell", item.get("BuySell", ""))).upper(),
                "quantity": qty,
                "price": price,
                "value_cr": round((qty * price) / 1e7, 2),
                "exchange": "BSE",
            })
        logger.info("Fetched {} bulk deals from BSE", len(results))
        return results
    except Exception as e:
        logger.error("Failed to fetch BSE bulk deals: {}", e)
        return []


async def fetch_announcements_bse(days: int = 1) -> list[dict]:
    """Fetch corporate announcements from BSE."""
    today = date.today()
    prev = (today - timedelta(days=days)).strftime("%Y%m%d")
    td_str = today.strftime("%Y%m%d")

    try:
        data = await _bse_get(
            "/BseIndiaAPI/api/AnnGetData/w",
            params={
                "strCat": "-1",
                "strPrevDate": prev,
                "strScrip": "",
                "strSearch": "P",
                "strToDate": td_str,
                "strType": "C",
            },
        )
        raw = data if isinstance(data, list) else data.get("Table", [])
        results = []
        for item in raw:
            results.append({
                "date": _normalize_date(item.get("News_submission_dt", "")),
                "symbol": str(item.get("SCRIP_CD", item.get("symbol", ""))).strip().upper(),
                "subject": str(item.get("NEWSSUB", item.get("Subject", ""))).strip(),
                "description": str(item.get("HEADLINE", "")).strip(),
                "attachment_url": str(item.get("ATTACHMENTNAME", "")).strip(),
                "exchange": "BSE",
            })
        logger.info("Fetched {} announcements from BSE", len(results))
        return results
    except Exception as e:
        logger.error("Failed to fetch BSE announcements: {}", e)
        return []


async def fetch_shareholding_bse(scrip_code: str) -> dict | None:
    """Fetch shareholding pattern from BSE by scrip code."""
    try:
        data = await _bse_get(
            f"/BseIndiaAPI/api/ShareholdingPattern/w",
            params={"scripcode": scrip_code, "qtrid": ""},
        )
        raw = data if isinstance(data, list) else data.get("Table", [])
        if not raw:
            return None
        item = raw[0]
        return {
            "quarter": str(item.get("QTR_DT", "")),
            "symbol": scrip_code,
            "promoter_pct": _parse_float(item.get("PROMOTERS", 0)),
            "promoter_pledged_pct": _parse_float(item.get("PROMOTERS_PLEDGED", 0)),
            "fii_pct": _parse_float(item.get("FII", 0)),
            "dii_pct": _parse_float(item.get("DII", 0)),
            "public_pct": _parse_float(item.get("PUBLIC", 0)),
        }
    except Exception as e:
        logger.error("Failed to fetch BSE shareholding for {}: {}", scrip_code, e)
        return None


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


def _normalize_date(val: str) -> str:
    from datetime import datetime
    if not val:
        return date.today().strftime("%Y-%m-%d")
    val = str(val).strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(val[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return val[:10]
