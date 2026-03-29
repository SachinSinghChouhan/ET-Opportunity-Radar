"""
Debug script — dumps raw API responses to understand actual field names.
Run: python scripts/debug_api.py
"""
import asyncio
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


async def main():
    from app.tools.nse_client import NSESession
    from app.tools import bse_client

    session = NSESession()

    print("\n" + "="*60)
    print("1. NSE INSIDER TRADES — raw sample (first 2 records)")
    print("="*60)
    try:
        data = await session.get("/api/corporates-pit", params={
            "index": "equities",
            "from_date": "01-03-2026",
            "to_date": "27-03-2026",
        })
        raw = data if isinstance(data, list) else data.get("data", data)
        if raw:
            print(json.dumps(raw[:2], indent=2))
            print(f"\nTotal records: {len(raw)}")
            if raw:
                print(f"Available keys: {list(raw[0].keys())}")
        else:
            print("No data returned. Full response:")
            print(json.dumps(data, indent=2)[:500])
    except Exception as e:
        print(f"ERROR: {e}")

    print("\n" + "="*60)
    print("2. NSE BULK DEALS — raw sample")
    print("="*60)
    try:
        data = await session.get("/api/snapshot-capital-market-largedeal")
        raw = data if isinstance(data, list) else data.get("data", data.get("BLOCK", data))
        if isinstance(raw, list) and raw:
            print(json.dumps(raw[:2], indent=2))
            print(f"\nKeys: {list(raw[0].keys())}")
        else:
            print("Response type:", type(data))
            print(json.dumps(data, indent=2)[:800])
    except Exception as e:
        print(f"ERROR: {e}")

    print("\n" + "="*60)
    print("3. NSE BHAVCOPY URL — checking what's available")
    print("="*60)
    import httpx
    from datetime import date, timedelta
    from app.config import NSE_HEADERS

    for days_ago in range(0, 5):
        d = date.today() - timedelta(days=days_ago)
        date_str = d.strftime("%d%m%Y")
        url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
        try:
            async with httpx.AsyncClient(headers=NSE_HEADERS, timeout=10) as client:
                resp = await client.head(url)
                print(f"  {d} → HTTP {resp.status_code} ({url.split('/')[-1]})")
                if resp.status_code == 200:
                    # Download first few bytes to check column names
                    resp2 = await client.get(url)
                    lines = resp2.text.split('\n')[:3]
                    print(f"    Columns: {lines[0][:200]}")
                    if len(lines) > 1:
                        print(f"    Row 1:   {lines[1][:200]}")
                    break
        except Exception as e:
            print(f"  {d} → ERROR: {e}")

    print("\n" + "="*60)
    print("4. BSE BULK DEALS — raw sample")
    print("="*60)
    try:
        from datetime import date, timedelta
        today = date.today()
        fd = (today - timedelta(days=7)).strftime("%Y%m%d")
        td = today.strftime("%Y%m%d")
        import httpx
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w",
                params={"Ession": "", "Type": "Bulk", "strDate": fd, "strEndDate": td},
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.bseindia.com/"},
            )
            data = resp.json()
        raw = data if isinstance(data, list) else data.get("Table", data)
        if isinstance(raw, list) and raw:
            print(json.dumps(raw[:2], indent=2, default=str))
            print(f"\nKeys: {list(raw[0].keys())}")
        else:
            print("Response:", json.dumps(data, indent=2, default=str)[:500])
    except Exception as e:
        print(f"ERROR: {e}")

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
