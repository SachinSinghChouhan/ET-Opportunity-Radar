"""
News client — fetches relevant news for a given stock from multiple free sources.
Priority: Google News RSS > ET RSS > NewsAPI (rate limited)
"""
import httpx
import feedparser
from datetime import date, timedelta
from loguru import logger

from app.config import settings

RSS_FEEDS = {
    "google": "https://news.google.com/rss/search?q={query}+NSE+stock+India&hl=en-IN&gl=IN&ceid=IN:en",
    "et_markets": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "moneycontrol": "https://www.moneycontrol.com/rss/marketreports.xml",
    "livemint": "https://www.livemint.com/rss/markets",
}

NEWSAPI_URL = "https://newsapi.org/v2/everything"


async def fetch_stock_news(symbol: str, company_name: str = "", max_articles: int = 5) -> list[dict]:
    """
    Fetch recent news for a stock. Returns list of article dicts.
    Falls back gracefully if sources fail.
    """
    query = company_name.strip() if company_name else symbol
    articles = []

    # Try Google News RSS first (best per-stock results, no API key needed)
    google_articles = await _fetch_google_news(query, max_articles)
    articles.extend(google_articles)

    # If not enough, try NewsAPI
    if len(articles) < 3 and settings.news_api_key:
        newsapi_articles = await _fetch_newsapi(query, max_articles - len(articles))
        articles.extend(newsapi_articles)

    # Deduplicate by title
    seen = set()
    unique = []
    for a in articles:
        key = a["title"][:60].lower()
        if key not in seen:
            seen.add(key)
            unique.append(a)

    logger.debug("Fetched {} news articles for {}", len(unique), symbol)
    return unique[:max_articles]


async def fetch_market_news(max_articles: int = 10) -> list[dict]:
    """Fetch general Indian market news."""
    articles = []
    for feed_name in ("et_markets", "moneycontrol"):
        feed_articles = await _fetch_rss_feed(
            RSS_FEEDS[feed_name].format(query=""),
            feed_name,
            max_articles,
        )
        articles.extend(feed_articles)
    return articles[:max_articles]


async def _fetch_google_news(query: str, limit: int) -> list[dict]:
    url = RSS_FEEDS["google"].format(query=query.replace(" ", "+"))
    return await _fetch_rss_feed(url, "google_news", limit)


async def _fetch_rss_feed(url: str, source: str, limit: int) -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        articles = []
        for entry in feed.entries[:limit]:
            articles.append({
                "title": entry.get("title", ""),
                "url": entry.get("link", ""),
                "source": source,
                "published": entry.get("published", ""),
                "summary": entry.get("summary", entry.get("description", ""))[:300],
            })
        return articles
    except Exception as e:
        logger.debug("RSS fetch failed for {}: {}", source, e)
        return []


async def _fetch_newsapi(query: str, limit: int) -> list[dict]:
    if not settings.news_api_key:
        return []
    try:
        today = date.today()
        from_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                NEWSAPI_URL,
                params={
                    "q": f"{query} NSE India",
                    "language": "en",
                    "from": from_date,
                    "sortBy": "publishedAt",
                    "pageSize": limit,
                    "apiKey": settings.news_api_key,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        return [
            {
                "title": a.get("title", ""),
                "url": a.get("url", ""),
                "source": a.get("source", {}).get("name", "newsapi"),
                "published": a.get("publishedAt", ""),
                "summary": (a.get("description") or "")[:300],
            }
            for a in data.get("articles", [])
        ]
    except Exception as e:
        logger.debug("NewsAPI fetch failed: {}", e)
        return []


def format_news_for_llm(articles: list[dict]) -> str:
    """Format news articles into a string for LLM context."""
    if not articles:
        return "No recent news found."
    lines = []
    for i, a in enumerate(articles, 1):
        lines.append(f"{i}. [{a['source']}] {a['title']}")
        if a.get("summary"):
            lines.append(f"   {a['summary'][:200]}")
    return "\n".join(lines)
