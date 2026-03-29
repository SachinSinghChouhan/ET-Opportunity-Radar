"""
LLM client — uses local Ollama (qwen2.5:7b) with Gemini/OpenAI as fallback.
Uses structured output (Pydantic) for reliable agent responses.
"""
import json
from loguru import logger
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings

OLLAMA_MODEL = "qwen2.5:7b"
OLLAMA_BASE_URL = "http://localhost:11434"


class SignalNarrative(BaseModel):
    summary: str
    key_points: list[str]
    risk_factors: list[str]
    historical_context: str


class RankedOpportunity(BaseModel):
    rank: int
    symbol: str
    signal_type: str
    confidence: float          # 0.0 – 1.0
    action: str                # BUY / WATCH / AVOID
    reasoning_chain: str
    timeframe: str
    key_catalysts: list[str]
    risk_factors: list[str]


class StrategistOutput(BaseModel):
    opportunities: list[RankedOpportunity]
    market_summary: str


def _get_ollama_llm():
    """Return a LangChain ChatOllama instance."""
    from langchain_ollama import ChatOllama
    return ChatOllama(
        model=OLLAMA_MODEL,
        base_url=OLLAMA_BASE_URL,
        temperature=0.3,
        format="json",
    )


def _get_gemini_llm(model: str = "gemini-2.5-flash"):
    from langchain_google_genai import ChatGoogleGenerativeAI
    return ChatGoogleGenerativeAI(
        model=model,
        google_api_key=settings.gemini_api_key,
        temperature=0.3,
        max_retries=2,
    )


def _get_openai_llm(model: str = "gpt-4o-mini"):
    from langchain_openai import ChatOpenAI
    return ChatOpenAI(
        model=model,
        api_key=settings.openai_api_key,
        temperature=0.3,
        max_retries=2,
    )


def get_llm(use_pro: bool = False):
    """
    Returns LLM. Priority: Gemini → Ollama → OpenAI.
    use_pro=True uses Gemini 2.5 Pro, False uses Gemini 2.5 Flash.
    """
    if settings.gemini_api_key:
        model = "gemini-2.5-flash"  # Flash works on free tier; Pro needs billing
        logger.debug("Using Gemini: {}", model)
        return _get_gemini_llm(model)

    # Ollama fallback
    try:
        import httpx
        resp = httpx.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=2.0)
        if resp.status_code == 200:
            models = [m["name"] for m in resp.json().get("models", [])]
            if any(OLLAMA_MODEL in m for m in models):
                logger.debug("Using Ollama: {}", OLLAMA_MODEL)
                return _get_ollama_llm()
    except Exception:
        pass

    if settings.openai_api_key:
        logger.debug("Using OpenAI")
        return _get_openai_llm()

    raise RuntimeError("No LLM available. Set GEMINI_API_KEY in .env or start Ollama.")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15))
async def generate_signal_narrative(
    symbol: str,
    signal_type: str,
    signal_data: dict,
    news_articles: list[dict],
    historical_data: list[dict],
) -> SignalNarrative:
    """
    Use LLM to generate a narrative explanation for a detected signal.
    Called by Context Builder agent.
    """
    from langchain_core.prompts import ChatPromptTemplate

    llm = get_llm(use_pro=False)

    news_text = "\n".join(
        f"- {a['title']}" for a in news_articles[:5]
    ) or "No recent news found."

    hist_text = _format_historical(historical_data)

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a senior Indian stock market analyst. Provide concise, factual analysis. Never give buy/sell advice. State only what the data shows."),
        ("human", """Analyze this market signal for Indian stock {symbol}:

SIGNAL TYPE: {signal_type}
SIGNAL DATA: {signal_data}

RECENT NEWS:
{news_text}

HISTORICAL PRICE CONTEXT (last 10 days):
{hist_text}

Provide:
1. A 2-3 sentence summary of what this signal means
2. 3-4 key data points supporting the signal
3. 2-3 risk factors / alternative explanations
4. Any historical pattern match (e.g., "Similar pattern in Oct 2024 preceded X% move")

Be specific and data-driven. Avoid speculation.

Respond in JSON format matching this schema:
{{"summary": "...", "key_points": ["...", "..."], "risk_factors": ["...", "..."], "historical_context": "..."}}"""),
    ])

    chain = prompt | llm.with_structured_output(SignalNarrative)
    result = await chain.ainvoke({
        "symbol": symbol,
        "signal_type": signal_type,
        "signal_data": json.dumps(signal_data, indent=2),
        "news_text": news_text,
        "hist_text": hist_text,
    })
    return result


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=30))
async def rank_and_reason(enriched_signals: list[dict], top_n: int = 5) -> StrategistOutput:
    """
    Use LLM to rank signals and generate full reasoning chains.
    Called by Strategist agent.
    """
    from langchain_core.prompts import ChatPromptTemplate

    llm = get_llm(use_pro=True)

    signals_text = json.dumps(enriched_signals, indent=2)

    prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a quantitative analyst specializing in Indian equity markets.
Your job is to rank market signals by their potential significance for retail investors.

Scoring criteria:
- Signal strength (how anomalous is the data?)
- Signal confluence (multiple signals on same stock = higher confidence)
- News context alignment (does news support or contradict the signal?)
- Historical pattern match (has this pattern led to price moves before?)
- Risk/reward balance

Confidence score guide:
- 0.85-1.0: Multiple converging signals, strong historical match, news aligned
- 0.70-0.84: Strong primary signal with supporting context
- 0.55-0.69: Interesting signal but limited supporting evidence
- Below 0.55: Noise — don't include in top 5

Action guide:
- BUY: Strong positive signals, upside catalyst identified
- WATCH: Signals present but insufficient conviction, monitor closely
- AVOID: Negative signals or red flags (promoter selling, bad results)"""),

        ("human", """Rank these {n_signals} detected market signals and identify the top {top_n} opportunities.

SIGNALS:
{signals_text}

Return ONLY valid JSON matching this exact schema:
{{"opportunities": [{{"rank": 1, "symbol": "...", "signal_type": "...", "confidence": 0.75, "action": "BUY", "reasoning_chain": "...", "timeframe": "1-2 weeks", "key_catalysts": ["..."], "risk_factors": ["..."]}}], "market_summary": "..."}}

Return exactly {top_n} opportunities. Base ALL reasoning on actual data provided."""),
    ])

    chain = prompt | llm.with_structured_output(StrategistOutput)
    result = await chain.ainvoke({
        "n_signals": len(enriched_signals),
        "top_n": top_n,
        "signals_text": signals_text,
    })
    logger.info("Strategist ranked {} → {} opportunities", len(enriched_signals), len(result.opportunities))
    return result


def _format_historical(historical_data: list[dict]) -> str:
    if not historical_data:
        return "No historical data available."
    lines = []
    for row in historical_data[:10]:
        lines.append(
            f"  {row.get('date', '')}: O={row.get('open', 0):.1f} "
            f"H={row.get('high', 0):.1f} L={row.get('low', 0):.1f} "
            f"C={row.get('close', 0):.1f} Vol={row.get('volume', 0):,}"
        )
    return "\n".join(lines)
