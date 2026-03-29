"""
Voice briefing generator — creates MP3 audio summary using gTTS (free).
"""
import os
from pathlib import Path
from loguru import logger


BRIEFINGS_DIR = Path("app/static/briefings")


def generate_voice_briefing(opportunities: list[dict], cycle_id: str) -> str | None:
    """
    Generate a 30-second voice briefing for today's top opportunities.
    Returns path to the generated MP3 file.
    """
    try:
        from gtts import gTTS
    except ImportError:
        logger.warning("gTTS not installed. Skipping voice briefing.")
        return None

    BRIEFINGS_DIR.mkdir(parents=True, exist_ok=True)

    script = _build_script(opportunities)
    if not script:
        return None

    output_path = BRIEFINGS_DIR / f"briefing_{cycle_id}.mp3"
    try:
        tts = gTTS(text=script, lang="en", tld="co.in")  # Indian English accent
        tts.save(str(output_path))
        logger.info("Voice briefing saved: {}", output_path)
        return str(output_path)
    except Exception as e:
        logger.error("Voice briefing generation failed: {}", e)
        return None


def _build_script(opportunities: list[dict]) -> str:
    if not opportunities:
        return "Good morning. No significant market signals detected today. Markets appear calm."

    lines = [
        "Good morning. Here is your Opportunity Radar briefing.",
        f"Today, we have detected {len(opportunities)} significant market signals.",
    ]

    for opp in opportunities[:3]:  # Top 3 only for brevity
        symbol = opp.get("symbol", "")
        action = opp.get("action", "WATCH")
        confidence = int(opp.get("confidence", 0) * 100)
        signal_type = opp.get("signal_type", "").replace("_", " ").title()
        timeframe = opp.get("timeframe", "near term")
        reasoning = opp.get("reasoning_chain", "")[:200]

        lines.append(
            f"Number {opp.get('rank', 1)}: {symbol}. Action: {action}. "
            f"Confidence: {confidence} percent. Signal: {signal_type}. "
            f"Timeframe: {timeframe}. {reasoning}"
        )

    lines.append("This is not financial advice. Always do your own research. Good luck today.")
    return " ".join(lines)
