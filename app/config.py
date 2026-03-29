from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # LLM
    gemini_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")

    # News
    news_api_key: str = Field(default="", alias="NEWS_API_KEY")

    # App
    demo_mode: bool = Field(default=False, alias="DEMO_MODE")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    db_path: str = Field(default="./data/opportunity_radar.db", alias="DB_PATH")

    # Pipeline
    pipeline_interval_seconds: int = Field(default=300, alias="PIPELINE_INTERVAL_SECONDS")
    max_signals_per_cycle: int = Field(default=20, alias="MAX_SIGNALS_PER_CYCLE")
    top_n_opportunities: int = Field(default=5, alias="TOP_N_OPPORTUNITIES")

    # NSE
    nse_request_delay_seconds: float = Field(default=1.5, alias="NSE_REQUEST_DELAY_SECONDS")
    nse_max_retries: int = Field(default=3, alias="NSE_MAX_RETRIES")


settings = Settings()

# NSE session headers — must match a real browser
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

NSE_BASE_URL = "https://www.nseindia.com"
BSE_BASE_URL = "https://api.bseindia.com"

# Signal type constants
class SignalType:
    VOLUME_SPIKE = "VOLUME_SPIKE"
    BULK_DEAL_BUY = "BULK_DEAL_BUY"
    BULK_DEAL_SELL = "BULK_DEAL_SELL"
    INSIDER_BUY = "INSIDER_BUY"
    INSIDER_SELL = "INSIDER_SELL"
    PROMOTER_HOLDING_UP = "PROMOTER_HOLDING_UP"
    PROMOTER_HOLDING_DOWN = "PROMOTER_HOLDING_DOWN"
    PRICE_BREAKOUT = "PRICE_BREAKOUT"
    RESULT_SURPRISE = "RESULT_SURPRISE"
    CONFLUENCE = "CONFLUENCE"

class Severity:
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"

class Action:
    BUY = "BUY"
    WATCH = "WATCH"
    AVOID = "AVOID"
