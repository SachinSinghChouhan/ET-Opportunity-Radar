"""
FastAPI application — serves the dashboard and API endpoints.
"""
import json
import os
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.database import init_db, get_latest_opportunities, get_latest_signals, get_opportunity_detail
from app.config import settings

scheduler = AsyncIOScheduler()


async def _run_pipeline():
    """Scheduled pipeline run."""
    from app.agents.graph import pipeline
    from app.agents.state import new_state
    state = new_state()
    logger.info("Scheduled pipeline cycle: {}", state["cycle_id"])
    try:
        await pipeline.ainvoke(state)
    except Exception as e:
        logger.error("Scheduled cycle failed: {}", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    # Vercel is a serverless environment — background tasks and schedulers
    # don't persist between requests. Skip them and seed demo data instead.
    is_vercel = IS_VERCEL

    if is_vercel or settings.demo_mode:
        # Auto-seed demo data if the DB is empty
        from app.database import get_latest_opportunities
        if not get_latest_opportunities(limit=1):
            logger.info("Empty DB detected — seeding demo data...")
            try:
                import runpy
                seed_path = ROOT_DIR / "scripts" / "seed_demo_data.py"
                if seed_path.exists():
                    runpy.run_path(str(seed_path), run_name='__main__')
                    logger.info("Demo data seeded successfully.")
            except Exception as e:
                logger.warning("Demo seed failed (non-fatal): {}", e)
    else:
        # Long-running server: start background pipeline + scheduler
        asyncio.create_task(_run_pipeline())
        scheduler.add_job(
            _run_pipeline,
            trigger=IntervalTrigger(seconds=settings.pipeline_interval_seconds),
            id="pipeline",
            replace_existing=True,
        )
        scheduler.start()
        logger.info("Pipeline scheduled every {} seconds.", settings.pipeline_interval_seconds)

    logger.info("Opportunity Radar started.")
    yield
    if not is_vercel and not settings.demo_mode:
        scheduler.shutdown()


app = FastAPI(title="Opportunity Radar", lifespan=lifespan)

# ── Resolve absolute paths (works on Vercel serverless too) ──────────────────
APP_DIR = Path(__file__).parent          # …/app/
ROOT_DIR = APP_DIR.parent               # …/opportunity-radar/
IS_VERCEL = os.environ.get("VERCEL") == "1"

# On Vercel the project FS is read-only except /tmp — put static there
if IS_VERCEL:
    static_dir = Path("/tmp/static")
else:
    static_dir = APP_DIR / "static"
static_dir.mkdir(parents=True, exist_ok=True)

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


# ── Pages ──────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    opportunities = _parse_opportunities(get_latest_opportunities(limit=5))
    signals = get_latest_signals(limit=20)
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "opportunities": opportunities,
            "signals": signals,
            "demo_mode": settings.demo_mode,
        },
    )


@app.get("/stock/{symbol}", response_class=HTMLResponse)
async def stock_detail(request: Request, symbol: str):
    detail = get_opportunity_detail(symbol.upper())
    if not detail:
        return templates.TemplateResponse(
            request=request, name="dashboard.html",
            context={"opportunities": [], "signals": [], "demo_mode": settings.demo_mode, "error": f"No data found for {symbol}"},
        )
    opp = _parse_opportunities([detail["opportunity"]])[0]
    return templates.TemplateResponse(
        request=request,
        name="stock_detail.html",
        context={
            "opp": opp,
            "signals": detail["signals"],
            "price_history": list(reversed(detail["price_history"])),
            "bulk_deals": detail["bulk_deals"],
            "insider_trades": detail["insider_trades"],
            "demo_mode": settings.demo_mode,
        },
    )


# ── HTMX Partials (auto-refresh) ──────────────────────────────────────────────

@app.get("/partials/signals", response_class=HTMLResponse)
async def signal_feed_partial(request: Request):
    signals = get_latest_signals(limit=20)
    return templates.TemplateResponse(
        request=request,
        name="signal_feed.html",
        context={"signals": signals},
    )


@app.get("/partials/opportunities", response_class=HTMLResponse)
async def opportunities_partial(request: Request):
    opportunities = _parse_opportunities(get_latest_opportunities(limit=5))
    return templates.TemplateResponse(
        request=request,
        name="opportunities.html",
        context={"opportunities": opportunities},
    )


def _parse_opportunities(opps: list[dict]) -> list[dict]:
    """Parse JSON string fields from DB into Python lists for templates."""
    for opp in opps:
        for field in ("key_catalysts", "risk_factors"):
            val = opp.get(field)
            if isinstance(val, str):
                try:
                    opp[field] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    opp[field] = []
            elif val is None:
                opp[field] = []
    return opps


# ── API endpoints (JSON) ───────────────────────────────────────────────────────

@app.get("/api/opportunities")
async def api_opportunities():
    return get_latest_opportunities(limit=5)


@app.get("/api/signals")
async def api_signals():
    return get_latest_signals(limit=30)


@app.get("/api/opportunity/{rank}")
async def api_opportunity_detail(rank: int):
    opps = get_latest_opportunities(limit=5)
    for opp in opps:
        if opp.get("rank") == rank:
            return opp
    return {"error": "Not found"}


@app.get("/api/briefing/{cycle_id}")
async def get_briefing(cycle_id: str):
    path = Path(f"app/static/briefings/briefing_{cycle_id}.mp3")
    if path.exists():
        return FileResponse(str(path), media_type="audio/mpeg")
    return {"error": "Briefing not found"}


# ── Pipeline trigger (for manual runs / cron) ─────────────────────────────────

@app.post("/api/run-cycle")
async def run_cycle():
    """Manually trigger one pipeline cycle."""
    if IS_VERCEL or settings.demo_mode:
        return {"status": "unavailable", "message": "Pipeline not available in demo mode."}

    state_ref = {}

    async def _run():
        from app.agents.graph import pipeline
        from app.agents.state import new_state
        state = new_state()
        state_ref["cycle_id"] = state["cycle_id"]
        logger.info("Manual pipeline cycle triggered: {}", state["cycle_id"])
        await pipeline.ainvoke(state)

    asyncio.create_task(_run())
    return {"status": "started", "message": "Pipeline running in background. Refresh in ~2 min."}


@app.get("/api/latest-briefing")
async def latest_briefing():
    """Return the most recent voice briefing file info."""
    briefings_dir = Path("app/static/briefings")
    if not briefings_dir.exists():
        return {"available": False}
    files = sorted(briefings_dir.glob("briefing_*.mp3"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not files:
        return {"available": False}
    latest = files[0]
    cycle_id = latest.stem.replace("briefing_", "")
    return {"available": True, "cycle_id": cycle_id, "url": f"/api/briefing/{cycle_id}"}


@app.get("/health")
async def health():
    return {"status": "ok", "demo_mode": settings.demo_mode}
