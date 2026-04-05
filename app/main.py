"""
FastAPI application — serves the dashboard and API endpoints.
"""
import json
import os
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, Depends, Form, APIRouter
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from pydantic import BaseModel
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.database import (
    init_db, get_latest_opportunities, get_latest_signals,
    get_opportunity_detail, get_user, create_user,
)
from app.config import settings
from app.auth import require_auth, verify_password, hash_password, create_access_token

# ── Resolve absolute paths ────────────────────────────────────────────────────
APP_DIR = Path(__file__).parent
ROOT_DIR = APP_DIR.parent
IS_VERCEL = os.environ.get("VERCEL") == "1"

scheduler = AsyncIOScheduler()


async def _run_pipeline():
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

    # Create default admin user if not exists
    from app.auth import hash_password as hp
    if not get_user(settings.admin_username):
        create_user(settings.admin_username, hp(settings.admin_password))
        logger.info("Default admin user created: {}", settings.admin_username)

    if IS_VERCEL or settings.demo_mode:
        from app.database import get_latest_opportunities as get_opps
        if not get_opps(limit=1):
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
    if not IS_VERCEL and not settings.demo_mode:
        scheduler.shutdown()


app = FastAPI(title="Opportunity Radar", lifespan=lifespan)

# ── Static files & templates ──────────────────────────────────────────────────
if IS_VERCEL:
    static_dir = Path("/tmp/static")
else:
    static_dir = APP_DIR / "static"
static_dir.mkdir(parents=True, exist_ok=True)

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


# ── Global error handlers ─────────────────────────────────────────────────────

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 401:
        # Browser requests → redirect to login; HTMX/API requests → JSON 401
        is_htmx = request.headers.get("hx-request")
        accept = request.headers.get("accept", "")
        if "text/html" in accept and not is_htmx:
            return RedirectResponse(url="/login", status_code=302)
        return JSONResponse(status_code=401, content={"error": "Not authenticated"})
    if exc.status_code == 404:
        return JSONResponse(status_code=404, content={"error": "Not found"})
    return JSONResponse(status_code=exc.status_code, content={"error": str(exc.detail)})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"error": "Validation error", "details": exc.errors()},
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled exception on {}: {}", request.url.path, exc)
    return JSONResponse(status_code=500, content={"error": "Internal server error"})


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(request=request, name="login.html", context={})


@app.post("/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    user = get_user(username)
    if not user or not verify_password(password, user["password_hash"]):
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": "Invalid username or password"},
            status_code=401,
        )
    token = create_access_token(username, settings.secret_key)
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=60 * 60 * 24,  # 24 hours
    )
    return response


@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("access_token")
    return response


# ── Pages (auth protected) ────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, current_user: str = Depends(require_auth)):
    opportunities = _parse_opportunities(get_latest_opportunities(limit=5))
    signals = get_latest_signals(limit=20)
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "opportunities": opportunities,
            "signals": signals,
            "demo_mode": settings.demo_mode,
            "current_user": current_user,
        },
    )


@app.get("/stock/{symbol}", response_class=HTMLResponse)
async def stock_detail(request: Request, symbol: str, current_user: str = Depends(require_auth)):
    detail = get_opportunity_detail(symbol.upper())
    if not detail:
        return templates.TemplateResponse(
            request=request, name="dashboard.html",
            context={
                "opportunities": [], "signals": [],
                "demo_mode": settings.demo_mode,
                "error": f"No data found for {symbol}",
                "current_user": current_user,
            },
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
            "current_user": current_user,
        },
    )


# ── HTMX Partials (auth protected) ───────────────────────────────────────────

@app.get("/partials/signals", response_class=HTMLResponse)
async def signal_feed_partial(request: Request, current_user: str = Depends(require_auth)):
    signals = get_latest_signals(limit=20)
    return templates.TemplateResponse(
        request=request, name="signal_feed.html", context={"signals": signals},
    )


@app.get("/partials/opportunities", response_class=HTMLResponse)
async def opportunities_partial(request: Request, current_user: str = Depends(require_auth)):
    opportunities = _parse_opportunities(get_latest_opportunities(limit=5))
    return templates.TemplateResponse(
        request=request, name="opportunities.html", context={"opportunities": opportunities},
    )


def _parse_opportunities(opps: list[dict]) -> list[dict]:
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


# ── API v1 (versioned, Pydantic models, JWT auth) ─────────────────────────────

class OpportunityOut(BaseModel):
    symbol: str
    action: str
    confidence: float
    signal_type: str
    timeframe: Optional[str] = None
    reasoning_chain: Optional[str] = None
    key_catalysts: list[str] = []
    risk_factors: list[str] = []
    created_at: Optional[str] = None


class SignalOut(BaseModel):
    symbol: str
    signal_type: str
    severity: str
    metric_label: Optional[str] = None
    metric_value: Optional[float] = None
    detected_at: Optional[str] = None


class CycleResponse(BaseModel):
    status: str
    message: str


api_v1 = APIRouter(prefix="/api/v1", tags=["v1"])


@api_v1.get("/opportunities", response_model=list[OpportunityOut])
async def v1_opportunities(current_user: str = Depends(require_auth)):
    opps = _parse_opportunities(get_latest_opportunities(limit=5))
    return opps


@api_v1.get("/opportunities/{symbol}", response_model=OpportunityOut)
async def v1_opportunity_detail(symbol: str, current_user: str = Depends(require_auth)):
    detail = get_opportunity_detail(symbol.upper())
    if not detail:
        raise StarletteHTTPException(status_code=404, detail=f"No opportunity found for {symbol}")
    opp = _parse_opportunities([detail["opportunity"]])[0]
    return opp


@api_v1.get("/signals", response_model=list[SignalOut])
async def v1_signals(current_user: str = Depends(require_auth)):
    return get_latest_signals(limit=30)


@api_v1.post("/run-cycle", response_model=CycleResponse)
async def v1_run_cycle(current_user: str = Depends(require_auth)):
    if IS_VERCEL or settings.demo_mode:
        return CycleResponse(status="unavailable", message="Pipeline not available in demo mode.")

    async def _run():
        from app.agents.graph import pipeline
        from app.agents.state import new_state
        state = new_state()
        logger.info("Manual pipeline cycle triggered by {}: {}", current_user, state["cycle_id"])
        await pipeline.ainvoke(state)

    asyncio.create_task(_run())
    return CycleResponse(status="started", message="Pipeline running in background. Refresh in ~60s.")


@api_v1.get("/health")
async def v1_health():
    return {"status": "ok", "demo_mode": settings.demo_mode, "version": "1"}


app.include_router(api_v1)


# ── Legacy API endpoints (kept for backward compatibility) ────────────────────

@app.get("/api/opportunities")
async def api_opportunities(current_user: str = Depends(require_auth)):
    return get_latest_opportunities(limit=5)


@app.get("/api/signals")
async def api_signals(current_user: str = Depends(require_auth)):
    return get_latest_signals(limit=30)


@app.post("/api/run-cycle")
async def run_cycle(current_user: str = Depends(require_auth)):
    if IS_VERCEL or settings.demo_mode:
        return {"status": "unavailable", "message": "Pipeline not available in demo mode."}

    async def _run():
        from app.agents.graph import pipeline
        from app.agents.state import new_state
        state = new_state()
        logger.info("Manual pipeline cycle triggered: {}", state["cycle_id"])
        await pipeline.ainvoke(state)

    asyncio.create_task(_run())
    return {"status": "started", "message": "Pipeline running in background. Refresh in ~2 min."}


@app.get("/api/latest-briefing")
async def latest_briefing(current_user: str = Depends(require_auth)):
    briefings_dir = Path("app/static/briefings")
    if not briefings_dir.exists():
        return {"available": False}
    files = sorted(briefings_dir.glob("briefing_*.mp3"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not files:
        return {"available": False}
    latest = files[0]
    cycle_id = latest.stem.replace("briefing_", "")
    return {"available": True, "cycle_id": cycle_id, "url": f"/api/briefing/{cycle_id}"}


@app.get("/api/briefing/{cycle_id}")
async def get_briefing(cycle_id: str, current_user: str = Depends(require_auth)):
    path = Path(f"app/static/briefings/briefing_{cycle_id}.mp3")
    if path.exists():
        return FileResponse(str(path), media_type="audio/mpeg")
    return JSONResponse(status_code=404, content={"error": "Briefing not found"})


@app.get("/health")
async def health():
    return {"status": "ok", "demo_mode": settings.demo_mode}
