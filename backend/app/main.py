"""FastAPI application entry point with scheduler lifecycle."""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.config import settings
from app.services.allegro_client import close_session
from app.sniper import scheduler, timing

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

app = FastAPI(
    title="LastBid API",
    description="Allegro auction sniping backend",
    version="1.0.0",
    docs_url="/docs" if settings.environment == "development" else None,
    redoc_url=None,
)

_cors_origins = list({settings.frontend_url, "http://localhost:3000"})

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


logger = logging.getLogger(__name__)


@app.on_event("startup")
async def startup():
    import os
    # Log all env var NAMES (not values) to see what Railway passes
    safe_keys = [k for k in os.environ if "ALLEGRO" in k or "SUPABASE" in k or "ENVIRONMENT" in k or "FRONTEND" in k]
    logger.info("Visible env vars: %s", safe_keys)
    logger.info("ALLEGRO_REDIRECT_URI (settings) = %s", settings.allegro_redirect_uri)
    logger.info("ALLEGRO_REDIRECT_URI (os.environ) = %s", os.environ.get("ALLEGRO_REDIRECT_URI", "NOT_IN_ENVIRON"))

    # NTP sync before anything else
    await timing.sync_ntp_async()

    # Start background scheduler
    scheduler.start()

    # Schedule periodic NTP re-sync every hour
    s = scheduler.get_scheduler()
    s.add_job(
        timing.sync_ntp_async,
        "interval",
        hours=1,
        id="ntp_sync",
        replace_existing=True,
    )


@app.on_event("shutdown")
async def shutdown():
    scheduler.stop()
    await close_session()
