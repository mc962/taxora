"""Taxora — GBIF occurrence ingestion pipeline.

FastAPI application exposing a thin HTTP layer for starting and inspecting Temporal
workflow runs. The actual ingestion logic lives in workflows.py/activities.py, executed
by the separate worker process (worker.py) — this app only talks to Temporal as a client.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse

from taxora.configuration.settings import application_settings
from taxora.data.database.connection import sessionmanager
from taxora.data.repository.session_repository import run_periodic_cleanup
from taxora.temporal_client import connect as connect_temporal
from taxora.web.api import health, ingest
from taxora.web.dependencies.auth import UnauthenticatedRedirect
from taxora.web.html import auth as auth_html
from taxora.web.html import pages

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Taxora starting up")
    cleanup_task = asyncio.create_task(run_periodic_cleanup())
    await connect_temporal()
    yield
    logger.info("Taxora shutting down")
    cleanup_task.cancel()
    await sessionmanager.close()


app = FastAPI(
    title="Taxora",
    description="GBIF occurrence ingestion pipeline — a Temporal exercise app.",
    version=application_settings.application_version,
    lifespan=lifespan,
)


@app.exception_handler(UnauthenticatedRedirect)
async def unauthenticated_redirect_handler(
    request: Request, exc: UnauthenticatedRedirect
) -> RedirectResponse:
    return RedirectResponse(url="/login", status_code=303)


app.include_router(health.router)
app.include_router(ingest.router)
app.include_router(pages.router)
app.include_router(auth_html.router)
