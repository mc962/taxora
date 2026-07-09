"""Taxora — GBIF occurrence ingestion pipeline.

FastAPI application exposing a thin HTTP layer for starting and inspecting Temporal
workflow runs. The actual ingestion logic lives in workflows.py/activities.py, executed
by the separate worker process (worker.py) — this app only talks to Temporal as a client.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from taxora.configuration.settings import application_settings
from taxora.data.database.connection import sessionmanager
from taxora.temporal_client import connect as connect_temporal
from taxora.web.api import health, ingest
from taxora.web.html import pages

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Taxora starting up")
    await connect_temporal()
    yield
    logger.info("Taxora shutting down")
    await sessionmanager.close()


app = FastAPI(
    title="Taxora",
    description="GBIF occurrence ingestion pipeline — a Temporal exercise app.",
    version=application_settings.application_version,
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(ingest.router)
app.include_router(pages.router)
