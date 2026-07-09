"""GET /health — basic liveness check."""

from fastapi import APIRouter

from taxora.configuration.settings import application_settings
from taxora.models import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(status="ok", version=application_settings.application_version)
