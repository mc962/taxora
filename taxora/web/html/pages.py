"""HTML page routes for the Taxora ingest UI."""

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from taxora.configuration.settings import application_settings
from taxora.web.dependencies.auth import SuperuserSessionDependency

_TEMPLATES_DIR = Path(__file__).parent.parent.parent.parent / "templates"

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory=_TEMPLATES_DIR)


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, _session: SuperuserSessionDependency) -> Response:
    """Simple form for starting an ingest run and checking on existing ones."""
    return templates.TemplateResponse(
        request,
        "index.html",
        {"settings": application_settings},
    )
