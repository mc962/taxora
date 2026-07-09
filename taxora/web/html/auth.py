"""Login/logout HTML routes for the browser UI session."""

import logging
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, URLSafeSerializer

from taxora.configuration.settings import application_settings
from taxora.data.repository.api_key_repository import ApiKeyRepository
from taxora.data.repository.session_repository import SessionRepository
from taxora.web.dependencies.auth import (
    SESSION_COOKIE_MAX_AGE,
    SESSION_COOKIE_NAME,
    DatabaseSessionDependency,
)

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).parent.parent.parent.parent / "templates"

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory=_TEMPLATES_DIR)


def _serializer() -> URLSafeSerializer:
    return URLSafeSerializer(application_settings.session_secret.get_secret_value())


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, db: DatabaseSessionDependency) -> Response:
    """Show the login page, or redirect to / if already authenticated."""
    cookie = request.cookies.get(SESSION_COOKIE_NAME)
    if cookie:
        try:
            raw_token = _serializer().loads(cookie)
            sess = await SessionRepository(db).get_by_token(raw_token)
            if sess is not None:
                return RedirectResponse(url="/", status_code=303)
        except BadSignature:
            pass

    return templates.TemplateResponse(
        request, "login.html", {"error": None, "settings": application_settings}
    )


@router.post("/login")
async def login(
    request: Request,
    db: DatabaseSessionDependency,
    api_key: str = Form(...),
) -> Response:
    """Verify API key, create a session, set the signed cookie, redirect to /."""
    key = await ApiKeyRepository(db).verify_key(api_key.strip())

    if key is None or not key.is_active or not key.is_superuser:
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "error": "Invalid or non-superuser API key.",
                "settings": application_settings,
            },
            status_code=401,
        )

    raw_token = await SessionRepository(db).create(api_key_id=key.id)
    signed_token = _serializer().dumps(raw_token)

    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie(
        SESSION_COOKIE_NAME,
        signed_token,
        httponly=True,
        samesite="lax",
        max_age=SESSION_COOKIE_MAX_AGE,
    )
    return response


@router.post("/logout")
async def logout(request: Request, db: DatabaseSessionDependency) -> Response:
    """Delete the session and clear the cookie."""
    cookie = request.cookies.get(SESSION_COOKIE_NAME)
    if cookie:
        try:
            raw_token = _serializer().loads(cookie)
            await SessionRepository(db).delete_by_token(raw_token)
        except BadSignature:
            pass

    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response
