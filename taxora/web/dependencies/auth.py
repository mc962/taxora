"""Auth dependencies: Bearer API keys (scripts/curl/the Schedule) and browser sessions
(the UI's own login, layered underneath Authelia at the Caddy edge — see homelab/manager).

The mutating /ingest* endpoints accept *either* — the UI's own JS calls the same endpoints
scripts do, so require_session_or_api_key lets both through without the UI needing to know
about API keys at all once a human has logged in.
"""

from typing import Annotated

from fastapi import BackgroundTasks, Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from itsdangerous import BadSignature, URLSafeSerializer
from sqlalchemy.ext.asyncio import AsyncSession

from taxora.configuration.settings import application_settings
from taxora.data.database.connection import get_db_session
from taxora.data.model.api_key import ApiKey
from taxora.data.repository.api_key_repository import ApiKeyRepository
from taxora.data.repository.session_repository import SessionRepository

_bearer = HTTPBearer(auto_error=False)

DatabaseSessionDependency = Annotated[AsyncSession, Depends(get_db_session)]

SESSION_COOKIE_NAME = "taxora_session"
SESSION_COOKIE_MAX_AGE = 86400  # 24 hours


class UnauthenticatedRedirect(Exception):
    """Raised by session dependencies to trigger a redirect to /login."""


def _serializer() -> URLSafeSerializer:
    return URLSafeSerializer(application_settings.session_secret.get_secret_value())


async def _update_last_used_bg(session: AsyncSession, key_id: int) -> None:
    try:
        await ApiKeyRepository(session).update_last_used(key_id)
    except Exception:
        pass


async def _key_from_session_cookie(
    request: Request, session: AsyncSession
) -> ApiKey | None:
    """Resolve the ApiKey behind a valid session cookie, or None if missing/invalid."""
    cookie = request.cookies.get(SESSION_COOKIE_NAME)
    if not cookie:
        return None
    try:
        raw_token = _serializer().loads(cookie)
    except BadSignature:
        return None
    sess = await SessionRepository(session).get_by_token(raw_token)
    if sess is None:
        return None
    key = await ApiKeyRepository(session).get_by_id(sess.api_key_id)
    if not key or not key.is_active:
        return None
    return key


async def require_api_key(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    session: DatabaseSessionDependency,
    background_tasks: BackgroundTasks,
) -> ApiKey:
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing API key")
    key = await ApiKeyRepository(session).verify_key(credentials.credentials)
    if not key:
        raise HTTPException(status_code=401, detail="Invalid or inactive API key")
    background_tasks.add_task(_update_last_used_bg, session, key.id)
    return key


async def require_superuser_api_key(
    key: Annotated[ApiKey, Depends(require_api_key)],
) -> ApiKey:
    if not key.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser key required")
    return key


async def require_superuser_session(
    request: Request,
    session: DatabaseSessionDependency,
) -> ApiKey:
    """Gate the browser UI pages. Raises UnauthenticatedRedirect (-> /login), not a 401 —
    this is for pages, not API calls."""
    key = await _key_from_session_cookie(request, session)
    if not key or not key.is_superuser:
        raise UnauthenticatedRedirect()
    return key


async def require_session_or_api_key(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    session: DatabaseSessionDependency,
    background_tasks: BackgroundTasks,
) -> ApiKey:
    """Gate the mutating JSON API. Accepts a logged-in browser session OR a Bearer API key."""
    session_key = await _key_from_session_cookie(request, session)
    if session_key:
        return session_key

    if not credentials:
        raise HTTPException(status_code=401, detail="Missing API key or session")
    key = await ApiKeyRepository(session).verify_key(credentials.credentials)
    if not key:
        raise HTTPException(status_code=401, detail="Invalid or inactive API key")
    background_tasks.add_task(_update_last_used_bg, session, key.id)
    return key


ApiKeyDependency = Annotated[ApiKey, Depends(require_api_key)]
SuperuserApiKeyDependency = Annotated[ApiKey, Depends(require_superuser_api_key)]
SuperuserSessionDependency = Annotated[ApiKey, Depends(require_superuser_session)]
SessionOrApiKeyDependency = Annotated[ApiKey, Depends(require_session_or_api_key)]
