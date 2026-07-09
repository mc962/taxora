"""FastAPI dependency for Bearer API-key authentication.

No session/cookie login flow here — the browser UI (`/`) is expected to sit behind Authelia
at the Caddy layer; this only gates the JSON API, which Caddy bypasses Authelia for (machine
clients shouldn't need an Authelia session) and which authenticates via its own API key instead.
"""

from typing import Annotated

from fastapi import BackgroundTasks, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from taxora.data.database.connection import get_db_session
from taxora.data.model.api_key import ApiKey
from taxora.data.repository.api_key_repository import ApiKeyRepository

_bearer = HTTPBearer(auto_error=False)

DatabaseSessionDependency = Annotated[AsyncSession, Depends(get_db_session)]


async def _update_last_used_bg(session: AsyncSession, key_id: int) -> None:
    try:
        await ApiKeyRepository(session).update_last_used(key_id)
    except Exception:
        pass


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


ApiKeyDependency = Annotated[ApiKey, Depends(require_api_key)]
SuperuserApiKeyDependency = Annotated[ApiKey, Depends(require_superuser_api_key)]
