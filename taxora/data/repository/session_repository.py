"""Repository for browser UI login session management."""

import asyncio
import hashlib
import logging
import secrets
from datetime import datetime, timedelta
from typing import cast

from sqlalchemy import delete, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from taxora.data.database.connection import sessionmanager
from taxora.data.model.session import Session

logger = logging.getLogger(__name__)

SESSION_TTL_HOURS = 24


def generate_session_token() -> str:
    """Generate a random session token."""
    return secrets.token_hex(32)


def hash_token(token: str) -> str:
    """SHA-256 hash of a session token."""
    return hashlib.sha256(token.encode()).hexdigest()


class SessionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, api_key_id: int) -> str:
        """Create a new session. Returns the plaintext token."""
        token = generate_session_token()
        sess = Session(
            session_hash=hash_token(token),
            api_key_id=api_key_id,
            expires_at=datetime.utcnow() + timedelta(hours=SESSION_TTL_HOURS),
        )
        self.session.add(sess)
        await self.session.commit()
        return token

    async def get_by_token(self, token: str) -> Session | None:
        """Look up an active (non-expired) session by its plaintext token."""
        token_hash = hash_token(token)
        result = await self.session.execute(
            select(Session).where(
                Session.session_hash == token_hash,
                Session.expires_at > datetime.utcnow(),
            )
        )
        return result.scalar_one_or_none()

    async def delete_by_token(self, token: str) -> None:
        """Delete a session by its plaintext token."""
        token_hash = hash_token(token)
        await self.session.execute(
            delete(Session).where(Session.session_hash == token_hash)
        )
        await self.session.commit()

    async def cleanup_expired(self) -> int:
        """Delete all expired sessions. Returns count deleted."""
        result = cast(
            CursorResult,
            await self.session.execute(
                delete(Session).where(Session.expires_at < datetime.utcnow())
            ),
        )
        await self.session.commit()
        return result.rowcount


async def run_periodic_cleanup() -> None:
    """Background task: delete expired sessions every hour."""
    cleanup_interval = 3600

    while True:
        try:
            await asyncio.sleep(cleanup_interval)
            async with sessionmanager.session() as session:
                repo = SessionRepository(session)
                deleted = await repo.cleanup_expired()
                if deleted > 0:
                    logger.info("Cleaned up %d expired sessions", deleted)
        except Exception:
            logger.exception("Error in session cleanup task")
