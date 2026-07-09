"""Async SQLAlchemy database session management."""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from taxora.configuration.settings import application_settings


class DatabaseSessionManager:
    def __init__(self, url: str, engine_kwargs: dict | None = None) -> None:
        self._engine: AsyncEngine = create_async_engine(url, **(engine_kwargs or {}))
        self._sessionmaker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            autocommit=False, bind=self._engine
        )

    async def close(self) -> None:
        await self._engine.dispose()

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    def get_engine(self) -> AsyncEngine:
        return self._engine


sessionmanager = DatabaseSessionManager(
    application_settings.database_connection_string,
    {"echo": application_settings.echo_sql},
)


async def get_db_session() -> AsyncIterator[AsyncSession]:
    async with sessionmanager.session() as session:
        yield session
