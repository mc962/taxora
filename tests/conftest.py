"""Shared fixtures for the Taxora test suite.

No real database is used in tests — Taxora only supports Postgres, and standing one up
just for unit tests is overkill for a Temporal exercise app. The DB session dependency is
mocked instead; repository logic that doesn't need a real engine (`_record_to_row`) is
tested directly in test_occurrence_repository.py.
"""

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from taxora.data.database.connection import get_db_session
from taxora.data.model.api_key import ApiKey
from taxora.main import app
from taxora.web.dependencies.auth import (
    require_api_key,
    require_session_or_api_key,
    require_superuser_session,
)

FAKE_API_KEY = ApiKey(
    id=1,
    name="test-key",
    key_prefix="txra_tes",
    key_hash="",
    is_superuser=True,
    is_active=True,
)


@pytest_asyncio.fixture
async def api_client():
    """FastAPI test client with the DB session, Temporal client, and all auth mocked out.

    Auth is overridden (not exercised) here so every other test can focus on its own concern —
    see test_api_auth.py for tests of the auth dependencies themselves, without this override.
    """
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalar_one.return_value = 0
    mock_session.execute = AsyncMock(return_value=mock_result)

    async def _override_get_db() -> AsyncIterator[MagicMock]:
        yield mock_session

    async def _override_auth() -> ApiKey:
        return FAKE_API_KEY

    app.dependency_overrides[get_db_session] = _override_get_db
    app.dependency_overrides[require_api_key] = _override_auth
    app.dependency_overrides[require_session_or_api_key] = _override_auth
    app.dependency_overrides[require_superuser_session] = _override_auth

    with patch("taxora.main.connect_temporal", new_callable=AsyncMock):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client

    app.dependency_overrides.clear()
