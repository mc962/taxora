"""Tests for the require_api_key dependency itself — uses its own client fixture
(the shared api_client fixture bypasses auth entirely so other tests don't need a key)."""

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from taxora.data.database.connection import get_db_session
from taxora.data.model.api_key import ApiKey
from taxora.main import app

TEST_KEY_PLAINTEXT = "txra_test0000000000000000000000000000000000000000000000000000"


@pytest_asyncio.fixture
async def unauthed_client():
    """Client with only the DB session and Temporal client mocked — auth runs for real,
    backed by a mocked ApiKeyRepository.verify_key rather than a real database."""

    async def _override_get_db() -> AsyncIterator[MagicMock]:
        yield MagicMock()

    app.dependency_overrides[get_db_session] = _override_get_db

    with patch("taxora.main.connect_temporal", new_callable=AsyncMock):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client

    app.dependency_overrides.clear()


def _mock_verify_key(active: bool = True) -> AsyncMock:
    key = ApiKey(id=1, name="test-key", key_prefix="txra_tes", key_hash="", is_active=active)
    return AsyncMock(return_value=key if active else None)


async def test_missing_auth_returns_401(unauthed_client):
    response = await unauthed_client.post("/ingest", json={"country_code": "IS"})
    assert response.status_code == 401


async def test_malformed_bearer_returns_401(unauthed_client):
    response = await unauthed_client.post(
        "/ingest",
        json={"country_code": "IS"},
        headers={"Authorization": "NotBearer token"},
    )
    assert response.status_code == 401


async def test_invalid_token_returns_401(unauthed_client):
    with patch(
        "taxora.data.repository.api_key_repository.ApiKeyRepository.verify_key",
        new=_mock_verify_key(active=False),
    ):
        response = await unauthed_client.post(
            "/ingest",
            json={"country_code": "IS"},
            headers={"Authorization": f"Bearer {TEST_KEY_PLAINTEXT}"},
        )
    assert response.status_code == 401


async def test_valid_token_passes_auth(unauthed_client):
    with (
        patch(
            "taxora.data.repository.api_key_repository.ApiKeyRepository.verify_key",
            new=_mock_verify_key(active=True),
        ),
        patch(
            "taxora.data.repository.api_key_repository.ApiKeyRepository.update_last_used",
            new=AsyncMock(),
        ),
        patch("taxora.web.api.ingest.get_client") as mock_get_client,
    ):
        mock_get_client.return_value.start_workflow = AsyncMock()
        response = await unauthed_client.post(
            "/ingest",
            json={"country_code": "IS"},
            headers={"Authorization": f"Bearer {TEST_KEY_PLAINTEXT}"},
        )
    assert response.status_code == 200


async def test_unprotected_endpoint_needs_no_key(unauthed_client):
    mock_result = MagicMock()
    mock_result.scalar_one.return_value = 0

    async def _override_get_db() -> AsyncIterator[MagicMock]:
        session = MagicMock()
        session.execute = AsyncMock(return_value=mock_result)
        yield session

    app.dependency_overrides[get_db_session] = _override_get_db
    response = await unauthed_client.get("/records/count")
    assert response.status_code == 200
