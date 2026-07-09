"""Tests for the browser session login/logout flow and the /-page's session gate."""

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from itsdangerous import URLSafeSerializer

from taxora.configuration.settings import application_settings
from taxora.data.database.connection import get_db_session
from taxora.data.model.api_key import ApiKey
from taxora.data.model.session import Session
from taxora.main import app


def _signed_cookie(raw_token: str) -> str:
    serializer = URLSafeSerializer(
        application_settings.session_secret.get_secret_value()
    )
    return serializer.dumps(raw_token)


SUPERUSER_KEY_PLAINTEXT = (
    "txra_admin000000000000000000000000000000000000000000000000000"
)
REGULAR_KEY_PLAINTEXT = "txra_regular00000000000000000000000000000000000000000000000000"


@pytest_asyncio.fixture
async def unauthed_client():
    async def _override_get_db() -> AsyncIterator[MagicMock]:
        yield MagicMock()

    app.dependency_overrides[get_db_session] = _override_get_db

    with patch("taxora.main.connect_temporal", new_callable=AsyncMock):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            follow_redirects=False,
        ) as client:
            yield client

    app.dependency_overrides.clear()


def _mock_verify_key(superuser: bool) -> AsyncMock:
    key = ApiKey(
        id=1,
        name="test-key",
        key_prefix="txra_adm",
        key_hash="",
        is_superuser=superuser,
        is_active=True,
    )
    return AsyncMock(return_value=key)


async def test_index_redirects_to_login_when_unauthenticated(unauthed_client):
    response = await unauthed_client.get("/")
    assert response.status_code == 303
    assert response.headers["location"] == "/login"


async def test_login_page_renders(unauthed_client):
    response = await unauthed_client.get("/login")
    assert response.status_code == 200
    assert "Sign in" in response.text


async def test_login_rejects_invalid_key(unauthed_client):
    with patch(
        "taxora.data.repository.api_key_repository.ApiKeyRepository.verify_key",
        new=AsyncMock(return_value=None),
    ):
        response = await unauthed_client.post("/login", data={"api_key": "txra_bogus"})
    assert response.status_code == 401
    assert "Invalid" in response.text


async def test_login_rejects_non_superuser_key(unauthed_client):
    with patch(
        "taxora.data.repository.api_key_repository.ApiKeyRepository.verify_key",
        new=_mock_verify_key(superuser=False),
    ):
        response = await unauthed_client.post(
            "/login", data={"api_key": REGULAR_KEY_PLAINTEXT}
        )
    assert response.status_code == 401


async def test_login_success_sets_cookie_and_redirects(unauthed_client):
    with (
        patch(
            "taxora.data.repository.api_key_repository.ApiKeyRepository.verify_key",
            new=_mock_verify_key(superuser=True),
        ),
        patch(
            "taxora.data.repository.session_repository.SessionRepository.create",
            new=AsyncMock(return_value="raw-token-123"),
        ),
    ):
        response = await unauthed_client.post(
            "/login", data={"api_key": SUPERUSER_KEY_PLAINTEXT}
        )
    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert "taxora_session" in response.cookies


async def test_session_cookie_grants_access_to_index_and_mutating_endpoint(
    unauthed_client,
):
    fake_session = Session(id=1, session_hash="x", api_key_id=1, expires_at=None)
    fake_key = ApiKey(
        id=1,
        name="admin",
        key_prefix="txra_adm",
        key_hash="",
        is_superuser=True,
        is_active=True,
    )

    with (
        patch(
            "taxora.data.repository.session_repository.SessionRepository.get_by_token",
            new=AsyncMock(return_value=fake_session),
        ),
        patch(
            "taxora.data.repository.api_key_repository.ApiKeyRepository.get_by_id",
            new=AsyncMock(return_value=fake_key),
        ),
    ):
        unauthed_client.cookies.set("taxora_session", _signed_cookie("raw-token-123"))

        index_response = await unauthed_client.get("/")
        assert index_response.status_code == 200

        with patch("taxora.web.api.ingest.get_client") as mock_get_client:
            mock_get_client.return_value.get_workflow_handle.return_value.signal = (
                AsyncMock()
            )
            pause_response = await unauthed_client.post(
                "/ingest/some-workflow-id/pause"
            )
        assert pause_response.status_code == 200


async def test_logout_clears_cookie(unauthed_client):
    mock_delete = AsyncMock()
    with patch(
        "taxora.data.repository.session_repository.SessionRepository.delete_by_token",
        new=mock_delete,
    ):
        unauthed_client.cookies.set("taxora_session", _signed_cookie("raw-token-123"))
        response = await unauthed_client.post("/logout")
    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    mock_delete.assert_awaited_once_with("raw-token-123")
