from unittest.mock import MagicMock, patch

import pytest

from taxora.engines import gbif_engine
from taxora.http_client import aiohttp_client


class _FakeResponse:
    def __init__(
        self, status: int, json_body: dict | None = None, text_body: str = ""
    ) -> None:
        self.status = status
        self._json_body = json_body
        self._text_body = text_body

    async def json(self) -> dict:
        return self._json_body or {}

    async def text(self) -> str:
        return self._text_body


class _FakeGetContextManager:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    async def __aenter__(self) -> _FakeResponse:
        return self._response

    async def __aexit__(self, *args: object) -> None:
        return None


@pytest.fixture(autouse=True)
def fake_session():
    """Stand in for the shared aiohttp session so gbif_engine doesn't need a real one."""
    session = MagicMock()
    with patch.object(aiohttp_client, "_session", session):
        yield session


async def test_fetch_page_success(fake_session):
    response = _FakeResponse(
        200,
        json_body={
            "offset": 0,
            "limit": 2,
            "endOfRecords": True,
            "count": 1,
            "results": [{"key": 1}],
        },
    )
    fake_session.get = MagicMock(return_value=_FakeGetContextManager(response))

    page = await gbif_engine.fetch_page("IS", 0, 2, base_url="https://api.gbif.org/v1")

    assert page["results"] == [{"key": 1}]
    assert page["endOfRecords"] is True


async def test_fetch_page_raises_on_error_status(fake_session):
    response = _FakeResponse(429, text_body="rate limited")
    fake_session.get = MagicMock(return_value=_FakeGetContextManager(response))

    with pytest.raises(RuntimeError, match="429"):
        await gbif_engine.fetch_page("IS", 0, 2, base_url="https://api.gbif.org/v1")
