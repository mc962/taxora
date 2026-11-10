"""Shared HTTP connection pools.

  aiohttp_tcp_client — shared aiohttp.ClientSession over TCP (default connector)

Call initialize() / close() on each manager at process startup/shutdown (see worker.py).

Consumers import only the manager(s) they need. Base URLs and auth headers are applied
at the call site (see engines/gbif_engine.py), keeping these pools generic and reusable.
One shared session matters here since a single ingest run can fire hundreds of sequential
GBIF requests.
"""

from abc import ABC, abstractmethod
from typing import Any

import aiohttp


class HttpClientManagerBase(ABC):
    """Lifecycle interface for all HTTP connection pool managers."""

    @abstractmethod
    async def initialize(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    def client(self) -> Any:
        """Return the underlying client or session."""
        ...


class AiohttpTcpClientManager(HttpClientManagerBase):
    """One shared aiohttp.ClientSession with the default TCPConnector.

    No base URL or auth headers are set — callers supply the full URL and
    any required headers per request. A 30s default timeout is set; callers
    can override per-request via timeout=aiohttp.ClientTimeout(...).
    """

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def initialize(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30.0),
        )

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def client(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError(
                "aiohttp TCP session is not initialized — call initialize() at startup"
            )
        return self._session


aiohttp_tcp_client = AiohttpTcpClientManager()
