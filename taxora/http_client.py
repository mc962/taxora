"""Shared aiohttp connection pool for outbound HTTP calls (GBIF).

One shared aiohttp.ClientSession, initialized once per worker process, rather than opening
a new connection per activity invocation — matters here since a single ingest run can fire
hundreds of sequential GBIF requests. No base URL or auth headers are set — callers supply
the full URL; base_url is applied at the call site (see engines/gbif_engine.py).
"""

import aiohttp


class AiohttpClientManager:
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
                "aiohttp session is not initialized — call initialize() at startup"
            )
        return self._session


aiohttp_client = AiohttpClientManager()
