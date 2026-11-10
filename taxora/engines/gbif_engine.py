"""Async GBIF occurrence-search HTTP client.

No API key required. See https://techdocs.gbif.org/en/openapi/v1/occurrence for the API this
wraps. Deliberately raises on any non-2xx response rather than swallowing errors — the caller
(a Temporal activity) relies on the exception propagating so the workflow's retry policy can
handle GBIF's real-world rate limiting/timeouts.

Uses the shared aiohttp session (taxora.http_client) rather than opening a new connection per
call — a single ingest run can fire hundreds of sequential requests.
"""

from typing import Any

import aiohttp

from taxora.http_client import aiohttp_tcp_client


async def fetch_page(
    country_code: str,
    offset: int,
    limit: int,
    *,
    base_url: str,
    event_date_range: str | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Fetch one page of GBIF occurrence-search results for a country.

    Args:
        event_date_range: Optional GBIF `eventDate` filter, e.g. "2020-01-01,2020-12-31" or
            a single date. See https://techdocs.gbif.org/en/openapi/v1/occurrence for format.

    Returns:
        The raw GBIF response envelope (``offset``, ``limit``, ``endOfRecords``, ``count``,
        ``results``).

    Raises:
        RuntimeError: On HTTP error or connection failure.
    """
    params: dict[str, str | int] = {
        "country": country_code,
        "offset": offset,
        "limit": limit,
    }
    if event_date_range:
        params["eventDate"] = event_date_range

    session = aiohttp_tcp_client.client()
    async with session.get(
        f"{base_url}/occurrence/search",
        params=params,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as response:
        if response.status != 200:
            body = await response.text()
            raise RuntimeError(f"GBIF returned HTTP {response.status}: {body[:200]}")
        result: dict[str, Any] = await response.json()
        return result
