"""Lazily-connected, process-wide Temporal client for the FastAPI app."""

from temporalio.client import Client

from taxora.configuration.settings import application_settings

_client: Client | None = None


async def connect() -> Client:
    global _client
    if _client is None:
        _client = await Client.connect(application_settings.temporal_address)
    return _client


def get_client() -> Client:
    if _client is None:
        raise RuntimeError("Temporal client not connected — call connect() during startup")
    return _client
