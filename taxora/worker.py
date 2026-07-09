import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from taxora.activities import fetch_gbif_page, normalize_country_code, persist_occurrences
from taxora.configuration.settings import application_settings
from taxora.http_client import aiohttp_client
from taxora.workflows import BatchIngestWorkflow, IngestOccurrencesWorkflow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("Connecting to Temporal at %s", application_settings.temporal_address)
    client = await Client.connect(application_settings.temporal_address)

    await aiohttp_client.initialize()
    try:
        worker = Worker(
            client,
            task_queue=application_settings.temporal_task_queue,
            workflows=[IngestOccurrencesWorkflow, BatchIngestWorkflow],
            activities=[fetch_gbif_page, persist_occurrences, normalize_country_code],
            # normalize_country_code is a sync def (no I/O, run as a local activity), which
            # requires an executor to run on — the rest are async and don't use this pool.
            activity_executor=ThreadPoolExecutor(5),
        )
        logger.info(
            "Starting worker on task queue '%s'", application_settings.temporal_task_queue
        )
        await worker.run()
    finally:
        await aiohttp_client.close()


if __name__ == "__main__":
    asyncio.run(main())
