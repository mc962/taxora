import uuid
from concurrent.futures import ThreadPoolExecutor

from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from taxora.activities import normalize_country_code
from taxora.workflows import IngestOccurrencesWorkflow, IngestParams


@activity.defn(name="fetch_gbif_page")
async def fake_fetch(_input) -> dict:
    return {"results": [], "endOfRecords": True}


@activity.defn(name="persist_occurrences")
async def fake_persist(records: list[dict]) -> int:
    return len(records)


async def test_paused_flag_survives_continue_as_new_params():
    """A run started with paused=True (as a continue-as-new run would be) must wait."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="t",
            workflows=[IngestOccurrencesWorkflow],
            activities=[fake_fetch, fake_persist, normalize_country_code],
            activity_executor=ThreadPoolExecutor(2),
        ):
            handle = await env.client.start_workflow(
                IngestOccurrencesWorkflow.run,
                IngestParams(country_code="IS", paused=True),
                id=str(uuid.uuid4()),
                task_queue="t",
            )
            progress = await handle.query(IngestOccurrencesWorkflow.get_progress)
            assert progress.paused is True
            assert progress.pages == 0  # blocked before fetching anything

            await handle.signal(IngestOccurrencesWorkflow.resume)
            result = await handle.result()
            assert result["pages"] == 1
