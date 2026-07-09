"""Ingest control plane: start/inspect single and batch runs, signals, updates, queries."""

from datetime import date
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio.client import WorkflowExecutionStatus

from taxora.configuration.settings import application_settings
from taxora.data.database.connection import get_db_session
from taxora.data.repository.occurrence_repository import OccurrenceRepository
from taxora.models import (
    BatchIngestRequest,
    BatchIngestResponse,
    IngestRequest,
    IngestResponse,
    PageSizeUpdateRequest,
    PageSizeUpdateResponse,
    ProgressResponse,
    RecordsCountResponse,
    WorkflowStatusResponse,
)
from taxora.temporal_client import get_client
from taxora.web.dependencies.auth import SessionOrApiKeyDependency
from taxora.workflows import (
    BatchIngestParams,
    BatchIngestWorkflow,
    IngestOccurrencesWorkflow,
    IngestParams,
)

router = APIRouter(tags=["ingest"])

DatabaseSessionDependency = Annotated[AsyncSession, Depends(get_db_session)]


def _build_event_date_range(
    start_date: date | None, end_date: date | None
) -> str | None:
    if start_date and end_date:
        return f"{start_date.isoformat()},{end_date.isoformat()}"
    if start_date:
        return start_date.isoformat()
    return None


@router.post("/ingest", response_model=IngestResponse)
async def start_ingest(
    request: IngestRequest, _api_key: SessionOrApiKeyDependency
) -> IngestResponse:
    client = get_client()
    workflow_id = f"ingest-{request.country_code}-{uuid4()}"
    event_date_range = _build_event_date_range(request.start_date, request.end_date)

    await client.start_workflow(
        IngestOccurrencesWorkflow.run,
        IngestParams(
            country_code=request.country_code, event_date_range=event_date_range
        ),
        id=workflow_id,
        task_queue=application_settings.temporal_task_queue,
        # Not indexed/queryable like a real search attribute would be (those need a one-time
        # `temporal operator search-attribute create` against the server) — but visible on
        # workflow describe/the Web UI, which is enough for a country-code tag here.
        memo={"country_code": request.country_code},
    )
    return IngestResponse(workflow_id=workflow_id)


@router.post("/ingest/batch", response_model=BatchIngestResponse)
async def start_batch_ingest(
    request: BatchIngestRequest, _api_key: SessionOrApiKeyDependency
) -> BatchIngestResponse:
    """Start one BatchIngestWorkflow, which fans out a child IngestOccurrencesWorkflow per
    country and runs them concurrently."""
    client = get_client()
    workflow_id = f"batch-ingest-{uuid4()}"
    event_date_range = _build_event_date_range(request.start_date, request.end_date)

    await client.start_workflow(
        BatchIngestWorkflow.run,
        BatchIngestParams(
            country_codes=request.country_codes, event_date_range=event_date_range
        ),
        id=workflow_id,
        task_queue=application_settings.temporal_task_queue,
        memo={"country_codes": ",".join(request.country_codes)},
    )
    child_ids = [f"{workflow_id}-{cc}" for cc in request.country_codes]
    return BatchIngestResponse(workflow_id=workflow_id, child_workflow_ids=child_ids)


@router.get("/status/{workflow_id}", response_model=WorkflowStatusResponse)
async def get_status(workflow_id: str) -> WorkflowStatusResponse:
    client = get_client()
    handle = client.get_workflow_handle(workflow_id)
    try:
        description = await handle.describe()
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Workflow not found") from exc

    result = None
    if description.status == WorkflowExecutionStatus.COMPLETED:
        result = await handle.result()

    return WorkflowStatusResponse(
        workflow_id=workflow_id,
        status=description.status.name if description.status else "UNKNOWN",
        result=result,
    )


@router.get("/ingest/{workflow_id}/progress", response_model=ProgressResponse)
async def get_progress(workflow_id: str) -> ProgressResponse:
    """Query the workflow's in-memory progress — doesn't touch workflow state, safe to poll."""
    client = get_client()
    handle = client.get_workflow_handle(workflow_id)
    try:
        progress = await handle.query(IngestOccurrencesWorkflow.get_progress)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Workflow not found") from exc
    return ProgressResponse(
        offset=progress.offset,
        total=progress.total,
        pages=progress.pages,
        paused=progress.paused,
    )


@router.post("/ingest/{workflow_id}/pause")
async def pause_ingest(
    workflow_id: str, _api_key: SessionOrApiKeyDependency
) -> dict[str, str]:
    client = get_client()
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(IngestOccurrencesWorkflow.pause)
    return {"status": "pause signal sent"}


@router.post("/ingest/{workflow_id}/resume")
async def resume_ingest(
    workflow_id: str, _api_key: SessionOrApiKeyDependency
) -> dict[str, str]:
    client = get_client()
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(IngestOccurrencesWorkflow.resume)
    return {"status": "resume signal sent"}


@router.patch("/ingest/{workflow_id}/page-size", response_model=PageSizeUpdateResponse)
async def update_page_size(
    workflow_id: str,
    request: PageSizeUpdateRequest,
    _api_key: SessionOrApiKeyDependency,
) -> PageSizeUpdateResponse:
    """Update (not signal) — synchronous, validated, and returns the new value directly."""
    client = get_client()
    handle = client.get_workflow_handle(workflow_id)
    try:
        new_size = await handle.execute_update(
            IngestOccurrencesWorkflow.set_page_size, request.page_size
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PageSizeUpdateResponse(page_size=new_size)


@router.get("/records/count", response_model=RecordsCountResponse)
async def get_records_count(
    session: DatabaseSessionDependency,
    country_code: str | None = None,
) -> RecordsCountResponse:
    count = await OccurrenceRepository(session).count(country_code)
    return RecordsCountResponse(country_code=country_code, count=count)
