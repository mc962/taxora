"""Recurring ingest jobs as Temporal Schedules (Temporal is the store — no jobs table).

Each job is one Schedule whose action starts a PartitionedIngestWorkflow with the job's
config as arguments. Replaces the "EventBridge -> polling Lambda -> check a jobs table"
pattern: Temporal fires each schedule on time, honours the time zone/DST, decides what to do
if the previous run is still going (overlap policy), and what to do about missed fires
(catchup window).
"""

from datetime import timedelta

from fastapi import APIRouter, HTTPException
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleCalendarSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
)
from temporalio.service import RPCError

from taxora.configuration.settings import application_settings
from taxora.models import ScheduleCreateRequest, ScheduleResponse
from taxora.temporal_client import get_client
from taxora.web.dependencies.auth import SessionOrApiKeyDependency
from taxora.workflows import PartitionedIngestParams, PartitionedIngestWorkflow

router = APIRouter(prefix="/schedules", tags=["schedules"])

PREFIX = "ingest-"


def _schedule_id(name: str) -> str:
    return f"{PREFIX}{name}"


@router.post("", response_model=ScheduleResponse)
async def create_schedule(
    request: ScheduleCreateRequest, _api_key: SessionOrApiKeyDependency
) -> ScheduleResponse:
    try:
        overlap = ScheduleOverlapPolicy[request.overlap.upper()]
    except KeyError as exc:
        raise HTTPException(status_code=400, detail="Unknown overlap policy") from exc

    schedule_id = _schedule_id(request.name)
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            PartitionedIngestWorkflow.run,
            PartitionedIngestParams(country_code=request.country_code),
            id=f"{schedule_id}-run",  # Temporal appends the scheduled time
            task_queue=application_settings.temporal_task_queue,
            memo={"country_code": request.country_code},
        ),
        spec=ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    hour=[ScheduleRange(request.hour)],
                    minute=[ScheduleRange(request.minute)],
                )
            ],
            time_zone_name=request.timezone,
            jitter=timedelta(minutes=request.jitter_minutes),
        ),
        policy=SchedulePolicy(
            overlap=overlap,
            catchup_window=timedelta(hours=request.catchup_window_hours),
        ),
        state=ScheduleState(
            note=f"Daily {request.country_code} ingest at "
            f"{request.hour:02d}:{request.minute:02d} {request.timezone}"
        ),
    )
    try:
        await get_client().create_schedule(schedule_id, schedule)
    except ScheduleAlreadyRunningError as exc:
        raise HTTPException(
            status_code=409, detail=f"Schedule '{schedule_id}' already exists"
        ) from exc
    return ScheduleResponse(schedule_id=schedule_id)


@router.get("", response_model=list[ScheduleResponse])
async def list_schedules() -> list[ScheduleResponse]:
    client = get_client()
    out: list[ScheduleResponse] = []
    async for entry in await client.list_schedules():
        if not entry.id.startswith(PREFIX):
            continue
        info = entry.info
        out.append(
            ScheduleResponse(
                schedule_id=entry.id,
                paused=bool(entry.schedule and entry.schedule.state.paused),
                next_action_times=[
                    t.isoformat() for t in (info.next_action_times if info else [])
                ],
                note=entry.schedule.state.note if entry.schedule else None,
            )
        )
    return out


@router.get("/{name}", response_model=ScheduleResponse)
async def get_schedule(name: str) -> ScheduleResponse:
    handle = get_client().get_schedule_handle(_schedule_id(name))
    try:
        desc = await handle.describe()
    except RPCError as exc:
        raise HTTPException(status_code=404, detail="Schedule not found") from exc
    return ScheduleResponse(
        schedule_id=desc.id,
        paused=desc.schedule.state.paused,
        next_action_times=[t.isoformat() for t in desc.info.next_action_times],
        recent_workflow_ids=[
            a.action.workflow_id
            for a in desc.info.recent_actions
            if hasattr(a.action, "workflow_id")
        ],
        note=desc.schedule.state.note,
    )


@router.post("/{name}/pause")
async def pause_schedule(
    name: str, _api_key: SessionOrApiKeyDependency
) -> dict[str, str]:
    await get_client().get_schedule_handle(_schedule_id(name)).pause(note="paused")
    return {"status": "paused"}


@router.post("/{name}/unpause")
async def unpause_schedule(
    name: str, _api_key: SessionOrApiKeyDependency
) -> dict[str, str]:
    await get_client().get_schedule_handle(_schedule_id(name)).unpause(note="resumed")
    return {"status": "unpaused"}


@router.post("/{name}/trigger")
async def trigger_schedule(
    name: str, _api_key: SessionOrApiKeyDependency
) -> dict[str, str]:
    """Run the job now, outside its schedule (still honours the overlap policy)."""
    await get_client().get_schedule_handle(_schedule_id(name)).trigger()
    return {"status": "triggered"}


@router.delete("/{name}")
async def delete_schedule(
    name: str, _api_key: SessionOrApiKeyDependency
) -> dict[str, str]:
    """Stops future runs; a run already in progress is not cancelled."""
    await get_client().get_schedule_handle(_schedule_id(name)).delete()
    return {"status": "deleted"}
