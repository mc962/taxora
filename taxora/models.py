"""Pydantic request/response models for all Taxora API endpoints."""

from datetime import date

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str
    version: str


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


class IngestRequest(BaseModel):
    country_code: str  # ISO 3166-1 alpha-2, e.g. "IS"
    start_date: date | None = None
    end_date: date | None = None


class IngestResponse(BaseModel):
    workflow_id: str


class WorkflowStatusResponse(BaseModel):
    workflow_id: str
    status: str
    result: dict | None = None


class RecordsCountResponse(BaseModel):
    country_code: str | None
    count: int


class BatchIngestRequest(BaseModel):
    country_codes: list[str]
    start_date: date | None = None
    end_date: date | None = None


class BatchIngestResponse(BaseModel):
    workflow_id: str
    child_workflow_ids: list[str]


class PartitionedIngestRequest(BaseModel):
    country_code: str
    start_date: date | None = None  # defaults to 2000-01-01
    end_date: date | None = None  # defaults to today
    max_window_records: int = 95_000
    max_concurrent_children: int = 8


class PartitionedIngestResponse(BaseModel):
    workflow_id: str


class PageSizeUpdateRequest(BaseModel):
    page_size: int


class PageSizeUpdateResponse(BaseModel):
    page_size: int


class ProgressResponse(BaseModel):
    offset: int
    total: int
    pages: int
    paused: bool


# ---------------------------------------------------------------------------
# Schedules (one Temporal Schedule per recurring ingest job)
# ---------------------------------------------------------------------------


class ScheduleCreateRequest(BaseModel):
    name: str  # becomes schedule id "ingest-<name>"
    country_code: str
    hour: int = 4  # local time in `timezone`
    minute: int = 0
    timezone: str = "America/New_York"  # IANA name; DST handled by Temporal
    overlap: str = "SKIP"  # SKIP | BUFFER_ONE | CANCEL_OTHER | ...
    catchup_window_hours: int = 6  # run a missed fire if back within this window
    jitter_minutes: int = 10  # random offset so many jobs don't start in lockstep


class ScheduleResponse(BaseModel):
    schedule_id: str
    paused: bool = False
    next_action_times: list[str] = []
    recent_workflow_ids: list[str] = []
    note: str | None = None
