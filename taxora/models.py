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


class PageSizeUpdateRequest(BaseModel):
    page_size: int


class PageSizeUpdateResponse(BaseModel):
    page_size: int


class ProgressResponse(BaseModel):
    offset: int
    total: int
    pages: int
    paused: bool
