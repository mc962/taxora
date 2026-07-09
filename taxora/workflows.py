import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from taxora.activities import (
        FetchPageInput,
        fetch_gbif_page,
        normalize_country_code,
        persist_occurrences,
    )

# Deliberately low so a single-country run demonstrates continue-as-new even though GBIF's
# own 100k/300-per-page cap (~334 pages) wouldn't otherwise come close to needing it.
CONTINUE_AS_NEW_EVERY_N_PAGES = 20


@dataclass
class IngestParams:
    country_code: str
    event_date_range: str | None = (
        None  # e.g. "2020-01-01,2020-12-31" — see GBIF eventDate docs
    )
    page_size: int = 300
    max_offset: int = 100_000
    # Carried across continue-as-new runs — not set by callers starting a fresh ingest.
    starting_offset: int = 0
    accumulated_total: int = 0
    accumulated_pages: int = 0


@dataclass
class Progress:
    offset: int
    total: int
    pages: int
    paused: bool


@workflow.defn
class IngestOccurrencesWorkflow:
    """Paginate through GBIF occurrence-search results for a country and persist them.

    Bounded by GBIF's own hard cap of offset + limit <= 100,000 per query, but continues-as-new
    every CONTINUE_AS_NEW_EVERY_N_PAGES pages regardless — demonstrates the pattern (keeping
    workflow history small on long-running workflows) even at a scale that wouldn't strictly
    require it. GBIF's real rate-limiting/timeouts drive genuine retry behavior via the fetch
    activity's retry_policy, rather than simulated failures.
    """

    def __init__(self) -> None:
        self._paused = False
        self._page_size = 300
        self._offset = 0
        self._total = 0
        self._pages = 0

    @workflow.run
    async def run(self, params: IngestParams) -> dict:
        self._page_size = params.page_size
        self._offset = params.starting_offset
        self._total = params.accumulated_total
        self._pages = params.accumulated_pages

        # Local activity: no I/O, so it skips the task-queue round trip a regular activity
        # takes and doesn't write a full activity task to workflow history.
        country_code = await workflow.execute_local_activity(
            normalize_country_code,
            params.country_code,
            start_to_close_timeout=timedelta(seconds=5),
        )

        pages_this_run = 0
        while self._offset < params.max_offset:
            # Signals (pause/resume) mutate this flag — block here rather than mid-fetch.
            await workflow.wait_condition(lambda: not self._paused)

            page = await workflow.execute_activity(
                fetch_gbif_page,
                FetchPageInput(
                    country_code,
                    self._offset,
                    self._page_size,
                    params.event_date_range,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    maximum_attempts=5,
                    maximum_interval=timedelta(seconds=30),
                ),
            )
            self._pages += 1
            pages_this_run += 1

            results = page.get("results", [])
            if results:
                persisted = await workflow.execute_activity(
                    persist_occurrences,
                    results,
                    start_to_close_timeout=timedelta(seconds=30),
                    heartbeat_timeout=timedelta(seconds=10),
                    retry_policy=RetryPolicy(maximum_attempts=5),
                )
                self._total += persisted

            end_of_records = page.get("endOfRecords", True)
            self._offset += self._page_size

            if end_of_records:
                break

            if pages_this_run >= CONTINUE_AS_NEW_EVERY_N_PAGES:
                workflow.continue_as_new(
                    IngestParams(
                        country_code=country_code,
                        event_date_range=params.event_date_range,
                        page_size=self._page_size,
                        max_offset=params.max_offset,
                        starting_offset=self._offset,
                        accumulated_total=self._total,
                        accumulated_pages=self._pages,
                    )
                )

        # Let any in-flight signal/update handler finish before returning a result — otherwise
        # e.g. a set_page_size update racing the final page could be cut off mid-handler.
        await workflow.wait_condition(workflow.all_handlers_finished)

        return {
            "country_code": country_code,
            "event_date_range": params.event_date_range,
            "total": self._total,
            "pages": self._pages,
        }

    @workflow.signal
    def pause(self) -> None:
        self._paused = True

    @workflow.signal
    def resume(self) -> None:
        self._paused = False

    @workflow.update
    def set_page_size(self, new_size: int) -> int:
        self._page_size = new_size
        return self._page_size

    @set_page_size.validator
    def validate_page_size(self, new_size: int) -> None:
        if not (1 <= new_size <= 300):
            # Rejected before being written to history at all (sync validators can do this;
            # the update handler itself always gets recorded once accepted).
            raise ValueError(
                "page_size must be between 1 and 300 — GBIF's own page-size cap"
            )

    @workflow.query
    def get_progress(self) -> Progress:
        return Progress(
            offset=self._offset,
            total=self._total,
            pages=self._pages,
            paused=self._paused,
        )


@dataclass
class BatchIngestParams:
    country_codes: list[str] = field(default_factory=list)
    event_date_range: str | None = None


@workflow.defn
class BatchIngestWorkflow:
    """Fan out one child IngestOccurrencesWorkflow per country, run concurrently, fan back in.

    Each child gets its own history/retries/continue-as-new independent of its siblings — one
    country hitting persistent GBIF errors doesn't block or fail the others.
    """

    @workflow.run
    async def run(self, params: BatchIngestParams) -> list[dict]:
        parent_id = workflow.info().workflow_id
        results = await asyncio.gather(
            *(
                workflow.execute_child_workflow(
                    IngestOccurrencesWorkflow.run,
                    IngestParams(
                        country_code=cc, event_date_range=params.event_date_range
                    ),
                    id=f"{parent_id}-{cc}",
                )
                for cc in params.country_codes
            )
        )
        return list(results)
