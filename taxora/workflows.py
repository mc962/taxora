import asyncio
from dataclasses import dataclass, field
from datetime import date, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ChildWorkflowError

with workflow.unsafe.imports_passed_through():
    from taxora.activities import (
        CountInput,
        FetchPageInput,
        count_gbif_occurrences,
        fetch_gbif_page,
        normalize_country_code,
        persist_occurrences,
    )
    from taxora.partitioning import Window, split_window, window_to_range

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
    paused: bool = False


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
        self._paused = params.paused

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
                        paused=self._paused,
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


# Keep leaf windows comfortably below GBIF's 100k offset cap. The page loop in
# IngestOccurrencesWorkflow requests offsets in page-size steps, so a window right at the cap
# would make its last page (offset + limit > cap) get rejected; 95k leaves a safety margin.
DEFAULT_MAX_WINDOW_RECORDS = 95_000


@dataclass
class PartitionedIngestParams:
    country_code: str
    start_date: str = "2000-01-01"  # ISO dates, inclusive
    end_date: str | None = None  # defaults to the workflow's "today"
    max_window_records: int = DEFAULT_MAX_WINDOW_RECORDS
    max_concurrent_children: int = 8


@workflow.defn
class PartitionedIngestWorkflow:
    """Ingest a country too big for one paginated walk, by partitioning it into date windows.

    GBIF refuses offset + limit beyond ~100k, so a multi-million-record country can't be
    paged through in one go. Instead:

    1. **Plan** — ask GBIF for the record count of a date window (a cheap ``limit=0`` count
       activity). If it fits under ``max_window_records`` it becomes a leaf; if not, split it
       in half and count both halves. Repeat level by level (all counts at a level run in
       parallel). The amount of work isn't known up front — it's discovered by the plan.
    2. **Fan out** — run one child ``IngestOccurrencesWorkflow`` per leaf window, at most
       ``max_concurrent_children`` at a time. Each child keeps its own history, retries and
       continue-as-new, and one failing window doesn't take down its siblings.

    Re-running is idempotent: children upsert by GBIF key.
    """

    def __init__(self) -> None:
        self._phase = "planning"
        self._leaves: list[Window] = []
        self._done = 0
        self._total = 0
        self._failed: list[str] = []

    @workflow.run
    async def run(self, params: PartitionedIngestParams) -> dict:
        country_code = await workflow.execute_local_activity(
            normalize_country_code,
            params.country_code,
            start_to_close_timeout=timedelta(seconds=5),
        )
        end = (
            date.fromisoformat(params.end_date)
            if params.end_date
            else workflow.now().date()
        )
        start = date.fromisoformat(params.start_date)

        self._leaves = await self._plan(country_code, (start, end), params)

        self._phase = "ingesting"
        semaphore = asyncio.Semaphore(params.max_concurrent_children)
        parent_id = workflow.info().workflow_id

        async def ingest_leaf(window: Window) -> dict | None:
            async with semaphore:
                try:
                    result: dict = await workflow.execute_child_workflow(
                        IngestOccurrencesWorkflow.run,
                        IngestParams(
                            country_code=country_code,
                            event_date_range=window_to_range(window),
                        ),
                        id=f"{parent_id}-{window[0].isoformat()}_{window[1].isoformat()}",
                    )
                except ChildWorkflowError:
                    self._failed.append(window_to_range(window))
                    return None
                self._done += 1
                self._total += result["total"]
                return result

        results = await asyncio.gather(*(ingest_leaf(w) for w in self._leaves))
        self._phase = "done"
        return {
            "country_code": country_code,
            "windows": len(self._leaves),
            "total": self._total,
            "failed_windows": self._failed,
            "child_results": [r for r in results if r],
        }

    async def _plan(
        self, country_code: str, root: Window, params: PartitionedIngestParams
    ) -> list[Window]:
        """Breadth-first window splitting. Returns the non-empty leaf windows."""
        leaves: list[Window] = []
        level = [root]
        while level:
            counts = await asyncio.gather(
                *(
                    workflow.execute_activity(
                        count_gbif_occurrences,
                        CountInput(country_code, window_to_range(w)),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=RetryPolicy(
                            maximum_attempts=5, maximum_interval=timedelta(seconds=30)
                        ),
                    )
                    for w in level
                )
            )
            next_level: list[Window] = []
            for window, count in zip(level, counts):
                if count == 0:
                    continue
                halves = (
                    split_window(window) if count > params.max_window_records else None
                )
                if halves is None:
                    # Fits (or it's a single day that can't be split further — if that
                    # exceeds the cap, the child will ingest what GBIF will serve).
                    leaves.append(window)
                else:
                    next_level.extend(halves)
            level = next_level
        leaves.sort()
        return leaves

    @workflow.query
    def get_plan_progress(self) -> dict:
        return {
            "phase": self._phase,
            "windows": len(self._leaves),
            "windows_done": self._done,
            "total": self._total,
            "failed_windows": self._failed,
        }
