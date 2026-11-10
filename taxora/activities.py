"""Temporal activities — the only place real I/O (HTTP, DB) happens."""

from dataclasses import dataclass

from temporalio import activity
from temporalio.exceptions import ApplicationError

from taxora.configuration.settings import application_settings
from taxora.data.database.connection import sessionmanager
from taxora.data.repository.occurrence_repository import OccurrenceRepository
from taxora.engines import gbif_engine


@dataclass
class FetchPageInput:
    country_code: str
    offset: int
    limit: int
    event_date_range: str | None = None


# GBIF's raw records carry large nested blobs (extensions, classifications, media) we
# don't use. Trimming here keeps activity results well under Temporal's payload-size
# warning threshold — the full envelope for a 300-record page can run 1-2MB, which
# would otherwise get written into workflow history on every page.
_RECORD_FIELDS = (
    "key",
    "scientificName",
    "countryCode",
    "decimalLatitude",
    "decimalLongitude",
    "eventDate",
    "basisOfRecord",
)


@activity.defn
def normalize_country_code(country_code: str) -> str:
    """Validate and normalize a country code. No I/O — a good fit for a local activity
    (workflow.execute_local_activity), which skips the task-queue round trip and doesn't
    write a full activity task to workflow history, unlike a regular activity.
    """
    normalized = country_code.strip().upper()
    if len(normalized) != 2 or not normalized.isalpha():
        # Not retryable — retrying won't turn an invalid code into a valid one.
        raise ApplicationError(
            f"'{country_code}' is not a valid ISO 3166-1 alpha-2 country code",
            non_retryable=True,
        )
    return normalized


@activity.defn
async def fetch_gbif_page(page_input: FetchPageInput) -> dict:
    activity.logger.info(
        "Fetching GBIF page: country=%s offset=%d limit=%d",
        page_input.country_code,
        page_input.offset,
        page_input.limit,
    )
    page = await gbif_engine.fetch_page(
        page_input.country_code,
        page_input.offset,
        page_input.limit,
        base_url=application_settings.gbif_base_url,
        event_date_range=page_input.event_date_range,
    )
    trimmed_results = [
        {field: record.get(field) for field in _RECORD_FIELDS}
        for record in page.get("results", [])
    ]
    return {"results": trimmed_results, "endOfRecords": page.get("endOfRecords", True)}


@dataclass
class CountInput:
    country_code: str
    event_date_range: str


@activity.defn
async def count_gbif_occurrences(count_input: CountInput) -> int:
    """How many records match a country + date window? One cheap ``limit=0`` request —
    GBIF returns the ``count`` without any records. Used by the partitioning planner to
    decide whether a window fits under the offset cap or needs splitting."""
    page = await gbif_engine.fetch_page(
        count_input.country_code,
        0,
        0,
        base_url=application_settings.gbif_base_url,
        event_date_range=count_input.event_date_range,
    )
    return int(page["count"])


@activity.defn
async def persist_occurrences(records: list[dict]) -> int:
    # A single page's batch is small enough to complete well within its timeout, so this
    # heartbeat is mostly illustrative — but any activity doing real work should heartbeat:
    # it's how the server detects a stuck/dead worker and how cancellation gets delivered.
    activity.heartbeat(f"persisting {len(records)} records")
    async with sessionmanager.session() as session:
        written = await OccurrenceRepository(session).upsert_batch(records)
    activity.logger.info("Persisted %d occurrence records", written)
    return written
