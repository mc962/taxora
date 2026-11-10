"""Synthetic GBIF occurrence-search server for load-testing and demoing Taxora.

Serves ``GET /v1/occurrence/search`` with the same envelope as the real API
(``offset``, ``limit``, ``endOfRecords``, ``count``, ``results``) but generates every
record on demand from ``(seed, country, index)`` — no fixtures on disk, no state, and
any dataset size. The same request always returns the same page.

Run:
    DATASET_SEED=1 MOCK_COUNTS="US=2000000,DE=500000" \\
        uv run uvicorn tools.mock_gbif.app:app --port 9000
Then point Taxora at it:
    GBIF_BASE_URL=http://localhost:9000/v1

Configuration (all environment variables):
    DATASET_SEED         Mixed into every key/value. Same seed -> same records (reruns are
                         idempotent upserts); new seed -> brand-new keys (rows accumulate).
                         Keep it <= 9000 so keys fit in a BIGINT. Default 1.
    MOCK_COUNTS          "US=2000000,DE=500000" records per country.
    MOCK_DEFAULT_COUNT   Records for any country not listed above. Default 50000.
    ENFORCE_OFFSET_CAP   Default "true". Reject offset + limit > 100000 like real GBIF,
                         which forces clients to partition big countries (see README).
    LATENCY_MS           "min,max" base random sleep per request. Default "250,500"
                         (measured against real GBIF: ~0.4s for a count query).
    PER_RECORD_MS        Extra sleep per record returned. Default 3 (a real 300-record,
                         1.7MB page took ~1.3s).
    RATE_LIMIT_PER_MIN   Return 429 + Retry-After beyond this many requests/minute.
                         0 disables. Default 0.
    FAIL_RATE            Fraction (0-1) of requests that return a 503. Default 0.
"""

import asyncio
import os
import random
import time
from collections import deque
from datetime import date, datetime, timedelta
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

MAX_OFFSET = 100_001  # real GBIF: "Max offset of 100001 exceeded: 99900 + 300"
MAX_LIMIT = 300

# All generated records fall in [START, START + SPAN_DAYS). Records are ordered by date
# (index -> date is monotonic), so an eventDate filter maps to a contiguous index range
# with O(1) arithmetic — counts and slices stay cheap at any scale.
START = date(2000, 1, 1)
SPAN_DAYS = 25 * 365

SPECIES = [
    "Turdus merula",
    "Parus major",
    "Quercus robur",
    "Vulpes vulpes",
    "Bellis perennis",
    "Apis mellifera",
    "Pieris rapae",
    "Rana temporaria",
    "Fagus sylvatica",
    "Larus argentatus",
    "Cervus elaphus",
    "Taraxacum officinale",
]
BASIS = [
    "HUMAN_OBSERVATION",
    "PRESERVED_SPECIMEN",
    "MACHINE_OBSERVATION",
    "OBSERVATION",
]

SEED = int(os.environ.get("DATASET_SEED", "1"))
DEFAULT_COUNT = int(os.environ.get("MOCK_DEFAULT_COUNT", "50000"))
ENFORCE_CAP = os.environ.get("ENFORCE_OFFSET_CAP", "true").lower() != "false"
LATENCY_MIN_MS, LATENCY_MAX_MS = (
    int(x) for x in os.environ.get("LATENCY_MS", "250,500").split(",")
)
PER_RECORD_MS = float(os.environ.get("PER_RECORD_MS", "3"))
RATE_LIMIT_PER_MIN = int(os.environ.get("RATE_LIMIT_PER_MIN", "0"))
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0"))


def _parse_counts(raw: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for part in filter(None, (p.strip() for p in raw.split(","))):
        cc, _, n = part.partition("=")
        counts[cc.strip().upper()] = int(n.replace("_", ""))
    return counts


COUNTS = _parse_counts(os.environ.get("MOCK_COUNTS", "US=2000000,DE=500000"))

app = FastAPI(title="Mock GBIF")
_request_times: deque[float] = deque()


def _country_total(cc: str) -> int:
    return COUNTS.get(cc, DEFAULT_COUNT)


def _day_offset(index: int, total: int) -> int:
    return (index * SPAN_DAYS) // total


def _first_index_on_or_after(day: int, total: int) -> int:
    """Smallest index whose day offset is >= ``day`` (clamped to [0, total])."""
    if day <= 0:
        return 0
    if day >= SPAN_DAYS:
        return total
    return -(-day * total // SPAN_DAYS)  # ceil division


def _parse_event_date(raw: str) -> tuple[date, date]:
    """Accept 'YYYY', 'YYYY-MM-DD' or 'YYYY-MM-DD,YYYY-MM-DD' (inclusive)."""
    if "," in raw:
        lo, hi = raw.split(",", 1)
        return date.fromisoformat(lo), date.fromisoformat(hi)
    if len(raw) == 4:
        return date(int(raw), 1, 1), date(int(raw), 12, 31)
    d = date.fromisoformat(raw)
    return d, d


def _index_range(cc: str, event_date: str | None) -> tuple[int, int]:
    total = _country_total(cc)
    if not event_date:
        return 0, total
    lo, hi = _parse_event_date(event_date)
    start = _first_index_on_or_after((lo - START).days, total)
    end = _first_index_on_or_after((hi - START).days + 1, total)
    return start, max(start, end)


def _country_slot(cc: str) -> int:
    return (ord(cc[0]) - 65) * 26 + (ord(cc[1]) - 65)


def _event_date_string(rng: random.Random, when: datetime) -> str:
    """Real GBIF returns mixed precision: full UTC timestamps, minute-precision with no
    timezone, and bare dates. Reproduce that so the ingest's parsing is exercised."""
    roll = rng.random()
    if roll < 0.6:
        return when.strftime("%Y-%m-%dT%H:%M:%SZ")
    if roll < 0.85:
        return when.strftime("%Y-%m-%dT%H:%M")
    return when.strftime("%Y-%m-%d")


def _record(cc: str, index: int) -> dict[str, Any]:
    total = _country_total(cc)
    rng = random.Random(f"{SEED}:{cc}:{index}")
    day = START + timedelta(days=_day_offset(index, total))
    when = datetime(
        day.year,
        day.month,
        day.day,
        rng.randrange(24),
        rng.randrange(60),
        rng.randrange(60),
    )
    name = rng.choice(SPECIES)
    key = (SEED * 1000 + _country_slot(cc)) * 1_000_000_000 + index
    return {
        "key": key,
        "scientificName": name,
        "countryCode": cc,
        "decimalLatitude": round(rng.uniform(-60, 75), 5),
        "decimalLongitude": round(rng.uniform(-180, 180), 5),
        "eventDate": _event_date_string(rng, when),
        "basisOfRecord": rng.choice(BASIS),
        # Filler resembling the bulk real GBIF records carry; stored in `raw`.
        "kingdom": "Animalia" if rng.random() < 0.7 else "Plantae",
        "taxonKey": rng.randrange(1_000_000, 9_999_999),
        "datasetKey": f"{rng.getrandbits(128):032x}",
        "recordedBy": f"observer-{rng.randrange(5000)}",
        "issues": rng.sample(["COORDINATE_ROUNDED", "GEODETIC_DATUM_ASSUMED_WGS84"], 1),
    }


@app.middleware("http")
async def chaos(request: Request, call_next):  # type: ignore[no-untyped-def]
    """Latency, rate limiting and random failures — simulates real internet traffic."""
    if RATE_LIMIT_PER_MIN > 0:
        now = time.monotonic()
        while _request_times and now - _request_times[0] > 60:
            _request_times.popleft()
        if len(_request_times) >= RATE_LIMIT_PER_MIN:
            retry_after = max(1, int(60 - (now - _request_times[0])) + 1)
            return JSONResponse(
                {"error": "Too Many Requests"},
                status_code=429,
                headers={"Retry-After": str(retry_after)},
            )
        _request_times.append(now)
    try:
        limit = min(int(request.query_params.get("limit", "20")), MAX_LIMIT)
    except ValueError:
        limit = 0
    base_ms = random.uniform(LATENCY_MIN_MS, LATENCY_MAX_MS)
    await asyncio.sleep((base_ms + PER_RECORD_MS * max(limit, 0)) / 1000)
    if FAIL_RATE and random.random() < FAIL_RATE:
        return JSONResponse({"error": "Service Unavailable"}, status_code=503)
    return await call_next(request)


@app.get("/v1/occurrence/search")
async def search(
    country: str,
    offset: int = 0,
    limit: int = 20,
    eventDate: str | None = None,  # noqa: N803 — matches the GBIF parameter name
) -> JSONResponse:
    cc = country.upper()
    if len(cc) != 2 or not cc.isalpha():
        return JSONResponse({"error": f"Invalid country '{country}'"}, status_code=400)
    if offset < 0 or not (0 <= limit <= MAX_LIMIT):
        return JSONResponse(
            {"error": f"limit must be 0-{MAX_LIMIT} and offset >= 0"}, status_code=400
        )
    if ENFORCE_CAP and offset + limit > MAX_OFFSET:
        return JSONResponse(
            {"error": f"Max offset of {MAX_OFFSET} exceeded: {offset} + {limit}"},
            status_code=400,
        )
    try:
        start, end = _index_range(cc, eventDate)
    except ValueError:
        return JSONResponse(
            {"error": f"Invalid eventDate '{eventDate}'"}, status_code=400
        )

    count = end - start
    first = start + offset
    last = min(first + limit, end)
    results = [_record(cc, i) for i in range(first, last)] if first < end else []
    return JSONResponse(
        {
            "offset": offset,
            "limit": limit,
            "endOfRecords": offset + limit >= count,
            "count": count,
            "results": results,
            "facets": [],
        }
    )
