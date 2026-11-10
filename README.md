# Taxora

A small GBIF occurrence-data ingestion pipeline, built to exercise as much of [Temporal](https://temporal.io)'s
workflow feature set as reasonably fits, rather than to be a serious biodiversity tool.

Fetches species-occurrence records from [GBIF](https://www.gbif.org/developer/occurrence)'s public
REST API, paginating through a country's records and persisting them to Postgres. Temporal's
retry policy handles GBIF's real-world flakiness (timeouts, rate limits) instead of hand-rolled
retry logic.

## Temporal features exercised

- **Retries** — `fetch_gbif_page` activity, real backoff against a real flaky API.
- **Signals** — `pause` / `resume`, checked via `workflow.wait_condition` before each page.
- **Updates** — `set_page_size`, with a `@validator` that rejects out-of-range values before
  they're even written to history.
- **Queries** — `get_progress`, safe to poll without touching workflow state.
- **Continue-As-New** — every `CONTINUE_AS_NEW_EVERY_N_PAGES` pages, carrying accumulated
  totals forward via `IngestParams`, deliberately set low so the pattern shows up even at a
  scale that wouldn't strictly need it.
- **Child workflows** — `BatchIngestWorkflow` fans out one `IngestOccurrencesWorkflow` child per
  country (`asyncio.gather` + `workflow.execute_child_workflow`), independent retries/history
  per child.
- **Local activities** — `normalize_country_code` (no I/O, so it skips the task-queue round
  trip a regular activity takes).
- **Heartbeats** — `persist_occurrences` heartbeats (mostly illustrative at this batch size, but
  it's how the server detects a stuck worker and delivers cancellation).
- **Memos** — country code(s) attached at workflow start, visible in `describe()`/the Web UI.
- **Schedules** — `taxora/cli.py create-schedule`, a recurring `BatchIngestWorkflow`.

## Architecture

- **`taxora/workflows.py` / `taxora/activities.py`** — the Temporal workflows and activities.
- **`taxora/worker.py`** — long-running process that polls the Temporal task queue and executes
  workflow/activity code.
- **`taxora/cli.py`** — admin CLI (API keys, the recurring Schedule, demo `reset-data`).
- **`tools/mock_gbif/`** — synthetic GBIF server for load testing and demos (see below).
- **`taxora/main.py`** + **`taxora/web/api/`** — FastAPI app; a thin HTTP layer for starting
  workflows and signaling/querying/updating them, not where the actual ingestion logic lives.
- **`taxora/engines/gbif_engine.py`** — thin async HTTP client for the GBIF API, using the
  shared `aiohttp` session pool in `taxora/http_client.py`.
- **`taxora/data/`** — SQLAlchemy models, repositories, and Alembic migrations.

Worker and API are separate processes (see `deployment/`) sharing one codebase/image — matches
how Temporal deployments are normally split in practice.

## Running it

### Docker Compose (the demo setup)

Everything runs in containers: Taxora's Postgres, Temporal (with its own Postgres and Web UI),
the mock GBIF server, the migration job, the worker(s) and the API. Services find each other by
compose service name (`db`, `temporal`, `mock-gbif`), much like ECS service discovery.

```bash
cd deployment/docker
cp taxora.env.example ../../.dev.env        # gitignored; set DATABASE__PASSWORD
docker compose --env-file ../../.dev.env up -d --build

# Create an API key, then start a big ingest (see "Demo at scale")
docker compose --env-file ../../.dev.env exec api \
  python -m taxora.cli create-key --name demo --superuser
curl -X POST localhost:8000/ingest/partitioned -H 'Authorization: Bearer txra_...' \
  -H 'Content-Type: application/json' -d '{"country_code": "DE"}'
```

| URL | What |
|---|---|
| http://localhost:8000 | Taxora API / UI |
| http://localhost:8080 | Temporal Web UI |
| http://localhost:9000 | mock GBIF |
| localhost:5433 / :7233 | app Postgres / Temporal gRPC |

- Scale workers: `docker compose --env-file ../../.dev.env up -d --scale worker=3`, then watch
  activities spread across them in the Temporal UI.
- Another env file: `ENV_FILE=../../.tst.env docker compose --env-file ../../.tst.env up -d`.
- Tear down: `docker compose --env-file ../../.dev.env down` (add `-v` to delete the databases).
- The mock's knobs (`MOCK_COUNTS`, `DATASET_SEED`, `LATENCY_MS`, `RATE_LIMIT_PER_MIN`,
  `FAIL_RATE`, `ENFORCE_OFFSET_CAP`) can be set in the shell before `up`. To hit the real GBIF
  instead, set `GBIF_BASE_URL=https://api.gbif.org/v1` in the env file.

Without Docker you need a reachable Temporal server (`TEMPORAL_ADDRESS`) and Postgres
(`DATABASE__*`), then `python -m taxora.worker` and `uvicorn taxora.main:app`.

## Auth

Mutating endpoints (`POST/PATCH /ingest*`) accept either a Bearer API key or a logged-in browser
session cookie — same Argon2-hashed, prefix-lookup key scheme as `aeron`/`praxeum`, plus a
DB-backed session table (also mirroring `aeron`) for the UI's own login. Read-only endpoints
(`/status`, `/records/count`, `/health`) require neither. Generate a key once the DB is migrated:

```bash
python -m taxora.cli create-key --name admin --superuser
```

The browser UI (`/`) is gated by a superuser session: visiting it without a valid `taxora_session`
cookie redirects to `/login`, where a superuser API key exchanges for a session cookie (24h TTL,
signed via `SESSION_SECRET`, cleaned up hourly by a background task). This sits *underneath*
Authelia at the Caddy layer (see `homelab/manager`) — Authelia gates the domain, this gates the
app itself so the UI's own `fetch()` calls to `/ingest*` don't need to know about API keys at all.
Scripts/the Schedule/curl still use the Bearer key directly.

## Usage

```bash
# Single-country ingest
curl -X POST http://localhost:8000/ingest \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer txra_...' \
  -d '{"country_code": "IS"}'
# {"workflow_id": "ingest-IS-..."}

curl http://localhost:8000/status/ingest-IS-...
curl http://localhost:8000/ingest/ingest-IS-.../progress
curl -X POST http://localhost:8000/ingest/ingest-IS-.../pause -H 'Authorization: Bearer txra_...'
curl -X POST http://localhost:8000/ingest/ingest-IS-.../resume -H 'Authorization: Bearer txra_...'
curl -X PATCH http://localhost:8000/ingest/ingest-IS-.../page-size \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer txra_...' -d '{"page_size": 100}'

# Multi-country batch (child workflows)
curl -X POST http://localhost:8000/ingest/batch \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer txra_...' \
  -d '{"country_codes": ["IS", "VA", "SM"]}'

curl http://localhost:8000/records/count?country_code=IS

# Recurring jobs as Temporal Schedules — one Schedule per job, Temporal is the store
# (no jobs table, no polling Lambda). Local time + time zone, DST-safe; overlap policy
# decides what happens if yesterday's run is still going; catchup window covers missed fires;
# jitter spreads many jobs so they don't all start at once.
curl -X POST localhost:8000/schedules -H 'Authorization: Bearer txra_...' \
  -H 'Content-Type: application/json' \
  -d '{"name": "de-daily", "country_code": "DE", "hour": 4, "minute": 30,
       "timezone": "America/New_York", "overlap": "SKIP"}'
curl localhost:8000/schedules                       # list with next run times
curl -X POST localhost:8000/schedules/de-daily/pause    -H 'Authorization: Bearer txra_...'
curl -X POST localhost:8000/schedules/de-daily/unpause  -H 'Authorization: Bearer txra_...'
curl -X POST localhost:8000/schedules/de-daily/trigger  -H 'Authorization: Bearer txra_...'
curl -X DELETE localhost:8000/schedules/de-daily        -H 'Authorization: Bearer txra_...'

# Older, simpler CLI version (fixed interval, batch workflow)
python -m taxora.cli create-schedule --countries IS VA --interval-hours 24
```

Watch a run in the Temporal Web UI to see activity retries, signals/updates landing in history,
continue-as-new run boundaries, and child workflow relationships as they happen.

## Demo at scale: the mock GBIF server

Hammering the real GBIF API during development gets you rate-limited (HTTP 429), and a demo
needs *repeatable* data at a size you choose. `tools/mock_gbif/` is a FastAPI app that
mimics `GET /v1/occurrence/search` with synthetic records generated on demand from
`(seed, country, index)` — nothing stored on disk, any dataset size, same request → same page.

```bash
# Terminal 1 — the mock: 2M US records, 500k DE, 50k for every other country
DATASET_SEED=1 MOCK_COUNTS="US=2000000,DE=500000" \
  uv run uvicorn tools.mock_gbif.app:app --port 9000

# Terminal 2 — point Taxora's worker at it
GBIF_BASE_URL=http://localhost:9000/v1 uv run python -m taxora.worker
```

Knobs (env vars on the mock): `LATENCY_MS` (random per-request sleep), `RATE_LIMIT_PER_MIN`
(real 429 + `Retry-After`), `FAIL_RATE` (random 503s), `ENFORCE_OFFSET_CAP`, `DATASET_SEED`.
See the docstring in `tools/mock_gbif/app.py` for details.

### Why the 100k offset cap is enforced (and why that's the interesting part)

Real GBIF refuses any query where `offset + limit` exceeds ~100,000 (its error says "Max offset of 100001 exceeded"), so a single paginated walk can
never see more than 100k records. The mock enforces the same rule by default. Without it, one
`IngestOccurrencesWorkflow` could just walk a 2M-record country sequentially (continue-as-new
every 20 pages) — it works, but it's one long serial stream and shows Temporal as a fancy
loop.

With the cap on, a big country *can't* be ingested that way. It has to be **partitioned**.
That is the kind of problem Temporal is good at: the amount of work isn't known up front, the
fan-out is dynamic, each piece has its own retries/history/continue-as-new, and one piece
failing doesn't take down the others.

### How the partitioned ingest works

`PartitionedIngestWorkflow` (`POST /ingest/partitioned`) is a two-stage process: **plan, then
execute**. This is a more advanced pattern than most apps need; it's here to show what
Temporal can express when the amount of work is only discovered at run time.

**The key idea:** the 100k cap applies *per query*. Add a filter and it's a different query with
its own 100k allowance. So split the country into date windows (`eventDate=start,end`) that each
hold fewer than ~95k records, and page through each window normally. Windows don't overlap, so
every record is covered exactly once.

**Stage 1 — plan.** Find windows small enough, by asking GBIF *how many* records each holds
(a cheap `limit=0` request, no data transferred) and splitting any that are too big:

```
level 0:  [2000 ───────────────────────────── 2026]   500,000  too big → split in half
level 1:  [2000 ─────────── 2013] [2013 ─────────── 2026]   250k, 250k  too big → split
level 2:  [  ][  ][  ][  ]                              ~125k each     too big → split
level 3:  [ ][ ][ ][ ][ ][ ][ ][ ]                      ~62k each      all fit → done
```

- It runs one level at a time. All windows at a level are counted in parallel, using the
  `count_gbif_occurrences` activity. This all happens inside the parent workflow; no child
  workflows are involved yet.
- Splitting halves the *date range*, not the record count, so real (lumpy) data gives uneven
  windows, and each window stops splitting independently. Some stop after 2 splits, some after 5.
  An empty window is dropped; a single day can't be split further.
- 95k rather than 100k leaves headroom: the page loop would otherwise request a final page past
  the cap (`offset=99900` + 300 is rejected), and a window can grow between being counted and
  being fetched.
- The splitting is tree-shaped, but only the **leaves** are kept. The output of this stage is a
  flat, sorted list of windows, not a tree of jobs.

**Stage 2 — execute.** The flat list of windows becomes a queue of ingest jobs. The parent starts
one child `IngestOccurrencesWorkflow` per window (workflow ID `<parent>-<start>_<end>`), at most
`max_concurrent_children` (default 8) at a time. A country with 8 windows runs all at once;
one with 30 runs 8 and starts the rest as slots free up. Each child:

- pages through its own window with plain `offset`/`limit`, no cap problem since it's under 95k;
- keeps its own history, retries and progress, and continues-as-new every 20 pages, so the
  parent's history stays small;
- can fail without affecting the others. The parent records the window in `failed_windows` and
  carries on.

Re-running is safe: children upsert by GBIF key, so a record seen twice (for example, a page
shifted by an insert while the ingest ran) just overwrites its own row.

Why not plain pagination with one worker? With the cap on, one query simply can't reach past
100k, however patient the worker is. Why not a deeper tree of child workflows that each split
themselves? It would work, but it makes a deeper, harder-to-follow workflow tree for no gain:
counting is cheap, so planning in the parent keeps the structure flat. Set
`ENFORCE_OFFSET_CAP=false` on the mock to let a single workflow walk a huge country sequentially
instead, for comparison.

For a real GBIF pull this large, their separate bulk download API is usually the better tool;
partitioning is the workaround when you have to use the search endpoint.

### Seeds and resetting

- **Same `DATASET_SEED`** → identical keys, so re-running an ingest upserts the same rows
  (row count stays flat — demonstrates idempotency).
- **New seed** → brand-new keys, so rows accumulate next to the old ones (a fresh, insert-heavy
  load). The seed only changes what the mock serves; it never touches the database.
- **Reset** the table between runs (leaves users/API keys and all Temporal state alone):

```bash
TAXORA_ALLOW_DATA_RESET=true uv run python -m taxora.cli reset-data   # add --yes to skip the prompt
```

## Development

```bash
uv sync --group dev --group test
uv run pytest
uv run ruff check .
uv run mypy .
```

Tests don't need a real Postgres — the DB session dependency is mocked (see `tests/conftest.py`).
