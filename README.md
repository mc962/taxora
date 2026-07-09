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
- **`taxora/cli.py`** — admin CLI (currently: create/delete the recurring Schedule).
- **`taxora/main.py`** + **`taxora/web/api/`** — FastAPI app; a thin HTTP layer for starting
  workflows and signaling/querying/updating them, not where the actual ingestion logic lives.
- **`taxora/engines/gbif_engine.py`** — thin async HTTP client for the GBIF API, using the
  shared `aiohttp` session pool in `taxora/http_client.py`.
- **`taxora/data/`** — SQLAlchemy models, repositories, and Alembic migrations.

Worker and API are separate processes (see `deployment/`) sharing one codebase/image — matches
how Temporal deployments are normally split in practice.

## Running it

Requires a reachable Temporal server (`TEMPORAL_ADDRESS`) — see the `homelab/manager` repo for
how the homelab's instance is deployed.

### Docker Compose (recommended)

```bash
cd deployment/docker
cp taxora.env.example taxora.env   # fill in TEMPORAL_ADDRESS, DB password, etc.
docker compose --env-file taxora.env up --build
```

Note both `taxora.env` (as an `env_file:` for each service's container) and `--env-file
taxora.env` (for `${DATABASE__PASSWORD}` substitution in the `db` service's config) are needed —
these are two separate Compose mechanisms that happen to read the same file here.

Brings up a self-contained Postgres, the worker, and the API (port 8000) — nothing else on the
host needs to be pre-installed beyond Docker.

### Plain systemd (alternative)

See `deployment/systemd/` — `taxora.service` runs the API, `taxora-worker.service` runs the
worker. Point `DATABASE__*` at a Postgres instance you manage yourself.

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

# Recurring schedule (from the worker host, needs TEMPORAL_ADDRESS reachable)
python -m taxora.cli create-schedule --countries IS VA --interval-hours 24
```

Watch a run in the Temporal Web UI to see activity retries, signals/updates landing in history,
continue-as-new run boundaries, and child workflow relationships as they happen.

## Development

```bash
uv sync --group dev --group test
uv run pytest
uv run ruff check .
uv run mypy .
```

Tests don't need a real Postgres — the DB session dependency is mocked (see `tests/conftest.py`).
