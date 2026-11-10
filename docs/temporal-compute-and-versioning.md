# Dispatching to different compute, and what Worker Deployments get us

Reference notes for the ECS/Lambda migration this demo is meant to model. Not implemented in
the demo itself (it runs one worker type); this is the "how it would work" summary.

## 1. Dispatch: task queues decide *where* work runs

Workers pull from a named **task queue**. The workflow chooses the queue per activity, and
has no idea what compute serves it:

```python
await workflow.execute_activity(fetch_gbif_page, ..., task_queue="taxora-long")   # Fargate/ECS workers
await workflow.execute_activity(normalize_country_code, ..., task_queue="taxora-fast")  # Lambda-style
```

| Work | Compute | How it's served |
|---|---|---|
| Long-running, heartbeating (page ingest, bulk persist) | ECS service / Fargate | Normal long-lived workers polling `taxora-long`. Closest to today's ECS app. |
| Quick, bursty one-offs | Lambda | Option A: **Serverless Workers** (Temporal invokes the function when tasks arrive; scales to zero; Public Preview, requires worker versioning). Option B: keep the existing SQS-triggered Lambdas — the activity sends to SQS and completes **asynchronously** when the Lambda calls back. |
| One-off heavy job | Fargate task per job | Activity calls `ECS RunTask`, heartbeats while it runs (or completes async via callback). More moving parts than a worker fleet; only worth it for isolation/very different resource needs. |

Notes:
- Lambda's 15-minute limit means it's only for the quick tasks; long work stays on ECS.
- Option B lets you migrate one task type at a time without rewriting the Lambdas.
- Serverless Workers list Lambda, Cloud Run and Bedrock AgentCore, **not** Fargate. Whether
  they work against a self-hosted Temporal server (as in this repo's compose stack) or only
  Temporal Cloud is unverified — check before designing around it.

## 2. Worker Deployments (Worker Versioning): the "old stacks must drain" problem

Dispatch is about *where*; Worker Deployments are about *which code version*. They solve the
situation where a new deploy would break workflows still running on old code, which today
means keeping old stacks around until everything finishes.

- A **Worker Deployment** is the service (e.g. `taxora-worker`); a **Deployment Version** is
  one build of it (deployment name + build ID). Several task queues can share a version.
- **Pinned** (per workflow type): a workflow runs to completion on the version it started
  on. No patching needed — old workers keep serving it, new workflows go to the new version.
- **Auto-Upgrade**: the workflow moves to the newest version as you roll out. It can then
  span versions, so the workflow code must stay replay-safe via `workflow.patched(...)`.
- **Current** version receives new workflows; a **Ramping** version receives a configurable
  percentage — a canary for workflow code.
- **Drainage:** Temporal reports when no workflows remain on an old version, so you know
  exactly when its workers (the old ECS service / Lambda version) are safe to shut down,
  instead of guessing or leaving them up indefinitely.

What it buys: deploy new workflow code without patching every change, canary it, and retire
old stacks on a signal rather than a timer.

Gotchas:
- A pinned workflow that runs for weeks keeps its old version alive for weeks. For long-running
  workflows that use continue-as-new (like `IngestOccurrencesWorkflow`), stay pinned and let
  the version upgrade at the continue-as-new boundary. Workflows that can't continue-as-new
  should use Auto-Upgrade plus patching.
- Serverless Workers require versioning, since Temporal must invoke an immutable build.
- Python SDK needs v1.11+ for versioning; check the Python SDK docs for exact worker config.

## 3. Batch operations: when to reach for them

Temporal's batch API applies one operation to many workflows, selected by a visibility query
(e.g. `WorkflowType='IngestOccurrencesWorkflow' AND ExecutionStatus='Running'`) or an explicit
list of IDs. Operations include signal, cancel, terminate, delete, reset, and updating
workflow options (this is how you change versioning overrides in bulk).

Use them when the same action is needed across many executions, typically operationally:
- **Version rollouts:** move a set of stuck or long-lived workflows to a newer build, or
  re-pin them, so an old version can finish draining.
- **Incidents:** pause/`signal` every in-flight ingest (our `pause` signal) while the upstream
  is misbehaving, then `resume` them; or terminate a runaway batch.
- **Bad deploys:** reset affected workflows to a point before the bug.
- **Cleanup:** delete or terminate old demo runs.

Don't use them for routine fan-out that's part of the business logic — that belongs in a
parent workflow with child workflows (as `BatchIngestWorkflow` and `PartitionedIngestWorkflow`
do), which keeps the work durable, observable and retried.

Details above are from the Temporal docs' overview and my recollection of the batch API;
confirm the exact operation list and CLI syntax against the current docs before relying on it.

## Sources

- https://docs.temporal.io/production-deployment/worker-deployments
- https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning
- https://docs.temporal.io/production-deployment/worker-deployments/serverless-workers
- https://docs.temporal.io/serverless-workers/aws-lambda