# External worker adapters

LASO worker plugins are adapters to external execution systems. They do not make
external systems part of LASO Core. A worker is a trusted native C ABI component
that accepts a structured job and reports status through the normal LASO event
ingress path.

## Configuration and pipeline syntax

Worker plugins are opt-in under `worker_plugins`; the existing `workers` setting
continues to mean the LASO executor thread count.

```yaml
plugin_dirs: [build/worker-plugins]
worker_plugins:
  offline:
    plugin: example-worker
    component: example-worker
    enabled: true
    config: {mode: deterministic}
max_worker_jobs: 32
max_worker_jobs_per_worker: 16
# Optional budgets; zero disables each one. Token and cost budgets are per run.
max_worker_wall_time_ms: 0
max_worker_tokens_per_run: 0
max_worker_cost_units_per_run: 0
```

A pipeline selects a configured worker by ID or by an unambiguous capability:

```yaml
nodes:
  execute:
    type: worker
    worker: offline
    task_type: deterministic
    instructions: process the structured input
    output_schema: schemas/result.schema.json
```

The input payload is submitted unchanged as structured JSON. The worker result
becomes the normal node payload. Node input/output schema declarations are still
enforced by the runtime, and worker result metadata, normalized usage, and
bounded artifact references are retained in message metadata.

Adapters may report any subset of `queue_duration_ms`, `wall_duration_ms`,
`input_tokens`, `output_tokens`, `total_tokens`, `tool_calls`, `action_count`,
`provider`, `model`, `executor`, `cost_units`, and bounded structured `metadata`
in a submission, status response, or completion event. Missing fields remain
missing; LASO does not assume that a worker is an LLM or invent vendor pricing.
When both input and output tokens are reported, LASO derives `total_tokens` if
it was not supplied. Usage is persisted in `WorkerJob` and added to the
completed node message.

## Durable jobs and recovery

Every submission creates a `WorkerJob` record with a stable idempotency identity
derived from `run_id`, `node_id`, and attempt. The durable record contains the
worker and external job IDs, state, bounded request metadata, result metadata,
errors, cancellation state, and artifact references; it does not contain the
instructions or input payload. Terminal states are immutable. A retry uses a new
attempt and therefore a new durable job; the prior job remains inspectable.

Worker states are `Created`, `Submitting`, `Queued`, `Running`, `Waiting`,
`Completed`, `Failed`, `Cancelled`, `TimedOut`, and `Unknown`. If a process stops
after submission, a recovery-capable adapter is queried using the persisted
external job ID. An adapter that cannot reconcile an ambiguous submission leaves
the job `Unknown` rather than pretending it completed or submitting endlessly.
This is not an exactly-once guarantee across an arbitrary external service.

## Events, limits, and cancellation

Adapters report `worker.job.started`, `worker.job.progress`,
`worker.job.completed`, `worker.job.failed`, and `worker.job.cancelled` through the
host callback. Events are validated, durably persisted, deduplicated by their
external event ID, and correlated using the LASO job ID, worker ID, and external
job ID. A terminal job ignores late status events.

Ingress, requests, results, metadata, artifact references, active jobs, and
per-worker jobs are bounded. Defaults are 32 active jobs globally and 16 per
worker; `max_worker_jobs` and `max_worker_jobs_per_worker` are configurable.
`max_worker_wall_time_ms` bounds a reported job wall duration, while
`max_worker_tokens_per_run` and `max_worker_cost_units_per_run` accumulate
reported usage across jobs in one run. Zero disables a budget. A violation is a
durable `Failed` job with `failure_kind: budget` and a stable diagnostic.
Native callbacks may run on plugin-owned threads, but the host callback is
thread-safe and returns explicit backpressure/stopped/rejected statuses.

Cancellation and deadlines use the normal LASO runtime. LASO asks the adapter to
cancel and records acknowledgement; a failed or unsupported cancellation is not
reported as confirmed. Adapter callbacks are never invoked while the runtime
mutex is held, preventing synchronous event callbacks from re-entering runtime
state and deadlocking.

`WorkerTransport` is the backend-neutral lifecycle boundary used by
`WorkerManager`: submit, status/recovery, result, cancel, start, and stop. The
existing `WorkerAdapter` native ABI wrapper remains the in-process adapter and
continues to work unchanged. The optional `process_workers` configuration now
provides a supervised local process implementation using the versioned NDJSON
protocol described in `docs/worker-process-protocol.md`. It is generic and can
host any executable that implements the protocol; it adds no vendor-specific
behavior. Transport exceptions are recorded separately from worker-declared
job failures.

Process workers use an absolute executable path and argument vector. Their
environment is empty by default; `environment_allowlist` and literal
`environment` overrides are explicit, with literal overrides taking precedence
on name collisions. Configuration validates NUL characters, duplicate
allowlist names, per-value limits, and a 64 KiB aggregate child-environment
limit. Startup and request timeouts are bounded.
LASO owns the child, attempts cooperative `shutdown`, and then terminates its
process group with bounded escalation. A broken child is not silently
restarted or resubmitted because the external outcome may be ambiguous. This
is process isolation and lifecycle supervision, not an OS or container
sandbox.

### Worker-originated interaction

A process worker may emit a bounded `approval`, `permission`, or `question`
request. LASO correlates it to the durable `WorkerJob`, evaluates configured
policy where applicable, and persists the request under
`GET /api/v1/worker-requests`. Pending requests can be resolved with
`POST /api/v1/worker-requests/{id}/respond` using `{"state":"approved"}` or
`{"state":"answered","payload":{...}}`; `deny` and `cancel` are also
available. Duplicate request IDs replay their durable decision. A pending
request is never auto-approved after restart, and job cancellation resolves its
pending requests as cancelled. This channel is not arbitrary LASO RPC.

### OpenCode adapter

`laso-opencode-worker` is an optional generic adapter outside Core. It supervises
the installed OpenCode headless server through loopback HTTP and maps the
structured session/message API into the process-worker protocol. The adapter
captures session IDs, accepts a later turn with the same session ID, reports
structured results and available usage, and uses OpenCode abort for cooperative
cancellation. See [OpenCode worker](opencode-worker.md). The current reference
implementation reports an in-flight turn as `Unknown` after adapter restart
when it cannot safely reconcile local turn state; it never silently resubmits.
OpenCode permission/question events are mapped to the interaction channel when
the installed server exposes them.

```yaml
process_workers:
  reference:
    executable: /absolute/path/to/laso-example-worker-host
    args: [--mode, success]
    startup_timeout_ms: 5000
    request_timeout_ms: 5000
    interaction_timeout_ms: 300000
    # environment_allowlist: [PATH]
    # environment: {WORKER_SETTING: value}
```

The deterministic `laso-example-worker-host` is a reference/test adapter, not
an AI assistant. The OpenCode adapter is opt-in and does not add an OpenCode
dependency to the default build or configuration. The optional
`laso-codex-worker` similarly targets the installed Codex app-server through
structured JSON-RPC, supports durable thread IDs and LASO-controlled approval
requests, and is built only with `-DLASO_BUILD_CODEX_ADAPTER=ON`; see
[Codex worker](codex-worker.md). Remote worker networking, mandatory process
isolation, and distributed worker leasing remain future work.

`laso-claude-worker` is an optional adapter outside Core. It starts the
configured Claude Code executable directly in headless `stream-json` mode and
maps its typed system, assistant, result, and supported control messages into
the process-worker protocol. Session IDs are returned as external job IDs and
can be supplied on a later request for `--resume`. See [Claude Code worker](claude-worker.md).

The adapter accepts Claude's `can_use_tool` control request as a LASO
permission request and accepts compatible question control messages when the
installed CLI emits them. Unsupported control requests are rejected rather
than approved. The documented CLI does not guarantee that every permission or
question interaction is exposed in headless mode, so vendor-specific
interaction coverage is version-dependent. The adapter reports unsupported
cancellation truthfully; LASO still applies bounded request timeouts and
process-group cleanup without resubmitting an ambiguous turn.

## Policy, secrets, and trust

Worker nodes pass through normal policy evaluation. Worker metadata distinguishes
local and remote adapters, and the worker ID is the policy resource. Plugins are
configured in trusted directories and run inside the LASO process without a
sandbox. Credentials must be obtained through a deployment secret boundary and
must not be placed in configuration metadata, job records, events, logs, or API
responses. Worker output is untrusted JSON and is subject to the same schema and
size checks as other node output.

## API and CLI

The inspection API is:

```text
GET  /api/v1/workers
GET  /api/v1/workers/{id}
GET  /api/v1/worker-jobs
GET  /api/v1/worker-jobs/{id}
POST /api/v1/worker-jobs/{id}/cancel
GET  /api/v1/worker-requests
GET  /api/v1/worker-requests/{id}
POST /api/v1/worker-requests/{id}/respond
```

The matching local CLI commands are `laso worker list|show` and
`laso worker-job list|show|cancel`. These commands inspect or cancel jobs; LASO
does not download or install plugins.
