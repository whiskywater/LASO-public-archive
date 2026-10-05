# Execution, durability and recovery

Run states: Queued, Starting, Running, WaitingTool, WaitingModel, WaitingApproval,
Retrying, Paused, Completed, Failed, Cancelled, TimedOut. Terminal runs cannot
transition again. Pending approvals can be decided once. Rejection fails the run.
Node attempts have separate states, start/end timestamps, attempt numbers, safe
error categories, and duration. Messages and events are durable.

The configured storage adapter maintains separate records for pipelines, runs,
attempts, messages, approvals, artifacts, events, event-source state, and durable
external-event claims. Each row has an ID, indexed run ID, insertion sequence and
JSON representation of the typed record. SQLite
uses WAL, `synchronous=FULL`, a busy timeout, prepared parameter bindings and
explicit transactions; its schema version is stored using `PRAGMA user_version`.
The optional PostgreSQL adapter uses equivalent tables, identity-backed sequence
values, parameterized libpqxx transactions, and a schema-local migration table.
Both adapters reject newer schema versions instead of silently interpreting them.

Each successful node checkpoint includes its final attempt, output message, run
cursor/branch queues and event in one transaction. A pending approval includes its
request, waiting attempt and run state in one transaction. An approval decision and
resumable queued state commit together. Storage history survives process restart.

SQLite has a single-service ownership lease using Linux `flock`. PostgreSQL
defaults to the same single-owner behavior using a session-held advisory lock.
With `execution_mode: multi_instance`, PostgreSQL instead permits multiple
services and the runtime claims whole runs with database-time leases and fencing
tokens. API readers share the same adapter. The run controller may persist
deterministic parallel branch work as `NodeWork` records so different instances
can execute eligible function/validator/router paths; side-effecting and
host-local node types remain under the run owner.

On startup, single-owner runs left in active states are marked Paused with a
recovery-required event. In multi-instance mode, active checkpoints remain
durable while the previous lease expires; another service then claims the run and
replays from its last checkpoint. Completed and approval-waiting runs are retained
unchanged. An in-flight operation may already have produced an external effect
before a crash or lease loss; inspect it before allowing takeover. There is no
general exactly-once guarantee or automatic replay of interrupted side effects.

Retries apply to node execution failures up to the declared total attempt count.
Timeout and cancellation are not retried. Retry delays are asynchronous. A timeout
is cooperative and checked before/after node calls and during framework delays.
Each active execution segment has a pipeline deadline; time spent waiting for an
approval or across a restart is excluded. A resumed segment gets a fresh deadline.
The per-attempt deadline is capped by the current pipeline segment deadline and
provider/tool timeout metadata, including for concurrent branches. Persisted visit,
edge and step budgets do not reset; branch work consumes the same run step budget.

Cancellation propagates via `std::stop_source`/`std::stop_token`, including all
durable child runs belonging to a parent (including parallel branch children). A
tool ignoring cancellation may finish its side effect before control returns. The
daemon cannot safely kill arbitrary native code. SIGTERM/SIGINT stop HTTP
acceptance, stop scheduler timers, request cancellation and drain workers. Pending
approval records remain durable. systemd may eventually terminate a noncooperative
process at its configured stop timeout.

Subpipeline revisions use immutable `name@version` registry records. A parent
stores its resolved child references, and each child has a separate normal run
record with `parent_id`, `parent_node_id`, `pipeline_id` and `pipeline_version`.
The default nesting limit is 16 and is configurable as
`max_subpipeline_depth`/`LASO_MAX_SUBPIPELINE_DEPTH` from 1 through 64. Registration
rejects missing revisions and direct or indirect dependency recursion; runtime
also enforces the depth limit.

The child receives the parent payload directly and its output is the parent
subpipeline node result. Child retries, provider/tool limits, policies, schemas,
events, approvals and persistence use the same runtime as a root run. A failed
subpipeline node retry starts a new child run and preserves the failed child for
inspection. The parent does not hold a node permit while waiting for a child, but
the child still consumes the shared global limiter and its own per-run limiter.
Child approval waits pause the parent; deciding the child approval resumes the
child and then the parent, including after a process restart. Parent cancellation
scans and requests cancellation on every active child. Parent/child creation uses
separate durable checkpoints; an interrupted dispatch can leave an inspectable
child run, but there is no distributed outbox or exactly-once dispatch guarantee.

Local artifacts use generated filenames, exclusive creation and fsync before
metadata registration. User names never select filesystem paths. A crash between
file creation and metadata commit may leave an unreferenced file; garbage collection
is deferred. Payloads/results and comments are stored as supplied: applications
must keep credentials out of them. Resolved secret-provider values are not recorded.

## Scheduler and triggers

Durable schedules and event triggers launch ordinary runtime runs. A schedule pins
an immutable `pipeline_id` and `pipeline_version`; it never resolves a newer
revision during a historical occurrence. Schedule inputs are passed as the root
run input. A matching event trigger passes the source event under the `event` key.

The scheduler supports `one_time`, `interval`, and standard five-field `cron`
(`minute hour day-of-month month day-of-week`). Persisted timestamps and cron
evaluation are UTC. `SKIP` misfire advances past missed occurrences; `RUN_ONCE`
performs one bounded catch-up. Recurring overlap is explicitly `ALLOW`, `SKIP`, or
`QUEUE_ONE`; the latter collapses multiple overlaps into one pending execution.

An insert-only occurrence claim (`schedule_id|due_at`) is committed before a
launch. Trigger delivery claims (`trigger_id|event_id`) similarly survive restart.
These mechanisms prevent obvious duplicate local launches but do not promise
distributed exactly-once execution. Event triggers enforce a maximum causal depth
and a bounded delivery queue. Scheduler-created runs expose `initiation_type`,
schedule/trigger IDs, due/event IDs, and root event IDs through normal run
inspection. The scheduler shuts down by cancelling its wait timer and does not
hold runtime node permits while waiting for capacity.

Worker nodes use this same runtime execution path. A submission is persisted
before the adapter callback, and the node yields while it observes the bounded
durable `WorkerJob` state. Retries use a distinct attempt/idempotency identity;
terminal worker states reject late status changes. Recovery-capable adapters are
queried after restart, while ambiguous submissions remain `Unknown`. Worker
callbacks are not invoked while the runtime mutex is held, so synchronous plugin
events cannot re-enter runtime state and deadlock. Global run/node limits and the
configured global/per-worker worker-job limits apply to worker execution.
