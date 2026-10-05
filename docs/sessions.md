# Durable sessions

LASO sessions are generic orchestration resources. LASO owns durable sessions,
ordered turns, execution, run linkage, and event replay. Applications own
accounts, session access/sharing policy, product-specific names and metadata,
navigation, and presentation. LASO does not define chat users, conversation
membership, product roles, or a user interface.

## Discovering server support

`GET /api/v1/version` returns the server version and an explicit `capabilities`
array. Clients should inspect the array instead of inferring support from a
version number. The current session capabilities are:

| Capability | Implemented behavior |
|---|---|
| `sessions.durable` | Create, list, and retrieve sessions from persistent storage. |
| `sessions.ordered_turns` | Accept idempotent turns with a durable per-session sequence and retrieve ordered turn history. |
| `sessions.sequential_execution` | Execute accepted turns in sequence, bind each turn to one run, and recover queued/bound work after restart using the existing fencing path. |
| `sessions.event_replay` | Retrieve durable session events after a sequence cursor. |
| `sessions.sse` | Stream/replay the event journal with `Last-Event-ID` reconnect support and bounded per-process admission. |
| `sessions.context_generations` | Store immutable provider-neutral context generations against completed history boundaries, with revision-checked ordering. |
| `sessions.run_context_snapshots` | Capture the selected generation and provider continuation state atomically with session turn-to-run binding. |
| `sessions.context_reduction` | Conditional: opt-in automatic generation using the configured registered reducer and byte-budget policy. |

Context reduction is advertised only when it is enabled and the configured
reducer is registered. LASO does not require reduction to mean natural-language
summarization and does not include a vendor-specific representation.

## Session API

All paths below are under `/api/v1`. Session list and turn history use the
standard bounded `limit`/`offset` query parameters (`limit` 1..100, default 50;
offset up to 100,000,000). Session records are listed in storage insertion
sequence order. Turn history is ordered by its assigned per-session sequence.

```http
POST /api/v1/sessions
{"pipeline_id":"review@1"}

GET /api/v1/sessions?limit=50&offset=0
GET /api/v1/sessions/{session_id}

POST /api/v1/sessions/{session_id}/turns
{"idempotency_key":"client-request-42","input":{"task":"inspect the change"}}

GET /api/v1/sessions/{session_id}/turns?limit=50&offset=0
GET /api/v1/sessions/{session_id}/context
POST /api/v1/sessions/{session_id}/context/generations
GET /api/v1/runs/{run_id}/context
GET /api/v1/sessions/{session_id}/events?after=0&limit=50
GET /api/v1/sessions/{session_id}/events/stream
POST /api/v1/sessions/{session_id}/close
```

Context generation creation uses an optimistic session-local generation number.
The response omits the stored payload; only providers advertising
`session-context` receive it as a separate request field.

```http
POST /api/v1/sessions/{session_id}/context/generations
{
  "expected_generation": 0,
  "through_turn_sequence": 0,
  "idempotency_key": "context-build-1",
  "representation_kind": "structured-context",
  "representation_version": "1",
  "payload": {"state": "provider-neutral derived data"}
}
```

The session response contains its ID, immutable pipeline identity, open/closing/
closed state, creation and update timestamps, and current active turn/run when
applicable. Internal dispatch counters and fencing data are omitted. Session
creation returns `201`; accepted turns and close return `202`. A repeated
idempotency key with identical input returns the original turn without another
execution. Reusing a key with different input returns `409`. Closed sessions
reject new turns. Full accepted input and turn history are durable; turn records
include sequence, state, timestamps, run ID, and result where available.

The session-to-turn-to-run relationship is explicit: each executed turn stores
its run ID, and each run stores both the session ID and turn ID. Session event
records include turn/run identifiers for execution lifecycle events. These
relations survive process restart. PostgreSQL assigns ordering under a session
row lock, and its journal can be read by another service instance. SQLite keeps
the documented single-instance semantics.

## Execution and event delivery

Accepted turns execute sequentially per session; different sessions can execute
concurrently subject to runtime limits. Run binding and the `turn.execution.started`
event commit atomically. Execution completion commits the run checkpoint, turn
result, session state, and terminal event under the existing ownership/fencing
rules. Provider continuation is stored separately from public session/turn
responses and is published only with authoritative fenced completion. It remains
adapter-owned opaque state, not a product-neutral transcript or compaction
generation.

The event journal assigns monotonically increasing per-session sequence numbers.
`GET .../events?after=N` returns events after `N`; SSE replays the same journal
and accepts `Last-Event-ID` as its cursor. Delivery is at least once across
disconnect boundaries, so clients should persist the last fully handled event
ID and deduplicate by sequence. PostgreSQL journal polling exposes commits from
other instances without sticky sessions. The SSE admission limit is per process,
not cluster-wide; see [the SSE contract](session-sse.md) for limits and lifecycle.

## Context generations and run snapshots

Full session turns/events remain the authoritative original history and are
never replaced by derived state. The API accepts a provider-neutral context
payload with an explicit representation kind/version, idempotency key, expected
current generation, and `through_turn_sequence`. The boundary must be zero or a
contiguous range of terminal turns. Generations are numbered per session, link
to their predecessor, and are immutable; a stale expected number returns `409`.
Creation and turn/run binding lock the same session row in PostgreSQL, so a
generation is either visible to a later turn or misses that turn
deterministically.

At atomic turn/run binding, LASO records an immutable `RunContextSnapshot` keyed
by run ID. It identifies the session, turn and sequence, history boundary,
selected generation (if any), and the generation's represented boundary. It
also captures the exact opaque provider continuation states that were current
at binding. Run execution reads continuation from this snapshot instead of the
mutable per-session current record. A later generation or continuation update
cannot rewrite a prior snapshot. The run record itself pins the pipeline
identity/version and current turn input.

When a generation is selected, LASO passes it separately as `session_context`
to model providers. That value includes the durable ordered turns after the
generation boundary and before the current turn, so turns accepted since the
generation are not omitted. Providers must advertise the generic
`session-context` capability to receive one; otherwise execution fails closed.
The API read
endpoint exposes generation metadata but not its payload. The run-context
endpoint never returns provider continuation payloads. The first implementation
has no content hash. Automatic reduction is an opt-in runtime policy configured
under `session_context_reduction`. It uses serialized-byte budgets for the
previous derived payload, eligible completed turn records, and the pending turn
input. If the effective bytes exceed the target, or the configured threshold is
reached, the configured `ContextReducer` derives a replacement generation
before run binding. It receives the latest committed generation and durable
turns after its boundary. It returns a representation kind/version, opaque
payload, and exact through-turn boundary; Core validates and commits the result
through the existing revision-checked immutable storage path.

The `ContextReducer` interface is provider-neutral and receives a deadline and
cancellation token. Service shutdown requests cancellation. Reducers must
cooperate with both; LASO rejects results returned after the deadline, but
cannot forcibly preempt an extension that ignores them. A reducer cannot write
storage; LASO owns validation, idempotency, persistence, and run-snapshot
selection. The included `recent-turns` reducer is a deterministic
reference implementation: it retains the newest complete turn records that
fit the configured serialized-byte budget. It does not tokenize, summarize
natural language, or claim byte counts equal a provider's token count.
Applications may register another reducer through the Core registry. A
pipeline-based reducer is not implemented; a later adapter can use this
interface, but must avoid mutating or recursively dispatching the source
session.

Reduction is fail-closed. A missing reducer, reducer error/deadline expiry,
invalid result, unsupported prior representation, over-budget output, stale
boundary, or irrecoverable generation revision conflict prevents the affected
turn from binding a run. The accepted turn remains durable for recovery/retry.
Reducers should be deterministic for an identical request so a stable
idempotency key makes retries converge. An application-created generation is
the reducer's predecessor; if it already covers all prior turns, no reduction
is needed. If manual or competing generation creation wins while reduction is
running, Core rereads the committed generation and reevaluates once. Run binding
then selects the latest committed generation atomically with its immutable
snapshot.

The target is a byte budget for serialized session context and the current turn
input. It is not a provider tokenizer or a promise to fit a provider's entire
prompt/context window; providers remain responsible for their model-specific
limits. Full accepted turn/event history remains unchanged and authoritative.
Runtime events record attempts, result/failure, byte counts, generation, and
duration without logging context payloads.

Provider continuation remains separate opaque adapter state: it is the
provider-specific continuation consumed by an adapter, and the run snapshot
pins the exact state used by a particular run. A context generation is the
provider-neutral derived representation; it does not replace original history
or continuation state. Applications must not independently compact
authoritative session context.

The API's identity provider and authorization hook are generic extension points.
Deployments must provide authenticated identity and enforce session resource
policy at the server/gateway boundary. The built-in local-development identity
is unauthenticated and allows requests; it is not suitable as a multi-user
authorization system.
