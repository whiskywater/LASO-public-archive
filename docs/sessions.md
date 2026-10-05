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

No context-generation, context-snapshot, membership, or authenticated-principal
capability is advertised. Provider continuation state used by session execution
is opaque and is not a general context-generation API.

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
GET /api/v1/sessions/{session_id}/events?after=0&limit=50
GET /api/v1/sessions/{session_id}/events/stream
POST /api/v1/sessions/{session_id}/close
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

## Context boundary and remaining runtime work

Session turn order, run linkage, and provider continuation make retries and
execution recoverable, but LASO does not yet provide durable derived context
generations or a durable record of the exact context/configuration consumed by
each run. Full turn/event history is not compacted or replaced. A future generic
context builder/reducer must keep original history intact, persist versioned
context generations, and bind each session run to an immutable context snapshot.
The reducer may be supplied by a configured pipeline/provider/plugin; Core does
not prescribe a vendor, summarization algorithm, or chat format. Applications
must not independently compact authoritative session context.

The API's identity provider and authorization hook are generic extension points.
Deployments must provide authenticated identity and enforce session resource
policy at the server/gateway boundary. The built-in local-development identity
is unauthenticated and allows requests; it is not suitable as a multi-user
authorization system.
