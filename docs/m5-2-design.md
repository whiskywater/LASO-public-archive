# M5.2 design: durable sequential turn execution

## Boundary

M5.2 connects an M5.1 accepted session turn to one ordinary LASO pipeline run,
then commits the authoritative turn result back into the session journal. Turns
in one session execute in acceptance-sequence order, with at most one active
turn. Different sessions remain eligible for concurrent runs under the existing
runtime limits. An accepted turn is queued work; acceptance never implies
execution or success.

LASO guarantees one authoritative, fenced result for a logical turn. Provider
and tool attempts remain at least once: a stale attempt may already have caused
an external side effect before its completion is rejected.

## Existing primitives to reuse

- `AgentSession`, `SessionTurn`, and `SessionEvent` are durable records. Storage
  assigns journal sequence numbers while locking the session row.
- `Runtime::run` creates the normal persisted pipeline checkpoint. The runtime's
  dispatcher owns `run:<run-id>` leases; every owner checkpoint is committed
  with the PostgreSQL fencing predicate. Node work uses the same coordination
  table and fencing model.
- Existing `Run`, `NodeExecution`, and `WorkerJob` records are the execution and
  attempt lineage. Worker recovery remains conservative: an ambiguous worker
  outcome is not blindly resubmitted.
- PostgreSQL multi-instance mode uses database-time leases and monotonic fences.
  SQLite continues to use its single-process lease and serialized transactions.

The M5.2 dispatcher will not create a second run engine, node-attempt model, or
lease table.

## Durable state machine

Session lifecycle:

```text
open -> closing -> closed
```

`closing` rejects new input immediately. Queued turns are cancelled in
acceptance order. The active turn receives a durable cancellation request and
reaches a fenced terminal state. `session.closed` is appended only after there
are no queued, claimed, or active turns; it remains the final event. Repeating
close is idempotent.

Turn lifecycle:

```text
queued -> claimed -> running -> succeeded
                  |           -> failed
                  |           -> cancelled
                  -> queued (claim lease expires before run binding)
queued -> cancelled
```

- `queued` means the input and `input.accepted` event committed.
- `claimed` records a session dispatcher owner, fence, and lease expiry. It does
  not mean provider execution began.
- `running` is committed atomically with the unique `turn -> run` association,
  the queued Run record, and `turn.execution.started`.
- A terminal state includes the run identity and authoritative output or a
  bounded safe failure description. Terminal turn state is immutable.
- A retry after a pre-run claim expiry keeps the same logical turn. A run retry
  uses the existing run/node attempt identities and fencing behavior.

A transaction that changes visible lifecycle state also appends its event in the
same transaction. Events contain only safe identifiers and state; they never
contain a lease token, provider continuation handle, or private runtime
metadata.

## Dispatch, run identity, and ownership

Every dispatcher scans durable sessions and selects the lowest accepted sequence
that is still queued. It obtains the existing PostgreSQL coordination lease
`session:<session-id>` before changing dispatch state. SQLite is single-instance
and performs the same transition under its serialized transaction and process
lease.

Claim and run binding are separate durable boundaries so a crash before run
creation is observable and recoverable. A claim records the current owner and
fence. After a claim, the owner creates a validated Run from the pinned session
pipeline revision and turn input. One storage transaction checks the session
fence, locks the session and turn, verifies there is no active turn, inserts the
Run, binds its ID to the turn, updates the session's active-turn pointer, and
appends `turn.execution.started`. Repeating the bind returns the already-bound
Run. The unique turn key and locked session row prevent two authoritative runs.

A crash before this transaction commits leaves no Run and no binding. A crash
after it commits leaves both. A replacement dispatcher can reclaim only an
expired pre-run claim. An associated Run is always resumed or recovered through
the existing LASO runtime and its `run:<run-id>` lease; a new dispatcher never
creates a second run for that turn.

Run terminal state, the terminal SessionTurn, its safe result reference, the
session active-turn release, any new continuation record, and the corresponding
session event are committed atomically under the current run fence. Storage
verifies the unexpired run lease in the same transaction as these writes. A
stale owner cannot publish a result or continuation after takeover or
cancellation.

## Close, cancellation, and races

- Close versus submission is serialized by the session row: if submission wins,
  the accepted turn is included in closing; if close wins, submission conflicts.
- Close versus claim is serialized by the same row. A queued turn either becomes
  claimed before closing and is then cancelled as claimed work, or is cancelled
  directly from queued state.
- Close versus an active run sets durable cancellation. Runtime cancellation
  remains cooperative; the session becomes `closed` only after the active turn
  reaches its fenced terminal state.
- An explicit turn cancellation can cancel queued work directly. For claimed or
  running work it records cancellation and asks the existing runtime to cancel
  the associated run.
- Cancellation versus completion is resolved by the run-fenced terminal
  transaction. The first valid terminal transaction wins; later transitions are
  rejected. A stale success after cancellation cannot become authoritative.
- A process crash during cancellation leaves the durable request in place for
  the next run owner. The provider may continue externally until its adapter
  acknowledges or its process is stopped; LASO does not infer termination from
  a recorded request.

## Provider continuation boundary

Continuation is a provider-neutral capability with an adapter identity, schema
version, and opaque payload. It is scoped to one LASO session and is persisted
only in a private continuation record. It is not included in AgentSession,
SessionTurn API responses, SSE events, pipeline messages, ordinary logs, error
strings, or public validation artifacts. No new encryption is introduced; the
configured database and its existing access controls remain the trusted storage
boundary.

A continuation-capable adapter receives the previous opaque payload and returns
a replacement payload with its result. LASO first checkpoints that candidate
with the successful provider-node result under the run fence. It promotes the
candidate only in the same fenced transaction that makes the turn result
authoritative. Failed and cancelled runs clear the candidate without changing
the last successful continuation. A crash before the node checkpoint may repeat
the provider attempt; that remains at-least-once behavior. Stateless providers
must declare stateless operation in their provider metadata. Unsupported
continuation, missing required state, invalid state, or lost state produces an
explicit durable failure; LASO never silently starts a new provider conversation
and calls it a continuation. Timeouts follow the existing attempt timeout and
retry policy and cannot create a successful turn.

`ModelProvider` advertises one of three modes: unsupported, stateless, or opaque
continuation. The deterministic mock provider exercises opaque continuation for
core acceptance tests; `local-openai` declares stateless behavior because its
current request protocol has no resume state. The Codex, Claude Code, and
OpenCode integrations are worker adapters rather than ModelProvider
implementations. Their current worker protocols carry provider-specific session
identifiers, but the session dispatcher does not yet map those identifiers into
the opaque continuation record. They therefore are not advertised as
continuation-capable for durable session turns, and worker nodes fail closed in
session runs until that mapping is implemented. No adapter will be advertised as
resumable beyond its tested behavior.

## Persistence and migration

Session turn status, sequence, and run association remain in the durable turn
record. Provider continuation payloads use a separate internal record kind so
ordinary turn reads cannot return them. PostgreSQL requires an additive,
transactional schema migration for that record and any dispatch lookup indexes;
SQLite's documented single-instance schema upgrade must create the equivalent
record table. Both adapters must implement matching transition and fence
semantics, with SQLite explicitly excluded from multi-instance claims.

Every meaningful transition is represented in the existing ordered session
event journal: `turn.execution.claimed`, `turn.execution.started`,
`turn.execution.completed`, `turn.execution.failed`, and
`turn.execution.cancelled`. Replay remains sufficient to rebuild the public
session lifecycle after restart or disconnect.

## Deterministic fault boundaries and acceptance

`LASO_ENABLE_SESSION_TEST_HOOKS` defaults to off and is rejected unless
`BUILD_TESTING` is enabled. It adds in-process fault points after claim, before
and after run binding, and before and after the authoritative completion
transaction. The PostgreSQL CI job opts in so the recovery tests exercise these
boundaries; normal builds do not compile the hooks. The current integration
tests inject an exception after claim and after binding, then restart the service
to verify recovery. These tests verify durable boundary recovery, not abrupt OS
process death.

Run insertion and turn binding are a single storage transaction, so there is no
durable state between those operations to crash into. A crash before that
transaction commits leaves only a reclaimable claim; a crash after commit leaves
the queued Run and its turn association together. Process-death tests remain
separate acceptance gates because an in-process injected exception is not a
substitute for killing the owner or worker process.

The acceptance suite will prove basic dispatch and restart persistence;
per-session ordering with a slow first turn; cross-session concurrency;
queued/running/completed idempotent retries; PostgreSQL claim contention;
recovery on both sides of run binding; owner and worker death; disposable
PostgreSQL interruption; stale completion rejection; deterministic provider
timeout and continuation; invalid continuation failure; close/cancel races; and
complete journal replay. Existing pipeline, worker, approval, cancellation,
artifact, and storage suites remain required. TSAN is reported blocked if its
known environment limitation persists.
