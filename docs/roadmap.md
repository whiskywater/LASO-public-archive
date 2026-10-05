# LASO milestone roadmap

## M3.6 — artifact transport: closed

M3.6 validated bounded content-addressed artifact transport across separate
Linux owner and worker OS/process/network boundaries. The accepted scope covered
PostgreSQL coordination, worker claims and heartbeats, shared artifact
transport, worker-loss and interrupted-upload recovery, stale-completion
fencing, PostgreSQL interruption, owner restart, and hash/size/provenance
verification. See [the validation record](../VALIDATION.md) for the evidence.

The implementation guarantees at-least-once attempts with one authoritative
fenced completion. It does not guarantee exactly-once execution or exactly-once
external side effects.

## M4 — production distributed artifact infrastructure

M4 improves the artifact infrastructure used by supported remote agent/worker
workloads. It does not expand distributed execution to arbitrary side-effecting
tools, general remote shell commands, or cluster scheduling.

### M4.1 — optional S3-compatible shared artifact store

Status: closed. Physical acceptance, hosted CI, merge, and post-merge smoke passed.
Merged as `fc26ea6ad67a5365d17cf22a164dd1e59c0d2fef`.

Add an optional S3-compatible object-store backend so owners and workers can
access content-addressed artifacts directly from shared object storage. The
filesystem backend remains the default and must not require cloud SDKs,
accounts, or credentials. Validation uses an isolated disposable S3-compatible
service, never production cloud credentials.

Physical acceptance has passed for backend failure handling, separate-machine
owner/worker execution, stale fencing, owner recovery, PostgreSQL interruption,
and trusted and untrusted TLS. The implementation and regression evidence are
recorded in [VALIDATION.md](../VALIDATION.md). Remote garbage collection
remains disabled unless it can be proven namespace-scoped, reference-aware, and
safe under pagination and concurrent writers.

## M5 — durable agent sessions

M5 adds persistent session identity and input delivery for applications that
need to address one logical agent session from multiple clients. Session event
sequences are durable and replayable; external applications retain user and
session authorization policy.

### M5.1 — durable input journal and replay

Status: closed. M5.1 acceptance and hosted CI passed on PR #13; the merge records this closeout.

Provide durable session creation, bounded idempotent input acceptance, ordered
per-session events, close semantics, event replay, and resumable SSE. Acceptance
requires identical retries to return the original turn, conflicting reuse of an
idempotency key to fail, concurrent submissions to receive one ordered sequence,
closed sessions to reject new inputs, and the journal to survive service restart.
PostgreSQL multi-instance acceptance also requires an event written through one
service instance to be replayed and streamed by another. PostgreSQL is required
for single-owner and multi-instance deployments.

This milestone stores accepted inputs; it does not claim they have run. PR #13
adds no provider continuation state, turn dispatcher, execution lease, or worker
recovery for sessions.

### M5.2 — durable sequential turn execution and provider continuation

Status: implementation merged in PR #14. The hosted regression matrix passed;
the PR records physical owner-death, PostgreSQL outage/recovery, and
restart-continuation cases as follow-up validation not repeated on its exact
candidate.

The durable state machine, run-binding transaction, execution ownership,
continuation boundary, and close/cancellation races are specified in
[m5-2-design.md](m5-2-design.md).

Accepted turns connect to LASO runs and the existing worker recovery and fencing
model. Per-session ordering and ownership, turn states, cancellation and close
races, provider continuation persistence, and lifecycle events are implemented.
Provider state remains opaque to clients and is protected from logs and public
artifacts. See [the M5.2 design](m5-2-design.md) and
[the session API](sessions.md).

The suite covers duplicate submission, two-instance claim contention, worker
and process recovery boundaries, stale-fence completion, provider timeout, and
replay of committed transitions. The physical follow-up scenarios listed above
remain evidence gaps. LASO may make at-least-once provider attempts with one
authoritative fenced completion; it does not promise exactly-once external side
effects.
