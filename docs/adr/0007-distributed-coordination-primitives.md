# ADR 0007: Prepare coordination primitives for opt-in distributed execution

This ADR records the coordination primitives that preceded the distributed
execution milestone. The single-owner defaults remain authoritative, while the
later opt-in `execution_mode: multi_instance` uses these primitives for whole-run
ownership and deterministic branch `NodeWork` claims. It now also provides the
first opt-in worker-claim foundation; it does not provide exactly-once side
effects.

## Context

LASO currently assumes one service process owns a selected database. Removing that
protection without a durable ownership protocol would permit duplicate scheduling
and ambiguous recovery. PostgreSQL also needs bounded concurrent database access
as coordination work grows, while SQLite must remain the default embedded backend.

## Decision

PostgreSQL storage uses a bounded RAII connection pool. Each running service gets a
fresh opaque UUID instance identity that is independent of host, user, network, and
hardware identifiers. PostgreSQL migration version 6 adds a coordination lease
table keyed by resource. Atomic acquisition uses database time and advances a
monotonic fencing token on every takeover. Renewals require the current owner and
token before expiry; release and inspection are token-aware; protected operations
can reject stale tokens.

The existing SQLite file lease and PostgreSQL session-held advisory owner lock
remain in force. `single_owner` remains the default. PostgreSQL can explicitly
enable multi-instance whole-run ownership after the version-7 instance registry
and fenced-checkpoint work; SQLite remains single-instance.

## Consequences

The pool bounds PostgreSQL connections and provides acquisition timeout,
replacement, and utilization diagnostics without exposing credentials. Lease
contention, heartbeat renewal, expiry takeover, stale-token rejection, process
exit, and concurrent takeover are testable safety properties.

The following invariants are explicit:

- at most one active owner holds a resource for a fencing generation;
- each successful takeover receives a greater fencing token;
- stale owners cannot pass the current-token check;
- lease expiry does not prove that old external side effects stopped;
- distributed exactly-once execution is not claimed.

## Supersedes

The historical statement that multi-instance configuration is rejected is
superseded by the opt-in distributed execution milestone. ADR 0003's single-owner
default remains unchanged.

## Future work

Remote worker execution is restricted to durable `NodeWork` branches whose
persisted worker requirement matches an advertised healthy local worker. Worker
capability advertisements are bounded and identity-free. The same database-time
lease and fence used by run/node ownership protects completion, including
duplicate and late completion. An equivalent terminal replay is idempotent; a
conflicting terminal replay is rejected.

The current inline workspace manifest is bounded and content-addressed with
SHA-256. It is staged under the worker's LASO data directory and never treats an
owner absolute path as portable state. Larger artifact transport, explicit
remote cancellation acknowledgement, and worker lease-loss signalling remain
future work. Pipeline-run ownership continues to use explicit multi-instance
mode with fenced writes; exactly-once execution is not claimed.
