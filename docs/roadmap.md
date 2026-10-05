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

Status: in progress.

Add an optional S3-compatible object-store backend so owners and workers can
access content-addressed artifacts directly from shared object storage. The
filesystem backend remains the default and must not require cloud SDKs,
accounts, or credentials. Validation uses an isolated disposable S3-compatible
service, never production cloud credentials.

M4.1 is complete only after backend conformance, bounded streaming and failure
tests, distributed owner/worker acceptance, stale-fence validation, owner
recovery, and the default cloud-independent build all pass. Remote garbage
collection must remain disabled unless it can be proven namespace-scoped,
reference-aware, and safe under pagination and concurrent writers.
