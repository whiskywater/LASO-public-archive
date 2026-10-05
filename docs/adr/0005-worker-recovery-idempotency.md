# ADR 0005: Worker recovery is idempotent and conservative

## Context

A service can stop after durable submission but before the external system has
reported a terminal result.

## Decision

The durable idempotency identity is persisted before submission. Recovery uses a
persisted external job ID and asks recovery-capable adapters for status. Unknown
outcomes remain inspectable and are not submitted repeatedly; terminal records
ignore late events.

## Consequences

Restart does not invent completion or claim arbitrary exactly-once behavior.
Cancellation remains cooperative, and operators may need to resolve external
effects that cannot be reconciled.

## Supersedes

None; this records the current architecture.
