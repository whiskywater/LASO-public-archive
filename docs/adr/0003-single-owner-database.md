# ADR 0003: One LASO owner per database by default

> Historical scope. PostgreSQL remains the default and only database for single-owner mode; SQLite ownership behavior no longer applies. See [ADR 0009](0009-postgres-only-storage.md).

The PostgreSQL-only opt-in `execution_mode: multi_instance` described in
`docs/distributed-execution.md` supersedes the former single-owner-only scope.
SQLite and the default PostgreSQL mode still follow this ADR.

## Context

Default scheduler and recovery semantics are local-service semantics, not a
distributed execution protocol.

## Decision

One LASO service owns a selected database. SQLite uses its process lease and
PostgreSQL uses a session-held advisory lock. A competing owner fails during
startup.

## Consequences

Claims and transactions are safe within the selected ownership model. Distributed
worker leasing and HA remain explicitly out of scope; opt-in PostgreSQL
multi-instance mode adds run leases and, at deterministic parallel boundaries,
fenced `NodeWork` claims. It does not provide exactly-once external effects.

## Supersedes

Superseded for PostgreSQL multi-instance mode by the distributed execution
milestone; retained as the default ownership decision.

> **Status: partially superseded** by ADR 0009. PostgreSQL single-owner mode remains; SQLite ownership no longer applies.
