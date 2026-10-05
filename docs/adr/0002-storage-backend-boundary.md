# ADR 0002: Storage is backend-neutral

## Context

LASO supports embedded SQLite and an optional PostgreSQL deployment without
duplicating runtime persistence semantics.

## Decision

Runtime code uses the backend-neutral `Storage` record/claim contract. SQLite
and PostgreSQL own their drivers, SQL, schema setup, transactions, and locking
behind that boundary.

## Consequences

SQLite remains the default embedded backend. Backend conformance tests define
shared behavior; backend-specific tests cover driver and deployment details.

## Supersedes

None; this records the current architecture.
