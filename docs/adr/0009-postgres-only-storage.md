# ADR 0009: PostgreSQL is the sole database implementation

## Status

Accepted

## Decision

PostgreSQL is required at build and runtime, including single-owner deployments.
The orchestration runtime continues to depend on the generic `Storage` contract;
`PostgresStorage` remains behind the storage factory. `storage_backend` is removed
because it would accept only one value and imply an available alternative. The
SQLite adapter, `db_path`, process file lock, SQLite dependency, and SQLite test
backend are removed.

Single-owner advisory locks are scoped to a PostgreSQL schema so independent LASO
schemas can run and migrate concurrently. A second owner of the same schema is
rejected. Multi-instance leases and fencing remain available. PostgreSQL migration
history stays authoritative and previously applied migrations are unchanged.

## Compatibility

This is a breaking change. SQLite state is not imported. The default `.laso/laso.db`
file is detected and startup fails safely; old `db_path` and `storage_backend`
configuration keys are rejected. Operators must preserve and back up SQLite state,
then perform and verify an application-specific export/import before switching. No
SQLite-linked migration utility is included.
