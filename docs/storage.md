# Storage backends

LASO application and runtime code depend on the backend-neutral `Storage` interface.
PostgreSQL is the sole shipped database implementation and is required at build and
runtime. Builds require `libpqxx-dev` and `libpq-dev`; runtime configuration requires
`postgres_dsn` (or `LASO_POSTGRES_DSN`) and accepts an optional schema name.
`storage_backend` and `db_path` were removed as misleading SQLite-era options.

## Contract

`Storage::commit` accepts a checkpoint batch and commits every record atomically or
none of them. Records are upserts by `(RecordKind, id)`; updates retain their
original insertion sequence so list order is stable. Pipeline records are immutable:
the same ID and exact body is idempotent, while a different body returns
`ErrorCode::Conflict`. Empty record IDs, unknown record kinds, invalid JSON, and
oversized bodies are rejected consistently by the adapter.

`get` returns the JSON body or `ErrorCode::NotFound`. `list` optionally filters by
`run_id`, orders by the durable insertion sequence, and supports bounded limit/offset
pagination. The application stores typed runs, attempts, messages, approvals,
artifacts, events, event-source state, external-event claims, worker jobs, schedules,
triggers, schedule occurrences, and trigger deliveries as JSON records; the storage layer
does not duplicate those domain objects into backend-specific tables.

`claim` is an atomic insert-only operation for a schedule occurrence, trigger
delivery, external event identity, or worker-job idempotency identity. It returns true only for the first claim of
an ID and never overwrites the winning body. For external events, the associated
normal Event record is inserted in the same transaction, so a successful claim
cannot expose a dedupe record without its event. This is a durable deduplication
boundary, not a distributed exactly-once guarantee.

## PostgreSQL

PostgreSQL uses a bounded RAII connection pool per `PostgresStorage`; each
transaction remains bound to one acquired connection. Pool size and acquisition
timeout are configurable and pool diagnostics are bounded. Startup creates the
configured validated schema and applies immutable version-1 through version-11
migrations in a transaction. Version 3 adds event-source state and external-event
claim records; version 4 adds durable worker jobs; version 6 adds coordination
lease state; version 7 adds service-instance state; version 8 adds durable
`NodeWork` records for eligible distributed branch execution. Version 11 adds durable session context-generation and run-context snapshot records. Context generation creation checks the expected generation under the session row lock; turn/run binding persists the immutable run snapshot with its run/turn association. A session-held advisory lock prevents two LASO services from owning the same schema by default. In
explicit `execution_mode: multi_instance`, schema migration uses a transaction
advisory lock and run writes use lease/fencing predicates instead. Schema
identifiers are validated before being quoted; table names come only from the
internal `RecordKind` mapping and values use parameterized queries. DSNs and raw
driver diagnostics are not returned to API callers or written to LASO logs.

The PostgreSQL-only `Coordination` abstraction provides atomic resource leases,
database-time heartbeats, expiry takeover, monotonically increasing fencing
tokens, release, inspection, and stale-token rejection. Each service gets a fresh
opaque UUID identity that is not derived from host or user information. These are
In multi-instance mode, the same primitives coordinate whole-run ownership,
service heartbeats, and fenced `NodeWork` claims. They do not provide distributed
scheduling, distributed worker leasing, or a cluster coordinator.

The public CI workflow starts an isolated PostgreSQL 16 service with disposable
test credentials. The storage conformance and runtime tests always run against PostgreSQL when
`LASO_TEST_POSTGRES_DSN` is configured, and a runtime/reopen test
also verifies normal pipeline and child-run persistence on PostgreSQL.

The storage implementation does not change pipeline revision immutability, checkpoint
atomicity, event ordering, approvals, recovery, artifacts, cancellation, or
parent/child persistence, schedule occurrence claims, or trigger delivery deduplication.
PostgreSQL is not a distributed worker or registry service. In multi-instance
execution, scheduler/event/manual launch paths all enqueue ordinary runs and the
same bounded dispatcher claims them. A lease loss fails closed: stale owners
cannot checkpoint after takeover. The experimental coordination setting remains
accepted only as a compatibility alias for `execution_mode: multi_instance`.

## Upgrading from SQLite

This is a breaking change. PostgreSQL is required in single-owner mode as well as
multi-instance mode. The `storage_backend` selector and SQLite-specific `db_path`
option have been removed. Old configuration files containing either key are
rejected so the previous file-backed state cannot be silently ignored.

Before upgrading, stop the old LASO process and make a verified backup of its data
directory, including `.laso/laso.db` and any configured external SQLite path. New
LASO detects the default `.laso/laso.db` SQLite signature and fails startup until
operators preserve and migrate the data. PostgreSQL initialization does not import
SQLite records, and no automatic conversion utility is included because the record
format and SQLite schema do not map safely to the PostgreSQL migration history.
Preserve the source database, plan and verify an application-specific export/import,
then point LASO at a dedicated PostgreSQL database/schema. Do not delete the backup
until approvals, execution history, schedules, events, artifacts, and worker/session
state have been checked after migration.
