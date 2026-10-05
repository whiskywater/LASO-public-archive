# Storage backend audit (resolved)

The original audit documented the SQLite-first architecture and an optional PostgreSQL adapter.
The storage migration is complete: PostgreSQL is now the only shipped implementation.

LASO keeps `Storage` as the runtime-facing record/checkpoint/claim abstraction.
`PostgresStorage` is constructed by the storage factory; runtime and orchestration code
do not issue PostgreSQL SQL or depend on libpqxx types. PostgreSQL connection management,
schema migrations, session ownership, and leases remain in the storage and coordination
modules.

`storage_backend` was removed because a field that accepted only `postgres` falsely
suggested a supported backend choice. `StorageOptions` contains only PostgreSQL connection
settings. A future adapter can be added behind `Storage` and its factory without changing
runtime or domain code. `db_path`, the public `SQLiteStorage` declaration, SQLite source,
SQLite process locks, SQLite test branches, and SQLite linkage were removed.

PostgreSQL is mandatory even for `execution_mode: single`. Single-owner mode uses a
schema-scoped session advisory lock. A second process targeting that schema is rejected.
Multi-instance mode retains its lease and fencing behavior. PostgreSQL migration history
versions 1 through 9 are preserved; existing applied migrations were not rewritten.

Tests use a disposable PostgreSQL database and unique per-test schemas. Test fixtures drop
those schemas during teardown. CI provisions PostgreSQL for the Debug, Release, sanitizer,
and optional integration suites.

Existing SQLite files are not imported. Startup detects the default `.laso/laso.db`
SQLite signature and fails with an actionable message. Old `db_path` and `storage_backend`
configuration keys are rejected. Operators must keep a backup and arrange a data export
and import before switching; LASO does not ship an automated cross-database converter.
See [the breaking storage upgrade notes](storage.md#upgrading-from-sqlite).
