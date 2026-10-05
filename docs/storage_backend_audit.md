# Storage backend architecture audit

This document records the pre-implementation storage audit that informed the
backend-neutral design. Its baseline was
`ef072429c96eb9b0964e56a561591a37ccb68fbd`; statements in the baseline and
recommendation sections describe that earlier state and are intentionally
historical. The current implementation is summarized below.

## Pre-implementation baseline (historical snapshot)

Persistence is a deliberately thin record adapter:

```text
Service ──owns──> SQLiteStorage ──sqlite3 C API──> SQLite file
   │                  │
   ├── Runtime ───────┘  (Storage& in RuntimeDependencies)
   ├── LocalArtifactStore ──metadata──> Storage; bytes──> local filesystem
   └── ProcessLease ──flock(database + ".lock")
```

`Storage` exposes exactly three methods:

| Method | Contract used by callers |
|---|---|
| `commit(vector<Record>)` | All records are committed atomically, or none are. A record is an upsert by `id`; pipeline revisions are immutable. |
| `get(kind, id)` | Returns the stored JSON body, or `ErrorCode::NotFound`. |
| `list(kind, run_id, limit, offset)` | Returns bodies ordered by backend-maintained insertion `sequence`, optionally filtered by `run_id`, with bounded offset pagination. |

`Record` carries `RecordKind`, `id`, `run_id`, and a JSON body. The seven kinds
are Pipeline, Run, Attempt, Message, Approval, Artifact, and Event. There are
seven SQLite tables, each with `id TEXT PRIMARY KEY`, `run_id TEXT NOT NULL`,
`body TEXT NOT NULL`, `CHECK(json_valid(body))`, and `sequence INTEGER NOT NULL`.
Typed domain objects are serialized to JSON before crossing the storage
boundary and deserialized by application/runtime code after reads.

`Service` is the remaining architectural hard-code: its header owns
`SQLiteStorage storage_`, constructs it directly from `Config::db_path`, and
links `laso_application` directly to `laso_storage_sqlite`. `Runtime` already
depends on `Storage&`, so most execution code does not need to know which
backend is active.

## Current implementation

LASO now exposes the backend-neutral `Storage` interface and selects an
adapter through `create_storage`. SQLite remains the default and preserves the
existing file/lease behavior. PostgreSQL is an explicit optional build and
runtime choice using `libpqxx`, a schema-local migration table, parameterized
transactions, and a session-held advisory lock for single-service ownership.
`Service`, `Runtime`, and artifact metadata use the interface rather than a
backend-specific concrete type. The PostgreSQL adapter is not enabled unless
`LASO_ENABLE_POSTGRES=ON` is selected at build time and
`storage_backend: postgres` is selected at runtime.

## Exact SQLite coupling points (historical baseline)

### Backend and build coupling

- `include/laso/storage/storage.hpp` includes `<filesystem>` and declares
  `SQLiteStorage`; the abstraction and concrete backend are in the same header.
- `src/storage/sqlite.cpp` includes `<sqlite3.h>`, uses `sqlite3*` and
  `sqlite3_stmt`, `sqlite3_exec`, `sqlite3_prepare_v2`, SQLite bind/step return
  codes, and `sqlite3_busy_timeout`.
- The constructor enables `PRAGMA journal_mode=WAL`, `synchronous=FULL`, and
  `foreign_keys=ON`; checks `PRAGMA user_version`; creates tables/indexes under
  `BEGIN IMMEDIATE`; and writes `PRAGMA user_version=1`.
- `CMakeLists.txt` unconditionally finds SQLite, builds
  `laso_storage_sqlite`, and links that target into `laso_application`.
- `Service` owns the concrete type, so backend selection cannot currently be
  configuration-driven.

### SQL/dialect coupling

- Table names are selected by an array indexed by `RecordKind`; this is safe
  only while enum ordering and the seven-table layout remain identical.
- SQL is dynamically assembled only from this internal table-name array, while
  values are parameter-bound. A PostgreSQL adapter must not reproduce this with
  user-controlled identifiers.
- The upsert uses SQLite syntax:
  `ON CONFLICT(id) DO UPDATE SET body=excluded.body,run_id=excluded.run_id`.
- New sequence values use
  `COALESCE(MAX(sequence),0)+1` inside each table. This relies on SQLite's
  `BEGIN IMMEDIATE` serialization and is not safe as-is under concurrent
  PostgreSQL writers.
- JSON validity is delegated to SQLite's `json_valid`; PostgreSQL would need a
  consciously chosen `text`/`jsonb` representation and equivalent validation.
- `LIMIT`/`OFFSET` and ordering are portable in shape, but integer binding,
  error classification, and parameter APIs are backend-specific.

### Transaction and locking semantics

- Every `commit` takes a process-local `std::mutex`, starts `BEGIN IMMEDIATE`,
  applies the entire batch, and commits or rolls back.
- The constructor also performs schema creation/versioning in one immediate
  transaction.
- One `SQLiteStorage` object owns one full-mutex SQLite connection. The mutex
  serializes all reads and writes through that connection; WAL permits readers
  in the SQLite engine but the adapter mutex still serializes this object.
- SQLite busy handling is a fixed five-second timeout. PostgreSQL needs an
  explicit lock/wait/deadline policy and error mapping rather than assuming
  `SQLITE_BUSY` behavior.
- Pipeline immutability is checked by a read in the same write transaction. A
  conflicting body throws `Conflict`; identical registration is idempotent.
  The application also has a race-recovery path in `Service::register_pipeline`
  that rereads after a backend conflict.
- Checkpoints created by `Runtime::checkpoint` include the Run, Event, and any
  Attempt/Message/Approval records in one `commit`. Approval decisions include
  the Approval, waiting Attempt updates, and queued/failed Run state in one
  commit. PostgreSQL must preserve these atomic boundaries exactly.

### IDs and ordering

- IDs are generated above the backend by `uuid()` in `src/core/types.cpp`, using
  Linux `getrandom`; Message, Run, NodeExecution, Approval, Event, and Artifact
  defaults all generate string IDs. PostgreSQL must not silently introduce
  database-generated IDs or integer identity assumptions.
- `sequence` is backend-generated insertion order, not a domain ID. Updates
  retain the existing sequence because the upsert does not assign it on the
  conflict path. Recovery, history, branch ordering, and tests depend on stable
  list order.
- `run_id` is always non-null in the schema, although pipeline and registration
  events use the empty string. A PostgreSQL schema must preserve that value or
  change the interface deliberately; converting it to SQL NULL would alter
  filtering semantics.
- The application treats record JSON as the durable schema. `Run` contains the
  complete cursor/ready queue, joins, child relationships, cancellation flag,
  state, and provenance-related message data; attempts, messages, approvals,
  and events are separate durable records.

### Migration/versioning

- There are no migration files or migration library. SQLite creates the seven
  tables and indexes on every startup and uses `PRAGMA user_version`; versions
  greater than 1 are rejected, while version 0 is upgraded in place to 1.
- The schema version is not represented in `Storage` and is not visible to the
  application. A PostgreSQL adapter cannot use `PRAGMA`, and must introduce a
  PostgreSQL schema-version table or an equivalent migration mechanism.
- There are no foreign keys between record tables, so referential integrity is
  currently enforced by runtime logic and durable IDs, not by the database.

### Ownership and concurrency

- `ProcessLease` in `src/core/security.cpp` appends `.lock` to `db_path`, opens
  it with Linux `O_NOFOLLOW`, and takes an exclusive non-blocking `flock`.
  `Service` acquires this before opening storage. It assumes `db_path` is a
  filesystem path and intentionally permits only one independent LASO service
  process per database.
- This is a single-process ownership protocol, not a distributed executor
  claim. It protects approvals, recovery, and mutable in-memory `Runtime`
  state from a second CLI/daemon, but it does not coordinate multiple hosts.
- A PostgreSQL connection string is not a valid `.lock` path. The lease must
  become a backend-aware ownership service: retain `flock` for SQLite and use a
  PostgreSQL advisory lock held by a live database connection (or explicitly
  document a different multi-process model). Releasing the lease when that
  connection closes must be guaranteed.
- Runtime has its own recursive mutex and active-run map. Storage calls are
  synchronous and can run from multiple Asio worker threads; the SQLite
  adapter's mutex is therefore part of the current safety behavior. A first
  PostgreSQL implementation should use one connection plus an adapter mutex,
  or a carefully bounded connection pool, rather than an unprotected shared
  connection.

## Recovery, approvals, artifacts, and callers

- `Service::recover_history` pages all runs. Terminal runs are untouched;
  cancellation-requested nonterminal runs become Cancelled; other interrupted
  nonterminal runs become Paused and receive a recovery event. It then pages
  all attempts for that run and changes Running attempts to Failed in the same
  final checkpoint. This requires repeatable enough list ordering and atomic
  multi-record commit, but no SQLite-specific API.
- `Runtime::resume` reloads a durable Run, checks state/approval/cancellation,
  transitions it to Queued, and schedules it. Explicit resume may replay an
  unfinished node by design. Child runs are ordinary Run records linked by
  parent IDs; parent completion/cancellation logic repeatedly calls `get` and
  `list`.
- Approval lookup/listing is storage-backed. `decide` reloads the Approval and
  Run, rejects stale/duplicate decisions, updates waiting Attempts, and commits
  the decision plus resumable state atomically. PostgreSQL transaction
  isolation must not permit two approvals to both win.
- `LocalArtifactStore` writes bytes to a local file using safe `open` flags,
  `fsync`s the file and directory, then commits only Artifact metadata through
  `Storage`. PostgreSQL does not remove the local-filesystem atomicity problem:
  a future remote/object artifact store would need an explicit pending/commit
  protocol. The current backend audit should keep artifacts unchanged.
- `InProcessEventBus` publishes only after storage commit. Events are durable
  records and in-process notifications, so a backend must never publish before
  its transaction succeeds.
- Direct storage use outside the abstraction occurs in tests and in no runtime
  production component other than the `Storage&` calls listed above. The most
  important production call sites are `Service::register_pipeline`,
  `Service::run_view`, `Service::recover_history`, `Runtime::checkpoint`,
  runtime child/approval/cancellation reads, and artifact metadata commit.

## Existing tests suitable for conformance

The following are already backend-behavior tests and should become a reusable
suite parameterized by a factory returning `std::unique_ptr<Storage>`:

- `Storage.PersistsAcrossConnections`
- `Storage.TransactionRollsBackWholeCheckpoint`
- immutable pipeline conflict/idempotence behavior currently exercised through
  `Runtime.VersionedRegistryKeepsRevisionsImmutable`
- `Storage.Recovery*` tests, once setup no longer constructs `SQLiteStorage`
  directly
- runtime history, retries, cancellation, approvals, subpipeline persistence,
  event persistence, and API run creation tests in
  `tests/integration/runtime.cpp` and `tests/integration/adapters.cpp`

The conformance contract should explicitly cover: all seven record kinds;
insert/upsert behavior; same-ID update; pipeline immutability; empty and
run-filtered lists; ordering across updates; pagination bounds; NotFound and
storage/conflict error classes; all-or-nothing multi-record commits; reopen
persistence; concurrent commits; and visibility only after commit.

Tests that should remain SQLite-specific include WAL/busy behavior, `PRAGMA`
versioning, filesystem `ProcessLease`, SQLite error translation, and SQLite
database-file setup. PostgreSQL-specific tests should cover advisory-lock
exclusion, connection loss/rollback, serialization/deadlock retry policy, and
schema migration on a disposable PostgreSQL instance.

The existing `tests/integration/process-smoke.sh` is already largely backend
neutral at the service boundary. It should run once per configured backend,
with a unique database/schema and port, and should retain its approval across
daemon restart checks.

## Smallest safe architecture

Keep the record model and JSON serialization initially. Introduce a backend
factory without changing runtime semantics:

1. Make `Storage` the only public persistence dependency. Move concrete
   `SQLiteStorage` and the new `PostgresStorage` declarations into backend
   headers or a storage factory header; remove SQLite from `Service`'s member
   type.
2. Add `Config::storage_backend` defaulting to `sqlite`, plus a PostgreSQL
   connection string (prefer an environment variable such as
   `LASO_POSTGRES_DSN`, with YAML/config accepting a non-secret reference or
   DSN policy appropriate to deployment). Do not log credentials.
3. Construct `std::unique_ptr<Storage>` in a factory after configuration
   validation. `Service` keeps a `Storage&`/pointer and passes that to Runtime,
   ArtifactStore, and its own helpers. SQLite remains the default and retains
   the current file layout and old database compatibility.
4. Separate ownership from the storage record API as an `OwnershipLease`
   interface/factory. SQLite uses the current lock file; PostgreSQL uses a
   session-held advisory lock. The lease is selected with the same backend and
   acquired before serving or recovering.
5. Define a logical schema version and migration runner owned by each backend
   adapter. Share record definitions, version numbers, validation limits, and
   migration intent; keep SQL/DDL and transaction syntax backend-local.

Do not start by normalizing JSON bodies into many relational domain tables.
That would expand the compatibility surface across Run cursor state, nested
pipeline provenance, approvals, and event history. A later query-oriented
projection can be added after backend parity is proven.

## Migration strategy

Use versioned, forward-only migrations with a single transaction per migration:

- SQLite v1 remains readable exactly as today. Keep `PRAGMA user_version` as the
  compatibility marker for the existing file format; future SQLite migrations
  can be represented by numbered migration functions.
- PostgreSQL creates a small `laso_schema_migrations(version INTEGER PRIMARY
  KEY, applied_at TIMESTAMPTZ NOT NULL)` table, acquires a migration lock, and
  applies the equivalent seven-table/index schema at version 1. A unique
  `(version)` row makes startup idempotent.
- Share a logical migration manifest in C++ or a neutral documentation/spec,
  but emit separate SQLite and PostgreSQL DDL. Do not generate PostgreSQL SQL
  by textual substitution of SQLite SQL.
- Preserve `id`, `run_id`, `body`, and `sequence` semantics. Use `TEXT` for
  `body` in both backends initially to preserve exact JSON serialization and
  immutable pipeline comparisons; validate JSON in the adapter/application.
  `jsonb` can be a later indexed projection, not a silent v1 behavior change.
- For PostgreSQL, use a per-table `BIGINT` identity/sequence for new
  `sequence` values. On conflict, update only `body`/`run_id`; never change
  `sequence`. This removes the SQLite-only `MAX(sequence)+1` race.
- Map PostgreSQL unique/serialization/deadlock/connection failures to stable
  `ErrorCode` values. Do not expose driver diagnostics or DSNs in API/log
  responses.
- A future migration must be tested against a copy of an existing SQLite v1
  database and a fresh PostgreSQL database. Cross-backend data migration is a
  separate tool/problem; it should not be hidden in normal startup.

## Recommended PostgreSQL driver

Use `libpqxx` (the C++ PostgreSQL client over `libpq`) in a private
`laso_storage_postgres` target. It provides RAII connections/transactions,
parameterized/prepared statements, typed result access, and a natural C++20
fit. Keep the PostgreSQL driver out of `laso_core`, `laso_runtime`, and public
domain headers. Require a version supplied by the supported Ubuntu/Debian
toolchains, and use a CMake package target where available; a fallback to
`pkg-config`/`libpq` should be a packaging concern, not a second application
abstraction.

A single synchronous connection protected by the storage mutex is the smallest
behavior-preserving first implementation. A pool can follow only after
measuring contention and defining transaction/session ownership rules.

## Proposed target/module layout

```text
include/laso/storage/storage.hpp       # backend-neutral Record/Storage contract
include/laso/storage/factory.hpp       # backend selection/factory types
include/laso/storage/lease.hpp         # backend-neutral ownership lease
src/storage/factory.cpp                # config dispatch; no SQL
src/storage/sqlite.cpp                 # existing adapter + SQLite migrations
src/storage/postgres.cpp               # optional libpqxx adapter + migrations
src/storage/sqlite_lease.cpp           # current flock behavior
src/storage/postgres_lease.cpp         # advisory-lock behavior
```

Corresponding CMake targets should be:

- `laso_storage` for the backend-neutral contract/factory and common limits;
- `laso_storage_sqlite`, always built and linked by the default application;
- `laso_storage_postgres`, built only when `LASO_ENABLE_POSTGRES=ON` and
  `libpqxx` is found;
- `laso_application` links `laso_storage` and selected backend target(s), not
  SQLite directly.

An alternative is to keep the factory in `laso_application`; the important
property is that `Service` and Runtime no longer name `SQLiteStorage`.

## Risks and compatibility concerns

- **Single-process assumption:** Removing the file lease without replacing it
  changes approval/recovery correctness. PostgreSQL advisory locking is the
  closest semantic replacement; a future horizontally scaled design requires
  a real claim/heartbeat protocol, not just a second connection.
- **Transaction races:** Pipeline registration performs application-level
  discovery before commit. The backend must retain the unique-ID conflict and
  idempotent reread path; PostgreSQL isolation must not allow conflicting
  revisions to overwrite each other.
- **Sequence/order drift:** `MAX(sequence)+1` is unsafe with PostgreSQL
  concurrency, while identity sequences can have gaps. The contract should
  require stable insertion ordering, not gaplessness.
- **Error mapping:** callers branch on `NotFound`, `Conflict`, `Validation`,
  `Capacity`, and generic `Storage`. Driver exceptions need one translation
  boundary.
- **Synchronous I/O:** libpqxx calls would block an Asio worker just as SQLite
  currently does, but network database latency is materially less bounded.
  Add a bounded storage executor or pool before production remote deployment.
- **JSON semantics:** `jsonb` canonicalizes/reorders representation and would
  undermine byte-for-byte body comparisons. Keep text first.
- **Filesystem artifacts:** PostgreSQL persistence does not make local artifact
  bytes durable or shareable across hosts. Artifact storage needs its own
  backend and commit/reconciliation design.
- **Secrets/configuration:** DSNs may contain passwords. Configuration parsing,
  process arguments, logs, crash reports, and error responses must avoid
  exposing them.
- **Schema evolution:** There is no existing migration framework or downgrade
  story. Forward-only startup migrations with explicit version rejection are
  safer than attempting automatic cross-database conversion.
- **Test environment:** PostgreSQL integration must be opt-in or containerized
  so the documented 116-test SQLite baseline remains fast and deterministic.

## Files likely to change

Required implementation files:

- `include/laso/storage/storage.hpp`
- new storage factory/lease headers and `src/storage/factory.cpp`
- `src/storage/sqlite.cpp`
- new `src/storage/postgres.cpp` and PostgreSQL lease module
- `include/laso/application/service.hpp`
- `src/application/service.cpp`
- `include/laso/core/config.hpp`
- `src/core/config.cpp`
- `CMakeLists.txt`
- `tests/integration/adapters.cpp`, `tests/integration/runtime.cpp`, and
  `tests/support.hpp`

Likely documentation/config updates:

- `README.md`, `docs/architecture.md`, `docs/runtime.md`,
  `docs/linux-deployment.md`, and an example config file
- CI/container files if PostgreSQL conformance is enabled (`.github/workflows`,
  `Dockerfile`, or a dedicated compose/test fixture)

Files that should not need persistence changes in the first pass:

- pipeline parsing/schema validation, node execution semantics, provider/tool
  registries, API routing, and local artifact byte handling.

## Recommended implementation sequence (3–6 commits)

1. **Extract the backend-neutral construction seam.** Introduce storage factory
   and lease interfaces, convert `Service` to own `unique_ptr<Storage>`, add
   `storage_backend: sqlite` config, and prove the unchanged SQLite default with
   the then-existing 116/116 suite.
2. **Make SQLite an explicit adapter with migration tests.** Preserve v1 file
   compatibility, move schema setup into a SQLite migration component, retain
   WAL/full-sync/file-lease behavior, and parameterize the storage conformance
   tests without changing runtime semantics.
3. **Add optional PostgreSQL adapter and migrations.** Add the libpqxx target,
   connection/DSN validation, v1 schema, prepared statements, transaction/error
   mapping, per-table sequence generation, and advisory lease. Do not enable it
   by default.
4. **Run backend conformance and lifecycle tests.** Execute the shared storage,
   recovery, approval, restart, artifact metadata, API, and process-smoke tests
   against disposable PostgreSQL and SQLite instances; add failure/reconnect
   and concurrent-registration coverage.
5. **Document/package operational behavior.** Add PostgreSQL config examples,
   secret handling, migration/runbook instructions, optional CI service setup,
   and explicit limits around synchronous remote database latency and local
   artifacts.
