# Architecture and ownership

The public API is C++20. The shared-library extension boundary is C. The framework
release identifier (0.1.0-rc.1), YAML format (1), SQLite schema (5), and plugin ABI (1) are distinct.

| Target | Responsibility and dependencies |
|---|---|
| `laso_core` | Domain types, safe parser, config, policy contracts, registry, events, security and artifacts; no HTTP includes |
| `laso_storage` | Backend-neutral storage factory and `Storage` boundary |
| `laso_storage_sqlite` | Native SQLite C API behind `Storage`, transactional checkpoints |
| `laso_storage_postgres` | Optional libpqxx backend behind `Storage`, transactional checkpoints |
| `laso_plugin_loader` | Linux dynamic loader and C adapters for tools, model providers, and event sources |
| `laso_runtime` | Async node execution, state transitions, durable scheduling and checkpoint decisions |
| `laso_application` | Owns dependencies, registration, recovery inspection and shared services |
| `laso_api` | Versioned JSON routes and Boost.Beast asynchronous HTTP |
| `laso_cli` | CLI11 parsing, calls the same services |

Boost.Beast was selected because Ubuntu and Debian ship Boost, it supplies a
maintained HTTP parser, and Asio also provides the execution/event-loop facilities.
No custom HTTP parser or provider SDK is in the runtime. SQLite uses its small C
API instead of an ORM, while the optional PostgreSQL adapter uses libpqxx. YAML and
JSON remain at parsing and payload boundaries.

Registries use shared mutexes and shared ownership of registered objects. Plugin
tool and provider objects retain their library handle; shutdown and `dlclose` occur only after
the last registered reference disappears. A batch registration either replaces the
registry snapshot atomically or leaves it unchanged. Registration is a startup
operation. Plugins are never hot-reloaded during execution.

The executor owns a configured finite number of `std::jthread` workers and runs
Asio coroutines. Slot limiters suspend rather than block. One coroutine owns each
run's mutable snapshot. Each storage adapter serializes its short synchronous
operations behind its own backend rules; the PostgreSQL adapter uses a bounded
RAII connection pool while transactions remain connection-local. Runtime admission
and cancellation maps have their own mutex.
HTTP connections and scheduler timers use strands. These are independent locks,
not a global framework lock. Implementations registered by applications must handle
concurrent calls.

The owner must stop HTTP acceptance and scheduling, request runtime cancellation,
and drain/join workers before destroying `Service`. The daemon follows this order.
Use service and runtime APIs from C++ without initializing HTTP:

```cpp
laso::Executor executor(2);
laso::Config config;
laso::Service service(executor.context(), config);
service.register_pipeline(yaml_text);
auto id = service.start("hello", laso::Json{{"value", 42}});
executor.start();
executor.join();
auto record = service.get(laso::RecordKind::Run, id);
```

An `ExecutionContext` gives extensions run/node identity, attempt/visit number,
cooperative stop token and a steady-clock deadline. `Task<T>` is an Asio awaitable.
The baseline performs no external network or model calls. Short SQLite calls and
v1 native callbacks are synchronous; future external I/O implementations must
suspend rather than hold a worker on a blocking operation.

Pipeline registration is part of `laso_application` and uses the configured
backend-neutral `Storage` record store. A revision is keyed by `name@version`, stores the original
definition and a non-security definition fingerprint, and rejects a conflicting
definition for an existing key. Registration resolves explicit subpipeline
references and checks the stored dependency graph for missing revisions and
recursion. A parent run persists its concrete resolved references; child runs are
ordinary runtime runs rather than a second execution engine. Global node/model/tool
limiters are shared by the runtime, while each child receives its own per-run node
limiter. A parent subpipeline wait does not retain a node permit, so a configured
limit of one cannot starve the child. Thus nesting consumes global capacity and
cannot bypass policy, schema, deadline, cancellation or step/edge budgets.

## Schema contracts

The runtime uses the pinned `pboettch/json-schema-validator` Draft 7 library
through `SchemaValidator`. `input_schema` is checked immediately before a node is
invoked and `output_schema` immediately after it produces a payload. `validator`
nodes call the same service explicitly. Parsed documents are cached by canonical
path behind a mutex, while each validation owns its validator instance, so branch
validation is safe concurrently. The cache stops retaining new documents after its
configured bound. Configured `schema_roots`, canonical path checks, disabled remote
references, rejected external reference cycles, and bounded document/depth/payload
limits protect the filesystem and runtime. Relative local references resolve from
the declaring schema document; absolute and parent-traversing references are rejected.

## Durable scheduling

`LocalScheduler` stores schedules, event triggers, occurrence claims, and trigger
delivery records through the generic `Storage` boundary. It launches the same
`Service::start`/`Runtime` path used by manual runs, so policies, schemas,
approvals, retries, plugins, limits, provenance, and persistence remain shared.
The scheduler uses UTC `system_clock` wall time for durable timestamps and a
`steady_timer` only for local waiting; tests use `TestClock`.

Schedule and trigger lifecycle events are persisted as ordinary events. A
schedule occurrence is claimed by an insert-only durable record, using
`schedule_id|due_at` as its identity. This prevents the obvious restart duplicate
within the storage ownership model; LASO does not claim distributed exactly-once
execution. PostgreSQL retains its existing session ownership lease, while the
claim operation is transactional and safe under concurrent adapter calls. Optional
PostgreSQL coordination provides opaque service identities, database-time leases,
heartbeats, expiry takeover, and fencing checks. Explicit
`execution_mode: multi_instance` uses those primitives to claim whole durable
runs and, at deterministic parallel boundaries, fenced `NodeWork` records. It
does not create a distributed worker cluster or provide exactly-once external
effects. See [distributed execution](distributed-execution.md).

Event triggers match an event type and optional scalar metadata fields. Delivery
records keyed by `trigger_id|event_id` provide restart deduplication. Trigger depth
and pending delivery limits bound event loops and storms. See [scheduling](scheduling.md)
for the supported policies and API/CLI surface.

Native event-source plugins use the same stable C ABI and are loaded only from
explicit directories. Their lifecycle callbacks submit bounded JSON through a
thread-safe host callback; the host assigns source identity, validates optional
payload schemas, durably claims external IDs, and publishes ordinary Events only
after persistence. Plugin shutdown drains ingress before library unload. Event
sources never create runs directly, so existing trigger, policy, provenance,
concurrency, and recovery paths remain authoritative. See [event sources](event-sources.md).

## External workers

Worker plugins extend the same native C ABI with size-aware submit/status/result/
cancel callbacks. `WorkerNode` creates a durable `WorkerJob`, submits through the
configured adapter, and waits by observing the durable record while status events
arrive through EventIngress. Completion becomes a normal node message, so the
existing schema, policy, retry, deadline, cancellation, provenance, and storage
paths remain authoritative. Normalized optional usage is persisted with the job,
and generic wall/token/cost budgets are enforced by the manager without vendor
pricing or billing logic. `WorkerAdapter` remains the trusted native in-process
implementation of the transport boundary. The optional
`ProcessWorkerTransport` supervises an explicitly configured local executable
through a bounded versioned NDJSON protocol; it is a lifecycle boundary, not a
sandbox. Remote transports and distributed worker leasing remain deferred. See
[worker adapters](workers.md) and the [process protocol](worker-process-protocol.md).

Workers may also send a bounded, durable approval/permission/question request
through that protocol. Policy remains in LASO and the request channel is not
general RPC. The optional OpenCode and Claude Code adapters use this seam
without placing vendor-specific parsing in Core. Both remain opt-in
executables; the default build has no vendor CLI dependency.
