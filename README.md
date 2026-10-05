# LASO

**LASO is an orchestration framework, not a prebuilt AI assistant.**

Local AI System for Orchestration is an early, Linux-first C++20 framework for
declarative workflows, deterministic functions, model and tool registries, policy
checks, durable human approval, and execution history. Applications supply their
own logic and integrations. Agents are one node type; pipelines are the root
abstraction. This tree is release candidate **0.1.0-rc.1**; read the support
matrix and security limitations before production deployment.

**Validation status:** implemented, statically reviewed, and validated on Ubuntu
with GCC and Clang, ASan/UBSan, a Debian 13 container build, a runtime image
health check, and GitHub Actions. These are development and CI results, not a
production-readiness claim. See [VALIDATION.md](VALIDATION.md) for exact scope,
systemd deployment validation status, and the blocked host TSan run.

```text
                 API / CLI
                     |
              Application services
                     |
               Pipeline runtime
                     |
          +----------+----------+
          |          |          |
        Nodes   Storage backends  Events
                   /      \\
               SQLite   PostgreSQL*
          |
       Registries
          |
     Versioned C plugin ABI
          |
 Native tool and model-provider adapters
```

## Linux requirements and build

Target environments: Ubuntu 24.04 LTS / Debian 13, x86-64, GCC or Clang,
C++20, CMake 3.22+, Ninja. No Python, Node.js, Java, model download, external AI
account, or GUI is required to build or run LASO. Dependencies come from the
distribution; CMake fetches the pinned small MIT-licensed JSON Schema validator
when it is not already available locally.

See [build and install](docs/build.md) for the clean-clone dependency list,
Debug/Release/sanitizer commands, PostgreSQL options, provider adapters, and
installation layout.

PostgreSQL is an optional build and runtime backend. Install `libpqxx-dev` and
`libpq-dev`, configure with `-DLASO_ENABLE_POSTGRES=ON`, then select it with
`storage_backend: postgres` and a `postgres_dsn` connection string (or the
`LASO_STORAGE_BACKEND`, `LASO_POSTGRES_DSN`, and `LASO_POSTGRES_SCHEMA`
environment variables). SQLite remains the default. DSNs are never included
in LASO error messages or logs. PostgreSQL uses a bounded connection pool;
`postgres_pool_min_connections`, `postgres_pool_max_connections`, and
`postgres_pool_acquisition_timeout_ms` tune it. Single-owner mode remains the
default. PostgreSQL builds may opt into `execution_mode: multi_instance`; this
enables durable run claiming and fencing for multiple LASO service processes.
SQLite remains single-instance. See [distributed execution](docs/distributed-execution.md)
for the exact ownership, crash-recovery, and non-exactly-once guarantees.

The OpenCode worker adapter is optional and disabled by default. Build it only
when needed with `-DLASO_BUILD_OPENCODE_ADAPTER=ON`; native and generic
process-worker support remain available in the default build.

The Codex worker adapter is also optional and disabled by default. Build it with
`-DLASO_BUILD_CODEX_ADAPTER=ON`; it uses the installed Codex app-server through
the same supervised process boundary. No Codex installation or account is
required for the default build or tests. See [Codex worker](docs/codex-worker.md).

The default build has no PostgreSQL development-library requirement. Produced
binaries are `build/bin/laso`, `build/bin/laso-server`, and
`build/laso_tests`. Example C plugins are
`build/plugins/liblaso_example_tool.so` and
`build/plugins/liblaso_example_model_provider.so`; the offline worker example is
`build/worker-plugins/liblaso_example_worker.so`. Core, runtime, storage factory,
SQLite, optional PostgreSQL, plugin loader, application, API, and CLI are separate
library targets. Installation includes the executables, public headers, C SDK
header, example configuration, public documentation, and a prefix-configured
systemd service unit. Disable it with `-DLASO_INSTALL_SYSTEMD_UNIT=OFF` for a
user-local CLI-only install. A relocatable CMake SDK package is deferred.

## First pipeline

```yaml
laso: "1"
name: hello
version: 1
nodes:
  greet:
    type: function
    function: hello
edges:
  - {from: input, to: greet}
  - {from: greet, to: output}
```

`input` and `output` are implicit boundary nodes. Functions and tools resolve
through registries; no shell command interpretation occurs. See
[pipeline format](docs/pipeline-format.md) for the intentionally small schema.

```sh
./build/bin/laso pipeline validate examples/hello-pipeline/pipeline.yaml
./build/bin/laso run start examples/hello-pipeline/pipeline.yaml --input '{"value":42}'
./build/bin/laso run start examples/agent-review/pipeline.yaml
./build/bin/laso run start examples/human-approval/pipeline.yaml
./build/bin/laso pipeline register examples/composition/normalize.yaml
./build/bin/laso pipeline register examples/composition/process.yaml
./build/bin/laso run start process@1 --input '{"value":42}'
./build/bin/laso approval list
./build/bin/laso approval approve APPROVAL_ID --actor operator --comment Reviewed
./build/bin/laso run list
./build/bin/laso run inspect RUN_ID
./build/bin/laso node-work list --run-id RUN_ID
./build/bin/laso instance list
./build/bin/laso artifact verify
./build/bin/laso artifact list --run-id RUN_ID
./build/bin/laso artifact gc
```

The approval example exits with `WaitingApproval`. A later CLI process opens the
same SQLite database, records the decision, and continues. `LASO_DATA_DIR` defaults
to `.laso` relative to the working directory. The CLI is a local service adapter,
not an HTTP command wrapper. Only one service process may own a database: while
the daemon is running, use its API. Stop it before using local CLI database commands.
`laso health` checks local storage initialization; use HTTP health to probe the daemon.
[Configuration reference](docs/configuration.md) explains storage, deadlines,
worker capabilities, budgets, cancellation, and security-sensitive values.
The [artifact-store guide](docs/artifacts.md) explains content-addressed
streaming, integrity checks, materialization, and safe cleanup.
The local filesystem backend remains the default. The optional S3-compatible
backend is described in the [artifact guide](docs/artifacts.md) and
[configuration reference](docs/configuration.md); M4.1 validation status is
tracked in [the roadmap](docs/roadmap.md) and [validation record](VALIDATION.md).

## API

```sh
./build/bin/laso-server --config config/laso.example.yaml
curl -fsS http://127.0.0.1:8080/api/v1/health
curl -fsS http://127.0.0.1:8080/api/v1/version
jq -n --rawfile yaml examples/hello-pipeline/pipeline.yaml '{yaml:$yaml}' |
  curl -fsS http://127.0.0.1:8080/api/v1/pipelines \
  -H 'Content-Type: application/json' --data-binary @-
curl -fsS -X POST http://127.0.0.1:8080/api/v1/pipelines/hello@1/runs \
  -H 'Content-Type: application/json' -d '{"input":{"value":42}}'
```

Development identity is unauthenticated and bound to `127.0.0.1:8080`. Remote
binding requires deliberate `allow_remote_api` configuration and deployment-owned
authentication. The API never accepts filesystem paths for pipeline registration.
[API and CLI reference](docs/access.md) lists all endpoints and commands. The [session SSE contract](docs/session-sse.md) documents stream admission, `Retry-After`, heartbeat, and replay behavior.

## Native plugins

```sh
LASO_PLUGIN_DIR=build/plugins ./build/bin/laso plugin list
LASO_PLUGIN_DIR=build/plugins ./build/bin/laso run start examples/native-plugin/pipeline.yaml
LASO_PLUGIN_DIR=build/plugins ./build/bin/laso --config examples/plugin-model/config.yaml \
  run start examples/plugin-model/pipeline.yaml
```

Plugins are loaded with `dlopen`/`dlsym`, only from configured directories.
The SDK uses a versioned C ABI, explicit structure sizes, borrowed inputs,
host-owned output callbacks, and no STL objects or exceptions across the boundary.
Tool, model-provider, event-source, and generic external-worker components are
operational. Model providers
are resolved through the existing provider registry, so built-in mock and local
providers remain available; other component kinds have reserved IDs and return
`LASO_UNSUPPORTED`. Event
sources use the ABI lifecycle suffix and a bounded thread-safe host callback to
submit canonical events; they never create runs directly. Worker adapters submit
durable jobs and report status through the same event ingress path; they never
create pipeline runs directly. Worker usage is optional and normalized when
reported; generic per-job wall-time and per-run token/cost budgets are available
without vendor pricing or billing logic. The transport boundary is
backend-neutral, while the shipped implementation remains the trusted native
in-process adapter by default. An opt-in supervised local process transport is
also available through `process_workers`; see the [worker process protocol](docs/worker-process-protocol.md).
It is generic and does not add vendor-specific behavior.

The optional `laso-opencode-worker` adapter uses that same supervised boundary
for the installed OpenCode headless session API. It is disabled by default;
see [OpenCode worker](docs/opencode-worker.md) for explicit project-root and
environment configuration. Worker-originated approvals, permissions, and
questions remain under LASO policy and durable operator control.

The optional `laso-claude-worker` adapter uses Claude Code's documented
headless `stream-json` CLI interface, including session resume where supported.
It is disabled by default and requires an explicitly configured Claude Code
executable and allowed project root; see [Claude Code worker](docs/claude-worker.md).

**Loading a native LASO plugin grants that plugin code execution inside the LASO
process.** Metadata validation does not isolate native code. See the
[SDK](plugin_sdk/README.md) and [ABI contract](docs/plugin-abi.md).

## Examples

| Directory | Behavior |
|---|---|
| `hello-pipeline` | Input → deterministic function → output |
| `agent-review` | Two offline mock model calls and a structured validator |
| `human-approval` | Durable pause and CLI/API decision |
| `native-plugin` | Harmless JSON echo through a native C plugin |
| `plugin-model` | Offline AgentNode response from a native model-provider plugin |
| `parallel-join` | Fork, checkpoint each branch, combine results in branch order |
| `schema-contract` | Offline function pipeline with input/output JSON Schema contracts |
| `bounded-loop` | Exactly two deterministic repetitions |
| `subpipeline` | Invoke registered `hello@1` through `parent@1`; register it first with `pipeline register` |
| `composition` | Offline versioned child pipeline and A → B → C composition |
| `scheduling` | Offline schedule and event-trigger definitions for a deterministic pipeline |
| `event-source` | Offline native event-source plugin → durable event trigger → pipeline |
| `worker-adapter` | Offline worker plugin → durable worker job → status event → validated output |
| `process-worker` | Supervised local process transport → deterministic reference worker host |
| `distributed` | PostgreSQL owner + worker → durable claim → deterministic validation |
| `opencode-worker` | Optional supervised OpenCode session adapter (requires a local OpenCode installation) |
| `codex-worker` | Optional supervised Codex session adapter (requires a local Codex installation) |
| `claude-worker` | Optional supervised Claude Code session adapter (requires a local Claude Code installation) |
| `local-openai` | Optional loopback-only OpenAI-compatible local model call |

For an optional loopback-only OpenAI-compatible local model service, see
[local model services](docs/local-models.md). It is disabled by default and does
not download or launch models.

## Development and deployment

See [architecture](docs/architecture.md), [runtime semantics](docs/runtime.md),
[persistent session primitives](docs/sessions.md), [session SSE admission and replay](docs/session-sse.md),
[pipeline composition](docs/pipelines.md),
[storage backends](docs/storage.md),
[scheduling and event triggers](docs/scheduling.md),
[build and install](docs/build.md), [configuration](docs/configuration.md),
[capability matrix](docs/capability-matrix.md),
[operations and recovery](docs/operations.md),
[security threat model](docs/security-threat-model.md),
[Linux deployment](docs/linux-deployment.md), [security](SECURITY.md), and
[contribution instructions](CONTRIBUTING.md). CI specifies Ubuntu GCC/Clang,
Debian 13, ASan/UBSan, formatting, clang-tidy, and a real PostgreSQL service job.
Tests use GoogleTest and CTest, plus a shell process/restart smoke test; no external
AI services are used.

```sh
docker compose -f deploy/docker/compose.yaml up --build
```

The Compose example deliberately uses Linux host networking with the API on host
loopback. Persistent data lives in a named volume. The installed systemd unit runs
the daemon in the foreground as an unprivileged `laso` service account and uses a
systemd-managed state directory. Dedicated-account system-service installation,
filesystem restrictions, startup/health, stop/restart, and durable recovery have
been exercised separately from rootless user-service acceptance; see the exact
scope and remaining limitations in `VALIDATION.md`.

## Current limitations and deferred work

- Linux builds, tests, sanitizer builds, the Debian container path, and a native
  dedicated-account systemd service lifecycle have documented validation. These
  results do not by themselves establish general production readiness.
- M3.6 cross-machine artifact transport and its worker-loss, fencing, database
  interruption, owner-recovery, and integrity acceptance are closed; see
  `VALIDATION.md`. M4.1 adds an opt-in S3-compatible store. Remote S3 garbage
  collection is intentionally not supported in this milestone.
- SQLite remains one-process only. PostgreSQL supports an explicit multi-instance
  execution mode with bounded run claims, database-time leases, heartbeats,
  fencing tokens, crash takeover, and durable deterministic branch work. A run
  has one control owner at a time; eligible pure branch paths may execute on
  different instances, while side-effecting/local-session branches remain with
  the owner. External side effects are not exactly once. PostgreSQL operations
  use bounded pooled connections.
- Fork branches execute concurrently through the bounded executor when capacity is
  available, while join results retain pipeline branch order. Global, per-run,
  model-call, and tool-call limits bound work; cancellation is cooperative. Approval
  pauses the entire run.
- Registered pipeline revisions are immutable `name@version` records. Subpipeline
  nodes execute normal durable child runs with persisted parent/child links,
  version resolution, approval/retry/recovery behavior, and a configurable maximum
  depth. Distributed execution is opt-in and requires PostgreSQL; LASO does not
  provide a package registry or cluster scheduler.
- Deadlines and cancellation are cooperative. A native plugin that blocks or
  misbehaves can block a worker or crash the process. The v1 tool/provider
  invocation ABI is for short local operations; event sources may emit from
  their own threads but must quiesce those threads before their stop callback
  returns. Native plugins are privileged in-process code and are not sandboxed.
- Approval waits and history survive restart. In-flight external effects are not
  exactly once; an explicit resume may replay an unfinished node. Operators must
  review interrupted runs and use the durable inspection commands during recovery.
- Validator nodes retain the legacy field/value routing mode when no `schema` is
  declared; declared schemas use the shared local JSON Schema engine. Prompt values
  are inline text, not automatically read from files.
- Mock is the default built-in provider. An optional loopback-only OpenAI-compatible
  adapter can call an already-running local model service. No model serving, remote
  adapters, streaming, secret persistence, sandboxing, authentication platform, or
  GUI is included.
- Schedules and event triggers are durable local framework records. One-time,
  interval, UTC five-field cron, and internal-event triggers launch normal runs;
  misfire, overlap, delivery-depth, and pending-work bounds are explicit. Vendor-
  specific adapters are optional and remain outside Core; arbitrary remote tools
  and exactly-once claims are not supported. Generic configured event-source and worker plugins can ingress validated
  events or submit durable external jobs; supplied external IDs deduplicate within
  the selected storage database. See [worker adapters](docs/workers.md).

Licensed under Apache License 2.0.
