# LASO v0.1 implementation report

The repository contains a native Linux C++20 baseline. **Implementation, static
review, Ubuntu GCC/Clang builds, the complete test suite, and ASan/UBSan validation
are complete for this snapshot.** See [the evidence record](VALIDATION.md).

## Architecture and repository

```text
laso/
├── CMakeLists.txt, README.md, LICENSE, CONTRIBUTING.md, SECURITY.md, CHANGELOG.md
├── VALIDATION.md, IMPLEMENTATION_REPORT.md
├── .github/workflows/linux.yml
├── .clang-format, .clang-tidy, .gitattributes
├── include/laso/
│   ├── core/, pipeline/, runtime/, nodes/, events/, policies/
│   ├── providers/, tools/, storage/, plugins/, scheduler/
│   └── artifacts/, security/, application/, api/, cli/
├── src/
│   ├── core/, pipeline/, nodes/, storage/, plugins/, scheduler/
│   ├── runtime/{runtime,execution,graph,executor}.cpp
│   └── application/, api/, cli/
├── apps/{laso,laso-server}/main.cpp
├── plugin_sdk/include/laso_plugin.h, examples/echo.c, README.md
├── tests/{unit,integration,fixtures}/, support.hpp
├── examples/{hello-pipeline,agent-review,human-approval,native-plugin}/
├── examples/{parallel-join,bounded-loop,subpipeline}/
├── templates/README.md
├── config/laso.example.yaml, .env.example
├── docs/
└── Dockerfile, .dockerignore, deploy/{systemd,docker,examples}/
```

API and CLI call shared application services. Runtime code has no HTTP dependency.
Typed domain models, interfaces and registries separate framework mechanisms from
adapters. Boost.Asio supplies coroutines and a finite worker executor; Boost.Beast
supplies HTTP parsing/I/O. SQLite, yaml-cpp, nlohmann/json, spdlog, CLI11 and
GoogleTest use distribution packages through CMake `find_package`.

## Build and binaries

```sh
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build libsqlite3-dev \
  libyaml-cpp-dev nlohmann-json3-dev libspdlog-dev libcli11-dev \
  libboost-system-dev libgtest-dev curl jq
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build
ctest --test-dir build --output-on-failure
```

Configured outputs are `build/bin/laso`, `build/bin/laso-server`,
`build/laso_tests`, `build/plugins/liblaso_example_tool.so`, an incompatible-ABI
test module, and separate native library targets. These outputs were compiled with
GCC 13.3.0 and Clang 18.1.3 on Ubuntu 24.04.5. No managed language runtime is
required by the project.

## Implemented execution and persistence

- Declarative typed YAML parsing, duplicate/unknown-field detection, graph and
  reference checks, conditional routing and finite cycle/step budgets.
- Common interfaces for all twelve node types, registered deterministic functions,
  mock models, tools, schema validators, durable approvals, branch/join tokens,
  bounded loops and registered subpipelines.
- Async execution across runs with explicit run/node/model/tool limits, bounded
  retry attempts/delays, cooperative timeouts and stop-token cancellation.
- SQLite tables for registrations, runs, attempts, messages, events, approvals and
  artifacts. Transactions keep important checkpoint records together; WAL and
  restrictive local process ownership are used.
- Restart inspection preserves history and approval waits. Explicit resume handles
  paused checkpoints; interrupted side effects require operator review.
- In-process event subscriptions, contextual structured logs, filesystem artifacts,
  finite local scheduling, deployment rules and identity/secret interfaces.

## Plugin ABI

Linux `.so` discovery uses configured directories only and `dlopen`/`dlsym`/`dlclose`.
ABI 1 uses size/version headers, plain C types, borrowed input buffers and
host-owned output callbacks. Registered tool wrappers retain library lifetime.
The example is a harmless C JSON echo tool. Tests include invalid files and an
incompatible ABI. Other extension-kind IDs are reserved and explicitly unsupported
until adapters are implemented. Native plugins are privileged in-process code.

## API and CLI

Working implementations exist for health/version; pipeline list/register/show;
run start/list/show/cancel/resume; events/attempts/messages; approval list/approve/
reject; and provider/tool/plugin lists. API routes are under `/api/v1`, defaulting
to `127.0.0.1:8080`, with size limits and bounded pagination.

The `laso` CLI exposes the same services locally, with validate/register/list/show,
run start/cancel/resume and approval commands. Database commands cannot run beside
a daemon owning the same database; use the API while it is running. See
[the full endpoint/command reference](docs/access.md). CLI, HTTP, approval,
restart, persistence, and plugin behavior are covered by executed Linux tests.

## Examples and deployment

Seven complete YAML examples cover deterministic hello, offline agent review,
human approval, native plugin invocation, parallel/join, bounded loops and a child
pipeline. Native installation, a foreground systemd service, a multi-stage Debian
Dockerfile, persistent data volume, unprivileged runtime and local healthcheck are
provided. The Compose example retains loopback API exposure using Linux host networking.

## Tests, static analysis and sanitizers

- **Implemented:** 73 GoogleTest cases plus CLI and process/restart CTest entries.
- **Development-host-tested:** clang-format 18.1.8 verification of 48 C/C++ files;
  CMake/YAML/source grammar, local references, and security-pattern checks passed.
- **Statically reviewed:** boundaries, CMake targets, include/symbol consistency,
  ABI ownership, checkpoint handling, failure paths and deployment configuration.
- **Ubuntu-tested:** GCC and Clang Debug/Release builds and 177/177 CTest entries
  each; ASan/UBSan 177/177; Linux formatting; clang-tidy exit 0; API/CLI/process/plugin
  tests; and a loopback local-model GPU inference run.
- **Debian-tested:** the public Debian workflow; the multi-stage runtime image
  built and served its health endpoint with host networking as the unprivileged
  `laso` user.
- **Pending/blocked:** full systemd deployment remains pending; TSan is blocked on this host by an `unexpected memory mapping` runtime
  abort during GoogleTest discovery.

CI specifies Ubuntu 24.04 GCC and Clang, Debian 13, clang-format, clang-tidy and
ASan/UBSan. Optional TSan is a separate CMake configuration. The GitHub Actions workflow completed successfully for the initial public commit.

## Known limitations and deferred work

This is a single-process SQLite skeleton. Branches execute concurrently within
the bounded run scheduler. Cancellation is cooperative; a
noncooperative native plugin can block or crash the process. In-flight effects
are not exactly once, child creation is not one transaction with its parent, and
general automatic crash replay is deferred. JSON Schema validation uses the pinned
Draft 7 library with local-root and resource-limit enforcement; remote references
and external recursive reference cycles are rejected. Only Mock models and the native tool adapter ship. Durable
cron schedules, remote model/storage adapters, streaming, distributed execution,
authentication implementations and plugin sandboxing belong to later releases.
Deployment validation may expose environment-specific issues outside the executed
Ubuntu build and integration-test scope.

## Security review

No organization-specific configuration, private data or credentials were added;
terminology and obvious credential-pattern scans found no matching private material.
No default external AI call, automatic model download or unrestricted shell tool
exists. Plugins load only from configured paths; the unauthenticated API defaults
to loopback. Plugin native-code trust limits and untrusted model output handling
are documented. This is a source review, not an independent security audit.
