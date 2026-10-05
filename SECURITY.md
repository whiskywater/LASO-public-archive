# Security boundaries

LASO v0.1.0-rc.1 is an early framework. It has not been independently audited or validated
in production. Ubuntu GCC/Clang, ASan/UBSan, Debian container, runtime-image, and
hosted CI validation have been performed; full systemd deployment behavior remains
unvalidated. See [VALIDATION.md](VALIDATION.md) for the exact record.

- The only built-in model provider is offline Mock. No external AI service is
  contacted and no model is downloaded automatically.
- Default tools/functions are harmless deterministic operations; there is no
  shell, script interpreter or arbitrary code execution endpoint.
- Pipelines are parsed using yaml-cpp and validated before execution. Duplicate
  keys, unsupported versions, custom YAML tags, invalid references and unbounded
  cycles are rejected. Payload and graph sizes are bounded.
- Policy checks occur in the runtime before invoking tools/providers. Network
  access defaults off, and remote providers cannot receive non-public metadata
  classifications through the baseline policy. Policy is not an OS sandbox.
- The management API is unauthenticated development mode on loopback by default.
  Non-loopback binding requires an explicit configuration change. A deployment
  exposing it remotely must supply an identity/authorization implementation or a
  correctly configured authenticating proxy. Do not equate loopback with complete
  access isolation from other local processes.
- Loading a native LASO plugin grants that plugin code execution inside the LASO
  process. Even shared-library constructors run before ABI checks. Only explicitly
  trusted, configured plugin directories should be used, with write access limited
  to the operator. Plugins are not isolated, and metadata can be false. Event-source
  plugins are privileged in-process code, are not sandboxed, and must stop and join
  their own producer threads before returning from the event stop callback.
- The environment secret provider resolves values on demand but is not wired into
  automatic logging or persistence. Core logs contain event identifiers, not node
  payloads, prompts or arbitrary exception messages. User input, node results and
  approval comments are deliberately durable; applications must not place resolved
  secrets in those values. No generic redaction guarantee is claimed.
- Native binaries set restrictive umask. Use a private state directory, restrict
  database backups and artifact access, and keep configuration outside public
  repositories. SQLite, PostgreSQL, and artifacts are not encrypted secret vaults.
- Cancellation and deadlines are cooperative. A faulty native extension can block
  a worker, corrupt memory or crash the daemon. Out-of-process isolation is deferred.
- Native worker plugins are privileged in-process adapters. Supervised process
  workers and PostgreSQL multi-instance workers are separate opt-in paths;
  neither is an OS sandbox. Worker requests omit instructions and input from
  durable job metadata; results, status events, and artifact references are
  bounded and treated as untrusted. Worker callbacks cannot assign LASO source
  identity or trigger depth, terminal jobs ignore late status events, and
  cancellation is reported as acknowledged only when the adapter confirms it.
  Workers must not receive production secrets through ordinary configuration.

For a vulnerability, use the repository host's private vulnerability reporting
feature when it is enabled, or contact the repository maintainer privately. Do not
post exploit details or sensitive records in public issues. No security contact
address or response-time commitment has been invented for this initial skeleton.

JSON Schema validation is local-only. Remote `$ref` retrieval is disabled, schema
paths are canonicalized beneath configured roots (including symlink checks), and
schema, reference, nesting, payload and cache limits bound validation work. External
reference cycles are rejected during declaration validation, and parsed schemas are
shared only through a mutex-protected bounded cache; validator instances remain
per-call.

Pipeline composition is local-only as well. Subpipeline references resolve only to
registered immutable `name@version` records; no package registry, URL, DNS lookup,
or remote worker is involved. Registration rejects missing revisions and recursive
dependency graphs, while runtime enforces the configured maximum depth (16 by
default). Child runs use the normal policy, schema, provider/tool, cancellation,
deadline and resource-limit paths, and parent/child identifiers are persisted
without logging payload contents.

Storage is backend-neutral at the application boundary. SQLite is the default local
backend; PostgreSQL is an explicit optional build/runtime choice. PostgreSQL DSNs are
never written to LASO logs or API error responses. PostgreSQL schema names are
validated as simple identifiers before they are used in DDL; SQL values are bound
parameters. The PostgreSQL test service in CI uses disposable credentials and data.

Opt-in PostgreSQL multi-instance execution uses database-time leases and fencing
for run control and deterministic `NodeWork` claims. Lease loss fails closed:
stale processes cannot commit node results after takeover. Supported agent/worker
workloads may execute on another LASO instance using bounded workspace manifests
and artifact metadata; arbitrary tools, shell commands, credentials, and other
side-effecting operations are not distributed. This protects ownership and
persistence, but does not provide exactly-once external side effects.

Scheduler definitions are bounded before persistence: schedule input is limited to
1 MiB, trigger metadata filters to 32 scalar entries and 16 KiB, intervals to one
year, cron fields to the documented five-field UTC form, pending launches and
deliveries to configured finite limits, and trigger recursion to a configured depth.
Due occurrences and event deliveries use insert-only durable claims, so restart
recovery does not replay the same local occurrence or event delivery casually.
Event metadata is not dumped into scheduler logs. The durable trigger engine
handles normal LASO Events, including those accepted from generic native
event-source plugins through bounded host ingress; vendor adapters, HTTP, DNS,
webhooks, filesystem, and message-bus sources are not part of Core. The host
assigns source identity and trigger depth, validates payloads before persistence,
and never persists resolved plugin secrets. A supplied `source_id` or nonzero
trigger depth in an ingress record is rejected.
