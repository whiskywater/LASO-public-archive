# Native ABI v1

`plugin_sdk/include/laso_plugin.h` is a standalone C header. The exported symbols
are `laso_plugin_query`, `laso_plugin_init`, and `laso_plugin_shutdown`. Compile them
as C or export them with `extern "C"`. The host uses `RTLD_NOW | RTLD_LOCAL`.

The descriptor begins with structure size and ABI version. Unknown ABI versions,
undersized descriptors, missing exports, oversized metadata, invalid registration,
duplicate components and failed init are rejected and reported. Known prefixes
allow future structs to append fields while retaining version-specific semantics.
Do not reorder or reinterpret fields under an unchanged ABI version.

Every buffer and string has defined ownership:

- Query descriptor and strings are plugin-owned and valid until `dlclose`.
- Host API is borrowed only during init. Registrations are accepted only then.
- The host copies registration names, JSON metadata, opaque instance pointers and
  callback addresses. Instances remain plugin-owned until shutdown.
- Input JSON and call context are borrowed until invoke returns. A callback must
  not retain them or asynchronously invoke a host callback after returning.
- Output is copied once through `write_json` into host-owned memory, capped at
  1 MiB. No allocator or release function crosses the ABI.
- Successful init produces a non-null handle. Failed init cleans up its allocations
  and leaves the output handle null. Successful handles are shut down once.
- No C++ exception may cross an exported function or callback. SDK examples are C;
  C++ plugins must catch their own exceptions and return status codes.

The callback context exposes cooperative cancellation/deadline checking. ABI v1
tool/provider callbacks are synchronous, short and local. Calls to a component
are guarded against concurrent entry; busy components fail explicitly and may
use bounded pipeline retries.

ABI v1 supports `TOOL`, `MODEL`, `EVENT`, and `WORKER` components. Existing tool/model
components remain valid because the host reads only the known prefix required by
their kind. Event components must provide the appended `event_start` and
`event_stop` callbacks; `health` is optional. An event source receives the
size-aware `emit_event` callback in its call context and may call it concurrently
from its own threads. The host callback is thread-safe, bounded, and never starts
a run synchronously. The source must stop producing events and join its threads
before `event_stop` returns. The callback is invalid after that return.

Event sources submit one JSON object containing `type`, optional `external_id`,
`occurred_at`, `payload`, `metadata`, and causal fields. LASO assigns source
identity and trigger depth, validates size/schema/security constraints, persists
the normal Event record, and invokes the existing durable trigger engine. Status
codes include `LASO_DUPLICATE`, `LASO_BACKPRESSURE`, and `LASO_STOPPED` in addition
to the original values. A duplicate means the source/external-ID identity was
already durably accepted; it is not a distributed exactly-once guarantee.

A model component's metadata is a bounded JSON object with `version`, `remote`,
`network`, `streaming`, `context_size`, `timeout_ms`, and `capabilities` fields.
Its `invoke` callback receives one JSON generation request containing `operation`,
`logical_model`, `model`, `prompt`, `input`, `options`, and `timeout_ms`. It must
write one JSON response containing `ok` and `output`; `model` and `provider` are
optional. A model `health` callback is optional and returns
`{ "healthy": boolean, "detail": string }`.

Plugins are discovered non-recursively in explicit configured directories.
Symlink entries and non-`.so` files are skipped. No default search of system library
paths occurs. Library constructors execute at `dlopen`, before ABI validation:
**an ABI check is not a sandbox or a security assessment**. Loading native code
grants execution inside the process. Bad pointers, memory corruption or fatal
signals cannot be made safe by metadata checks or exception handling.

The loader retains shared library ownership through every registered object, so an
event source library cannot be unloaded before its stop callback and accepted
ingress work have drained. No hot unloading or isolation is advertised. Tests
include valid load/invoke, event lifecycle and ingress, incompatible ABI, invalid
file, symlink exclusion, duplicate delivery, and empty discovery paths.

Worker components use the appended size-aware `worker_start`, `worker_stop`,
`worker_submit`, `worker_status`, `worker_cancel`, and `worker_result` callbacks.
The original tool/model prefix remains valid, so existing ABI-v1 tool and model
plugins do not need recompilation. Worker callbacks receive borrowed bounded JSON
and may emit events from plugin-owned threads until `worker_stop` returns. The
host serializes calls per worker, retains the shared library until shutdown, and
translates callback failures into bounded LASO transport errors. Worker event-source
identity is assigned by LASO and cannot be supplied by the plugin payload. Worker
submit/status/result JSON may optionally contain a `usage` object with normalized
fields documented in [worker adapters](workers.md); this is an additive JSON
protocol feature and does not change the C ABI. A worker-declared failed status is
kept distinct from a callback/transport failure. The optional supervised process
transport is not a native ABI revision; it uses the separate versioned local
[worker process protocol](worker-process-protocol.md).

The process protocol also carries the bounded worker-originated interaction
channel. Native ABI plugins retain their existing callback contract; they are
not required to implement process requests in this change.
