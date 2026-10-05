# ADR 0006: Native workers use an isolated transport seam

## Context

The native C worker ABI is functional and must remain compatible, while future
deployments may need supervised child processes or remote transports.

## Decision

`WorkerManager` depends on the backend-neutral `WorkerTransport` lifecycle
interface. The existing `WorkerAdapter` remains the in-process native-ABI
implementation. `ProcessWorkerTransport` is the first supervised adapter: it
executes an explicitly configured executable directly and speaks the versioned
bounded local protocol in `docs/worker-process-protocol.md`. The interface
covers start/stop, submit, status/recovery, result, and cancel; transport
exceptions are distinct from job failures.

## Consequences

Current plugins and API semantics remain unchanged. Process workers have
bounded framing, timeouts, cooperative cancellation, process-group cleanup,
and conservative failure handling. The boundary is lifecycle isolation, not an
OS/container sandbox. Unix sockets, remote transports, and distributed worker
leasing remain future work; opt-in whole-run distributed execution uses the
separate PostgreSQL coordination layer. Vendor-specific behavior remains
outside Core.

## Supersedes

None; this records the current architecture.
