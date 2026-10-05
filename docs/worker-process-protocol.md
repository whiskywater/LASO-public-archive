# Local worker process protocol

LASO supports a versioned, local-only worker-host protocol for adapters that
must not run inside the LASO server process. Version 1 uses newline-delimited
JSON (NDJSON) over the child's stdin and stdout. The newline is the frame
boundary; each request receives exactly one response, and unsolicited messages
are rejected by the host transport. A worker may send one correlated
`worker_request` while LASO is waiting for a response; the host answers it with
one `worker_response` before continuing the original operation. No other
unsolicited message type is accepted.

## Request

Every request is an object with the following fields:

```json
{
  "protocol_version": 1,
  "request_id": "req-1",
  "operation": "submit",
  "job_id": "worker-123",
  "external_job_id": "",
  "payload": {}
}
```

`operation` is one of `hello`, `submit`, `status`, `result`, `cancel`, or
`shutdown`. `job_id` is the durable LASO job identifier. `external_job_id` is
required for operations that address an already-submitted external job.
Payloads are structured JSON and are bounded by the transport.

## Worker-originated requests

The request channel is deliberately narrower than RPC. Supported request types
are `approval`, `permission`, and `question`:

```json
{
  "protocol_version": 1,
  "message_type": "worker_request",
  "request_id": "request-17",
  "worker_job_id": "worker-123",
  "worker_id": "example",
  "external_job_id": "child-123",
  "session_id": "session-1",
  "request_type": "permission",
  "title": "Run local test",
  "summary": "The worker requests permission to run a command",
  "payload": {"resource": "project.tests", "command": "..."},
  "created_at": "2026-09-17T12:00:00Z",
  "deadline": "",
  "risk": "medium",
  "category": "tool"
}
```

LASO evaluates permission/approval requests through its configured policy and
otherwise stores a durable pending decision. Questions require a human answer.
The response is:

```json
{"protocol_version":1,"message_type":"worker_response",
 "request_id":"request-17","decision":"approved","payload":{},"reason":"..."}
```

Durable interaction states are `pending`, `approved`, `denied`, `answered`,
`cancelled`, and `expired`. Duplicate request IDs replay the existing decision
only when the job and request type match. Restart never auto-approves a pending
request, and cancelling the owning job cancels its pending requests. The
configured interaction timeout is bounded; a timeout becomes `expired`.
Workers cannot call arbitrary LASO methods or submit pipelines through this
channel.

## Response

```json
{
  "protocol_version": 1,
  "request_id": "req-1",
  "ok": true,
  "state": "Completed",
  "external_job_id": "child-123",
  "payload": {"ok": true},
  "metadata": {},
  "artifacts": [],
  "usage": {"executor": "reference"},
  "error": ""
}
```

`ok: false` is a transport/protocol error and is not a worker-declared job
failure. A response with `ok: true` and `state: Failed` is a worker-declared
job failure. `state` uses LASO's normalized worker states. `usage` is optional
and uses the backend-neutral `WorkerUsage` fields. Artifact entries are
metadata/reference objects only; the protocol does not authorize arbitrary
filesystem paths.

The transport enforces a 1 MiB maximum frame, 64 KiB maximum captured stderr,
64 KiB metadata, 16 artifact references, one outstanding request, 4 KiB
interaction text fields, a 512-byte interaction identity, 64 KiB interaction
payload, and bounded startup/request/interaction timeouts from configuration.
Malformed, oversized, truncated, unexpected, or version-incompatible frames are
transport failures.

## Process boundary

LASO executes the configured absolute executable directly with an argument
vector; it never interpolates a shell command. The child receives an empty
environment by default, plus explicitly configured overrides and values named
by an environment allowlist. Explicit overrides win when a name is present in
both sources. Configuration rejects NUL characters, duplicate allowlist names,
and oversized values; the total child environment is bounded to 64 KiB at
launch. This is process isolation and lifecycle supervision, not an
OS/container sandbox. The child can affect LASO only by returning protocol
messages.
