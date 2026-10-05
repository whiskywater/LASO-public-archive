# OpenCode worker adapter

`laso-opencode-worker` is the first coding-agent adapter built on LASO's
supervised `ProcessWorkerTransport`. It is an adapter executable, not part of
LASO Core:

```text
WorkerNode -> WorkerManager -> ProcessWorkerTransport
           -> laso-opencode-worker -> opencode serve (loopback HTTP)
```

## Installed interface

Validation used OpenCode `1.18.29` and its local headless server interface. The
adapter starts `opencode serve` directly on a loopback-only
configured port, uses structured session creation and message endpoints, and
consumes returned session/message JSON. It does not scrape human terminal
output. The adapter has no Codex, Claude, provider-auth, or pricing logic.

## Configuration

Configure it only when OpenCode is installed and a project root is explicitly
allowed:

```yaml
process_workers:
  opencode:
    executable: /absolute/path/to/laso-opencode-worker
    args:
      - --opencode
      - /absolute/path/to/opencode
      - --allowed-root
      - /srv/laso/workspaces
      - --timeout-ms
      - "120000"
    startup_timeout_ms: 120000
    request_timeout_ms: 120000
    interaction_timeout_ms: 300000
```

The outer process transport starts the adapter with an empty environment unless
explicit variables are allowlisted or literal overrides are configured. Do not
put credentials in YAML, worker metadata, or logs. The adapter passes its own
explicit executable and argument vector to OpenCode and binds the server to
127.0.0.1.

Each job must include `metadata.project_dir`. The adapter canonicalizes it and
rejects paths outside `--allowed-root` directories. A caller may provide an
existing Git worktree as that project directory; automatic worktree creation is
not implemented in this first adapter. The adapter does not manage branches.

## Sessions, results, and recovery

The first turn creates a session and returns its OpenCode session ID in the
generic result and metadata. A follow-up supplies that ID in
`metadata.opencode_session_id`; the adapter sends the next instruction to the
same session. Result normalization includes the final text, session ID,
provider/model when reported, bounded files/tool information when reported,
duration, and normalized usage. Missing OpenCode usage fields remain missing.

The durable LASO `WorkerJob` retains the external session identifier. The
reference adapter keeps active-turn state in its own process, so after an
adapter/LASO restart an in-flight turn is reported as `Unknown` unless a safe
reconciliation is available. It is never blindly resubmitted. A later explicit
follow-up may reuse a known session ID; this is not an exactly-once guarantee.

Cancellation calls OpenCode's session abort endpoint and reports acknowledgement
only when the request succeeds. If the HTTP call or child process is lost, the
outer transport reports a transport failure and LASO preserves its existing
ambiguous/`Unknown` recovery semantics.

## Permissions and questions

OpenCode `permission.asked` and `question.asked` events are observed through its
structured event stream when available. They become LASO `permission` or
`question` requests, wait for a bounded LASO response, and then call the
corresponding OpenCode reply/reject endpoint. Policy and durable human decisions
remain in LASO; the worker cannot approve itself. If an event is malformed,
unmatched, or cannot be answered, the adapter does not approve it.

## Security boundary and limitations

This is supervised process isolation, not a sandbox. It does not restrict what
OpenCode can do inside its configured project/environment. The explicit project
root check prevents adapter-directed path escape, while OpenCode itself must be
trusted for the selected project. Remote worker networking, OS/container
sandboxing, distributed leasing, and automatic multi-instance execution are
outside this change.

The adapter is optional and is disabled in the default CMake build. Enable it
with `-DLASO_BUILD_OPENCODE_ADAPTER=ON` when OpenCode integration is required.
The generic worker-request and process-transport tests remain part of the
default build. A bounded end-to-end exercise with OpenCode `1.18.29` and a
disposable synthetic workspace observed a real `permission.asked` event,
persisted it as a LASO worker request, resolved it through the LASO API, and
verified that OpenCode continued and the enclosing pipeline completed. The
OpenCode question path remains not directly validated.
