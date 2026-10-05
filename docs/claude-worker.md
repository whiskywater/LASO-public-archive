# Claude Code worker adapter

`laso-claude-worker` is an optional coding-worker adapter outside LASO Core:

```text
WorkerNode -> WorkerManager -> ProcessWorkerTransport
           -> laso-claude-worker -> Claude Code CLI
```

## Supported interface

The adapter targets Claude Code's documented headless CLI interface:

```text
claude --print --output-format stream-json --input-format stream-json --verbose
```

It consumes newline-delimited structured messages, not terminal formatting.
The adapter records the `system/init` session ID, typed assistant text and tool
blocks, and the terminal `result` message. Claude Code's `--resume` option is
used for a later turn when `metadata.claude_session_id` is supplied.

Claude Code was not installed in the validation environment used for this
change, so the real-provider test is opt-in and skips unless both
`LASO_RUN_REAL_CLAUDE=1` and `CLAUDE_BIN` are supplied. The deterministic CTest
fixture exercises the same stream framing, session, result, failure, timeout,
and interaction paths without requiring an account or network access.

## Configuration

The adapter is disabled by default. Build it with:

```sh
cmake -S . -B build -G Ninja -DLASO_BUILD_CLAUDE_ADAPTER=ON
```

Configure it through the existing `process_workers` map:

```yaml
process_workers:
  claude:
    executable: /srv/laso/bin/laso-claude-worker
    args:
      - --claude
      - /usr/local/bin/claude
      - --allowed-root
      - /projects/example
    startup_timeout_ms: 5000
    request_timeout_ms: 120000
    interaction_timeout_ms: 300000
```

The adapter's `--allowed-root` options are repeated as needed. The submitted
job must contain `metadata.project_dir`. Both roots and project directories are
canonicalized; missing directories, traversal, and detectable symlink escapes
are rejected. An explicit existing Git worktree may be used as the project
directory. LASO does not automatically create or delete worktrees.

The outer process transport supplies an empty child environment by default.
Use its explicit `environment_allowlist` and literal `environment` overrides
for narrowly selected provider configuration. Credentials are not copied into
worker metadata, results, durable records, or logs.

## Sessions, interactions, and recovery

The first completed turn returns a generic result containing the session ID,
summary, model when reported, bounded tool count, duration, and optional token
usage. A follow-up passes the session ID in `metadata.claude_session_id`; the
adapter invokes Claude Code with `--resume` and returns the same session when
the CLI supports it.

When Claude emits a supported `can_use_tool` control request, the adapter emits
a LASO `permission` request. Compatible question control messages are mapped to
LASO `question` requests. LASO policy and durable operator decisions remain
authoritative; the adapter never self-approves. Unsupported or malformed
control messages are transport failures. Claude Code versions may differ in
which headless permission/question events they expose, so this path is not
claimed as universal.

The CLI turn is bounded. The adapter does not claim cooperative cancellation
unless Claude acknowledges a supported control operation; timeout, broken pipe,
or child exit is a transport failure. Process-group shutdown is bounded and
escalates to forced termination. An ambiguous mid-turn outcome is not
resubmitted and remains subject to LASO's existing `Unknown` recovery semantics.

This is supervised process isolation, not an OS or container sandbox. Remote
workers, distributed leases, automatic worktree management, vendor pricing,
and billing are outside this adapter.
