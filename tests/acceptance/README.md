# Real-agent Phase 1 acceptance

`real-agent-phase1.sh` is a manual, opt-in acceptance harness. It uses the
normal LASO process-worker adapters and HTTP API; it does not invoke `git`,
`gh`, or any GitHub API. The harness creates a temporary synthetic C++ project,
limits both provider adapters to a temporary allowed-root tree, and removes
all runtime state on exit.

Build LASO with the optional adapters enabled, then run the harness on a Linux
host that already has authenticated provider executables:

```sh
cmake -S . -B build-phase1 -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DLASO_BUILD_CODEX_ADAPTER=ON \
  -DLASO_BUILD_OPENCODE_ADAPTER=ON
cmake --build build-phase1
bash tests/acceptance/real-agent-phase1.sh build-phase1 .
```

`CODEX_BIN` and `OPENCODE_BIN` can override executable discovery. The harness
does not accept credentials as arguments and never prints environment values.
Claude is not part of the mandatory pipeline because its availability and
headless interaction contract are deployment-specific; when a Claude setup is
safe and approved, its separately gated adapter test should be run with
`LASO_RUN_REAL_CLAUDE=1` and `CLAUDE_BIN`.

The successful pipeline has parallel Codex, OpenCode, and deterministic
validation branches, a durable join, an approval checkpoint, and a final
build/test performed outside the agent process. The first run is killed at the
approval checkpoint and recovered by a new LASO server process. The harness
also tests real-agent cancellation and an allowed-root failure, and repeats
the successful workload twice. A non-zero exit is a failed acceptance, while
exit 77 means a required host prerequisite is unavailable.

The cancellation line reports `acknowledged` when the locally owned provider
process group has been terminated and LASO has committed the cancellation.
A provider completion that races with cancellation may win before the
cancellation acknowledgement; in that case the completed durable state is
preserved. A result arriving after a terminal cancellation is ignored as a
non-authoritative late completion. Transports that cannot expose a pending
invocation handle cannot claim the same acknowledgement strength.

Local process-worker providers now establish an owned process group, use
bounded graceful-then-forced teardown, and arm parent-death cleanup in the
adapter hosts. This covers normal completion, provider failure, timeout, and
cooperative cancellation without broad process-name termination. A hard
orchestrator kill cannot guarantee cleanup for a provider that daemonizes or
escapes its owned process group; a future reaper/lease design is required for
that boundary.

`distributed-m3.sh` is the opt-in PostgreSQL topology gate for the first M3
worker-claim path. It starts separate owner and worker LASO instances, requires
`LASO_TEST_POSTGRES_DSN`, and exercises real Codex/OpenCode workers with an
inline bounded workspace manifest. It exits 77 when PostgreSQL or a supported
provider executable is unavailable; SQLite runs are not treated as distributed
validation. The focused PostgreSQL suite covers concurrent claims, lease
renewal/expiry, fencing, duplicate completion, cancellation recovery, and
multi-instance recovery. The full worker-death, stale-live-worker, database
interruption, and owner-recovery scenarios remain separate operational gates;
they must not be inferred from SQLite or from a successful basic run.

The loopback-only `artifact-chaos-proxy.py` provides deterministic upload
barriers for artifact failure experiments. It bounds declared upload size and
upload duration, marks request start/publication using caller-selected files,
and can hold the successful response until a release file appears. Its local
barrier behavior can be tested without PostgreSQL, an SSH target, or a worker
provider:

```sh
python3 tests/acceptance/test_artifact_chaos_proxy.py
```

These tests validate the fault injector only; they are not evidence of LASO
artifact recovery or cross-machine behavior. The proxy binds to loopback and is
intended for isolated acceptance runs, not production deployment.
