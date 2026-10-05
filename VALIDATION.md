> Historical validation record: dated sections below describe earlier SQLite-first work and prior PostgreSQL validation. Current PostgreSQL-only validation appears in the final section.

# Validation record

## PostgreSQL-only storage validation (2026-09-26)

All local database tests used a disposable PostgreSQL 16.15 cluster on loopback
with a dedicated `laso_pr16_qa` database. No unrelated database service or schema
was used. Tests exercised independent schemas and ran with deterministic session
recovery hooks enabled.

| Build | Configure/build/test | Result |
|---|---|---|
| Debug | `cmake -S . -B build-debug-final -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DLASO_ENABLE_SESSION_TEST_HOOKS=ON`; `cmake --build build-debug-final --parallel 4`; `ctest --test-dir build-debug-final --output-on-failure --parallel 4` | **PASS: 264 total; 261 passed, 0 failed, 3 skipped** |
| Release | `cmake -S . -B build-release-final -G Ninja -DCMAKE_BUILD_TYPE=Release -DLASO_ENABLE_SESSION_TEST_HOOKS=ON`; `cmake --build build-release-final --parallel 4`; `ctest --test-dir build-release-final --output-on-failure --parallel 4` | **PASS: 264 total; 261 passed, 0 failed, 3 skipped** |
| ASan + UBSan | `cmake -S . -B build-sanitizers-final -G Ninja -DCMAKE_BUILD_TYPE=Debug -DLASO_ENABLE_SESSION_TEST_HOOKS=ON -DLASO_ENABLE_ASAN=ON -DLASO_ENABLE_UBSAN=ON`; `cmake --build build-sanitizers-final --parallel 4`; `ctest --test-dir build-sanitizers-final --output-on-failure --parallel 3` with `ASAN_OPTIONS=detect_leaks=1:halt_on_error=1` and `UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1` | **PASS: 264 total; 261 passed, 0 failed, 3 skipped** |

The three skips in each suite were the two S3-only configuration checks in the
non-S3 build and `distributed_m3_acceptance`, whose opt-in Codex/OpenCode agent
executables were unavailable. The CTest PostgreSQL distributed, coordination,
process-crash, session recovery, ownership, fencing, restart, CLI/API, schema
migration/reopen, and connection failure cases all ran. The separate M3 script
remains in CI and reports its missing optional prerequisites as a skip.

The Release install completed under `/tmp/laso-postgres-only-install`, and the
installed CLI reported `0.1.0-rc.1`. `ldd` on both `laso` and `laso-server`
showed `libpqxx` and `libpq`, with no SQLite library. Clang 18 format validation,
YAML parsing for the workflow and deployment examples, shell syntax checks, and
`git diff --check` passed. Docker image build/startup/health validation could not
run because Docker is not installed in this environment.


## Latest systemd deployment validation (2026-09-23)

This section records the deployment-hardening work from the current source tree;
the older dated matrices below are retained as historical snapshots. Validation
used Linux x86-64, systemd 255, GCC 13.3.0, CMake 3.28.3, and Ninja. Debug,
Release, sanitizer, and PostgreSQL builds all configured, built, and staged
installed binaries, headers, example configuration, documentation, and the
generated systemd unit under isolated install prefixes. `systemd-analyze verify`
accepted the generated unit. The unit embeds the configured install prefix and
does not refer to the source/build tree.

| Check | Result |
|---|---|
| GCC Debug CTest (SQLite default) | **PASS: 202 scheduled; 198 passed, 4 skipped** (three PostgreSQL-only tests and the PostgreSQL backend-not-built guard) |
| GCC Release CTest (SQLite default) | **PASS: 202 scheduled; 198 passed, 4 skipped** (same expected PostgreSQL-disabled cases) |
| ASan + UBSan CTest (SQLite default, leak detection enabled) | **PASS: 202 scheduled; 198 passed, 4 skipped** (same expected PostgreSQL-disabled cases) |
| PostgreSQL-enabled GCC Debug CTest | **PASS serial run: 220 scheduled; 219 passed, 1 skipped** (cross-machine M3 acceptance requires a separate remote setup) |
| PostgreSQL CTest parallel diagnostic run | **Not supported with the shared single-owner test database:** overlapping test processes were rejected by the intentional PostgreSQL database ownership lock (`PostgreSQL database is owned by another LASO process`). Two of the four initial parallel failures reproduced with this explicit cause; the full serial suite passed. Parallel result is not claimed as passing.** |
| Unit installation and syntax | **PASS:** `cmake --install` into isolated prefixes; expected executables/config/docs/unit present; `systemd-analyze verify` passed |
| Native systemd user-service acceptance, SQLite | **PASS:** health, durable approval recovery across SIGTERM and SIGKILL restart, exactly-once completion event, SIGINT, repeated lifecycle, configuration failures, worker-child cleanup |
| Native systemd user-service acceptance, PostgreSQL | **PASS:** same lifecycle and durable recovery checks against a unique temporary schema, removed after service shutdown |
| Dedicated system account and system-unit sandbox | **PASS:** installed system unit ran with the dedicated unprivileged account/group; observed process credentials matched the unit and had no extra supplementary groups. `StateDirectory` was created with restrictive ownership/mode; SQLite state remained writable across restart. The service mount namespace exposed configuration/system paths read-only while allowing its state path. A write probe outside state failed with `Read-only file system`. |
| System-manager startup, health, stop, restart | **PASS:** installed `laso.service` reached active/running; health and version endpoints responded only while running. `systemctl stop` returned success without forced SIGKILL, the endpoint stopped responding, and a subsequent start/restart returned healthy. |
| Durable run across system-service restart | **PASS:** an approval-waiting run retained the same durable identity and state over graceful restart, then completed once after approval. Completion event count was one; forced daemon termination/restart did not duplicate the terminal completion. |
| Abnormal daemon death and restart policy | **PASS:** controlled SIGKILL incremented the systemd restart counter and `Restart=on-failure` restored health after the configured delay. A malformed-config failure burst reached systemd's configured start-rate limit; valid configuration was restored and the service recovered. Deliberate stop did not trigger a restart. |
| System-manager configuration failures | **PASS:** the installed unit failed without a health response for missing, malformed, unreadable, invalid-storage, unavailable-plugin-directory, and unwritable-state configurations. Diagnostics were bounded and did not include configuration contents or failing filesystem paths. The unwritable-state case was denied by the systemd read-only filesystem boundary. |
| Journald and child cleanup | **PASS:** system-manager journal inspection covered startup, graceful stop, abnormal restart, recovery, and configuration failures; no credentials/DSNs were found. Raw journal output was not added to the repository. The reference worker child exited on both graceful stop and forced daemon death; no LASO/reference-worker process remained after cleanup. |
| Child-process/orphan check | **PASS:** acceptance observed worker-host termination on restart/stop and no LASO/reference worker processes remained afterward |

The rootless acceptance procedure is `tests/acceptance/systemd-lifecycle.sh`. Run
`--preflight <installed-unit>` for non-mutating systemd/unit checks; run
`--user <install-prefix> <source-tree>` for the rootless user-service lifecycle.
For PostgreSQL, set `LASO_SYSTEMD_ACCEPTANCE_POSTGRES_DSN` to a disposable test
database DSN and use `--user-postgres`; the harness creates and drops only its
unique test schema. These modes do not install users/groups or alter system
services. The system-manager configuration-failure checks are
available as `tests/acceptance/systemd-system-config-failures.sh`. They require
an explicitly disposable `/etc/laso/laso.yaml` containing the marker
`# LASO_SYSTEMD_ACCEPTANCE_FIXTURE`, the installed `laso.service`, and root
privileges. Run only on a test installation: the harness temporarily replaces
that marked configuration and adds a runtime-only `Restart=no` drop-in, then
restores the configuration/unit behavior and restarts LASO on exit. It does not
create or remove the service account, installed unit, or durable state.

Native system-manager acceptance was completed on Linux x86-64 with systemd
255. The persistent system installation used SQLite; PostgreSQL lifecycle
coverage remains the separate rootless user-service run above. The system
service was not enabled at boot as part of this validation. These results
validate the tested installation/lifecycle paths and are not a general
production-readiness claim.

The build/test matrix recorded below contains historical snapshots from earlier
stages. Current privileged systemd lifecycle results are listed in the table
above; installation and durable test state remain local to the validation
environment and were not copied into this repository.

## Implemented

C++20 source, public headers, CMake targets, native C plugin SDK and examples,
SQLite persistence, runtime, API/CLI, policies, scheduling, event-source ingress,
and artifact interfaces, tests, systemd/Docker deployment files, documentation, and
Linux CI are present.

The current default build registers **182 GoogleTest cases** plus **3 CTest entries**
for CLI validation, process smoke/restart, and the SQLite multi-instance guard, for
**185 CTest entries**. The PostgreSQL-enabled build registers **198 GoogleTest
cases** plus the same **3 CTest entries**, for **201 CTest entries**. Composition,
storage, event-ingress, and worker-adapter coverage includes
revision immutability, cross-boundary payload and schema behavior, child retry
identity, approval-compatible persistence, parallel children, recursion, depth,
run inspection, backend conformance, pagination, rollback, concurrent persistence,
PostgreSQL runtime/reopen behavior, event-source ABI/lifecycle, bounded host ingress,
source schema validation, durable external-event deduplication, source-state restart,
offline plugin-to-trigger-to-pipeline execution, durable worker-job lifecycle,
worker idempotency, concurrent duplicate submission, worker status/result/cancel
correlation, optional normalized usage, execution budgets, transport/job failure
classification, supervised process-worker framing, protocol mismatch, bounded
timeouts, cancellation, shutdown, process exit, and separate-process pipeline
execution, schema and policy enforcement at the worker boundary, late terminal
event rejection, and manager restart reconciliation.

## Statically reviewed

- All source paths named by CMake exist; source/header references, namespaces,
  definitions, dependency discovery, and target boundaries were reviewed.
- GCC/Clang flags, C++20 requirements, runtime output paths, and the Linux-only
  platform check were reviewed.
- ABI version/size checks, C exports, buffer ownership, library lifetime, and
  explicitly configured plugin discovery paths were reviewed.
- Transaction checkpoints, approval/restart handling, cancellation persistence,
  finite edge/loop/retry limits, join state, and subpipeline waits were reviewed.
- HTTP isolation and limits, loopback defaults, policy checks, error/log contents,
  and deployment configuration were reviewed.
- Event-source lifecycle isolation, thread-safe host emission, source identity checks,
  bounded ingress, durable external-event claims, callback shutdown behavior, and
  trigger provenance/depth integration were reviewed.
- Worker ABI prefix compatibility, bounded request/result handling, durable job
  idempotency, status/result/cancel correlation, callback serialization, recovery
  reconciliation, terminal-state protection, worker limits, and secret-safe job
  metadata were reviewed.
- Ubuntu GCC/Clang, Debian 13, PostgreSQL, clang-tidy, formatting, and ASan/UBSan
  CI jobs are defined in `.github/workflows/linux.yml`.

Static review found and corrected mismatched binding acceptance, fixture replacement
lengths, cancelled approval records, cancellation persistence, join resume state,
scheduler stop-before-start handling, completed-attempt recording on edge-budget
failure, a GoogleTest name lookup collision, Boost discovery under CMake 3.31, and
unclear failures for unavailable configured plugin directories.

The release-hardening audit additionally corrected terminal checkpoint immutability,
recovery pagination beyond one record page, concurrent-branch policy and shared step
and edge budgets, branch deadline inheritance, plugin callback exception containment,
and safe relative `$ref` resolution from the declaring schema document. Regression
tests cover these failure paths and schema diagnostics. Composition coverage also
includes nested A → B → C execution and child output-contract failure propagation.

## Tested in the development environment

| Check | Result |
|---|---|
| Native clang-format | **PASS: clang-format-18 check executed on Linux; no violations** |
| CMake source paths | PASS: all referenced source files present |
| LASO include resolution and documentation links | PASS |
| Safe YAML/configuration document scan | PASS: 13 documents |
| Git whitespace check | PASS |
| Private terminology and obvious credential-pattern scan | PASS: no matches |

No non-Linux C++ compilation was attempted because LASO is intentionally Linux-only.

## Tested on Ubuntu 24.04.5 x86-64

| Check | Result |
|---|---|
| CMake 3.28.3 + Ninja configure | **PASS** |
| GCC 13.3.0 Debug build | **PASS** |
| Clang 18.1.3 Debug build | **PASS** |
| GCC Debug CTest suite | **PASS: 201/201; all PostgreSQL and distributed-execution tests ran against isolated PostgreSQL 16** |
| GCC Release CTest suite | **PASS: 201/201; all PostgreSQL and distributed-execution tests ran against isolated PostgreSQL 16** |
| Clang Debug CTest suite | **PASS: 201/201; all PostgreSQL and distributed-execution tests ran against isolated PostgreSQL 16** |
| Clang Release CTest suite | **PASS: 201/201; all PostgreSQL and distributed-execution tests ran against isolated PostgreSQL 16** |
| ASan + UBSan build and CTest, leak detection enabled | **PASS: 185/185 scheduled; 182 passed and 3 expected PostgreSQL-disabled cases skipped** |
| PostgreSQL-enabled GCC Debug CTest suite | **PASS: 201/201; all PostgreSQL storage, coordination, and distributed-execution tests ran against isolated PostgreSQL 16** |
| clang-format `--dry-run --Werror` on Linux | **PASS** |
| clang-tidy 18 against the Clang compilation database | **PASS: exit 0; advisory warnings remain** |
| Debian 13 container | **PASS: public workflow 35380101673** |
| Multi-stage Debian runtime image build | **PASS** |
| Runtime image health endpoint and unprivileged UID | **PASS: host-network health endpoint; image runs as `laso:laso`** |
| systemd unit syntax and dependency verification | **PASS** |
| CLI and process restart smoke tests | **PASS: CLI validation and process smoke executed; process smoke used a local extracted jq 1.7 because jq is not installed system-wide** |
| Offline shipped examples | **PASS: all shipped offline pipeline definitions validated; event-source plugin → durable event trigger → completed run and worker plugin → durable job → validated output exercised; optional local-openai not run** |
| HTTP health/run integration tests | **PASS** |
| valid, invalid, incompatible, and symlinked `.so` plugin tests | **PASS** |
| GPU inference | **Not part of this worker-hardening audit** |

clang-tidy reported advisory findings because `WarningsAsErrors` is intentionally
empty in the v0.1 baseline. They include enum-size suggestions, explicit handling
of ignored networking return values and exception boundaries, integer widening,
and small copy/allocation opportunities. The configured CI command exits zero.

## Other remaining validation items

| Check | Status |
|---|---|
| GitHub Actions execution | **PASS: public workflow 35380101673; GCC, Clang, Debian, ASan/UBSan, formatting, clang-tidy, and PostgreSQL jobs succeeded** |
| Optional TSan execution | **BLOCKED ON HOST: GCC runtime aborted during test discovery with `unexpected memory mapping`** |

Use [the Linux validation procedure](docs/first-linux-validation.md) when validating
another distribution or deployment environment. Ubuntu and Debian results above
are actual executions; pending and blocked rows do not imply success. The TSan
failure occurred before LASO tests ran and must be repeated on a compatible kernel
and sanitizer runtime; global ASLR settings were not weakened to work around it.

## Distributed execution milestone validation

The Milestone 2 implementation was validated from the current source tree on an
isolated Linux x86-64 host. The PostgreSQL-enabled matrix used a disposable
PostgreSQL 16 instance; the default and sanitizer matrices kept PostgreSQL
disabled and verified that the SQLite build remains independent of libpq.

- SQLite/default GCC Debug: **185/185 scheduled; 182 passed and 3 expected
  PostgreSQL-disabled cases skipped**.
- PostgreSQL GCC Debug and Release: **201/201 passed**.
- PostgreSQL Clang Debug and Release: **201/201 passed**.
- ASan + UBSan: **185/185 scheduled; 182 passed and 3 expected
  PostgreSQL-disabled cases skipped** with leak detection enabled.
- The focused distributed tests cover run-owner takeover, durable `NodeWork`
  claiming, fencing, retries with distinct attempt IDs, cancellation, approval
  resume, process-crash recovery, and SQLite multi-instance rejection.
- Storage failure handling is fail-closed: lease renewal and authoritative
  node mutations stop when coordination/storage errors prevent proof of current
  ownership. Connection failure is bounded and redacted by the PostgreSQL pool;
  a live database-partition test remains an operational exercise rather than a
  claim of exactly-once execution.
- `clang-format-18 --dry-run --Werror` completed successfully after formatting
  six distributed-execution source/test files. The repository clang-tidy command
  completed with exit 0; its remaining output is advisory, non-LASO-owned or
  intentionally non-fatal baseline guidance.

## Historical validation snapshots

The following sections preserve earlier milestone evidence and are intentionally
historical. Their older test counts and branch names are not the current release-
readiness result above.

## Worker-hardening branch validation

This branch was validated in an isolated Linux x86-64 environment from upstream
commit `c9887eabd585a414789d6c43514e1ee3a221ca8b` using GCC Debug, an isolated
local PostgreSQL 16 cluster, and serial Ninja builds. SQLite CTest ran **179
tests: 175 passed and 4 skipped** (the PostgreSQL cases and gated real OpenCode
cases); PostgreSQL-enabled CTest completed **179/179** (the two gated real
OpenCode cases skipped); and ASan/UBSan CTest ran **179 tests: 175 passed and 4
skipped** with leak detection enabled. The added worker tests cover durable
approval/permission/question requests, policy decisions, idempotent replay,
cancellation, strict protocol bounds, OpenCode session continuation after
adapter restart, explicit project-root rejection, and normalized results and
usage. The real OpenCode test used the installed adapter and a disposable
synthetic workspace; it did not require a paid provider for the test suite.
OpenCode `1.18.29` emitted a real `permission.asked` event. LASO persisted the
request, resolved it through the worker-request API, and the OpenCode turn,
WorkerNode, and enclosing pipeline completed with the expected synthetic result.
The OpenCode question path remains not directly validated.

The schema-contract tests additionally cover valid and invalid input/output,
registration-time missing or malformed schemas, safe local references, forbidden
remote references, traversal rejection, payload limits, explicit ValidatorNode
use, and concurrent cache access.

The worker-adapter tests additionally cover ABI-compatible plugin discovery and
health, normal `WorkerNode` execution, worker-boundary schemas and policy approval,
durable idempotency under sequential and concurrent submission, optional usage
metadata, budget acceptance/rejection and accumulation, transport-versus-job
failure classification, terminal late-event handling, and reconciliation after
manager restart. Worker job requests persist bounded metadata only; instructions
and payloads are not copied into job records.

The shared storage conformance tests run against SQLite on every default build and
against a real disposable PostgreSQL service when `LASO_TEST_POSTGRES_DSN` is set.
They cover immutable pipeline revisions, pagination, invalid-record rejection,
structured operational records, rollback boundaries, concurrent writes and claims,
durable schedules/triggers, occurrence/delivery deduplication, and restart/reopen
recovery. Scheduler tests cover UTC one-time/interval/cron behavior, misfire and
overlap policies, bounded capacity retry, event matching/depth/deduplication,
API/CLI surfaces, and normal-runtime launch provenance.

## Current optional Codex-adapter validation

The final local Codex matrix for this branch is: default SQLite Debug CTest
**180 total, 178 passed, 2 PostgreSQL cases skipped**; PostgreSQL-enabled Debug
CTest **188/188 passed**; and ASan/UBSan CTest **188 total, 186 passed, 2
PostgreSQL cases skipped**. The PostgreSQL and sanitizer runs both included the
opt-in real Codex fixture test.

This branch adds seven deterministic and one separately gated Codex adapter test
to the existing suite. The default build remains Codex-free with 180 CTest
entries. With `-DLASO_BUILD_CODEX_ADAPTER=ON`, the fixture-only suite contains
187 entries and the full optional registration contains 188 entries including
the gated real-installation test. The seven deterministic tests use a local
app-server-shaped fixture and cover structured startup, session follow-up,
adapter restart/resume, project-root rejection, LASO permission forwarding and
denial, question forwarding, and malformed-protocol failure. They do not require
an account or network access.

The real installation test is separately gated with
`LASO_RUN_REAL_CODEX=1`, uses a temporary fixture project, and is not counted as
passing unless it is explicitly run. It exercises the installed Codex
app-server, session capture, a bounded file edit, follow-up, and adapter
restart/resume. Its result must be recorded from the actual run; a skipped
gated test is not a pass. The adapter targets the structured app-server
interface observed in Codex CLI 0.154.0. The explicit real test passed in
19.08 seconds, including a temporary fixture edit, follow-up, and
adapter restart/resume. It is not part of the default or fixture-only totals.

## Claude worker validation snapshot

This snapshot covers the optional Claude Code adapter. The adapter is disabled
in the default build and the real-provider test is gated; no Claude executable
was available in the validation environment.

| Configuration | Result |
|---|---|
| Claude-enabled Debug | **PASS: 189 scheduled; 186 passed and 3 expected cases skipped (2 PostgreSQL, 1 real Claude)** |
| PostgreSQL + Claude Debug | **PASS: 189 scheduled; 188 passed and 1 real-Claude case skipped; PostgreSQL cases executed** |
| Claude-enabled ASan/UBSan | **PASS: 189 scheduled; 186 passed and 3 expected cases skipped (2 PostgreSQL, 1 real Claude)** |
| Claude deterministic adapter tests | **PASS: 8/8 executed; session resume, project/symlink boundaries, interactions, failures, cancellation truthfulness, and bounded process cleanup** |
| Real Claude Code integration | **SKIPPED: no installed Claude Code executable; no provider credentials were changed** |

The deterministic fixture emits Claude-shaped structured `stream-json` system,
assistant, result, and control messages without a network or account. The
vendor-specific permission/question path remains version-dependent because the
headless Claude CLI does not guarantee that every interaction is exposed to a
custom stream host. No real Claude session or provider transcript was used in
this validation.

## PostgreSQL coordination validation snapshot

The PostgreSQL-enabled build scheduled **194/194 CTest entries** against an
isolated PostgreSQL 16 service. Coordination coverage includes bounded pool
acquisition and replacement, opaque instance IDs, renewal, owner binding,
expiry takeover, fencing rejection, concurrent takeover, and process-exit
recovery. The distributed execution integration also exercises two LASO service
instances sharing PostgreSQL, a durable queued run, a single owner claim, a
multi-instance subpipeline with `max_runs: 1`, and normal runtime completion.
Multi-instance mode is not used by SQLite.

## Real-agent Phase 1.1 runtime-hardening acceptance (2026-09-19)

The manual acceptance harness in `tests/acceptance/` uses a disposable synthetic
C++ repository and the normal LASO HTTP API, worker adapters, durable SQLite
state, and process-worker isolation. Its pipeline runs Codex and OpenCode in
parallel with a deterministic validator, joins the branches deterministically,
waits for a reviewer approval checkpoint, and then builds and tests the
resulting workspace. The harness also includes an allowed-root failure case and
an in-flight cancellation case. It is intentionally opt-in because it invokes
real locally installed agent executables.

Phase 1.1 hardens the provider lifecycle at the framework and adapter
boundaries. Process-worker invocations now own an exact process group, use
bounded graceful-then-forced teardown for timeout/cancellation, and expose a
pending-submission cancellation hook. The Codex and OpenCode adapter hosts
also use parent-death cleanup; OpenCode explicitly stops its server before
acknowledging shutdown. Worker-node deadlines are propagated to process
transport requests and timeout results remain distinguishable from ordinary
provider failures.

The completed acceptance run exercised real Codex and OpenCode workers, two
successful repeated pipeline runs, parallel filesystem edits, durable worker
attempts, deterministic joining, reviewer approval, controlled orchestrator
termination and restart, recovery from the persisted checkpoint, and the
synthetic project's build and CTest suite. The final harness result was
`REAL_AGENT_PHASE1_OK` with `successful_repeated_runs=2`,
`cancellation=acknowledged`, and `build_and_tests=pass`.

The prior real OpenCode gate left three provider-server child processes. After
the lifecycle fix, the normal, failure, timeout, cancellation, and shutdown
regressions found zero LASO-owned provider descendants/listeners after
completion. Cleanup after a hard orchestrator kill is best-effort when a
provider daemonizes or escapes its owned process group; this is documented as
a future reaper/lease boundary rather than an unconditional guarantee.

Cancellation is durable and terminal-state protected. If cancellation wins,
LASO records the request and acknowledgement and commits `Cancelled` (or
`TimedOut` for an acknowledged deadline). If provider completion wins first,
the completed result remains authoritative even if a cancellation request was
also recorded. A late completion cannot rewrite a terminal cancelled or
completed job. The regression suite also covers cancellation during pending
submission and provider completion racing acknowledgement.

The real-provider gates passed for Codex (1/1) and OpenCode (2/2). Claude was
not invoked because no Claude executable was safely available. The failure
boundary used an out-of-allowed-root workspace and retained the detailed
failure on the durable worker job without exposing it through the redacted run
summary.

| Validation | Result |
|---|---|
| GCC Debug with Codex/OpenCode adapters | **PASS: 200 scheduled; 193 passed and 7 expected cases skipped** |
| GCC Release with Codex/OpenCode adapters | **PASS: 200 scheduled; 193 passed and 7 expected cases skipped** |
| ASan/UBSan Debug | **PASS: 189 scheduled; 186 passed and 3 PostgreSQL-related cases skipped** |
| Real Codex adapter gate | **PASS: 1/1** |
| Real OpenCode adapter gate | **PASS: 2/2** |
| Real Claude adapter | **SKIPPED: executable unavailable; no credentials changed** |
| PostgreSQL-backed validation | **BLOCKED: PostgreSQL server/tools unavailable on the execution host** |
| clang-format / clang-tidy | **BLOCKED: tools unavailable on the execution host** |

The harness documents the remaining Milestone 3 distributed-execution needs:
capability advertisement, remote task claims with lease expiry and fencing,
provider-aware remote cancellation handles, workspace/artifact transport,
retry and duplicate-completion idempotency, ownership-loss handling, and
explicit semantics that do not claim exactly-once execution across process or
network failure. The Phase 1.1 lifecycle hooks provide a local foundation but
do not by themselves make remote provider execution safe.

## Distributed Execution Milestone 3 implementation status (2026-09-19)

This milestone adds the first opt-in remote-agent foundation without broadening
distributed execution to arbitrary tools or side effects. Supported worker
branches now use the existing durable `NodeWork` record with persisted worker
ID/capability requirements. Multi-instance services advertise a bounded,
identity-free capability document and refresh it with their coordination
heartbeat. Claim selection checks local worker health and capability before
acquiring the node lease. Existing database-time lease and fencing predicates
remain authoritative; workers may physically continue after lease loss, but a
stale completion cannot commit.

Terminal `NodeWork` records now accept equivalent replay safely while rejecting
conflicting terminal rewrites. Cancelled parent runs permit a replacement
instance to claim still-running branch work for fenced cancellation cleanup,
including when the original worker disappeared. These semantics remain
at-least-once and do not claim exactly-once execution.

The first bounded workspace transport is an inline manifest of relative paths,
byte content, sizes, and SHA-256 hashes. It rejects traversal, absolute paths,
duplicate paths, symlinks, oversized files/workspaces, and integrity failures.
The remote worker stages it below its local LASO data directory per
run/work/attempt. Provider `project_dir` values are ephemeral and are removed
from distributed durable result metadata; validated output manifests are carried
through the deterministic join. Larger artifact/object-store transport and
remote provider-control protocols remain follow-on work.

## Historical M3 PostgreSQL validation pass before timeout closure

The current pass used a disposable PostgreSQL 16.15 instance bound to Linux
loopback only. LASO was compiled with PostgreSQL, Codex, and OpenCode support;
the coordination database tests ran against PostgreSQL rather than SQLite.
Credentials were supplied only through the test environment and are not part of
the repository.

| Validation | Result |
|---|---|
| SQLite/default regression matrix | **PASS: 219 total; 198 passed, 0 failed, 21 expected skips** |
| GCC Release regression matrix without PostgreSQL | **PASS: 220 total; 198 passed, 0 failed, 22 expected skips** |
| ASan/UBSan regression matrix | **PASS: 200 total; 196 passed, 0 failed, 4 expected skips** |
| PostgreSQL storage/coordination/distributed focus | **PASS: 42/42** |
| PostgreSQL-enabled full CTest excluding the real acceptance harness | **PASS: 215 passed, 4 expected provider-gate skips** |
| PostgreSQL-enabled full CTest including the real acceptance harness | **PARTIAL: 215 passed, 4 expected skips, 1 real-acceptance failure** |
| Real Codex adapter gate | **PASS: 1/1** |
| Real OpenCode adapter gate | **PASS: 3/3** |
| Real distributed basic runs | **PARTIAL: 5 attempted, 2 completed, 3 Codex-timeout runs failed/aborted and cleaned up** |

The 42 PostgreSQL-focused tests cover migrations/backend initialization,
concurrent claims, lease renewal and expiry, stale completion fencing,
equivalent and conflicting duplicate completion, cancellation recovery,
owner/process recovery, and SQLite multi-instance rejection. The two completed
real runs used separate owner and worker LASO instances sharing PostgreSQL on
the validation host; both real Codex and OpenCode work executed on the worker
instance and returned through the durable join.

The remaining real-acceptance failure is an operational boundary, not a green
result hidden by the harness: the Codex provider exceeded its configured
distributed request window, leaving the run paused with an unfinished NodeWork
while the OpenCode branch had completed. The harness terminated its owned
instances and left zero provider processes/listeners. A work-computer-owner to
Linux-worker run, live stale-worker/database-interruption chaos, and real remote
cancellation remain unvalidated in this pass. The implementation retains
at-least-once semantics and one authoritative fenced completion; it does not
claim exactly-once execution. SQLite remains explicitly single-instance.

## M3 closure validation pass (2026-09-21)

The timeout/reconciliation defect was fixed and then revalidated against the
real PostgreSQL-backed runtime. The provider transport had been converting a
quiet but valid provider into a false timeout because its polling interval was
limited to 60 seconds even when the overall request deadline was longer. The
transport now waits against the actual remaining deadline, bounded only by the
platform integer limit. A quiet-provider regression now runs past that interval
and completes successfully.

The runtime also reconciles terminal worker attempts from durable state. A
terminal success, provider failure, timeout, cancellation, or fenced ownership
loss is mapped to its current `NodeWork`; retry policy is evaluated and the
logical work is committed as completed, retryable, failed, cancelled, or
superseded. Recovery does not depend on an in-memory callback or a surviving
owner process. Temporary PostgreSQL storage failures are deferred while a
currently valid lease remains authoritative; they do not become false terminal
worker failures. Pending cancellation and synchronous submission races now
wait for the durable terminal cancellation state before a transport failure can
be persisted.

The final disposable PostgreSQL environment was PostgreSQL 16.15, bound to
loopback only and reached from the work-computer owner through a loopback SSH
forward. The build used libpq 16.15 and libpqxx 7.8.1. The database used a
dedicated synthetic test role/database and was removed after validation; no
production or LAN/public database endpoint was used.

The true cross-machine gate ran the owner on the authoritative work computer
and the worker/provider on a separate Linux test workstation. Codex execution
was observed on the worker side, with no owner-local provider fallback. Three
complete Codex runs passed consecutively. The owner verified remote artifact
identity, attempt/fence provenance, relative paths, sizes, SHA-256 hashes, and
content before rebuilding and testing the reconstructed synthetic project.

The required cross-machine chaos gates also passed: remote cancellation,
worker death and lease recovery, a live stale worker after lease expiry,
owner death and recovery, and bounded worker-side database interruption. The
stale-worker gate produced `STALE_RESULT_REJECTED`; no stale worker committed
authoritative state. Remote cancellation reached authoritative `Cancelled`.
The provider termination acknowledgement was false with a recorded pending
cancellation error, so the acceptance did not claim termination confirmation
that was not observed. Late completion remained non-authoritative.

| Validation | Result |
|---|---|
| PostgreSQL GCC Debug full CTest, excluding opt-in acceptance harness | **PASS: 223/223; 4 expected skips** |
| PostgreSQL GCC Release full CTest, excluding opt-in acceptance harness | **PASS: 223/223; 4 expected skips** |
| ASan + UBSan SQLite-only CTest | **PASS: 206/206; 8 expected skips** |
| PostgreSQL focused distributed/storage/coordination coverage | **PASS: 42/42** |
| Quiet-provider overall-deadline regression | **PASS: 1/1** |
| Pending-submission cancellation race repetition | **PASS: 20/20** |
| PostgreSQL schema upgrade v7 to current v8 | **PASS in Debug and Release** |
| Complete cross-machine Codex runs | **PASS: 3/3** |
| Cross-machine cancellation | **PASS: 2/2; termination acknowledgement accurately unconfirmed** |
| Cross-machine worker death, stale worker, owner death, DB interruption | **PASS: 1/1 each** |
| Cross-machine owner-side artifact rebuild and tests | **PASS for all 3 complete runs** |
| Cross-machine OpenCode | **SKIPPED: executable unavailable on the Linux test host** |
| Real Claude | **SKIPPED: executable unavailable; no credentials changed** |
| Clang, clang-format, clang-tidy, TSAN | **SKIPPED: unavailable or environment-blocked** |

The final cleanup inspection found zero LASO-owned provider processes,
provider listeners, and acceptance staging workspaces. The acceptance harness
cleanup is scoped to exact LASO-owned process trees and uses bounded escalation;
it does not kill by provider name. PostgreSQL and the SSH forwarding resources
were removed after the run.

This pass preserves the distributed execution contract: attempts may execute
at least once and may be duplicated after failure or lease expiry, while only a
valid current fence can commit one authoritative result. LASO does not claim
exactly-once execution. SQLite remains explicitly single-instance, and remote
execution remains limited to the supported agent/worker path.

## M3.6 artifact transport status — closed and validated

The earlier partial-validation note below is retained as historical evidence.
M3.6 was subsequently closed using a clean Ubuntu Server virtual machine as a
separate operating-system, process, and network boundary. The validation used
PostgreSQL coordination and shared artifact transport, and confirmed normal
remote claims and heartbeats before exercising recovery cases. No private host
identifiers or infrastructure addresses are part of this record.

| Final M3.6 acceptance | Result |
|---|---|
| Normal distributed artifact baseline | **PASS: 3/3** |
| Worker loss after input materialization | **PASS**; lease expiry, replacement claim, and recovery completed |
| Worker loss during output upload | **PASS**; incomplete upload was not accepted as authoritative |
| Worker loss after artifact publication | **PASS**; publication alone did not confer workflow authority |
| Stale completion and fencing | **PASS**; stale fence rejected |
| PostgreSQL interruption during artifact execution | **PASS**; recovery completed without manual database repair |
| Owner restart and recovery | **PASS**; durable state and artifacts were recovered |
| Artifact integrity | **PASS**; hashes, size, and provenance verified |
| Short-TTL lease regression | **PASS**: 3-second lease with 500 ms heartbeat |

These results close the artifact-specific cross-machine chaos acceptance. The
previously unreachable physical worker remains an infrastructure follow-up; no
evidence links that outage to LASO, and it is not required for M3.6 acceptance.
The separate Ubuntu VM supplied the supported cross-OS/process/network
boundary. LASO provides at-least-once attempts with one authoritative fenced
completion; it does not claim exactly-once execution.

### Historical partial-validation record (superseded)

At the earlier artifact-publication checkpoint, the table and checklist below
described the then-incomplete state before the final VM-based chaos pass. They
are retained as historical evidence, not as the current milestone status.

The checklist records what remained pending at that checkpoint. The later
acceptance results in the M3.6 closure section above supersede that status.

| Artifact validation | Result |
|---|---|
| 64 MiB object-backed cross-machine transfer | **PASS** |
| Approximate materialization peak RSS for the 64 MiB object | **~13 MiB** |
| Deterministic cross-machine object-backed runs | **PASS: 3/3** |
| OpenCode 1.18.29 object-backed runs | **PASS: 3/3** |
| Clean-clone SQLite artifact workflow, including generated 64 MiB put/download/hash verification, listing, integrity, and GC dry-run | **PASS** |
| Clean-clone PostgreSQL distributed artifact example | **PASS: `state=Completed`, `remote_artifact=owner_verified`** |
| PostgreSQL Debug matrix | **PASS: 233 total; 228 passed, 0 failed, 5 expected skips** |
| Focused PostgreSQL matrix | **PASS: 44/44** |
| Artifact-focused tests | **PASS: 6/6** |
| SQLite Debug matrix | **PASS: 202 total; 198 passed, 0 failed, 4 expected skips** |
| Release matrix | **PASS: 202 total; 198 passed, 0 failed, 4 expected skips** |
| ASan/UBSan equivalent matrix | **PASS; expected PostgreSQL skips; no actionable sanitizer failures reported** |

The following artifact-specific acceptance work was open at that checkpoint:

- [ ] Worker death after artifact materialization
- [ ] Worker death during output upload
- [ ] Worker death after publication/pre-completion
- [ ] Network-separated stale artifact completion
- [ ] Artifact-stage PostgreSQL interruption
- [ ] Full owner-restart artifact recovery
- [ ] Diagnose remote worker health/heartbeat stall

In that closure attempt, the remote worker stopped advancing its health/
heartbeat and did not claim queued work, so the requested artifact chaos barriers
were not reached. The root cause was not established. Later acceptance on the
clean Ubuntu VM demonstrated normal heartbeat/claim behavior and passed the
artifact-specific worker-loss, stale-completion, database-interruption, and
owner-restart scenarios listed in the M3.6 closure table above. The old physical
worker outage remains an infrastructure follow-up; there is no evidence linking
it to LASO.

The artifact gateway uses a deployment-local bearer token. SQLite remains
single-instance. The optional S3-compatible artifact backend is now implemented
under M4.1, whose distributed acceptance and failure validation are still in
progress. Arbitrary remote side-effecting tools remain unsupported. LASO
provides at-least-once attempt semantics with one authoritative fenced
completion, not exactly-once execution.

## M4.1 S3 implementation status — validation incomplete

The optional S3-compatible backend is implemented behind `LASO_ENABLE_S3`; the
filesystem backend remains the default and does not require the AWS SDK. The
S3-enabled PostgreSQL Debug tree built and its full regression matrix completed
with 228 passed, 0 failed, and 1 expected skip. The focused S3/configuration
tests passed 7/7, including a generated 64 MiB streaming upload/download and
materialization, concurrent duplicate publication, unavailable endpoint,
invalid credentials, and content-corruption detection. The default cloud-free
SQLite Debug matrix completed with 201 passed, 0 failed, and 4 expected skips.

This does not validate M4.1. Distributed owner/worker execution through direct
shared S3 access, interrupted S3 transfers, S3 outage during authoritative
artifact retrieval, stale-worker publication against the S3 backend, and owner
restart using S3 artifacts remain pending. S3 garbage collection is intentionally
unsupported. See the [M4 roadmap](docs/roadmap.md) and
[artifact-store guide](docs/artifacts.md) for the implemented boundary.

### Independent PR #11 S3 validation (2026-09-24)

Validation was run from PR head `928eca18635b818c207fef7195e057b98f682673`
with PostgreSQL 16 and the S3-enabled build. MinIO was built from the
`RELEASE.2025-09-07T16-13-09Z` source on a separate physical Linux ARM64 host.
The test runner reached MinIO through an SSH loopback tunnel; the test-only
HTTP exception was enabled only for loopback endpoints. Synthetic disposable
credentials were used. No AWS service or production credentials were involved.

| Check | Result |
|---|---|
| PostgreSQL-enabled baseline CTest | **PASS: 225 scheduled, 223 passed, 0 failed, 2 expected skips** (S3-only configuration guard and the external distributed-acceptance gate) |
| S3-focused integration against MinIO on the separate host | **PASS: 5/5** |
| Full PostgreSQL + S3 CTest against the same isolated PostgreSQL and MinIO services | **PASS: 230 scheduled, 229 passed, 0 failed, 1 expected skip** (`distributed_m3_acceptance` requires its separately built remote acceptance executable) |
| ThreadSanitizer attempt (separate PostgreSQL-enabled build) | **BLOCKED before tests**; GoogleTest discovery could not start the instrumented binary: `FATAL: ThreadSanitizer: unexpected memory mapping` (exit 66). No TSan test result is claimed. |
| 64 MiB object upload, verification, materialization, and integrity scan | **PASS** over the SSH tunnel; focused test completed in 78.88 seconds |
| Concurrent duplicate writers and missing-object rejection | **PASS** |
| Unavailable endpoint during artifact put preflight | **PASS**; the existence check failed within its configured bound and no artifact metadata was published |
| Invalid credentials and error redaction | **PASS** against MinIO authentication |
| Corruption at a content-addressed object key | **PASS**; retrieval and integrity scanning rejected the changed bytes |

The cross-machine workflow ran the x86 owner/controller on the test runner and
the ARM64 worker and MinIO on a separate physical Linux host. Both LASO
instances coordinated through the same PostgreSQL schema over a scoped SSH
forward; each process reached the same MinIO namespace. A parallel worker
branch received a 31-byte input workspace manifest, staged it on the worker,
and the deterministic Codex protocol fixture wrote a 29-byte
`remote-artifact.txt`. The run reached **Completed**. PostgreSQL contained the
input and output content-addressed manifests; the owner-side integrity command
downloaded and verified both S3 objects (2/2). After a clean owner restart, the
same run remained Completed and the owner verified both objects again (2/2).
The two hosts did not share a local workspace path. This used a deterministic
fixture, not the real Codex CLI/provider.

At that earlier checkpoint, worker loss during upload, in-flight transfer interruption, owner death while a worker was active, and stale-worker publication against S3 were **NOT RUN**. The recovered physical checkpoint below adds a partial pre-publication attempt and TLS certificate results; the other failure gates remain open.
With the workflow already Completed, stopping MinIO made the owner's artifact
verification exit nonzero and report both objects invalid (2/2); the durable
workflow state remained Completed. After
MinIO was restored, verification recovered to 2/2 with no errors. This is a
retrieval outage/recovery check, not an in-flight worker interruption test. The
two-service PostgreSQL and stale-fence integration cases did pass in the full
suite, but they do not substitute for S3-specific worker chaos cases. AWS S3 compatibility was **NOT RUN** at that checkpoint; the recovered physical checkpoint below records the private-CA TLS checks. The earlier endpoint tests used MinIO.

| Distributed / integrity scenario | Result |
|---|---|
| S3 client on x86 host to MinIO on separate ARM64 physical host | **PASS** |
| Owner/controller on one machine and worker on another, sharing PostgreSQL and S3 | **PASS**; parallel worker branch completed and published a remote workspace result manifest |
| Owner restart after completed cross-machine run; durable state and S3 objects readable | **PASS**; state remained Completed and integrity scan verified 2/2 objects after restart |
| Content-addressed upload/download and SHA-256 verification, including 64 MiB streamed object | **PASS** |
| Duplicate concurrent publication and retrieval of existing content | **PASS** |
| Corrupt bytes at expected content key rejected by retrieval/integrity scan | **PASS** |
| Unavailable S3 endpoint during put preflight fails bounded and publishes no artifact metadata | **PASS** |
| Worker killed before artifact publication | **PASS**; the corrected physical rerun verified the selected process tree exited, the S3 object was absent at the fault point, and a replacement attempt completed with authoritative output |
| Worker killed during S3 publication | **PASS**; process tree killed during an 8 MiB transfer and the run recovered |
| Worker killed after S3 publication but before completion | **PASS**; published object survived worker loss and a replacement completed the run |
| Owner killed/restarted while a remote worker is active | **PASS**; only the owner process was terminated and the run recovered to Completed |
| S3 outage during an active transfer | **PASS**; only the disposable S3-compatible service was interrupted and restored; the transfer retried |
| PostgreSQL outage/recovery during an S3-backed worker attempt | **PASS**; only the disposable PostgreSQL service was stopped briefly; the API recovered and the run completed |
| Stale worker fencing against an S3 artifact completion | **PASS**; stale job and attempt became Failed; the accepted manifest names the completed replacement attempt |
| S3 outage during artifact retrieval and service recovery | **PASS**; post-completion verification failed closed while run state stayed Completed, then recovered after service restoration |
| Trusted TLS certificate | **PASS**; private CA accepted with verification enabled |
| Untrusted TLS certificate rejection | **PASS**; untrusted CA rejected with verification enabled and no bypass |
| Direct AWS S3 compatibility | **NOT RUN**; physical acceptance used a disposable S3-compatible service |

### Recovered physical M4.1 checkpoint history (2026-09-24)

The table below preserves the state recovered from the interrupted session before the follow-up physical runs. Current acceptance results are recorded in the next section.

The interrupted acceptance session completed one normal physical cross-machine S3
run using the generic roles `owner-host` and `worker-host`. The disposable
PostgreSQL database and S3-compatible service were on the owner side; the worker
connected over the real network. Test-only credentials and a private CA were
used. This checkpoint records the results recovered from the session log and
disposable database.

| Gate | Run / attempt | Initial state and fault | Evidence and final state | Result |
|---|---|---|---|---|
| Physical baseline | Run `61a7bbb7-c541-4775-81c4-4cdd861f1a6d`; worker attempt `9eecabde-842c-4009-b7e6-e9ed983b5ed7` | Fresh disposable schema and S3 prefix; no injected fault | Run reached Completed. Owner retrieved and verified the 29-byte `remote-artifact.txt`; SHA-256 `c2b480081bd1f9af06c970024ad88096dd5b0cfefe1a52e894135882865d9600`. PostgreSQL recorded the completed run and S3 artifact metadata. | **PASS** |
| Worker death before publication | Runs `8df44fb9-0c95-4268-990a-fc84bd1f6f9a` and `aa90c1ab-1aca-4ce6-89b4-4dc795d00029`; attempts `a66f7f49-fd1b-4203-a973-5cfde2dc3b13`, `d4e228f8-2004-4c00-b3f1-7d9bb7f9a0f8`, `3e5e3539-b396-4d5e-a664-04d488892be5` | Fresh disposable schemas and prefixes; worker was targeted for termination at the pre-publication barrier | First run paused at the barrier and the expected S3 object returned 404, but the original harness did not verify the target process had exited and an empty release marker blocked recovery. In the follow-up run, retry lease renewals failed, the worker result was rejected, and the owner run ended Failed with `Pipeline execution failed`. No authoritative artifact completion was established. The follow-up ended in the disposable PostgreSQL database; no S3 artifact SHA-256 applies. | **PARTIAL**; recovery failed and verified process termination is still required |
| Worker death during S3 publication | — | Not run | No physical transfer was interrupted. | **NOT RUN** |
| Worker death after S3 publication, before completion | — | Not run | No post-publication barrier was exercised. | **NOT RUN** |
| S3 outage during active transfer | — | Not run | Only completed-run retrieval outage/recovery had passed at the earlier checkpoint; it does not cover an active transfer. | **NOT RUN** |
| Owner death during worker execution | — | Not run | No owner process was terminated during active S3-backed work. | **NOT RUN** |
| PostgreSQL interruption during S3-backed work | — | Not run | No database interruption was injected into the disposable PostgreSQL service. | **NOT RUN** |
| Stale worker publication | — | Not run | No stale attempt was challenged against the authoritative S3 result. | **NOT RUN** |
| Trusted TLS certificate | Focused S3 suite; no workflow run ID | Disposable HTTPS S3 service with generated private CA installed as trusted | TLS-enabled focused suite passed 19/19; a trusted private CA was accepted with verification enabled. Per-case artifact digest was not retained in the recovered summary. | **PASS** |
| Untrusted TLS certificate rejection | Focused S3 suite; no workflow run ID | Disposable HTTPS S3 service using a CA absent from the trust bundle | TLS-enabled focused suite passed 19/19; the untrusted certificate was rejected with verification enabled and no bypass configured. | **PASS** |

The acceptance harness verifies the selected remote PID tree exits after
`KILL`, allows an empty fault-release marker, and checks that a stale worker job
and its scheduler attempt both become terminal before accepting a replacement
result. Its PostgreSQL and S3 outage paths validate an explicitly configured
test-only container and restore it during cleanup. These checks were exercised
in the physical cases below. The harness uses only disposable resources.

### Current M4.1 physical acceptance results (2026-09-25)

The owner and worker ran on separate physical systems over the real network.
The owner hosted the disposable PostgreSQL database and S3-compatible service;
all credentials and endpoints were test-only. The deterministic worker fixture
produced fixed artifacts. Each completed run was checked in PostgreSQL, and the
owner downloaded and verified the returned content-addressed object by size and
SHA-256.

| Case | Run and authoritative attempt | Fault and lease/fence evidence | PostgreSQL, S3, and recovery | Result |
|---|---|---|---|---|
| Physical baseline | Run `61a7bbb7-c541-4775-81c4-4cdd861f1a6d`; attempt `9eecabde-842c-4009-b7e6-e9ed983b5ed7` | No injected fault; distinct owner and worker machines | Run Completed; owner verified the 29-byte artifact, SHA-256 `c2b480081bd1f9af06c970024ad88096dd5b0cfefe1a52e894135882865d9600` | **PASS** |
| Worker killed before publication | Run `6aa7a8db-3966-481e-904a-2f8850a4454d`; interrupted attempt `d02b959a-e4c3-4371-8a42-12d93ea3889d`; accepted attempt `176e681b-68e7-45a2-b4b0-0cb5a2d22771` | S3 object absent at the pre-publication barrier; the worker process tree was confirmed dead. Recovery acquired a higher fence; accepted provenance fence `2` | PostgreSQL NodeWork and run completed on retry. Owner retrieved and verified the 29-byte artifact, SHA-256 `c2b480081bd1f9af06c970024ad88096dd5b0cfefe1a52e894135882865d9600` | **PASS** |
| Worker killed during publication | Run `5dd2aee5-4cd6-4c7f-96c4-525bc95ff2f7`; accepted attempt `159ae2b6-91ac-4a62-8c48-9395316f4090` | Worker process tree killed at 65,524 of 8,388,608 bytes uploaded; accepted provenance fence `11` | Retry recovered and owner verified the 8 MiB object, SHA-256 `695861265ca767585d7ea8e6e5f1f0f7718087d0c3af5fbc75382d6f3df8a6e6`; final run Completed | **PASS** |
| Worker killed after publication | Run `625ba770-8cc8-49bf-bdcc-6a2f04e7d6d7`; accepted attempt `e7031807-9f94-4f1e-b803-293473d9e05f` | S3 publication marker observed before worker death and before run completion; accepted provenance fence `7` | Replacement recovered the run; owner verified the 8 MiB object, SHA-256 `695861265ca767585d7ea8e6e5f1f0f7718087d0c3af5fbc75382d6f3df8a6e6`; final run Completed | **PASS** |
| S3 outage during active transfer | Run `257107a8-6add-4e9f-9f63-f211a2edc510`; accepted attempt `d62aece9-6ece-40ab-8eb4-892520d9ff53` | Disposable S3-compatible service stopped after 131,048 of 8,388,608 bytes; service restored; final provenance fence `20` | Transfer retried; PostgreSQL recorded completion; owner verified the 8 MiB object, SHA-256 `695861265ca767585d7ea8e6e5f1f0f7718087d0c3af5fbc75382d6f3df8a6e6` | **PASS** |
| Owner death during worker execution | Run `b90dbc32-3acb-456b-933f-0cba69791631`; accepted attempt `4e387c8f-63ba-4b55-af54-429acb7af8cb` | Owner process killed while the worker job was active; only that process was terminated; accepted provenance fence `1` | Owner restarted; PostgreSQL run and NodeWork recovered to Completed; owner verified the 29-byte artifact, SHA-256 `c2b480081bd1f9af06c970024ad88096dd5b0cfefe1a52e894135882865d9600` | **PASS** |
| PostgreSQL interruption | Run `3a3f08c5-f101-4267-a33c-53e9635d404e`; accepted attempt `1834f653-35c2-4e26-a122-c15ee183597f` | Disposable PostgreSQL service stopped for 3 seconds during active work; lease TTL was 30 seconds; bounded startup timeout prevented a TCP-accepted but silent endpoint from blocking indefinitely; accepted provenance fence `2` | One attempt failed during the outage and a retry completed after database recovery; owner API passed 20 health cycles; owner verified the 29-byte artifact, SHA-256 `c2b480081bd1f9af06c970024ad88096dd5b0cfefe1a52e894135882865d9600` | **PASS** |
| Stale worker publication/fencing | Run `5d0aec03-7fb8-4f1d-9638-db08077ab769`; stale attempt `2e93d477-4277-4b98-82b9-d867eb04ac63`; accepted attempt `c0ede17b-3c40-45bd-9e27-d1587075ce60` | Old worker was suspended past the staleness interval. Its job and attempt were recorded Failed after losing authority. Replacement attempt completed with fence `2`; accepted manifest names that completed attempt | PostgreSQL run and NodeWork completed. Owner retrieved `winning-attempt` (16 bytes), SHA-256 `50723b3848c94826d73c5b60fed653601b35451d2ae3d4e4e1ade650c4b8040a`; stale output did not become authoritative | **PASS** |
| Trusted TLS | Focused TLS-enabled S3 suite | Generated private CA trusted by the client; verification remained enabled | Focused suite passed 19/19; certificate accepted | **PASS** |
| Untrusted TLS | Focused TLS-enabled S3 suite | Certificate authority absent from the trust bundle; no verification bypass | Focused suite passed 19/19; certificate rejected | **PASS** |

The first stale-worker run correctly fenced the result but left its historical
attempt record marked `Running` after the worker job was `Failed`. Recovery now
marks that prior attempt `Failed` in the same PostgreSQL transaction that
claims the replacement NodeWork under its newer lease. A crash-takeover
regression and the physical stale-worker rerun both verify the terminal history.

A stalled PostgreSQL startup was also reproduced with a server that accepted TCP
but never answered the PostgreSQL protocol. The prior synchronous libpq startup
could block beyond the pool acquisition timeout because `connect_timeout` does
not bound the post-TCP startup handshake. The pool now uses `pqxx::connecting`
and a deadline-driven socket poll; the new probe fails within its 100 ms bound.

The lease release path now expires the row instead of deleting it, preserving
monotonic fencing tokens. Its regression verifies that reacquisition increases
the token and that a prior token cannot write. The focused owner tests and the
physical recovery runs passed with this behavior.

The full local CTest suite, excluding the opt-in physical acceptance test,
passed 237 tests; 6 opt-in S3/Codex tests were skipped. The owner and worker
builds succeeded. Hosted CI and PR review remain before merge.

The deterministic Codex protocol fixture now has a mode that writes a fixed
artifact into the workspace supplied by the worker request. Its focused local
test passed (1/1). This provides a reproducible artifact-producing worker for a
future distributed acceptance run; it does not change or validate production
worker routing.


## M5.1 durable session journal and replay (2026-09-25)

The M5.1 acceptance boundary was reviewed against the implementation and tested
with SQLite and the disposable PostgreSQL backend. M5.1 persists accepted input
and observable events; it does not execute accepted turns or persist provider
continuation state.

| Acceptance case | Evidence | Result |
|---|---|---|
| Identical idempotent retry | API retry returns the original turn record and leaves one `input.accepted` event in the ordered journal. | **PASS** |
| Conflicting idempotency-key reuse | API returns HTTP 409; the original turn and event remain unchanged. | **PASS** |
| Concurrent submissions | Twelve actual concurrent storage writers produce one contiguous, duplicate-free per-session event sequence on SQLite and PostgreSQL. | **PASS** |
| Close versus input acceptance | Concurrent close and submit serialize transactionally: either the input commits before the final close event, or it is rejected after close; no partial turn/event is stored. | **PASS** |
| Service restart | Recreated SQLite-backed service retains open and closed session state, accepted turns, event sequence, replay, and rejection of new closed-session input. | **PASS** |
| Storage reopen | Session acceptance, ordered journal, close event, and replay survive storage connection reopen on both enabled backends. | **PASS** |
| SSE cursor resume | Disconnect/reconnect with nonzero `Last-Event-ID` returns only later events; the final `session.closed` event is delivered and the stream terminates. | **PASS** |
| PostgreSQL cross-instance replay/live observation | Independent LASO service instances share a disposable schema; an observer replays a committed event and receives a later writer event over an open SSE stream using a nonzero `Last-Event-ID`. | **PASS** |
| SQLite deployment boundary | Existing configuration validation rejects SQLite with `execution_mode: multi_instance`; session documentation makes SQLite's single-instance scope explicit. | **PASS** |
| Query bounds and request sizes | API pagination, cursor parsing, request-size checks, event-page limits, and bounded SSE duration/stream count were reviewed; existing API limit regression passes. | **PASS** |

The Linux GCC Debug build with PostgreSQL and S3 enabled completed. The serial
normal CTest suite listed 251 tests: 245 passed, 6 opt-in S3/Codex fixtures were
skipped, and none failed. The separate physical `distributed_m3_acceptance` test
was not repeated for M5.1 because M5.1's cross-instance requirement is covered
by independent services sharing PostgreSQL. `clang-format-18 --dry-run
--Werror` and `git diff --check` passed. Hosted CI on PR #13 head `0fef776` passed all 7 checks: GCC, Clang, Release, PostgreSQL, S3, Debian, and ASan/UBSan.

## M5.2 physical worker recovery and stale fencing retest (2026-09-26)

This is targeted recovery evidence, not M5.2 closure. Both valid cases used
separate physical test hosts, a disposable PostgreSQL backend, and the
side-effect-free continuation fixture. Hostnames, account names, process IDs,
run identifiers, continuation sentinels, and private log paths are omitted.

| Case | Controlled fault and observation | Result |
|---|---|---|
| Worker death after durable binding and provider start | The worker was held after entering the deterministic provider, then only its verified test process received `SIGKILL`. Its bound run was reclaimed under a newer fence. The session remained open, cancellation stayed false, one logical run and one completion event remained authoritative, and only the recovery result's continuation was stored. The next queued turn also succeeded. | **PASS: 1/1 fresh physical run** |
| Stale worker while replacement is active | The original worker was paused after acquiring the run lease. A replacement acquired a newer fence and was held in its provider call. The original then resumed and reached the real fenced checkpoint rejection while the replacement was still active. At that barrier the durable turn remained running with no completion event or continuation. After release, the replacement completed; its output and continuation were authoritative. | **PASS: 1/1 fresh physical run** |

The earlier worker-death symptom was not reproduced by either the fresh hard-kill
run or the two retained controlled hard-kill runs. The earlier diagnostic logs
were not retained, so the exact historical caller cannot be attributed
conclusively. Source review did identify a concrete fixture path capable of
producing the reported `Execution cancelled`: the previous process fixture
called `Service::shutdown()` after its fixed polling deadline; shutdown stops
active execution tokens, and the execution cancellation path may persist a
cancelled run without an explicit session cancellation request. Explicit
session close and recovery of a persisted closing session route through
`Runtime::cancel()` / `Runtime::cancel_locked()`, which records durable
cancellation. The revised fixture verifies the held process and barrier, refuses
graceful teardown before a terminal state, and uses hard process death for this
gate. The physical run showed no cancellation request or cancelled event.

Run-binding and pre-completion crash recovery passed in an earlier controlled
block on the then-current recovered source tree; the retained snapshots do not
embed a Git SHA or executable hash, so they remain historical rather than
fresh exact-candidate evidence. The physical PostgreSQL interruption result is
also retained only as historical evidence without an embedded tested SHA.
Neither case was repeated in this retest. M5.2 closure status and remaining
gates are summarized in the following ledger; PR #14 stays Draft.



## M5.2 bounded acceptance closure review (2026-09-26)

This ledger maps the acceptance boundary in `docs/m5-2-design.md`, the M5.2 roadmap entry, and PR #14. PR #14 remains open and Draft. The executable candidate `8132de1` passed both exact-SHA hosted workflows: pull-request run `36276098931` and push run `36276095925`. Since the inherited physical runtime candidate, `d5bb982` changed close handling; later commits `9c40e65` and `8132de1` changed the test harness and validation docs. Worker-death and stale-fence execution paths were not changed.

Earlier candidate `d5bb982` exposed an intermittent PostgreSQL test hang. Its pull-request full CTest run passed 256 tests with 3 skips, but its separate three-repeat contention step timed out on `Sessions.PostgresTwoInstancesDeduplicateConcurrentSubmissions` after 1,500 seconds. The push full CTest run timed out on the same test. Candidate `9c40e65` bounded the fixture and recorded stages, but its PR repeat failed on the second test repetition and its push full CTest failed the case. Both traces showed the provider watchdog expiring before the retry returned, followed by extra provider calls. Source inspection identified the fixture's synchronous condition-variable wait inside an async provider coroutine; that fixture blocked the service executor during the retry observation. This was a fixture defect; those results did not establish a runtime idempotency defect.

Commit `8132de1` changes only that fixture to cooperatively suspend through `ExecutionContext::delay`, preserving the 10-second watchdog and assertions. Pull-request CTest passed 256, failed 0, skipped 3 in 94.03 seconds; push CTest passed 256, failed 0, skipped 3 in 106.50 seconds. On each workflow, both dispatch/order and concurrent-idempotency tests passed all three dedicated repetitions (3/3 each). All eight hosted jobs passed on both events: GCC Debug/Release, Clang Debug/Release, PostgreSQL, S3-enabled, ASan/UBSan, and Debian. The two S3-disabled configuration tests and opt-in `distributed_m3_acceptance` were skipped; the dedicated S3-enabled job passed. The M3 test requires its separate DSN and is outside the deterministic M5.2 core suite.

No local build/test or physical fault injection was run in this closure pass. A fresh read-only owner-host preflight found an active backup workload and unrelated compute/storage/monitoring activity; GPU telemetry again failed NVML initialization. The other physical host was not preflighted because cross-host fault injection was unsafe without a safe owner-host window.

| Requirement and source | Implementation / focused test | Backend, topology, and evidence | Result and smallest action |
|---|---|---|---|
| Durable accepted-to-run dispatch, sequence order, one run binding, cross-session progress (`docs/m5-2-design.md`) | `Sessions.AcceptedTurnsExecuteDurablyInAcceptanceOrder`, `Sessions.QueuedRunRecoversAfterServiceRestartWithoutDuplicateRun`, `Sessions.DifferentSessionsExecuteConcurrently` | Exact 8132de1 pull-request PostgreSQL CTest, run `36276098931`; SQLite and PostgreSQL-enabled build. | **PASS**: full suite passed; run binding, restart reclaim, ordering, and cross-session progress tests were included. |
| Identical and conflicting idempotency retries, including concurrent submissions across service instances (roadmap; PR acceptance) | `Sessions.PostgresTwoInstancesDeduplicateConcurrentSubmissions`; queued/running/completed retries, conflicting reuse, one event/run lineage, conflict preservation | Exact 8132de1 full PostgreSQL CTest passed in both workflow events; dedicated contention step passed this test 3/3 in each. Two independent LASO instances shared only the test's isolated schema. | **PASS** for deterministic PostgreSQL acceptance. |
| Opaque continuation persistence, restart, isolation, invalid/unsupported state, timeout/cancellation, stale promotion, and secrecy (`docs/m5-2-design.md`) | SQLite/PostgreSQL continuation restart tests, invalid/timeout, unsupported/stateless, stale-completion and close-after-provider tests; runtime canary checks API/run/event/SSE/log/error surfaces | Exact 8132de1 full PostgreSQL CTest passed on both workflow events; deterministic fixtures on SQLite and disposable PostgreSQL. Codex/Claude/OpenCode worker adapters remain non-continuation-capable; no live provider call is required by this contract. | **PASS** for fixture and client-boundary coverage; no real continuation handle or credential used. |
| PostgreSQL multi-instance ownership/fencing and same-session ordered completion (`docs/m5-2-design.md`, roadmap) | `Sessions.PostgresTwoInstancesFenceDispatchAndPreserveSessionOrdering`, `Sessions.PostgresStaleCompletionCannotReplaceContinuation` | Exact 8132de1 full PostgreSQL CTest passed on both events; dispatch/order repeat passed 3/3 in each, and concurrent idempotency passed 3/3. | **PASS** for deterministic multi-instance ordering/fencing coverage. |
| Claim/run-binding crash boundaries, process owner/worker death, PostgreSQL claim interruption and recovery (design acceptance) | `Sessions.ClaimedTurnRecoversAfterInterruptionBeforeRunBinding`, `Sessions.QueuedRunRecoversAfterServiceRestartWithoutDuplicateRun`, `Sessions.PostgresClaimInterruptionIsRecoveredByAnotherInstance`, deterministic owner/worker death and session crash-boundary tests | All listed deterministic tests passed in exact 8132de1 pull-request CTest. Run insertion and turn binding are one atomic transaction, so an after-insert/before-bind committed half-state is impossible. | **PASS** for deterministic CI coverage. Older physical run-binding/pre-completion snapshots have no embedded tested SHA and remain historical. |
| Close/submit/claim/completion/cancellation arbitration and continuation preservation (`docs/m5-2-design.md`) | `Storage.SessionCloseRacesInputAcceptanceTransactionally`, `Storage.SessionCloseRacesTurnClaimTransactionally`, `Sessions.CloseAfterProviderCallDoesNotAdvanceContinuation` | Exact 8132de1 pull-request CTest passed the SQLite/PostgreSQL close and continuation tests. The close fix cancels claimed pre-run work only when no run is bound; active-run cancellation and fencing remain covered. | **PASS** for deterministic races; no physical close/cancel fault injection was performed. |
| Durable lifecycle journal, independent clients, SSE cursor reconnect and service restart (design acceptance) | `Api.SessionSseTwoClientsReplayExecutionAcrossRestart`, `Api.SessionSseResumesAfterCursorAndEndsAfterClose`, PostgreSQL cross-instance stream tests | Exact 8132de1 pull-request CTest passed; replay uses durable events and the documented cursor. | **PASS** for the core/API replay contract; this does not claim LASO-Web integration. |
| Database test-resource collision handling and regression matrix | PostgreSQL fixtures use isolated schemas where supported. The PostgreSQL CTest configuration uses a database-wide `RESOURCE_LOCK` to serialize independent GoogleTest processes against the shared disposable DSN; the two LASO service instances within contention tests remain concurrent. | Both 8132de1 hosted workflows passed all eight jobs. Each PostgreSQL CTest run had 256 passed, 0 failed, 3 skipped; each dedicated contention step passed dispatch/order 3/3 and idempotency 3/3. Skips: two S3-disabled configuration tests and `distributed_m3_acceptance` without its opt-in DSN. The dedicated S3-enabled job passed. TSAN is not configured in the supported matrix. | **PASS** for the configured regression matrix and shared-resource protection; CTest does not claim independent PostgreSQL cases ran concurrently. |
| Physical cross-host worker death and stale-worker fencing (PR acceptance record) | Retained private timelines show hard worker death after durable binding/provider start; stale completion rejected through the real fenced path while the replacement owner remained active, then replacement completion became authoritative. | Inherited evidence attributed by the prior run report to the 739 runtime candidate: latest worker-death block 1/1, 3/3 valid controlled worker-death runs in combined evidence; stale-worker run 1/1. Private snapshots/timelines are retained in the restricted recovery archive, but do not embed a binary SHA. Changes since the physical runtime candidate were limited to close handling and test harness/docs; worker-death and stale-fence execution paths were not changed. | **PASS, inherited with explicit provenance and scope**; no binary-SHA provenance is claimed. |
| Other physical cross-host recovery cases from the documented M5.2 scope | Physical owner death, PostgreSQL outage/recovery, and continuation after physical service restart have no exact-candidate physical evidence. Older physical PostgreSQL interruption and run-binding/pre-completion snapshots lack a tested SHA. | Fresh owner-host preflight found an active backup workload, unrelated compute/storage/monitoring activity, and unavailable GPU telemetry. The other host was not preflighted, and no physical fault was injected. | **GAP / BLOCKED** for physical confirmation. Repeat only when both-host preflight supports a safe window; deterministic CI does not replace these documented physical gates. |


The d5, 9c40e65, and 8132de1 CI summaries and failed logs are retained in a restricted private archive. No test resources or production services were changed. PR #14 remains Draft. M5.2 is **not acceptance-ready** because physical owner-death, PostgreSQL outage/recovery, and continuation-after-restart remain unvalidated for an exact candidate, and the physical host safety preflight did not permit fault injection.

## PR #16 independent review retest (2026-09-26)

On PR head `1a9c2b7894a01ab356d4eed860c26d96cfec0222`, the isolated PostgreSQL 16.15 cluster at port 65439 ran the full Debug, Release, and ASan+UBSan suites. Each reported 264 tests: 261 passed, 0 failed, and 3 skipped (two S3-specific tests in the non-S3 build and optional `distributed_m3_acceptance`). PostgreSQL-backed owner, lease/fencing, migration, schema reopen, persistence, crash recovery, pool, CLI/API, and connection-error cases ran in these suites. The new unreadable legacy SQLite file rejection test passed. The branch remained based on `3cf8bed43d841086d58716bad223e08d6bf22a74`; upstream main had not moved. The process smoke test now drops both its normal and failed-start schemas from its EXIT cleanup; a query after the targeted test and full matrix found no `laso_smoke_*` schemas.

The installed Release tree passed `cmake --install`; `ldd` on `laso-server` showed `libpqxx` and `libpq` and no SQLite library. The systemd user lifecycle acceptance passed against the disposable cluster after correcting stale test assumptions. This validates the invoking-user lifecycle, approvals through SIGTERM/SIGKILL restart, SIGINT, child cleanup, repeated transitions, and startup diagnostics; it does not validate a dedicated service account or system-unit sandbox. Docker/Podman and a Docker socket were unavailable, so image startup/health validation was not run. M3 remained skipped because active Codex sessions were present and its external worker adapters/prerequisites were unavailable; the acceptance was not launched to avoid interfering with those sessions.

The independent review found an undefined legacy `$backend` reference in the systemd lifecycle script, missing DSN provisioning in its transient test units, config-failure probes running from the repository directory, missing schema cleanup in process smoke, and absent coverage for an unreadable legacy state file. These were corrected; each transient systemd probe now runs from its private acceptance directory and writes the disposable DSN only to a mode-0700 temporary directory. Process smoke now removes both schemas it creates on success and failure. One Release run during three-matrix concurrent load reported a segfault in `Api.SessionSseTwoClientsReplayExecutionAcrossRestart`; the focused case and complete Release rerun without the other matrices both passed. The transient failure's root cause was not established. No system-level unit/config test was run because it requires opt-in mutation under `/etc` and the installed dedicated account/config fixture was absent.

### Concurrent Release segfault investigation (2026-09-26)

The original failure was reproduced from the retained systemd-coredump (PID 3149434) and the Release CTest log. GDB identified the failing test as `Api.SessionSseTwoClientsReplayExecutionAcrossRestart`. At the crash, the main test thread was in `TemporaryDirectory::~TemporaryDirectory()` opening its cleanup PostgreSQL connection while an HTTP API worker was still executing `Service::agent_session()` / `Service::session_events()` for an SSE request. The test's cleanup called `HttpServer::stop()`, `Service::shutdown()`, then `io.stop()`. `HttpServer::stop()` posts cancellation work to the IO strand; immediately stopping the IO context prevented those cancellation handlers from draining while the server's separate API thread pool still held a request against the service and its state directory.

The test now allows the IO context to drain naturally after server and service shutdown, and the adjacent cross-instance SSE test shuts down its service before joining the server IO thread. This corrects the test teardown lifecycle rather than adding timing delays. The focused Release SSE case then passed 40/40 executions across eight independent test processes; ASan+UBSan passed 12/12 across four processes. No new core or sanitizer report was produced. The post-fix full concurrent Debug, Release, and ASan+UBSan CTest matrices each passed 264 scheduled tests: 261 passed, 0 failed, 3 skipped. They ran concurrently against the disposable PostgreSQL 16.15 instance with one CTest worker per matrix. The three skips were the two S3-disabled configuration cases and optional `distributed_m3_acceptance`.

The first higher-pressure rerun used CTest parallelism 4/4/3 and exposed a separate timing-bound test issue: `DistributedExecution.ShortLeaseRenewsDuringLongAsyncWork` measured one renewal gap of 2.509s against its 2.500s assertion bound. The same test passed five consecutive isolated Debug repetitions and passed in each final full matrix. The assertion and runtime were not changed. A separate first invocation that omitted `LASO_POSTGRES_DSN` failed 10 CLI/configuration tests with the explicit “PostgreSQL DSN is required” diagnostic; it was an invocation error, not counted as validation, and all final matrices were run with both DSN variables set.

The leftover-schema audit after the first matrix set also found one schema per build tree from `Storage.PostgresRejectsSecondOwner`, which iterated storage fixtures without calling each fixture's cleanup callback. A scoped fixture cleanup now drops the schema on normal and exceptional exits, after the storage owner and temporary directory are destroyed. The targeted case passed in Debug, Release, and ASan+UBSan. After the final matrices, the disposable database had no `laso_test_*` schemas, no LASO test/server process, and only the audit's own `psql` connection. PostgreSQL had a 100-connection limit; observed peak during the higher-pressure run was 54, with no connection-limit or server resource errors in PostgreSQL logs. Available RAM remained above 8 GiB in sampled observations; swap was already nearly full before stress and remained stable. No new core was found.

TSan was configured and built, but its runtime could not start the test binary on this host (`FATAL: ThreadSanitizer: unexpected memory mapping`), so it yielded no race diagnostic. This leaves residual concurrency risk, but the original test-harness use-after-free is established by the retained core and GDB trace, corrected at its shutdown ordering, and did not recur in 40 focused Release runs, 12 focused sanitizer runs, or the final three-way full matrix. The full post-fix matrix result is the accepted validation evidence; the earlier 2.509s bound miss remains recorded rather than hidden.

### PR #16 readiness follow-up (2026-09-27)

The final Clang-format check found one continuation-indent mismatch in `tests/unit/domain.cpp`; it was corrected and the complete source-format command then passed. The first S3-enabled hosted run also exposed two configuration tests that omitted the required PostgreSQL DSN. Both test configs now receive `test_dsn()`, so the negative S3 validation case reaches its intended S3 checks instead of passing on the missing-DSN error. The S3-enabled job passed on the resulting source: **270 scheduled, 268 passed, 2 skipped** (the untrusted-TLS case and optional M3 acceptance). Its Debug, Release, Debian, Clang, PostgreSQL, and formatting jobs passed.

The ASan+UBSan job in that same hosted run timed out only on `Workers.WorkerInteractionsAreDurableIdempotentAndCancellable`. Its 100 ms polling loop did not observe the interaction, and the failed assertion then unwound through a `std::jthread` still waiting for resolution until CTest's 120 second test timeout. No sanitizer diagnostic was reported. This test was not changed. The exact test was run **20 consecutive times** under the local ASan+UBSan build against the disposable PostgreSQL 16.15 database; all 20 passed (9 seconds total). This appears to be a one-off scheduling/observation-window failure under hosted sanitizer load, but remains a check failure until a complete subsequent hosted sanitizer run passes. No PostgreSQL schemas or sessions remained after the local repetitions; the disposable cluster was stopped.

### PR #16 hosted owner-lock failure follow-up (2026-09-27)

Inspected the raw sanitizer job log for run `36289695663` / job `108537281728`. The failing step was the sequential command `ctest --test-dir build --output-on-failure`; CTest test 169 was `Api.SessionSseTwoClientsReplayExecutionAcrossRestart`. The first service completed cancellation, and construction of the replacement service failed after 683 ms with `PostgreSQL database is owned by another LASO process`. The workflow used PostgreSQL 16.15, database `laso_test`, schema derived from the test's unique temporary directory, and the same database/schema only for the intentional old-owner/replacement pair. No other test process ran concurrently in that job. The job did not record PostgreSQL backend PIDs or `pg_locks` at the failure, so the precise hosted lock holder could not be identified from its logs. The CI invocation and schema isolation show no accidental cross-test ownership-key collision.

The ownership lock is a session-level `pg_try_advisory_lock(bigint)` acquired by a dedicated `PostgresStorage::Impl::owner` connection, separate from the connection pool. Previously, orderly destruction relied on closing that connection and allowed PostgreSQL to observe disconnect asynchronously; immediate replacement startup could reach its lock attempt before that disconnect released the session lock. Storage teardown now first destroys the pool and then explicitly executes `pg_advisory_unlock` on the exact dedicated owner connection/key that acquired the lock, before closing that connection. If the unlock query itself fails, the same owner session is still closed so PostgreSQL releases its session lock. This keeps ownership implementation inside the PostgreSQL storage boundary.

Added `Storage.PostgresOwnerCanBeReacquiredImmediatelyAfterStorageDestruction`: for 25 cycles it confirms a competing storage is rejected while the original owner exists, then immediately constructs a replacement after the original storage is destroyed. Against the disposable PostgreSQL 16.15 instance, the ASan+UBSan CTest command repeated both this regression and the restart-SSE test with `--repeat until-fail:20`; the run completed without early failure. The final repeat of each test passed. It produced no sanitizer diagnostic. Post-run queries found zero advisory locks, zero other sessions connected to the test database, and zero `laso_test_*`, `laso_smoke_*`, or manually created test schemas. The only LASO-adjacent process left was this task's explicitly managed disposable PostgreSQL server.

This is local reproduction evidence, not hosted resolution. The hosted ASan+UBSan check must pass on the corrected commit before PR #16 can leave Draft.

The first pushed fix candidate (`c430221`) passed full builds but its hosted GCC and Clang Debug suites both exposed a `process_smoke` failure: run-event log lines appeared in the CLI's redirected JSON output, causing `jq` to reject it. The original hosted sanitizer failure was distinct. The new storage destructor's direct `spdlog` reference was removed to keep logging out of the storage layer; after rebuilding the local Debug CLI, `process_smoke` passed. This also avoids introducing a logger dependency above the PostgreSQL adapter's existing diagnostics boundary. A fresh hosted run is required to confirm both the smoke test and owner-lock fix on the revised candidate.

## PostgreSQL durable-session two-process acceptance (2026-09-27)

Added `distributed_session_two_process` to CTest. The acceptance harness launches two independent `laso-server` processes with separate PIDs, loopback listener ports, and local state directories against one private PostgreSQL schema. Through HTTP it creates one durable session, submits three ordered turns across both instances, retries an idempotency key, observes/replays the event journal through SSE `Last-Event-ID`, and verifies a context generation and run-context snapshot. It forcibly terminates the sole active instance while a turn waits at a public approval boundary, completes that turn through its peer, restarts the failed service, and verifies all accepted turns and provenance remain readable.

Local validation used PostgreSQL 16.15 and the Debug build with session test hooks enabled. The new CTest passed **10/10 consecutive repetitions** after a bounded poll was added between approval completion and context-generation creation. An earlier repetition had exposed that the test advanced before the asynchronous turn completion was durable and received HTTP 409; it was a test-harness synchronization defect, corrected without changing LASO runtime code. The final full Debug CTest passed **271/274 tests, 0 failures, 3 skips**. The skips were the two S3-only configuration tests because this build did not enable S3, and `distributed_m3_acceptance` because authenticated Codex/OpenCode worker executables were unavailable. The existing public API SSE tests, PostgreSQL two-instance turn ordering/idempotency/fencing tests, and distributed process-crash/session recovery tests passed in the full suite. Hosted CI for this acceptance branch had not run at the time of this entry. This is cross-process validation on one host; no second physical machine, external host, or AWS S3 service was used.
