#!/usr/bin/env bash
set -Eeuo pipefail

# This is intentionally manual and opt-in: it invokes installed coding agents
# and may consume provider quota. It never invokes git, gh, or any GitHub API.

if [[ $# -ne 2 ]]; then
  echo "usage: $0 BUILD_DIR SOURCE_DIR" >&2
  exit 2
fi

build_dir=$(realpath "$1")
source_dir=$(realpath "$2")
laso_bin="$build_dir/bin/laso"
server_bin="$build_dir/bin/laso-server"
codex_worker="$build_dir/bin/laso-codex-worker"
opencode_worker="$build_dir/bin/laso-opencode-worker"

for required in "$laso_bin" "$server_bin" "$codex_worker" "$opencode_worker"; do
  [[ -x "$required" ]] || { echo "missing executable: $(basename "$required")" >&2; exit 77; }
done
command -v curl >/dev/null || { echo "curl is required" >&2; exit 77; }
jq_bin=${JQ_BIN:-$(command -v jq || true)}
if [[ -z "$jq_bin" && -x "$source_dir/local-deps/root/usr/bin/jq" ]]; then
  jq_bin="$source_dir/local-deps/root/usr/bin/jq"
fi
[[ -n "$jq_bin" && -x "$jq_bin" ]] || { echo "jq is required" >&2; exit 77; }
function jq() { "$jq_bin" "$@"; }
command -v cmake >/dev/null || { echo "cmake is required" >&2; exit 77; }
command -v ctest >/dev/null || { echo "ctest is required" >&2; exit 77; }

codex_bin=${CODEX_BIN:-$(command -v codex || true)}
opencode_bin=${OPENCODE_BIN:-$(command -v opencode || true)}
[[ -n "$codex_bin" && -x "$codex_bin" ]] || {
  echo "Codex executable is unavailable; set CODEX_BIN to an installed executable" >&2
  exit 77
}
[[ -n "$opencode_bin" && -x "$opencode_bin" ]] || {
  echo "OpenCode executable is unavailable; set OPENCODE_BIN to an installed executable" >&2
  exit 77
}

run_root=$(mktemp -d)
server_pid=
cleanup() {
  if [[ -n "${server_pid:-}" ]]; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [[ "${LASO_PHASE1_KEEP_ARTIFACTS:-0}" == "1" ]]; then
    echo "Phase 1 temporary diagnostics retained for inspection" >&2
  else
    rm -rf -- "$run_root"
  fi
}
trap cleanup EXIT

workspace_root="$run_root/workspaces"
state_dir="$run_root/state"
mkdir -p "$workspace_root" "$state_dir"

escape_sed() {
  printf '%s' "$1" | sed 's/[&|]/\\&/g'
}
render_config() {
  local output=$1
  local opencode_port=$2
  sed \
    -e "s|@DATA_DIR@|$(escape_sed "$state_dir")|g" \
    -e "s|@CODEX_WORKER@|$(escape_sed "$codex_worker")|g" \
    -e "s|@CODEX_BIN@|$(escape_sed "$codex_bin")|g" \
    -e "s|@OPENCODE_WORKER@|$(escape_sed "$opencode_worker")|g" \
    -e "s|@OPENCODE_BIN@|$(escape_sed "$opencode_bin")|g" \
    -e "s|@WORKSPACE_ROOT@|$(escape_sed "$workspace_root")|g" \
    -e "s|@OPENCODE_PORT@|$(escape_sed "$opencode_port")|g" \
    -e "s|@OPENCODE_DATA@|$(escape_sed "$run_root/opencode-data")|g" \
    -e "s|@OPENCODE_CONFIG@|$(escape_sed "$run_root/opencode-config")|g" \
    "$source_dir/tests/acceptance/real-agent-config.yaml.in" > "$output"
}

config_file="$run_root/laso.yaml"
opencode_port=$((20000 + (BASHPID % 10000)))
render_config "$config_file" "$opencode_port"

port=$((30000 + (BASHPID % 10000)))
base="http://127.0.0.1:$port/api/v1"
start_server() {
  "$server_bin" --config "$config_file" --host 127.0.0.1 --port "$port" \
    >"$run_root/server.stdout" 2>"$run_root/server.stderr" &
  server_pid=$!
  for _ in $(seq 1 300); do
    if curl -fsS "$base/health" >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      echo "LASO server exited during startup" >&2
      exit 1
    fi
    sleep 0.1
  done
  echo "LASO server did not become healthy" >&2
  exit 1
}
stop_server() {
  if [[ -n "${server_pid:-}" ]]; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
    server_pid=
  fi
}

register_pipeline() {
  local pipeline_file=$1
  local name=$2
  local response
  response=$(jq -n --rawfile yaml "$pipeline_file" '{yaml: $yaml}')
  curl -fsS -X POST "$base/pipelines" -H 'Content-Type: application/json' \
    --data-binary "$response" >/dev/null
  jq -e --arg name "$name" '.name == $name' < <(curl -fsS "$base/pipelines/$name@1") >/dev/null
}

start_run() {
  local pipeline=$1
  local project_dir=$2
  local input=${3:-'{"validation":true}'}
  local response
  response=$(jq -n --argjson input "$input" --arg project "$project_dir" \
    '{input: $input, metadata: {project_dir: $project, classification: "public"}}')
  curl -fsS -X POST "$base/pipelines/$pipeline/runs" -H 'Content-Type: application/json' \
    --data-binary "$response" | jq -r '.id'
}

run_state() {
  curl -fsS "$base/runs/$1" | jq -r '.state'
}

wait_for_state() {
  local run_id=$1
  local wanted=$2
  local seconds=${3:-300}
  local state
  for _ in $(seq 1 $((seconds * 10))); do
    state=$(run_state "$run_id")
    [[ "$state" == "$wanted" ]] && return 0
    [[ "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]] && {
      error=$(curl -fsS "$base/runs/$run_id" | jq -r '.error // ""' |
        sed -E 's#/[hH][oO][mM][eE]/[^ ]+#<redacted-path>#g; s#/[tT][mM][pP]/[^ ]+#<redacted-temp>#g')
      echo "run $run_id reached unexpected terminal state $state while waiting for $wanted: $error" >&2
      return 1
    }
    sleep 0.1
  done
  echo "run $run_id did not reach $wanted" >&2
  return 1
}

approve_run() {
  local run_id=$1
  local approval_id
  for _ in $(seq 1 300); do
    approval_id=$(curl -fsS "$base/approvals?limit=100" | jq -r --arg run "$run_id" \
      '.[] | select(.run_id == $run and .decision == "pending") | .id' | head -1)
    [[ -n "$approval_id" ]] && break
    sleep 0.1
  done
  [[ -n "${approval_id:-}" ]] || { echo "no pending approval for $run_id" >&2; return 1; }
  curl -fsS -X POST "$base/approvals/$approval_id/approve" \
    -H 'Content-Type: application/json' -d '{"comment":"Phase 1 acceptance review"}' >/dev/null
}

validate_run_records() {
  local run_id=$1
  local workspace=$2
  local run attempts jobs
  run=$(curl -fsS "$base/runs/$run_id")
  attempts=$(curl -fsS "$base/runs/$run_id/attempts?limit=100")
  jobs=$(curl -fsS "$base/worker-jobs?limit=100")
  printf 'Durable records: payload_items=%s worker_jobs=%s agent_attempts=%s deterministic_attempts=%s\n' \
    "$(jq -r '.message.payload | if type == "array" then length else -1 end' <<<"$run")" \
    "$(jq -r --arg run "$run_id" '[.[] | select(.run_id == $run and .status == "Completed")] | length' <<<"$jobs")" \
    "$(jq -r '[.[] | select(.node_id == "codex" or .node_id == "opencode") | .state] | join(",")' <<<"$attempts")" \
    "$(jq -r '[.[] | select(.node_id == "deterministic") | .state] | join(",")' <<<"$attempts")"
  jq -e '.state == "Completed" and (.message.payload | length == 3)' <<<"$run" >/dev/null
  jq -e --arg run "$run_id" \
    '[.[] | select(.run_id == $run)] | map(select(.status == "Completed")) | length >= 2' \
    <<<"$jobs" >/dev/null
  jq -e '[.[] | select(.node_id == "codex" or .node_id == "opencode")] |
         group_by(.node_id) | map(any(.[]; .state == "Completed")) | all' \
    <<<"$attempts" >/dev/null
  jq -e '[.[] | select(.node_id == "deterministic" and .state == "Completed")] | length == 1' \
    <<<"$attempts" >/dev/null
  if cmp -s "$workspace/src/math.cpp" \
    "$source_dir/tests/acceptance/fixtures/real-agent-project/src/math.cpp"; then
    echo "Assigned-file check: math=unchanged label=pending" >&2
    echo "Codex branch did not change its assigned file" >&2
    return 1
  fi
  if cmp -s "$workspace/src/label.cpp" \
    "$source_dir/tests/acceptance/fixtures/real-agent-project/src/label.cpp"; then
    echo "Assigned-file check: math=changed label=unchanged" >&2
    echo "OpenCode branch did not change its assigned file" >&2
    return 1
  fi
  echo "Assigned-file check: math=changed label=changed" >&2
}

build_and_test_fixture() {
  local workspace=$1
  echo "Building synthetic fixture" >&2
  if ! cmake -S "$workspace" -B "$workspace/build" -G Ninja -DCMAKE_BUILD_TYPE=Debug \
    >"$run_root/fixture-configure.log"; then
    echo "Synthetic fixture configure failed" >&2
    return 1
  fi
  if ! cmake --build "$workspace/build" >"$run_root/fixture-build.log"; then
    echo "Synthetic fixture build failed" >&2
    return 1
  fi
  if ! ctest --test-dir "$workspace/build" --output-on-failure >"$run_root/fixture-ctest.log"; then
    echo "Synthetic fixture tests failed" >&2
    return 1
  fi
  echo "Synthetic fixture build/tests passed" >&2
}

kill_server_for_recovery() {
  local pid=$1
  local descendants
  descendants=$(pgrep -P "$pid" || true)
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  for child in $descendants; do
    kill -KILL "$child" 2>/dev/null || true
  done
  server_pid=
}

if [[ "${LASO_PHASE1_SKIP_REAL_ADAPTER_TESTS:-0}" == "1" ]]; then
  echo "Skipping separately gated adapter tests (pipeline still invokes both providers)"
else
  echo "Running opt-in real adapter tests"
  LASO_RUN_REAL_CODEX=1 CODEX_BIN="$codex_bin" ctest --test-dir "$build_dir" \
    --output-on-failure -R 'CodexWorker.*RealInstalled' >"$run_root/codex-real-ctest.log"
  LASO_RUN_REAL_OPENCODE=1 OPENCODE_BIN="$opencode_bin" ctest --test-dir "$build_dir" \
    --output-on-failure -R 'OpenCodeWorker.*RealInstalled' >"$run_root/opencode-real-ctest.log"
fi

echo "Running baseline CTest suite"
ctest --test-dir "$build_dir" --output-on-failure >"$run_root/ctest.log"

start_server
register_pipeline "$source_dir/tests/acceptance/real-agent-pipeline.yaml" phase1-real-agents
register_pipeline "$source_dir/tests/acceptance/real-agent-cancel-pipeline.yaml" phase1-real-agent-cancel
register_pipeline "$source_dir/tests/acceptance/real-agent-failure-pipeline.yaml" phase1-real-agent-failure

successful_runs=0
for iteration in 1 2; do
  iteration_complete=0
  for trial in 1 2 3; do
    echo "Starting real-agent pipeline iteration $iteration (trial $trial)"
    workspace="$workspace_root/success-$iteration-$trial"
    mkdir -p "$workspace"
    cp -a "$source_dir/tests/acceptance/fixtures/real-agent-project/." "$workspace/"
    chmod +x "$workspace/slow-task.sh" 2>/dev/null || true
    if [[ ! -e "$workspace/slow-task.sh" ]]; then
      printf '#!/usr/bin/env bash\nsleep 30\n' > "$workspace/slow-task.sh"
      chmod 700 "$workspace/slow-task.sh"
    fi
    run_id=$(start_run phase1-real-agents "$workspace")
    if ! wait_for_state "$run_id" WaitingApproval 300; then
      stop_server
      start_server
      continue
    fi
    echo "Iteration $iteration reached durable approval checkpoint"
    if [[ "$iteration" == "1" ]]; then
      kill_server_for_recovery "$server_pid"
      start_server
      state=$(run_state "$run_id")
      if [[ "$state" != "Paused" && "$state" != "WaitingApproval" ]]; then
        echo "recovery produced unexpected state $state" >&2
        stop_server
        start_server
        continue
      fi
      if [[ "$state" == "Paused" ]]; then
        curl -fsS -X POST "$base/runs/$run_id/resume" >/dev/null
        if ! wait_for_state "$run_id" WaitingApproval 60; then
          stop_server
          start_server
          continue
        fi
      fi
    fi
    if ! approve_run "$run_id" || ! wait_for_state "$run_id" Completed 300; then
      stop_server
      start_server
      continue
    fi
    echo "Iteration $iteration completed; validating durable records and fixture"
    if ! validate_run_records "$run_id" "$workspace" || ! build_and_test_fixture "$workspace"; then
      stop_server
      start_server
      continue
    fi
    successful_runs=$((successful_runs + 1))
    iteration_complete=1
    stop_server
    start_server
    break
  done
  [[ "$iteration_complete" == "1" ]] || {
    echo "iteration $iteration did not produce a valid successful run after three trials" >&2
    exit 1
  }
done

cancel_workspace="$workspace_root/cancel"
echo "Starting real-agent cancellation exercise"
mkdir -p "$cancel_workspace"
cp -a "$source_dir/tests/acceptance/fixtures/real-agent-project/." "$cancel_workspace/"
printf '#!/usr/bin/env bash\nsleep 30\n' > "$cancel_workspace/slow-task.sh"
chmod 700 "$cancel_workspace/slow-task.sh"
cancel_run=$(start_run phase1-real-agent-cancel "$cancel_workspace")
active_job=
for _ in $(seq 1 300); do
  active_job=$(curl -fsS "$base/worker-jobs?limit=100" | jq -r --arg run "$cancel_run" \
    '.[] | select(.run_id == $run and (.status == "Submitting" or .status == "Running" or
      .status == "Waiting" or .status == "Queued")) | .id' | head -1)
  [[ -n "$active_job" ]] && break
  sleep 0.1
done
[[ -n "$active_job" ]] || { echo "real cancellation job never became active" >&2; exit 1; }
curl -fsS -X POST "$base/runs/$cancel_run/cancel" >/dev/null
wait_for_state "$cancel_run" Cancelled 120
cancel_job=$(curl -fsS "$base/worker-jobs/$active_job")
cancel_state=$(jq -r '.status' <<<"$cancel_job")
cancel_acknowledged=$(jq -r '.cancellation_acknowledged // false' <<<"$cancel_job")
if [[ "$cancel_state" == "Cancelled" && "$cancel_acknowledged" == "true" ]]; then
  cancellation_outcome="acknowledged"
else
  echo "unexpected worker state after cancellation: $cancel_state" >&2
  exit 1
fi
stop_server
start_server

failure_workspace="$run_root/outside-root"
echo "Starting allowed-root failure exercise"
mkdir -p "$failure_workspace"
failure_run=$(start_run phase1-real-agent-failure "$failure_workspace")
wait_for_state "$failure_run" Failed 120
failure_job_error=$(curl -fsS "$base/worker-jobs?limit=100" | jq -r --arg run "$failure_run" \
  '.[] | select(.run_id == $run and .status == "Failed") | .error' | head -1)
grep -qi 'outside an allowed root' <<<"$failure_job_error" || {
  echo "allowed-root failure did not reach the durable worker-job error" >&2
  exit 1
}

stop_server
printf 'REAL_AGENT_PHASE1_OK\n'
printf 'successful_repeated_runs=%s\n' "$successful_runs"
printf 'crash_restart=pass\n'
printf 'cancellation=%s\n' "$cancellation_outcome"
printf 'failure_boundary=pass\n'
printf 'build_and_tests=pass\n'
