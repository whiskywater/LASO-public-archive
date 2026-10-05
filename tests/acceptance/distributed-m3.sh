#!/usr/bin/env bash
set -Eeuo pipefail

# Opt-in only. This starts two real LASO instances and real supported workers;
# it never invokes git, gh, or any GitHub API. PostgreSQL credentials are read
# from the environment and are never printed or written to repository files.
if [[ $# -ne 2 ]]; then
  echo "usage: $0 BUILD_DIR SOURCE_DIR" >&2
  exit 2
fi
build_dir=$(realpath "$1")
source_dir=$(realpath "$2")
dsn=${LASO_TEST_POSTGRES_DSN:-}
[[ -n "$dsn" ]] || { echo "LASO_TEST_POSTGRES_DSN is required" >&2; exit 77; }
for required in "$build_dir/bin/laso-server" "$build_dir/bin/laso-codex-worker" \
               "$build_dir/bin/laso-opencode-worker"; do
  [[ -x "$required" ]] || { echo "missing distributed acceptance executable" >&2; exit 77; }
done
command -v curl >/dev/null || { echo "curl is required" >&2; exit 77; }
jq_bin=${JQ_BIN:-$(command -v jq || true)}
if [[ -z "$jq_bin" && -x "$source_dir/local-deps/root/usr/bin/jq" ]]; then
  jq_bin="$source_dir/local-deps/root/usr/bin/jq"
fi
[[ -n "$jq_bin" && -x "$jq_bin" ]] || { echo "jq is required" >&2; exit 77; }
jq() { "$jq_bin" "$@"; }
codex_bin=${CODEX_BIN:-$(command -v codex || true)}
opencode_bin=${OPENCODE_BIN:-$(command -v opencode || true)}
[[ -n "$codex_bin" && -x "$codex_bin" ]] || { echo "Codex executable is unavailable" >&2; exit 77; }
[[ -n "$opencode_bin" && -x "$opencode_bin" ]] || { echo "OpenCode executable is unavailable" >&2; exit 77; }

run_root=$(mktemp -d)
server_a=; server_b=
cleanup() {
  for pid in "${server_a:-}" "${server_b:-}"; do
    if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  done
  rm -rf -- "$run_root"
}
trap cleanup EXIT

# The schema is unique to this invocation. A test database should be disposable;
# the C++ PostgreSQL integration helpers remain the authoritative schema-cleanup
# path for automated backend tests.
schema="laso_m3_$(printf '%s' "$RANDOM$RANDOM" | tr -cd '[:alnum:]')"
artifact_port=$((36000 + (BASHPID % 1000)))
artifact_token="synthetic-m3-artifact-token"
mkdir -p "$run_root/workspaces" "$run_root/owner" "$run_root/worker"
escape_sed() { printf '%s' "$1" | sed 's/[&|]/\\&/g'; }
render_config() {
  local out=$1 data=$2 port=$3 opencode_port=$4 role=$5 artifact_service_port=$6 artifact_service_url=$7
  local rendered="$run_root/rendered-$role.yaml"
  mkdir -p "$data/distributed-workspaces"
  sed -e "s|@DATA_DIR@|$(escape_sed "$data")|g" \
      -e "s|@POSTGRES_DSN@|$(escape_sed "$dsn")|g" \
      -e "s|@POSTGRES_SCHEMA@|$(escape_sed "$schema")|g" \
      -e "s|@CODEX_WORKER@|$(escape_sed "$build_dir/bin/laso-codex-worker")|g" \
      -e "s|@CODEX_BIN@|$(escape_sed "$codex_bin")|g" \
      -e "s|@OPENCODE_WORKER@|$(escape_sed "$build_dir/bin/laso-opencode-worker")|g" \
      -e "s|@OPENCODE_BIN@|$(escape_sed "$opencode_bin")|g" \
      -e "s|@WORKSPACE_ROOT@|$(escape_sed "$data/distributed-workspaces")|g" \
      -e "s|@OPENCODE_PORT@|$(escape_sed "$opencode_port")|g" \
      -e "s|@OPENCODE_DATA@|$(escape_sed "$run_root/opencode-data-$port")|g" \
      -e "s|@OPENCODE_CONFIG@|$(escape_sed "$run_root/opencode-config-$port")|g" \
      -e "s|@ARTIFACT_SERVICE_PORT@|$artifact_service_port|g" \
      -e "s|@ARTIFACT_SERVICE_URL@|$(escape_sed "$artifact_service_url")|g" \
      -e "s|@ARTIFACT_SERVICE_TOKEN@|$(escape_sed "$artifact_token")|g" \
      "$source_dir/tests/acceptance/distributed-m3-config.yaml.in" > "$rendered"
  if [[ "$role" == "owner" ]]; then
    sed '/^process_workers:/,$d' "$rendered" > "$out"
  else
    mv "$rendered" "$out"
  fi
}
port_a=$((30000 + (BASHPID % 1000)))
port_b=$((31000 + (BASHPID % 1000)))
render_config "$run_root/owner.yaml" "$run_root/owner" "$port_a" "$((32000 + BASHPID % 1000))" owner "$artifact_port" ""
render_config "$run_root/worker.yaml" "$run_root/worker" "$port_b" "$((33000 + BASHPID % 1000))" worker 0 "http://127.0.0.1:$artifact_port"
base_a="http://127.0.0.1:$port_a/api/v1"
base_b="http://127.0.0.1:$port_b/api/v1"
start_instance() {
  local config=$1 port=$2 var=$3
  "$build_dir/bin/laso-server" --config "$config" --host 127.0.0.1 --port "$port" \
    >"$run_root/$var.stdout" 2>"$run_root/$var.stderr" &
  printf -v "$var" '%s' "$!"
  for _ in $(seq 1 300); do
    if curl -fsS "http://127.0.0.1:$port/api/v1/health" >/dev/null 2>&1; then return; fi
    if ! kill -0 "${!var}" 2>/dev/null; then
      echo "$var failed during startup" >&2
      sed -E 's#postgresql://[^[:space:]"}]+#<redacted-dsn>#g; s#/(home|tmp)/[^[:space:]"}]+#<redacted-path>#g' \
        "$run_root/$var.stderr" "$run_root/$var.stdout" 2>/dev/null || true
      return 1
    fi
    sleep 0.1
  done
  return 1
}
start_instance "$run_root/owner.yaml" "$port_a" server_a
start_instance "$run_root/worker.yaml" "$port_b" server_b

pipeline=$(jq -n --rawfile yaml "$source_dir/tests/acceptance/distributed-m3-pipeline.yaml" '{yaml: $yaml}')
curl -fsS -X POST "$base_a/pipelines" -H 'Content-Type: application/json' --data-binary "$pipeline" >/dev/null
fixture_root="$source_dir/tests/acceptance/fixtures/real-agent-project"
for fixture in \
  "$fixture_root/CMakeLists.txt" \
  "$fixture_root/TASK.md" \
  "$fixture_root/include/laso_phase1/label.hpp" \
  "$fixture_root/include/laso_phase1/math.hpp" \
  "$fixture_root/src/label.cpp" \
  "$fixture_root/src/math.cpp" \
  "$fixture_root/tests/fixture_tests.cpp"; do
  [[ -f "$fixture" ]] || { echo "synthetic fixture is missing" >&2; exit 1; }
done
sha_cmake=$(sha256sum "$fixture_root/CMakeLists.txt" | awk '{print $1}')
sha_task=$(sha256sum "$fixture_root/TASK.md" | awk '{print $1}')
sha_label_h=$(sha256sum "$fixture_root/include/laso_phase1/label.hpp" | awk '{print $1}')
sha_math_h=$(sha256sum "$fixture_root/include/laso_phase1/math.hpp" | awk '{print $1}')
sha_label=$(sha256sum "$fixture_root/src/label.cpp" | awk '{print $1}')
sha_math=$(sha256sum "$fixture_root/src/math.cpp" | awk '{print $1}')
sha_tests=$(sha256sum "$fixture_root/tests/fixture_tests.cpp" | awk '{print $1}')
manifest=$(jq -n \
  --rawfile cmake "$fixture_root/CMakeLists.txt" --arg sha_cmake "$sha_cmake" \
  --rawfile task "$fixture_root/TASK.md" --arg sha_task "$sha_task" \
  --rawfile label_h "$fixture_root/include/laso_phase1/label.hpp" --arg sha_label_h "$sha_label_h" \
  --rawfile math_h "$fixture_root/include/laso_phase1/math.hpp" --arg sha_math_h "$sha_math_h" \
  --rawfile label "$fixture_root/src/label.cpp" --arg sha_label "$sha_label" \
  --rawfile math "$fixture_root/src/math.cpp" --arg sha_math "$sha_math" \
  --rawfile tests "$fixture_root/tests/fixture_tests.cpp" --arg sha_tests "$sha_tests" \
  '{version:1,files:[
    {path:"CMakeLists.txt",size:($cmake|length),sha256:$sha_cmake,data:($cmake|explode)},
    {path:"TASK.md",size:($task|length),sha256:$sha_task,data:($task|explode)},
    {path:"include/laso_phase1/label.hpp",size:($label_h|length),sha256:$sha_label_h,data:($label_h|explode)},
    {path:"include/laso_phase1/math.hpp",size:($math_h|length),sha256:$sha_math_h,data:($math_h|explode)},
    {path:"src/label.cpp",size:($label|length),sha256:$sha_label,data:($label|explode)},
    {path:"src/math.cpp",size:($math|length),sha256:$sha_math,data:($math|explode)},
    {path:"tests/fixture_tests.cpp",size:($tests|length),sha256:$sha_tests,data:($tests|explode)}
  ]}')
request=$(jq -n --argjson manifest "$manifest" \
  '{input:{request:"repair the synthetic project"},metadata:{classification:"public",workspace_manifest:$manifest}}')
run_id=$(curl -fsS -X POST "$base_a/pipelines/distributed-real-workers@1/runs" \
  -H 'Content-Type: application/json' --data-binary "$request" | jq -r '.id')
report_failure() {
  local state=$1 jobs attempts
  echo "distributed run reached $state" >&2
  jq -c '{id, state, error, message}' <<<"$run" 2>/dev/null |
    sed -E 's#/(home|tmp)/[^[:space:]"}]+#<redacted-path>#g' >&2 || true
  jobs=$(curl -fsS "$base_a/worker-jobs?limit=100" 2>/dev/null || true)
  if [[ -n "$jobs" ]]; then
    jq -c --arg run "$run_id" '[.[] | select(.run_id == $run) |
      {id, node_id, status, attempt, error, cancellation_acknowledged}]' <<<"$jobs" 2>/dev/null |
      sed -E 's#/(home|tmp)/[^[:space:]"}]+#<redacted-path>#g' >&2 || true
  fi
  interactions=$(curl -fsS "$base_a/worker-interactions?limit=100" 2>/dev/null || true)
  if [[ -n "$interactions" ]]; then
    jq -c --arg run "$run_id" '[.[] | select(.run_id == $run) |
      {id, worker_id, type, state, reason}]' <<<"$interactions" 2>/dev/null >&2 || true
  fi
  attempts=$(curl -fsS "$base_a/runs/$run_id/attempts?limit=100" 2>/dev/null || true)
  if [[ -n "$attempts" ]]; then
    jq -c '[.[] | {id, node_id, attempt, state, error, worker, provider}]' <<<"$attempts" 2>/dev/null |
      sed -E 's#/(home|tmp)/[^[:space:]"}]+#<redacted-path>#g' >&2 || true
  fi
}
for _ in $(seq 1 3600); do
  run=$(curl -fsS "$base_a/runs/$run_id")
  state=$(jq -r '.state' <<<"$run")
  [[ "$state" == "Completed" ]] && break
  [[ "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]] && {
    report_failure "$state"; exit 1;
  }
  sleep 0.1
done
terminal_state=$(jq -r '.state' <<<"$run")
[[ "$terminal_state" == "Completed" ]] || { report_failure "TimedOut"; exit 1; }
echo "distributed_state=$terminal_state"
if ! payload_count=$(jq -r '.message.payload | length' <<<"$run"); then
  echo "unable to inspect joined payload" >&2
  exit 1
fi
echo "joined_payload_count=$payload_count"
[[ "$payload_count" == "3" ]] || { echo "unexpected joined payload count: $payload_count" >&2; exit 1; }
if ! jobs=$(curl -fsS "$base_a/worker-jobs?limit=100"); then
  echo "worker-job listing request failed" >&2
  exit 1
fi
if ! completed_jobs=$(jq -r --arg run "$run_id" '[.[] | select(.run_id == $run and .status == "Completed")] | length' <<<"$jobs"); then
  echo "unable to inspect worker-job listing" >&2
  exit 1
fi
echo "completed_worker_jobs=$completed_jobs"
[[ "$completed_jobs" == "2" ]] || {
  echo "unexpected completed worker-job count: $completed_jobs" >&2
  jq -c --arg run "$run_id" '[.[] | select(.run_id == $run) |
    {id, node_id, status, error, cancellation_acknowledged}]' <<<"$jobs" >&2 || true
  exit 1
}
echo "DISTRIBUTED_M3_BASIC_OK"
