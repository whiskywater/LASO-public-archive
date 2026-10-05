#!/usr/bin/env bash
set -Eeuo pipefail

# Opt-in cross-machine gate.  The owner runs in the caller's Linux-compatible
# environment and the worker/provider runs over SSH on a separately prepared
# Linux test host.  Credentials are supplied only through the environment and
# are never written to repository files or printed.
if [[ $# -ne 2 ]]; then
  echo "usage: $0 OWNER_BUILD_DIR SOURCE_DIR" >&2
  exit 2
fi
owner_build=$(realpath "$1")
source_dir=$(realpath "$2")
target=${LASO_REMOTE_SSH_TARGET:-}
remote_build=${LASO_REMOTE_BUILD_DIR:-}
owner_dsn=${LASO_TEST_POSTGRES_DSN:-}
owner_pgpassword=${LASO_TEST_PGPASSWORD:-${LASO_REMOTE_PGPASSWORD:-}}
remote_dsn=${LASO_REMOTE_POSTGRES_DSN:-}
runs=${LASO_CROSS_MACHINE_RUNS:-3}
scenario=${LASO_CROSS_MACHINE_SCENARIO:-complete}
[[ -n "$target" && -n "$remote_build" && -n "$owner_dsn" && -n "$remote_dsn" ]] || {
  echo "LASO_REMOTE_SSH_TARGET, LASO_REMOTE_BUILD_DIR, LASO_TEST_POSTGRES_DSN, and LASO_REMOTE_POSTGRES_DSN are required" >&2
  exit 77
}
[[ "$runs" =~ ^[1-9][0-9]*$ ]] || { echo "LASO_CROSS_MACHINE_RUNS must be a positive integer" >&2; exit 2; }
pipeline_file="$source_dir/tests/acceptance/distributed-m3-cross-machine-codex-pipeline.yaml"
if [[ "$scenario" == "cancel" ]]; then
  pipeline_file="$source_dir/tests/acceptance/distributed-m3-cross-machine-codex-cancel-pipeline.yaml"
  pipeline_ref="distributed-cross-machine-codex-cancel@1"
elif [[ "$scenario" == "worker-death" ]]; then
  pipeline_ref="distributed-cross-machine-codex@1"
elif [[ "$scenario" == "stale-worker" ]]; then
  pipeline_ref="distributed-cross-machine-codex@1"
elif [[ "$scenario" == "owner-death" ]]; then
  pipeline_ref="distributed-cross-machine-codex@1"
elif [[ "$scenario" == "db-interruption" ]]; then
  pipeline_ref="distributed-cross-machine-codex@1"
elif [[ "$scenario" != "complete" ]]; then
  echo "unsupported cross-machine scenario: $scenario" >&2
  exit 2
else
  pipeline_ref="distributed-cross-machine-codex@1"
fi
for required in "$owner_build/bin/laso-server" "$pipeline_file"; do
  [[ -f "$required" || -x "$required" ]] || { echo "missing owner acceptance input: $required" >&2; exit 77; }
done
command -v curl >/dev/null || { echo "curl is required" >&2; exit 77; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 77; }
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 77; }
command -v cmake >/dev/null || { echo "cmake is required for owner-side artifact validation" >&2; exit 77; }

ssh_options=(-o BatchMode=no -o StrictHostKeyChecking=no)
if [[ -n "${LASO_SSH_CONTROL_PATH:-}" ]]; then
  ssh_options+=(-o "ControlPath=$LASO_SSH_CONTROL_PATH")
fi

remote_has() { ssh "${ssh_options[@]}" "$target" "$*"; }
remote_stop_process_tree() {
  local pid=${1:-}
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0
  remote_has "
    descend() {
      for child in \$(pgrep -P \"\$1\" 2>/dev/null || true); do
        descend \"\$child\"
      done
      printf '%s\\n' \"\$1\"
    }
    if kill -0 '$pid' 2>/dev/null; then
      owned_pids=\$(descend '$pid' | sort -rn | uniq)
      kill -TERM \$owned_pids 2>/dev/null || true
      sleep 1
      kill -KILL \$owned_pids 2>/dev/null || true
    fi" || true
}
remote_root=$(remote_has 'mktemp -d /tmp/laso-m3-cross.XXXXXX')
remote_server_pid=
remote_b_root=
remote_b_server_pid=
remote_db_interrupt_pid=
artifact_tunnel_pid=
run_root=$(mktemp -d)
owner_pid=
cleanup() {
  local status=$?
  if [[ "$status" != 0 ]]; then
    echo "cross-machine acceptance diagnostics (sanitized)" >&2
    if [[ -f "${run_root:-}/owner.stderr" ]]; then
      echo "--- owner stderr ---" >&2
      tail -n 80 "$run_root/owner.stderr" >&2 || true
    fi
    if [[ -n "${remote_root:-}" ]]; then
      echo "--- remote worker stderr ---" >&2
      remote_has "tail -n 80 '$remote_root/worker.stderr' 2>/dev/null || true" >&2 || true
    fi
  fi
  if [[ -n "${owner_pid:-}" ]]; then kill -TERM "$owner_pid" 2>/dev/null || true; wait "$owner_pid" 2>/dev/null || true; fi
  if [[ -n "${remote_server_pid:-}" ]]; then remote_stop_process_tree "$remote_server_pid"; fi
  if [[ -n "${remote_b_server_pid:-}" ]]; then remote_stop_process_tree "$remote_b_server_pid"; fi
  if [[ -n "${remote_db_interrupt_pid:-}" ]]; then kill "$remote_db_interrupt_pid" 2>/dev/null || true; wait "$remote_db_interrupt_pid" 2>/dev/null || true; fi
  if [[ -n "${artifact_tunnel_pid:-}" ]]; then kill "$artifact_tunnel_pid" 2>/dev/null || true; wait "$artifact_tunnel_pid" 2>/dev/null || true; fi
  if [[ -n "${remote_b_root:-}" ]]; then remote_has "rm -rf -- '$remote_b_root'" || true; fi
  remote_has "rm -rf -- '$remote_root'" || true
  rm -rf -- "$run_root"
  return "$status"
}
trap cleanup EXIT

escape_sed() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }
schema="laso_m3_cross_$(printf '%s' "$RANDOM$RANDOM" | tr -cd '[:alnum:]')"
owner_data="$run_root/owner"
owner_port=$((34000 + ((BASHPID + RANDOM) % 1000)))
remote_port=$((35000 + ((BASHPID + RANDOM) % 2000)))
owner_artifact_port=$((37000 + ((BASHPID + RANDOM) % 1000)))
remote_artifact_port=$((owner_artifact_port + 1))
artifact_token="synthetic-m3-artifact-token"
mkdir -p "$owner_data/distributed-workspaces"

sed -e "s|@DATA_DIR@|$(escape_sed "$owner_data")|g" \
    -e "s|@POSTGRES_DSN@|$(escape_sed "$owner_dsn")|g" \
    -e "s|@POSTGRES_SCHEMA@|$(escape_sed "$schema")|g" \
    -e "s|@ARTIFACT_SERVICE_PORT@|$owner_artifact_port|g" \
    -e "s|@ARTIFACT_SERVICE_URL@||g" \
    -e "s|@ARTIFACT_SERVICE_TOKEN@|$(escape_sed "$artifact_token")|g" \
    "$source_dir/tests/acceptance/distributed-m3-cross-machine-worker-config.yaml.in" |
  sed '/^process_workers:/,$d' > "$run_root/owner.yaml"

# The provider is installed for the remote user's login environment.  Resolve
# it through a login shell and pass the absolute path into LASO so the worker
# does not depend on a non-login SSH PATH.
remote_codex_bin=$(remote_has 'bash -lc "command -v codex"')
remote_data="$remote_root/data"
remote_worker_root="$remote_data/distributed-workspaces"
remote_config="$remote_root/worker.yaml"
remote_connection_dsn="$remote_dsn"
if [[ "$scenario" == "db-interruption" ]]; then
  if [[ "$remote_connection_dsn" == *"?"* ]]; then
    remote_connection_dsn="${remote_connection_dsn}&application_name=laso-m3-worker"
  else
    remote_connection_dsn="${remote_connection_dsn}?application_name=laso-m3-worker"
  fi
fi
remote_has "mkdir -p '$remote_worker_root' '$remote_data'"
remote_template="$run_root/worker-template.yaml"
sed -e "s|@DATA_DIR@|$(escape_sed "$remote_data")|g" \
    -e "s|@POSTGRES_DSN@|$(escape_sed "$remote_connection_dsn")|g" \
    -e "s|@POSTGRES_SCHEMA@|$(escape_sed "$schema")|g" \
    -e "s|@ARTIFACT_SERVICE_URL@|http://127.0.0.1:$remote_artifact_port|g" \
    -e "s|@ARTIFACT_SERVICE_TOKEN@|$(escape_sed "$artifact_token")|g" \
    -e "s|@CODEX_WORKER@|$(escape_sed "$remote_build/bin/laso-codex-worker")|g" \
    -e "s|@CODEX_BIN@|$(escape_sed "$remote_codex_bin")|g" \
    -e "s|@WORKSPACE_ROOT@|$(escape_sed "$remote_worker_root")|g" \
    "$source_dir/tests/acceptance/distributed-m3-cross-machine-worker-config.yaml.in" > "$remote_template"
scp -q "${ssh_options[@]}" "$remote_template" "$target:$remote_config"

env PGPASSWORD="$owner_pgpassword" LASO_INSTANCE_STALE_AFTER_MS=5000 "$owner_build/bin/laso-server" --config "$run_root/owner.yaml" --host 127.0.0.1 --port "$owner_port" \
  >"$run_root/owner.stdout" 2>"$run_root/owner.stderr" &
owner_pid=$!
base="http://127.0.0.1:$owner_port/api/v1"
for _ in $(seq 1 300); do
  if curl -fsS "$base/health" >/dev/null 2>&1; then break; fi
  kill -0 "$owner_pid" 2>/dev/null || { echo "owner failed during startup" >&2; exit 1; }
  sleep 0.1
done
curl -fsS "$base/health" >/dev/null || { echo "owner health check failed" >&2; exit 1; }

# The worker's artifact URL is loopback-local on the remote host. Carry it
# over a scoped reverse SSH tunnel so PostgreSQL coordination and object
# transport remain private to the two test instances.
ssh -N "${ssh_options[@]}" -o ExitOnForwardFailure=yes \
  -R "127.0.0.1:${remote_artifact_port}:127.0.0.1:${owner_artifact_port}" "$target" \
  >"$run_root/artifact-tunnel.stdout" 2>"$run_root/artifact-tunnel.stderr" &
artifact_tunnel_pid=$!
for _ in $(seq 1 100); do
  kill -0 "$artifact_tunnel_pid" 2>/dev/null || {
    echo "artifact reverse tunnel failed during startup" >&2
    cat "$run_root/artifact-tunnel.stderr" >&2 || true
    exit 1
  }
  response_code=$(remote_has "curl -sS -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer $artifact_token' -I 'http://127.0.0.1:$remote_artifact_port/api/v1/artifacts/invalid-id'" 2>/dev/null || true)
  [[ "$response_code" == 400 ]] && break
  sleep 0.1
done
[[ "${response_code:-}" == 400 ]] || { echo "artifact reverse tunnel health check failed" >&2; exit 1; }

# The remote DSN is passed to the remote shell only for this disposable test.
# It is never echoed, logged, or copied into the repository.
start_remote_server() {
  remote_server_pid=$(ssh "${ssh_options[@]}" "$target" \
    "nohup env PGPASSWORD=\"${LASO_REMOTE_PGPASSWORD:-}\" LASO_INSTANCE_STALE_AFTER_MS=5000 '$remote_build/bin/laso-server' --config '$remote_config' --host 127.0.0.1 --port '$remote_port' >'$remote_root/worker.stdout' 2>'$remote_root/worker.stderr' </dev/null & echo \$!")
  for _ in $(seq 1 300); do
    if ! remote_has "kill -0 '$remote_server_pid' 2>/dev/null" >/dev/null 2>&1; then
      echo "remote worker process exited during startup" >&2
      exit 1
    fi
    if remote_has "curl -fsS 'http://127.0.0.1:$remote_port/api/v1/health' >/dev/null" >/dev/null 2>&1; then break; fi
    sleep 0.1
  done
  remote_has "kill -0 '$remote_server_pid' 2>/dev/null" >/dev/null || {
    echo "remote worker process is not alive after startup" >&2
    exit 1
  }
  remote_has "curl -fsS 'http://127.0.0.1:$remote_port/api/v1/health' >/dev/null" >/dev/null || {
    echo "remote worker failed during startup" >&2
    exit 1
  }
}

start_remote_server

if [[ "$scenario" == "stale-worker" ]]; then
  remote_b_root=$(remote_has 'mktemp -d /tmp/laso-m3-cross.XXXXXX')
  remote_b_data="$remote_b_root/data"
  remote_b_worker_root="$remote_b_data/distributed-workspaces"
  remote_b_config="$remote_b_root/worker.yaml"
  remote_b_port=$((remote_port + 1))
  remote_has "mkdir -p '$remote_b_worker_root' '$remote_b_data'"
  remote_template_b="$run_root/worker-template-b.yaml"
  sed -e "s|@DATA_DIR@|$(escape_sed "$remote_b_data")|g" \
      -e "s|@POSTGRES_DSN@|$(escape_sed "$remote_dsn")|g" \
      -e "s|@POSTGRES_SCHEMA@|$(escape_sed "$schema")|g" \
      -e "s|@ARTIFACT_SERVICE_URL@|http://127.0.0.1:$remote_artifact_port|g" \
      -e "s|@ARTIFACT_SERVICE_TOKEN@|$(escape_sed "$artifact_token")|g" \
      -e "s|@CODEX_WORKER@|$(escape_sed "$remote_build/bin/laso-codex-worker")|g" \
      -e "s|@CODEX_BIN@|$(escape_sed "$remote_codex_bin")|g" \
      -e "s|@WORKSPACE_ROOT@|$(escape_sed "$remote_b_worker_root")|g" \
      -e "s|api_port: 0|api_port: $remote_b_port|" \
      "$source_dir/tests/acceptance/distributed-m3-cross-machine-worker-config.yaml.in" > "$remote_template_b"
  scp -q "${ssh_options[@]}" "$remote_template_b" "$target:$remote_b_config"
  start_remote_b() {
    remote_b_server_pid=$(ssh "${ssh_options[@]}" "$target" \
      "nohup env PGPASSWORD=\"${LASO_REMOTE_PGPASSWORD:-}\" LASO_INSTANCE_STALE_AFTER_MS=5000 '$remote_build/bin/laso-server' --config '$remote_b_config' --host 127.0.0.1 --port '$remote_b_port' >'$remote_b_root/worker.stdout' 2>'$remote_b_root/worker.stderr' </dev/null & echo \$!")
    for _ in $(seq 1 300); do
      if remote_has "curl -fsS 'http://127.0.0.1:$remote_b_port/api/v1/health' >/dev/null" >/dev/null 2>&1; then return; fi
      sleep 0.1
    done
    echo "stale-worker replacement failed during startup" >&2
    exit 1
  }
fi

pipeline=$(jq -n --rawfile yaml "$pipeline_file" '{yaml: $yaml}')
curl -fsS -X POST "$base/pipelines" -H 'Content-Type: application/json' --data-binary "$pipeline" >/dev/null
fixture_root="$source_dir/tests/acceptance/fixtures/real-agent-project"
manifest=$(jq -n \
  --rawfile cmake "$fixture_root/CMakeLists.txt" \
  --rawfile task "$fixture_root/TASK.md" \
  --rawfile label_h "$fixture_root/include/laso_phase1/label.hpp" \
  --rawfile math_h "$fixture_root/include/laso_phase1/math.hpp" \
  --rawfile label "$fixture_root/src/label.cpp" \
  --rawfile math "$fixture_root/src/math.cpp" \
  --rawfile tests "$fixture_root/tests/fixture_tests.cpp" \
  '{version:1,files:[
    {path:"CMakeLists.txt",size:($cmake|length),sha256:"",data:($cmake|explode)},
    {path:"TASK.md",size:($task|length),sha256:"",data:($task|explode)},
    {path:"include/laso_phase1/label.hpp",size:($label_h|length),sha256:"",data:($label_h|explode)},
    {path:"include/laso_phase1/math.hpp",size:($math_h|length),sha256:"",data:($math_h|explode)},
    {path:"src/label.cpp",size:($label|length),sha256:"",data:($label|explode)},
    {path:"src/math.cpp",size:($math|length),sha256:"",data:($math|explode)},
    {path:"tests/fixture_tests.cpp",size:($tests|length),sha256:"",data:($tests|explode)}
  ]}')

# The public fixture is ASCII.  Compute the manifest hashes with the same
# SHA-256 command used by the owner-side verifier rather than trusting jq.
manifest_file="$run_root/input-manifest.json"
jq --argjson m "$manifest" '.files |= map(.sha256 = "")' <<<"$manifest" > "$manifest_file"
python3 - "$manifest_file" "$fixture_root" <<'PY'
import hashlib, json, pathlib, sys
path = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2])
value = json.loads(path.read_text())
for item in value["files"]:
    data = bytes(item["data"])
    item["sha256"] = hashlib.sha256(data).hexdigest()
path.write_text(json.dumps(value, separators=(",", ":")))
PY
manifest=$(<"$manifest_file")

for iteration in $(seq 1 "$runs"); do
  request=$(jq -n --argjson manifest "$manifest" \
    '{input:{request:"repair the synthetic project"},metadata:{classification:"public",workspace_manifest:$manifest}}')
  run_id=$(curl -fsS -X POST "$base/pipelines/$pipeline_ref/runs" \
    -H 'Content-Type: application/json' --data-binary "$request" | jq -r '.id')
  observed_remote=0
  if [[ "$scenario" == "cancel" ]]; then
    cancellation_sent=0
    for _ in $(seq 1 900); do
      run=$(curl -fsS "$base/runs/$run_id")
      state=$(jq -r '.state' <<<"$run")
      jobs=$(curl -fsS "$base/worker-jobs?limit=100")
      active_job=$(jq -r --arg run "$run_id" '[.[] | select(.run_id == $run and (.status == "Submitting" or .status == "Running" or .status == "Queued"))] | first // empty | .id // empty' <<<"$jobs")
      if [[ "$observed_remote" == 0 ]] && remote_has "pgrep -f '$remote_build/bin/laso-codex-worker' >/dev/null" >/dev/null 2>&1; then
        observed_remote=1
      fi
      if [[ "$observed_remote" == 1 && -n "$active_job" && "$cancellation_sent" == 0 ]]; then
        curl -fsS -X POST "$base/runs/$run_id/cancel" >/dev/null
        cancellation_sent=1
        echo "cross_machine_cancel_requested=1 provider=codex worker_job=$active_job"
      fi
      if [[ "$state" == "Completed" || "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]]; then
        break
      fi
      sleep 0.1
    done
    [[ "$observed_remote" == 1 ]] || { echo "remote provider host was not observed before cancellation" >&2; exit 1; }
    if [[ "$cancellation_sent" != 1 ]]; then
      echo "remote cancellation was not requested; run reached $state" >&2
      jq -c --arg run "$run_id" '[.[] | select(.run_id == $run) | {id,status,error,cancellation_acknowledged,cancellation_error}]' <<<"$jobs" >&2 || true
      exit 1
    fi
    [[ "$state" == "Cancelled" ]] || { echo "cross-machine cancellation reached $state" >&2; exit 1; }
    jobs=$(curl -fsS "$base/worker-jobs?limit=100")
    jq -c --arg run "$run_id" '[.[] | select(.run_id == $run) | {id,worker_id,status,error,cancellation_acknowledged,cancellation_error}]' <<<"$jobs"
    echo "DISTRIBUTED_M3_CROSS_MACHINE_CANCEL_OK"
    continue
  fi
  if [[ "$scenario" == "worker-death" ]]; then
    worker_killed=0
    for _ in $(seq 1 900); do
      run=$(curl -fsS "$base/runs/$run_id")
      state=$(jq -r '.state' <<<"$run")
      jobs=$(curl -fsS "$base/worker-jobs?limit=100")
      active_job=$(jq -r --arg run "$run_id" '[.[] | select(.run_id == $run and (.status == "Submitting" or .status == "Running"))] | first // empty | .id // empty' <<<"$jobs")
      if [[ -n "$active_job" ]]; then
        remote_has "kill -KILL '$remote_server_pid' 2>/dev/null || true" || true
        echo "cross_machine_worker_killed=1 worker_job=$active_job"
        sleep 1
        start_remote_server
        worker_killed=1
        break
      fi
      [[ "$state" == "Completed" || "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]] && break
      sleep 0.1
    done
    [[ "$worker_killed" == 1 ]] || { echo "active remote worker was not observed before death injection" >&2; exit 1; }
  fi
  if [[ "$scenario" == "stale-worker" ]]; then
    stale_injected=0
    for _ in $(seq 1 900); do
      run=$(curl -fsS "$base/runs/$run_id")
      state=$(jq -r '.state' <<<"$run")
      jobs=$(curl -fsS "$base/worker-jobs?limit=100")
      active_job=$(jq -r --arg run "$run_id" '[.[] | select(.run_id == $run and (.status == "Submitting" or .status == "Running"))] | first // empty | .id // empty' <<<"$jobs")
      if [[ -n "$active_job" ]]; then
        remote_has "kill -STOP '$remote_server_pid'"
        echo "cross_machine_stale_worker_suspended=1 worker_job=$active_job"
        sleep 4
        start_remote_b
        stale_injected=1
        break
      fi
      [[ "$state" == "Completed" || "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]] && break
      sleep 0.1
    done
    [[ "$stale_injected" == 1 ]] || { echo "live stale worker was not injected" >&2; exit 1; }
  fi
  if [[ "$scenario" == "owner-death" ]]; then
    owner_killed=0
    for _ in $(seq 1 900); do
      run=$(curl -fsS "$base/runs/$run_id")
      state=$(jq -r '.state' <<<"$run")
      jobs=$(curl -fsS "$base/worker-jobs?limit=100")
      active_job=$(jq -r --arg run "$run_id" '[.[] | select(.run_id == $run and (.status == "Submitting" or .status == "Running"))] | first // empty | .id // empty' <<<"$jobs")
      if [[ -n "$active_job" ]]; then
        kill -KILL "$owner_pid" 2>/dev/null || true
        wait "$owner_pid" 2>/dev/null || true
        owner_pid=
        echo "cross_machine_owner_killed=1 worker_job=$active_job"
        sleep 6
        env PGPASSWORD="$owner_pgpassword" LASO_INSTANCE_STALE_AFTER_MS=5000 "$owner_build/bin/laso-server" --config "$run_root/owner.yaml" \
          --host 127.0.0.1 --port "$owner_port" >"$run_root/owner.stdout" 2>"$run_root/owner.stderr" &
        owner_pid=$!
        for _ in $(seq 1 300); do
          if curl -fsS "$base/health" >/dev/null 2>&1; then break; fi
          sleep 0.1
        done
        curl -fsS "$base/health" >/dev/null || { echo "owner recovery failed" >&2; exit 1; }
        owner_killed=1
        break
      fi
      [[ "$state" == "Completed" || "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]] && break
      sleep 0.1
    done
    [[ "$owner_killed" == 1 ]] || { echo "owner was not killed during active remote work" >&2; exit 1; }
  fi
  if [[ "$scenario" == "db-interruption" ]]; then
    db_interruption_sent=0
    provider_observed=0
    [[ -n "${LASO_REMOTE_SUDO_PASSWORD:-}" ]] || {
      echo "LASO_REMOTE_SUDO_PASSWORD is required for the bounded database interruption scenario" >&2
      exit 77
    }
    for _ in $(seq 1 900); do
      run=$(curl -fsS "$base/runs/$run_id")
      state=$(jq -r '.state' <<<"$run")
      jobs=$(curl -fsS "$base/worker-jobs?limit=100")
      active_job=$(jq -r --arg run "$run_id" '[.[] | select(.run_id == $run and (.status == "Submitting" or .status == "Running") and .worker_id == "codex")] | first // empty | .id // empty' <<<"$jobs")
      if [[ "$provider_observed" == 0 ]] && remote_has "pgrep -f '$remote_codex_bin' >/dev/null" >/dev/null 2>&1; then
        provider_observed=1
      fi
      if [[ "$provider_observed" == 1 && -n "$active_job" ]]; then
        printf '%s\n' "$LASO_REMOTE_SUDO_PASSWORD" |
          ssh "${ssh_options[@]}" "$target" \
            "sudo -S -p '' -u postgres sh -c 'for i in \$(seq 1 4); do psql -d laso_m3_validation -Atqc \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = chr(108)||chr(97)||chr(115)||chr(111)||chr(45)||chr(109)||chr(51)||chr(45)||chr(119)||chr(111)||chr(114)||chr(107)||chr(101)||chr(114) AND datname = current_database() AND pid <> pg_backend_pid();\" >/dev/null 2>&1; sleep 0.1; done'" \
            >/dev/null 2>&1 &
        remote_db_interrupt_pid=$!
        echo "cross_machine_db_interruption=1 worker_job=$active_job"
        db_interruption_sent=1
        break
      fi
      [[ "$state" == "Completed" || "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]] && break
      sleep 0.1
    done
    [[ "$provider_observed" == 1 ]] || { echo "remote provider was not observed before database interruption" >&2; exit 1; }
    [[ "$db_interruption_sent" == 1 ]] || { echo "active remote worker was not observed before database interruption" >&2; exit 1; }
    wait "$remote_db_interrupt_pid" 2>/dev/null || true
    remote_db_interrupt_pid=
  fi
  for _ in $(seq 1 1800); do
    run=$(curl -fsS "$base/runs/$run_id")
    state=$(jq -r '.state' <<<"$run")
    if [[ "$state" == "Completed" || "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]]; then break; fi
    if [[ "$observed_remote" == 0 ]] && remote_has "pgrep -f '$remote_build/bin/laso-codex-worker' >/dev/null" >/dev/null 2>&1; then
      observed_remote=1
    fi
    sleep 0.1
  done
  if [[ "$scenario" == "stale-worker" ]]; then
    remote_has "kill -CONT '$remote_server_pid' 2>/dev/null || true" || true
    sleep 2
    attempts=$(curl -fsS "$base/runs/$run_id/attempts?limit=100")
    codex_attempts=$(jq -r '[.[] | select(.node_id == "codex")] | length' <<<"$attempts")
    [[ "$codex_attempts" -ge 2 ]] || { echo "replacement worker did not create a distinct attempt" >&2; exit 1; }
    echo "STALE_RESULT_REJECTED codex_attempts=$codex_attempts"
  fi
  [[ "$state" == "Completed" ]] || {
    echo "cross-machine run $iteration reached $state" >&2
    jq -c '{id,state,error,active_node,cancellation_requested}' <<<"$run" >&2 || true
    curl -fsS "$base/runs/$run_id/attempts?limit=100" |
      jq -c '[.[] | {id,node_id,state,error,attempt,worker_job_id,finished_at}]' >&2 || true
    curl -fsS "$base/worker-jobs?limit=100" |
      jq -c --arg run "$run_id" '[.[] | select(.run_id == $run) | {id,worker_id,status,error,attempt_id,finished_at,result_metadata}]' >&2 || true
    exit 1
  }
  [[ "$observed_remote" == 1 ]] || { echo "remote provider host was not observed" >&2; exit 1; }
  manifest_count=$(jq -r '.message.metadata.workspace_result_manifests // [] | length' <<<"$run")
  [[ "$manifest_count" == "1" ]] || { echo "owner did not receive exactly one remote artifact manifest" >&2; exit 1; }
  jq -c '.message.metadata.workspace_result_manifests[0]' <<<"$run" > "$run_root/result-manifest.json"
  jq -e '.work_id != "" and .attempt_id != "" and (.manifest.files | length) > 0' \
    "$run_root/result-manifest.json" >/dev/null
  python3 - "$run_root/result-manifest.json" "$run_root/accepted-$iteration" \
    "http://127.0.0.1:$owner_artifact_port" "$artifact_token" <<'PY'
import hashlib, json, pathlib, sys, urllib.request
value = json.loads(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.Path(sys.argv[2])
artifact_base = sys.argv[3].rstrip("/")
artifact_token = sys.argv[4]
root.mkdir(parents=True, exist_ok=True)
provenance = value["manifest"].get("provenance", {})
if not all(provenance.get(key) for key in ("run_id", "node_work_id", "attempt_id", "worker_id")):
    raise SystemExit("owner artifact provenance is incomplete")
if not isinstance(provenance.get("fencing_token"), int) or provenance["fencing_token"] <= 0:
    raise SystemExit("owner artifact provenance fence is invalid")
seen = set()
for item in value["manifest"]["files"]:
    rel = pathlib.PurePosixPath(item["path"])
    if rel.is_absolute() or ".." in rel.parts or "\\" in item["path"] or str(rel) in seen:
        raise SystemExit("invalid owner artifact path")
    seen.add(str(rel))
    destination = root.joinpath(*rel.parts)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if "data" in item:
        data = bytes(item["data"])
        if len(data) != item["size"] or hashlib.sha256(data).hexdigest() != item["sha256"]:
            raise SystemExit("owner artifact integrity mismatch")
        destination.write_bytes(data)
        continue
    object_id = item.get("object_id", "")
    if not object_id.startswith("sha256:"):
        raise SystemExit("owner artifact missing content-addressed object")
    request = urllib.request.Request(
        f"{artifact_base}/api/v1/artifacts/{object_id}",
        headers={"Authorization": f"Bearer {artifact_token}"},
    )
    digest = hashlib.sha256()
    size = 0
    with urllib.request.urlopen(request, timeout=30) as response, destination.open("wb") as output:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
            output.write(chunk)
    if size != item["size"] or digest.hexdigest() != item["sha256"]:
        raise SystemExit("owner artifact integrity mismatch")
PY
  cmake -S "$run_root/accepted-$iteration" -B "$run_root/accepted-$iteration/build" -G Ninja -DCMAKE_BUILD_TYPE=Debug >/dev/null
  cmake --build "$run_root/accepted-$iteration/build" >/dev/null
  ctest --test-dir "$run_root/accepted-$iteration/build" --output-on-failure >/dev/null
  echo "cross_machine_run=$iteration provider=codex state=Completed remote_artifact=verified owner_build_tests=pass"
done
echo "DISTRIBUTED_M3_CROSS_MACHINE_CODEX_OK"
