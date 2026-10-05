#!/usr/bin/env bash
set -euo pipefail
build=$(realpath "$1")
source_dir=$(realpath "$2")
jq_bin=${JQ_BIN:-$(command -v jq || true)}
if [[ -z "$jq_bin" && -x "$source_dir/local-deps/root/usr/bin/jq" ]]; then
  jq_bin="$source_dir/local-deps/root/usr/bin/jq"
fi
[[ -n "$jq_bin" && -x "$jq_bin" ]] || { echo "jq is required" >&2; exit 77; }
jq() { "$jq_bin" "$@"; }
temp=$(mktemp -d)
server_pid=
cleanup() {
  if [[ -n "$server_pid" ]]; then kill -TERM "$server_pid" 2>/dev/null || true; wait "$server_pid" || true; fi
  rm -rf -- "$temp"
}
trap cleanup EXIT
export LASO_DATA_DIR="$temp/state"
unset LASO_CONFIG LASO_DB_PATH LASO_PLUGIN_DIR || true
"$build/bin/laso" pipeline validate "$source_dir/examples/hello-pipeline/pipeline.yaml"
"$build/bin/laso" run start "$source_dir/examples/hello-pipeline/pipeline.yaml" > "$temp/hello.json"
jq -e '.state == "Completed"' "$temp/hello.json"
"$build/bin/laso" run start "$source_dir/examples/agent-review/pipeline.yaml" > "$temp/agent.json"
jq -e '.state == "Completed"' "$temp/agent.json"
"$build/bin/laso" run start "$source_dir/examples/human-approval/pipeline.yaml" > "$temp/wait.json"
jq -e '.state == "WaitingApproval"' "$temp/wait.json"
run_id=$(jq -r '.id' "$temp/wait.json")
# These are distinct processes: pending approval must survive all process exits.
"$build/bin/laso" run show "$run_id" > "$temp/restarted.json"
jq -e '.state == "WaitingApproval"' "$temp/restarted.json"
"$build/bin/laso" approval list > "$temp/approvals.json"
approval_id=$(jq -r --arg run "$run_id" '.[] | select(.run_id == $run and .decision == "pending") | .id' "$temp/approvals.json")
"$build/bin/laso" approval approve "$approval_id" --actor smoke-test > "$temp/approved.json"
jq -e '.state == "Completed" and .node_visits.prepare == 1' "$temp/approved.json"
LASO_PLUGIN_DIR="$build/plugins" "$build/bin/laso" run start "$source_dir/examples/native-plugin/pipeline.yaml" > "$temp/plugin.json"
jq -e '.state == "Completed"' "$temp/plugin.json"
# Daemon restart while waiting, followed by API approval.
"$build/bin/laso" pipeline register "$source_dir/examples/human-approval/pipeline.yaml" > /dev/null
port=$((20000 + ($$ % 20000)))
start_server() {
  "$build/bin/laso-server" --host 127.0.0.1 --port "$port" > "$temp/server.log" 2>&1 &
  server_pid=$!
  for _ in $(seq 1 100); do
    if curl -fsS "http://127.0.0.1:$port/api/v1/health" > "$temp/health.json"; then return; fi
    kill -0 "$server_pid"
    sleep 0.1
  done
  cat "$temp/server.log" >&2
  return 1
}
base="http://127.0.0.1:$port/api/v1"
start_server
curl -fsS "$base/version" | jq -e '.version == "0.1.0-rc.1"'
cat > "$temp/occupied-port.yaml" <<EOF
data_dir: "$temp/occupied-port-state"
db_path: "$temp/occupied-port-state/laso.db"
api_host: 127.0.0.1
api_port: $port
EOF
if env -u LASO_DATA_DIR "$build/bin/laso-server" --config "$temp/occupied-port.yaml" \
  >"$temp/occupied-port.log" 2>&1; then
  echo "second server unexpectedly bound an occupied listener" >&2
  exit 1
fi
grep -Fq "Address already in use" "$temp/occupied-port.log" || {
  echo "occupied-listener startup failure omitted its safe diagnostic" >&2
  exit 1
}
printf 'not a directory\n' > "$temp/not-a-directory"
cat > "$temp/invalid-state-path.yaml" <<EOF
data_dir: "$temp/not-a-directory"
db_path: "$temp/not-a-directory/laso.db"
api_host: 127.0.0.1
api_port: $((port + 1))
EOF
if env -u LASO_DATA_DIR "$build/bin/laso-server" --config "$temp/invalid-state-path.yaml" \
  >"$temp/invalid-state-path.log" 2>&1; then
  echo "server unexpectedly initialized under a non-directory state path" >&2
  exit 1
fi
grep -Fq "LASO server initialization failed:" "$temp/invalid-state-path.log" || {
  echo "filesystem startup failure omitted its safe diagnostic" >&2
  exit 1
}
if grep -Fq "$temp" "$temp/invalid-state-path.log"; then
  echo "filesystem startup diagnostic exposed its local path" >&2
  exit 1
fi
curl -fsS -X POST "$base/pipelines/human-approval/runs" -H 'Content-Type: application/json' -d '{}' > "$temp/api-run.json"
run_id=$(jq -r '.id' "$temp/api-run.json")
for _ in $(seq 1 100); do
  state=$(curl -fsS "$base/runs/$run_id" | jq -r '.state')
  [[ "$state" == WaitingApproval ]] && break
  sleep 0.05
done
[[ "$state" == WaitingApproval ]]
kill -TERM "$server_pid"
wait "$server_pid"
server_pid=
start_server
curl -fsS "$base/runs/$run_id" | jq -e '.state == "WaitingApproval"'
approval_id=$(curl -fsS "$base/approvals" | jq -r --arg run "$run_id" '.[] | select(.run_id == $run and .decision == "pending") | .id')
curl -fsS -X POST "$base/approvals/$approval_id/approve" -H 'Content-Type: application/json' -d '{"comment":"Reviewed"}' > /dev/null
for _ in $(seq 1 100); do
  state=$(curl -fsS "$base/runs/$run_id" | jq -r '.state')
  [[ "$state" == Completed ]] && break
  sleep 0.05
done
[[ "$state" == Completed ]]
kill -TERM "$server_pid"
wait "$server_pid"
server_pid=
