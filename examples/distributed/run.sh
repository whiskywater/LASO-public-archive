#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 BUILD_DIR SOURCE_DIR" >&2
  exit 2
fi

build_dir=$(realpath "$1")
source_dir=$(realpath "$2")
dsn=${LASO_TEST_POSTGRES_DSN:-}
[[ -n "$dsn" ]] || { echo "LASO_TEST_POSTGRES_DSN is required" >&2; exit 77; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 77; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 77; }
for executable in "$build_dir/bin/laso" "$build_dir/bin/laso-server" \
                 "$build_dir/bin/laso-example-worker-host"; do
  [[ -x "$executable" ]] || { echo "missing executable: $executable" >&2; exit 77; }
done

root=$(mktemp -d)
owner_pid= worker_pid=
schema="laso_example_$(printf '%s' "$RANDOM$RANDOM" | tr -cd '[:alnum:]')"
owner_port=$((30000 + BASHPID % 1000))
worker_port=$((31000 + BASHPID % 1000))
artifact_port=$((32000 + BASHPID % 1000))
artifact_token="synthetic-example-artifact-token"
cleanup() {
  for pid in "${worker_pid:-}" "${owner_pid:-}"; do
    if [[ -n "$pid" ]]; then
      kill -TERM "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -rf -- "$root"
}
trap cleanup EXIT

escape_sed() { printf '%s' "$1" | sed 's/[&|]/\\&/g'; }
render() {
  local template=$1 output=$2
  sed -e "s|@OWNER_DATA@|$(escape_sed "$root/owner")|g" \
      -e "s|@WORKER_DATA@|$(escape_sed "$root/worker")|g" \
      -e "s|@POSTGRES_DSN@|$(escape_sed "$dsn")|g" \
      -e "s|@POSTGRES_SCHEMA@|$(escape_sed "$schema")|g" \
      -e "s|@OWNER_PORT@|$owner_port|g" \
      -e "s|@WORKER_PORT@|$worker_port|g" \
      -e "s|@ARTIFACT_SERVICE_PORT@|$artifact_port|g" \
      -e "s|@ARTIFACT_SERVICE_URL@|http://127.0.0.1:$artifact_port|g" \
      -e "s|@ARTIFACT_SERVICE_TOKEN@|$(escape_sed "$artifact_token")|g" \
      -e "s|@WORKER_HOST@|$(escape_sed "$build_dir/bin/laso-example-worker-host")|g" \
      "$template" > "$output"
}
mkdir -p "$root/owner" "$root/worker"
render "$source_dir/examples/distributed/owner.yaml.in" "$root/owner.yaml"
render "$source_dir/examples/distributed/worker.yaml.in" "$root/worker.yaml"

start() {
  local config=$1 port=$2 name=$3
  local pid_var="${name}_pid"
  "$build_dir/bin/laso-server" --config "$config" --host 127.0.0.1 --port "$port" \
    >"$root/$name.stdout" 2>"$root/$name.stderr" &
  printf -v "$pid_var" '%s' "$!"
  for _ in $(seq 1 200); do
    if curl -fsS "http://127.0.0.1:$port/api/v1/health" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  echo "$name did not become healthy" >&2
  return 1
}
start "$root/owner.yaml" "$owner_port" owner
start "$root/worker.yaml" "$worker_port" worker
owner_api="http://127.0.0.1:$owner_port/api/v1"

pipeline=$(jq -n --rawfile yaml "$source_dir/examples/distributed/pipeline.yaml" '{yaml:$yaml}')
curl -fsS -X POST "$owner_api/pipelines" -H 'Content-Type: application/json' \
  --data-binary "$pipeline" >/dev/null

input_file="$source_dir/examples/distributed/pipeline.yaml"
digest=$(sha256sum "$input_file" | awk '{print $1}')
size=$(wc -c < "$input_file" | tr -d ' ')
input_bytes=$(od -An -v -tu1 "$input_file" | tr -s '[:space:]' ',' | sed 's/^,//;s/,$//')
manifest=$(jq -n --arg digest "$digest" --argjson size "$size" \
  --argjson data "[$input_bytes]" \
  '{version:1, files:[{path:"pipeline.yaml", sha256:$digest, size:$size, data:$data}]}')
request=$(jq -n --argjson manifest "$manifest" \
  '{input:{request:"reference"}, metadata:{classification:"public", workspace_manifest:$manifest}}')
run_id=$(curl -fsS -X POST "$owner_api/pipelines/distributed-reference@1/runs" \
  -H 'Content-Type: application/json' --data-binary "$request" | jq -r '.id')

for _ in $(seq 1 300); do
  run=$(curl -fsS "$owner_api/runs/$run_id")
  state=$(jq -r '.state' <<<"$run")
  if [[ "$state" == "Completed" ]]; then
    object_id=$(jq -r '.message.metadata.workspace_result_manifests[0].manifest.files[] | select(.path == "result/artifact.bin") | .object_id' <<<"$run")
    result_sha=$(jq -r '.message.metadata.workspace_result_manifests[0].manifest.files[] | select(.path == "result/artifact.bin") | .sha256' <<<"$run")
    result_size=$(jq -r '.message.metadata.workspace_result_manifests[0].manifest.files[] | select(.path == "result/artifact.bin") | .size' <<<"$run")
    [[ -n "$object_id" && "$object_id" != "null" ]] || { echo "owner did not receive an artifact object" >&2; exit 1; }
    [[ "$result_size" == "2097152" ]] || { echo "unexpected artifact size: $result_size" >&2; exit 1; }
    expected="$root/expected-artifact.bin"
    head -c 2097152 /dev/zero | tr '\0' 'A' > "$expected"
    expected_sha=$(sha256sum "$expected" | awk '{print $1}')
    [[ "$result_sha" == "$expected_sha" ]] || { echo "artifact manifest hash mismatch" >&2; exit 1; }
    curl -fsS -H "Authorization: Bearer $artifact_token" \
      "http://127.0.0.1:$artifact_port/api/v1/artifacts/$object_id" \
      -o "$root/downloaded-artifact.bin"
    cmp "$expected" "$root/downloaded-artifact.bin"
    downloaded_sha=$(sha256sum "$root/downloaded-artifact.bin" | awk '{print $1}')
    [[ "$downloaded_sha" == "$result_sha" ]] || { echo "downloaded artifact hash mismatch" >&2; exit 1; }
    echo "distributed_reference_run=$run_id"
    echo "state=$state"
    echo "remote_artifact=owner_verified"
    jq -c '{id, state, active_node, worker, worker_job_id}' <<<"$run"
    exit 0
  fi
  if [[ "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]]; then
    echo "distributed reference run reached $state" >&2
    jq -c '{id, state, active_node, error, worker, worker_job_id}' <<<"$run" >&2
    exit 1
  fi
  sleep 0.1
done

echo "distributed reference run did not reach a terminal state" >&2
exit 1
