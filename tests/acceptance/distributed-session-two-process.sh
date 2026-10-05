#!/usr/bin/env bash
set -euo pipefail

build=$(realpath "$1")
source_dir=$(realpath "$2")
postgres_dsn=${LASO_TEST_POSTGRES_DSN:-${LASO_POSTGRES_DSN:-}}
[[ -n "$postgres_dsn" ]] || { echo "PostgreSQL test DSN is required" >&2; exit 1; }
for command in curl jq psql; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done

temp=$(mktemp -d "${TMPDIR:-/tmp}/laso-session-two-process.XXXXXX")
schema="laso_session_acceptance_${$}"
pid_a=
pid_b=
port_a=
port_b=
base_a=
base_b=

stop_process() {
  local pid=${1:-}
  [[ -n "$pid" ]] || return 0
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 1 50); do
    kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null || true; return 0; }
    sleep 0.1
  done
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  local result=$?
  if (( result != 0 )); then
    for log in "$temp"/server-*.log; do
      [[ -f "$log" ]] || continue
      echo "--- ${log##*/} ---" >&2
      cat "$log" >&2
    done
  fi
  stop_process "$pid_a"
  stop_process "$pid_b"
  if ! psql "$postgres_dsn" -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS \"$schema\" CASCADE" >/dev/null 2>&1; then
    echo "could not clean up isolated PostgreSQL schema $schema" >&2
    result=1
  fi
  rm -rf -- "$temp"
  return "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

port() {
  local dynamic_start=32768 dynamic_end=60999
  if [[ -r /proc/sys/net/ipv4/ip_local_port_range ]]; then
    read -r dynamic_start dynamic_end < /proc/sys/net/ipv4/ip_local_port_range
  fi

  # PostgreSQL client connections use kernel-assigned ephemeral source ports.
  # Do not offer those ports to the HTTP listeners: a live or recently closed
  # DB connection can make bind(2) fail even though a TCP connect probe says
  # that no listener owns the candidate.
  local min_port=20000 max_port=65000
  local lower_end=$((dynamic_start - 1))
  (( lower_end > max_port )) && lower_end=$max_port
  local lower_count=$((lower_end >= min_port ? lower_end - min_port + 1 : 0))
  local upper_start=$((dynamic_end + 1))
  (( upper_start < min_port )) && upper_start=$min_port
  local upper_count=$((upper_start <= max_port ? max_port - upper_start + 1 : 0))
  local count=$((lower_count + upper_count))
  (( count > 0 )) || { echo "no non-ephemeral HTTP listener ports available" >&2; return 1; }

  local start=$((RANDOM % count)) candidate offset
  for offset in $(seq 0 $((count - 1))); do
    local index=$(((start + offset) % count))
    if (( index < lower_count )); then
      candidate=$((min_port + index))
    else
      candidate=$((upper_start + index - lower_count))
    fi
    if ! (exec 3<>"/dev/tcp/127.0.0.1/$candidate") 2>/dev/null; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  echo "could not find an unused loopback port" >&2
  return 1
}
port_a=$(port)
port_b=$(port)
while [[ "$port_a" == "$port_b" ]]; do port_b=$(port); done
postgres_dsn_yaml=$(jq -Rn --arg dsn "$postgres_dsn" '$dsn')

write_config() {
  local path=$1 state=$2 listen=$3
  cat > "$path" <<EOF
postgres_dsn: $postgres_dsn_yaml
postgres_schema: $schema
data_dir: "$state"
artifact_root: "$state/artifacts"
coordination:
  mode: experimental_multi_instance
  lease_ttl_ms: 2000
  heartbeat_interval_ms: 500
api_host: 127.0.0.1
api_port: $listen
execution_mode: multi_instance
workers: 2
EOF
}
write_config "$temp/instance-a.yaml" "$temp/state-a" "$port_a"
write_config "$temp/instance-b.yaml" "$temp/state-b" "$port_b"

"$build/bin/laso" --config "$temp/instance-a.yaml" pipeline register "$source_dir/examples/human-approval/pipeline.yaml" > "$temp/register.json"

wait_ready() {
  local base=$1 pid=$2 log=$3
  for _ in $(seq 1 150); do
    if curl -fsS "$base/health" > /dev/null 2>&1; then return 0; fi
    kill -0 "$pid" 2>/dev/null || { cat "$log" >&2; return 1; }
    sleep 0.1
  done
  cat "$log" >&2
  echo "server readiness timed out" >&2
  return 1
}
start_a() {
  "$build/bin/laso-server" --config "$temp/instance-a.yaml" >> "$temp/server-a.log" 2>&1 &
  pid_a=$!
  base_a="http://127.0.0.1:$port_a/api/v1"
  wait_ready "$base_a" "$pid_a" "$temp/server-a.log"
}
start_b() {
  "$build/bin/laso-server" --config "$temp/instance-b.yaml" >> "$temp/server-b.log" 2>&1 &
  pid_b=$!
  base_b="http://127.0.0.1:$port_b/api/v1"
  wait_ready "$base_b" "$pid_b" "$temp/server-b.log"
}
post_json() {
  local url=$1 body=$2 output=$3 status
  status=$(curl -sS -H 'Content-Type: application/json' -X POST "$url" --data "$body" -o "$output" -w '%{http_code}')
  if [[ ! "$status" =~ ^2 ]]; then
    echo "POST $url returned HTTP $status: $(cat "$output")" >&2
    return 1
  fi
}
wait_json() {
  local url=$1 jq_filter=$2 output=$3
  for _ in $(seq 1 150); do
    if curl -fsS "$url" > "$output" 2>/dev/null && jq -e "$jq_filter" "$output" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  curl -fsS "$url" > "$output" || true
  cat "$output" >&2
  echo "condition timed out for $url" >&2
  return 1
}

start_a
start_b
[[ "$pid_a" != "$pid_b" && "$port_a" != "$port_b" ]] || {
  echo "server processes or listener ports are not independent" >&2; exit 1;
}
curl -fsS "$base_a/health" > "$temp/health-a.json"
curl -fsS "$base_b/health" > "$temp/health-b.json"

post_json "$base_a/sessions" '{"pipeline_id":"human-approval@1"}' "$temp/session.json"
session_id=$(jq -er '.id' "$temp/session.json")
curl -fsS "$base_b/sessions/$session_id" > "$temp/shared-session.json"
jq -e --arg id "$session_id" '.id == $id and .pipeline_id == "human-approval@1"' "$temp/shared-session.json" >/dev/null

# Turn 1 reaches a durable approval wait. Retry its idempotency key and submit
# turn 2 through the other process while turn 1 remains active.
post_json "$base_a/sessions/$session_id/turns" '{"idempotency_key":"two-process-turn-1","input":{"value":"first"}}' "$temp/turn-1.json"
turn_1=$(jq -er '.id' "$temp/turn-1.json")
post_json "$base_a/sessions/$session_id/turns" '{"idempotency_key":"two-process-turn-1","input":{"value":"first"}}' "$temp/turn-1-retry.json"
jq -e --arg id "$turn_1" '.id == $id' "$temp/turn-1-retry.json" >/dev/null
post_json "$base_b/sessions/$session_id/turns" '{"idempotency_key":"two-process-turn-2","input":{"value":"second"}}' "$temp/turn-2.json"
turn_2=$(jq -er '.id' "$temp/turn-2.json")
jq -e --arg first "$turn_1" '.sequence == 1 and .id == $first' "$temp/turn-1.json" >/dev/null
jq -e --arg second "$turn_2" '.sequence == 2 and .id == $second' "$temp/turn-2.json" >/dev/null

approval_id=
for _ in $(seq 1 150); do
  curl -fsS "$base_b/approvals" > "$temp/approvals.json"
  approval_id=$(jq -r '[.[] | select(.decision == "pending")][0].id // empty' "$temp/approvals.json")
  [[ -n "$approval_id" ]] && break
  sleep 0.1
done
[[ -n "$approval_id" ]] || { echo "session did not reach pending approval" >&2; exit 1; }

# Capture a durable cursor and disconnect. Later events are generated while
# this client remains disconnected.
sse_url="$base_a/sessions/$session_id/events/stream"
curl -sS -N --max-time 1 "$sse_url" > "$temp/initial.sse" 2>/dev/null || [[ $? == 28 ]]
last_event=$(sed -n 's/^id: //p' "$temp/initial.sse" | tail -n 1)
[[ "$last_event" =~ ^[0-9]+$ ]] || { echo "initial SSE stream returned no durable event ID" >&2; exit 1; }

# Force instance A to exit while the accepted session still has a pending turn.
# Instance B must continue the durable session without A's local state.
kill -KILL "$pid_a"
wait "$pid_a" 2>/dev/null || true
pid_a=
curl -fsS "$base_b/sessions/$session_id" > "$temp/after-kill-session.json"
jq -e --arg id "$session_id" '.id == $id' "$temp/after-kill-session.json" >/dev/null
post_json "$base_b/approvals/$approval_id/approve" '{"comment":"acceptance-test"}' "$temp/approval-1.json"

# The queued second turn runs only after the first approval completes.
approval_id=
for _ in $(seq 1 150); do
  curl -fsS "$base_b/approvals" > "$temp/approvals.json"
  approval_id=$(jq -r '[.[] | select(.decision == "pending")][0].id // empty' "$temp/approvals.json")
  [[ -n "$approval_id" ]] && break
  sleep 0.1
done
[[ -n "$approval_id" ]] || { echo "second queued turn did not reach approval" >&2; exit 1; }
post_json "$base_b/approvals/$approval_id/approve" '{"comment":"acceptance-test"}' "$temp/approval-2.json"
wait_json "$base_b/sessions/$session_id/turns" 'length == 2 and all(.[]; .state == "succeeded")' "$temp/first-two-completed.json"

# Isolate execution ownership to A for one accepted turn, then bring B back and
# forcibly terminate A while that turn is waiting at the approval boundary.
start_a
stop_process "$pid_b"
pid_b=
post_json "$base_a/sessions/$session_id/context/generations" \
  '{"expected_generation":0,"through_turn_sequence":2,"idempotency_key":"two-process-context-1","representation_kind":"summary","representation_version":"1","payload":{"summary":"first two turns"}}' \
  "$temp/context-generation.json"
context_generation_id=$(jq -er '.id' "$temp/context-generation.json")
curl -fsS "$base_a/sessions/$session_id/context" > "$temp/current-context.json"
jq -e --arg id "$context_generation_id" '.current_generation.id == $id and .current_generation.generation == 1' "$temp/current-context.json" >/dev/null
post_json "$base_a/sessions/$session_id/turns" '{"idempotency_key":"two-process-turn-3","input":{"value":"third"}}' "$temp/turn-3.json"
turn_3=$(jq -er '.id' "$temp/turn-3.json")
jq -e --arg id "$turn_3" '.sequence == 3 and .id == $id' "$temp/turn-3.json" >/dev/null
approval_id=
for _ in $(seq 1 150); do
  curl -fsS "$base_a/approvals" > "$temp/approvals.json"
  approval_id=$(jq -r '[.[] | select(.decision == "pending")][0].id // empty' "$temp/approvals.json")
  [[ -n "$approval_id" ]] && break
  sleep 0.1
done
[[ -n "$approval_id" ]] || { echo "third turn did not reach its approval boundary" >&2; exit 1; }
start_b
kill -KILL "$pid_a"
wait "$pid_a" 2>/dev/null || true
pid_a=
post_json "$base_b/approvals/$approval_id/approve" '{"comment":"acceptance-test"}' "$temp/approval-3.json"

wait_json "$base_b/sessions/$session_id/turns" 'length == 3 and all(.[]; .state == "succeeded")' "$temp/completed-turns.json"
curl -fsS "$base_b/approvals" > "$temp/approvals-final.json"
jq -e 'length == 3 and all(.[]; .decision == "approved")' "$temp/approvals-final.json" >/dev/null
wait_json "$base_b/sessions/$session_id/events?after=$last_event&limit=100" 'length > 0' "$temp/replayed-events.json"

# Reconnect through B using Last-Event-ID and compare every SSE id to the
# independently fetched durable event sequence.
curl -sS -N --max-time 1 -H "Last-Event-ID: $last_event" \
  "$base_b/sessions/$session_id/events/stream" > "$temp/replay.sse" 2>/dev/null || [[ $? == 28 ]]
mapfile -t replay_ids < <(sed -n 's/^id: //p' "$temp/replay.sse")
mapfile -t durable_ids < <(jq -r '.[].sequence' "$temp/replayed-events.json")
[[ ${#replay_ids[@]} -eq ${#durable_ids[@]} && ${#replay_ids[@]} -gt 0 ]] || {
  echo "SSE replay count does not match persisted events" >&2; exit 1;
}
for i in "${!durable_ids[@]}"; do
  [[ "${replay_ids[$i]}" == "${durable_ids[$i]}" ]] || {
    echo "SSE replay sequence differs from persisted event order" >&2; exit 1;
  }
done

# Restart the terminated process and verify the durable records are readable.
start_a
[[ "$pid_a" != "$pid_b" ]] || { echo "server processes unexpectedly share a PID" >&2; exit 1; }
curl -fsS "$base_a/sessions/$session_id/turns" > "$temp/restarted-turns.json"
jq -e --arg first "$turn_1" --arg second "$turn_2" --arg third "$turn_3" \
  'length == 3 and .[0].id == $first and .[1].id == $second and .[2].id == $third and all(.[]; .state == "succeeded") and ([.[].run_id] | unique | length) == 3' \
  "$temp/restarted-turns.json" >/dev/null
run_id=$(jq -er --arg turn "$turn_3" '.[] | select(.id == $turn) | .run_id' "$temp/restarted-turns.json")
curl -fsS "$base_a/runs/$run_id/context" > "$temp/run-context.json"
jq -e --arg id "$context_generation_id" \
  '.context_generation == 1 and .context_generation_id == $id and .context_through_turn_sequence == 2' \
  "$temp/run-context.json" >/dev/null
curl -fsS "$base_a/sessions/$session_id/events?after=0&limit=100" > "$temp/final-events.json"
jq -e 'length > 0 and ([.[].sequence] as $s | ($s | sort) == $s and ($s | unique | length) == ($s | length))' "$temp/final-events.json" >/dev/null

echo "PASS: two independent LASO processes, shared PostgreSQL, ordered/idempotent turns, SSE resume, owner-process failure recovery through peer, and restart durability"
