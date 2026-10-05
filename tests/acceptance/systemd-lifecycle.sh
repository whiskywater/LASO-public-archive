#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  echo "usage: $0 --preflight UNIT_FILE | --user INSTALL_PREFIX SOURCE_DIR | --user-postgres INSTALL_PREFIX SOURCE_DIR" >&2
  exit 2
}

[[ $# -ge 1 ]] || usage
mode=$1
shift

systemd_available() {
  [[ "$(cat /proc/1/comm 2>/dev/null || true)" == systemd ]] || return 1
  command -v systemctl >/dev/null && command -v systemd-run >/dev/null &&
    command -v systemd-analyze >/dev/null && command -v journalctl >/dev/null
}

user_manager_available() {
  systemd-run --user --wait --pipe --collect \
    --unit="laso-preflight-$$" /usr/bin/true >/dev/null 2>&1
}

if [[ "$mode" == --preflight ]]; then
  [[ $# -eq 1 ]] || usage
  if ! systemd_available; then
    echo "BLOCKED: systemd manager/tools unavailable" >&2
    exit 77
  fi
  systemd-analyze verify "$1"
  user_manager_available || { echo "BLOCKED: systemd user manager unavailable" >&2; exit 77; }
  echo "systemd preflight passed (unit syntax and user manager invocation)"
  exit 0
fi

if [[ ("$mode" != --user && "$mode" != --user-postgres) || $# -ne 2 ]]; then
  usage
fi
prefix=$(realpath "$1")
source_dir=$(realpath "$2")
backend=sqlite
postgres_dsn=
postgres_schema=
if [[ "$mode" == --user-postgres ]]; then
  backend=postgres
  postgres_dsn=${LASO_SYSTEMD_ACCEPTANCE_POSTGRES_DSN:-}
  [[ -n "$postgres_dsn" ]] || {
    echo "BLOCKED: LASO_SYSTEMD_ACCEPTANCE_POSTGRES_DSN is required" >&2
    exit 77
  }
  command -v psql >/dev/null || { echo "BLOCKED: psql unavailable" >&2; exit 77; }
  postgres_schema="laso_systemd_accept_$$"
fi
server="$prefix/bin/laso-server"
worker_host="$prefix/bin/laso-example-worker-host"
[[ -x "$prefix/bin/laso" && -x "$server" && -x "$worker_host" ]] || {
  echo "BLOCKED: install prefix must contain laso-server and reference worker" >&2
  exit 77
}
[[ -r "$prefix/share/laso/laso.example.yaml" &&
   -r "$prefix/share/laso/laso.systemd.example.yaml" ]] || {
  echo "BLOCKED: install prefix is missing the shipped example configurations" >&2
  exit 77
}
systemd_available || { echo "BLOCKED: native systemd unavailable" >&2; exit 77; }
command -v curl >/dev/null || { echo "BLOCKED: curl unavailable" >&2; exit 77; }
command -v jq >/dev/null || { echo "BLOCKED: jq unavailable" >&2; exit 77; }
user_manager_available || { echo "BLOCKED: systemd user manager unavailable" >&2; exit 77; }

root=$(mktemp -d "${TMPDIR:-/tmp}/laso-systemd-acceptance.XXXXXX")
chmod 0700 "$root"
unit="laso-acceptance-$$.service"
bad_unit="laso-config-failure-$$.service"
sigint_unit="laso-sigint-$$.service"
bad_units=()
port=$((40000 + ($$ % 20000)))
base="http://127.0.0.1:$port/api/v1"
server_pid=
child_pid=

if curl -fsS --max-time 1 "$base/health" >/dev/null 2>&1; then
  echo "BLOCKED: selected acceptance port is already serving a health endpoint" >&2
  exit 77
fi

cleanup() {
  local status=$?
  systemctl --user stop "$unit" "$sigint_unit" "${bad_units[@]}" >/dev/null 2>&1 || true
  systemctl --user reset-failed "$unit" "$sigint_unit" "${bad_units[@]}" \
    >/dev/null 2>&1 || true
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    echo "acceptance cleanup found a surviving server pid" >&2
    status=1
  fi
  if [[ -n "$child_pid" ]] && kill -0 "$child_pid" 2>/dev/null; then
    echo "acceptance cleanup found a surviving worker child pid" >&2
    status=1
  fi
  if [[ -n "$postgres_schema" ]]; then
    psql "$postgres_dsn" -v ON_ERROR_STOP=1 -c \
      "DROP SCHEMA IF EXISTS \"$postgres_schema\" CASCADE" >/dev/null 2>&1 || {
      echo "acceptance cleanup could not drop its isolated PostgreSQL schema" >&2
      status=1
    }
  fi
  rm -rf -- "$root"
  return "$status"
}
trap cleanup EXIT

cat >"$root/config.yaml" <<EOF
data_dir: "$root/state"
db_path: "$root/state/laso.db"
storage_backend: $backend
api_host: 127.0.0.1
api_port: $port
json_logs: true
workers: 2
process_workers:
  reference:
    executable: "$worker_host"
    args: [--mode, success]
    startup_timeout_ms: 5000
    request_timeout_ms: 5000
EOF
if [[ "$backend" == postgres ]]; then
  cat >>"$root/config.yaml" <<EOF
postgres_dsn: "$postgres_dsn"
postgres_schema: "$postgres_schema"
EOF
fi
sed "s|/var/lib/laso|$root/installed-example-state|g" \
  "$prefix/share/laso/laso.systemd.example.yaml" >"$root/installed-example.yaml"
"$prefix/bin/laso" --config "$root/installed-example.yaml" health >/dev/null

start_service() {
  systemd-run --user --unit="$unit" --property=Type=simple \
    --property=Restart=on-failure --property=RestartSec=2s \
    --property=TimeoutStopSec=30s --property=UMask=0077 \
    --property="WorkingDirectory=$root" "$server" --config "$root/config.yaml" \
    >/dev/null
}

wait_health() {
  local deadline=$((SECONDS + 20))
  while ((SECONDS < deadline)); do
    if curl -fsS --max-time 1 "$base/health" 2>/dev/null | jq -e '.status == "ok"' >/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for systemd-managed LASO health" >&2
  journalctl --user -u "$unit" --no-pager -n 20 >&2 || true
  return 1
}

wait_run_state() {
  local run_id=$1 expected=$2 deadline=$((SECONDS + 20)) response
  while ((SECONDS < deadline)); do
    response=$(curl -fsS --max-time 1 "$base/runs/$run_id" 2>/dev/null || true)
    if [[ -n "$response" ]] && jq -e --arg expected "$expected" \
      '.state == $expected' <<<"$response" >/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "run failed to reach expected state $expected" >&2
  return 1
}

start_service
wait_health
server_pid=$(systemctl --user show --property=MainPID --value "$unit")
[[ "$server_pid" =~ ^[1-9][0-9]*$ ]]
child_pid=$(pgrep -P "$server_pid" | head -n 1 || true)
[[ -n "$child_pid" ]] || { echo "reference worker child was not observed" >&2; exit 1; }

pipeline=$(jq -n --rawfile yaml "$source_dir/examples/human-approval/pipeline.yaml" \
  '{yaml:$yaml}')
curl -fsS -X POST "$base/pipelines" -H 'Content-Type: application/json' \
  --data-binary "$pipeline" >/dev/null
run=$(curl -fsS -X POST "$base/pipelines/human-approval@1/runs" \
  -H 'Content-Type: application/json' -d '{}')
run_id=$(jq -er '.id' <<<"$run")
wait_run_state "$run_id" WaitingApproval
approval_id=$(curl -fsS "$base/approvals" | jq -er \
  --arg run_id "$run_id" '[.[] | select(.run_id == $run_id) | .id][0]')

# A normal systemd restart exercises SIGTERM and proves the approval checkpoint
# is restored from SQLite rather than process-local state.
old_child_pid=$child_pid
systemctl --user restart "$unit"
server_pid=$(systemctl --user show --property=MainPID --value "$unit")
wait_health
if kill -0 "$old_child_pid" 2>/dev/null; then
  echo "worker child survived graceful systemd restart" >&2
  exit 1
fi
child_pid=$(pgrep -P "$server_pid" | head -n 1 || true)
[[ -n "$child_pid" ]] || { echo "restarted service worker child missing" >&2; exit 1; }
wait_run_state "$run_id" WaitingApproval

# SIGKILL deliberately bypasses LASO cleanup; Restart=on-failure must recover
# the same durable approval without creating another run.
old_pid=$server_pid
systemctl --user kill --signal=SIGKILL "$unit"
deadline=$((SECONDS + 30))
while ((SECONDS < deadline)); do
  server_pid=$(systemctl --user show --property=MainPID --value "$unit" 2>/dev/null || true)
  if [[ "$server_pid" =~ ^[1-9][0-9]*$ && "$server_pid" != "$old_pid" ]] &&
     curl -fsS --max-time 1 "$base/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
[[ "$server_pid" =~ ^[1-9][0-9]*$ && "$server_pid" != "$old_pid" ]] || {
  echo "systemd did not restart LASO after forced termination" >&2
  exit 1
}
if kill -0 "$child_pid" 2>/dev/null; then
  echo "worker child survived forced service termination" >&2
  exit 1
fi
child_pid=$(pgrep -P "$server_pid" | head -n 1 || true)
[[ -n "$child_pid" ]] || { echo "restarted service worker child missing" >&2; exit 1; }
wait_run_state "$run_id" WaitingApproval
curl -fsS -X POST "$base/approvals/$approval_id/approve" \
  -H 'Content-Type: application/json' -d '{"comment":"systemd acceptance"}' >/dev/null
wait_run_state "$run_id" Completed
completion_count=$(curl -fsS "$base/runs/$run_id/events" | jq \
  '[.[] | select(.type == "run.completed")] | length')
[[ "$completion_count" == 1 ]] || {
  echo "durable run has $completion_count completion events instead of one" >&2
  exit 1
}

# A completed run stays terminal when the owner dies abruptly after completion.
old_pid=$server_pid
old_child_pid=$child_pid
systemctl --user kill --signal=SIGKILL "$unit"
deadline=$((SECONDS + 30))
while ((SECONDS < deadline)); do
  server_pid=$(systemctl --user show --property=MainPID --value "$unit" 2>/dev/null || true)
  if [[ "$server_pid" =~ ^[1-9][0-9]*$ && "$server_pid" != "$old_pid" ]] &&
     curl -fsS --max-time 1 "$base/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
[[ "$server_pid" =~ ^[1-9][0-9]*$ && "$server_pid" != "$old_pid" ]] || {
  echo "systemd did not restart LASO after post-completion termination" >&2
  exit 1
}
if kill -0 "$old_child_pid" 2>/dev/null; then
  echo "worker child survived post-completion termination" >&2
  exit 1
fi
child_pid=$(pgrep -P "$server_pid" | head -n 1 || true)
[[ -n "$child_pid" ]] || { echo "restarted service worker child missing" >&2; exit 1; }
wait_run_state "$run_id" Completed
completion_count=$(curl -fsS "$base/runs/$run_id/events" | jq \
  '[.[] | select(.type == "run.completed")] | length')
[[ "$completion_count" == 1 ]] || {
  echo "restart duplicated the durable run completion" >&2
  exit 1
}

# Repeated service transitions catch stale listeners, PID files and held DB
# handles without introducing nondeterministic long stress loops.
for cycle in 1 2; do
  systemctl --user stop "$unit"
  if curl -fsS --max-time 1 "$base/health" >/dev/null 2>&1; then
    echo "health remained available after systemd stop" >&2
    exit 1
  fi
  if kill -0 "$server_pid" 2>/dev/null || kill -0 "$child_pid" 2>/dev/null; then
    echo "server or worker child survived graceful systemd stop" >&2
    exit 1
  fi
  start_service
  wait_health
  server_pid=$(systemctl --user show --property=MainPID --value "$unit")
  [[ "$server_pid" =~ ^[1-9][0-9]*$ ]]
  child_pid=$(pgrep -P "$server_pid" | head -n 1 || true)
  [[ -n "$child_pid" ]] || { echo "restarted service worker child missing" >&2; exit 1; }
done

probe_config_failure() {
  local name=$1 config=$2 diagnostic=$3 result=0 output
  bad_units+=("$name")
  output=$(systemd-run --user --unit="$name" --wait --pipe \
    --property=Restart=no "$server" --config "$config" 2>&1) || result=$?
  [[ "$result" -ne 0 ]] || { echo "invalid config unexpectedly succeeded" >&2; return 1; }
  if ! grep -Fq -- "$diagnostic" <<<"$output"; then
    echo "systemd failure unit $name omitted its expected safe diagnostic" >&2
    return 1
  fi
  [[ "$(systemctl --user show --property=Result --value "$name")" == exit-code ]] || {
    echo "systemd did not retain a failed config unit state" >&2
    return 1
  }
}

# Configuration and state-path failures must exit clearly rather than report a
# healthy service or remain stuck at startup.
printf 'data_dir: [malformed\n' >"$root/invalid.yaml"
probe_config_failure "${bad_unit%.service}-malformed.service" "$root/invalid.yaml" \
  "Invalid configuration YAML"
probe_config_failure "${bad_unit%.service}-missing.service" "$root/missing.yaml" \
  "Cannot open configuration file"
printf 'storage_backend: postgres\n' >"$root/invalid-storage.yaml"
probe_config_failure "${bad_unit%.service}-storage.service" "$root/invalid-storage.yaml" \
  "PostgreSQL DSN is required"
printf 'plugin_dirs: ["%s/missing-plugins"]\n' "$root" >"$root/invalid-plugin.yaml"
probe_config_failure "${bad_unit%.service}-plugin.service" "$root/invalid-plugin.yaml" \
  "Configured plugin directory is unavailable"
printf 'data_dir: "%s/readonly"\n' "$root" >"$root/unwritable.yaml"
mkdir "$root/readonly"
chmod 0500 "$root/readonly"
probe_config_failure "${bad_unit%.service}-state.service" "$root/unwritable.yaml" \
  "Cannot open database process lease"
chmod 0700 "$root/readonly"
printf 'data_dir: "%s/state"\n' "$root" >"$root/unreadable.yaml"
chmod 000 "$root/unreadable.yaml"
probe_config_failure "${bad_unit%.service}-unreadable.service" "$root/unreadable.yaml" \
  "Cannot open configuration file"
chmod 0600 "$root/unreadable.yaml"

systemctl --user stop "$unit"
if curl -fsS --max-time 1 "$base/health" >/dev/null 2>&1; then
  echo "health remained available after final systemd stop" >&2
  exit 1
fi
if kill -0 "$server_pid" 2>/dev/null || kill -0 "$child_pid" 2>/dev/null; then
  echo "server or worker child survived final systemd stop" >&2
  exit 1
fi

# Exercise SIGINT separately by making it systemd's configured stop signal.
sigint_port=$((port == 59999 ? 40000 : port + 1))
sed -e "s|$root/state|$root/sigint-state|g" \
    -e "s|api_port: $port|api_port: $sigint_port|" \
    "$root/config.yaml" >"$root/sigint.yaml"
base="http://127.0.0.1:$sigint_port/api/v1"
systemd-run --user --unit="$sigint_unit" --property=Type=simple \
  --property=Restart=no --property=KillSignal=SIGINT --property=TimeoutStopSec=30s \
  --property=WorkingDirectory="$root" "$server" --config "$root/sigint.yaml" \
  >/dev/null
wait_health
sigint_pid=$(systemctl --user show --property=MainPID --value "$sigint_unit")
systemctl --user stop "$sigint_unit"
[[ "$(systemctl --user show --property=ActiveState --value "$sigint_unit")" == inactive ]] || {
  echo "SIGINT service did not stop cleanly" >&2
  exit 1
}
if curl -fsS --max-time 1 "$base/health" >/dev/null 2>&1 || kill -0 "$sigint_pid" 2>/dev/null; then
  echo "SIGINT left LASO listening or running" >&2
  exit 1
fi
echo "systemd user-service lifecycle passed: install, health, durable approval recovery, SIGTERM, SIGINT, SIGKILL restart, child cleanup, repeated cycles, configuration failures"
echo "LIMITATION: this mode runs under the invoking user and does not validate a dedicated system account or system-unit filesystem sandbox."
