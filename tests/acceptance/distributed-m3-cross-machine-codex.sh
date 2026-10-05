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
artifact_backend=${LASO_TEST_ARTIFACT_BACKEND:-filesystem}
s3_endpoint=${LASO_TEST_S3_ENDPOINT:-}
s3_bucket=${LASO_TEST_S3_BUCKET:-}
s3_region=${LASO_TEST_S3_REGION:-us-east-1}
s3_ca_file=${LASO_TEST_S3_CA_FILE:-}
remote_artifact_mode=${LASO_REMOTE_ARTIFACT_MODE:-gateway}
remote_s3_endpoint=${LASO_REMOTE_TEST_S3_ENDPOINT:-}
remote_s3_tunnel_target=${LASO_REMOTE_S3_TUNNEL_TARGET:-}
remote_s3_ca_file=${LASO_REMOTE_TEST_S3_CA_FILE:-}
remote_ready_path=${LASO_CROSS_MACHINE_REMOTE_READY_PATH:-}
s3_started_marker=${LASO_CROSS_MACHINE_S3_STARTED_MARKER:-}
s3_progress_marker=${LASO_CROSS_MACHINE_S3_PROGRESS_MARKER:-}
s3_published_marker=${LASO_CROSS_MACHINE_S3_PUBLISHED_MARKER:-}
s3_release_file=${LASO_CROSS_MACHINE_S3_RELEASE_FILE:-}
expected_upload_bytes=${LASO_CROSS_MACHINE_EXPECTED_UPLOAD_BYTES:-}
fault_release_file=${LASO_CROSS_MACHINE_FAULT_RELEASE_FILE:-}
container_runtime=${LASO_TEST_CONTAINER_RUNTIME:-podman}
postgres_container=${LASO_TEST_POSTGRES_CONTAINER:-}
s3_test_container=${LASO_TEST_S3_CONTAINER:-}
outage_seconds=${LASO_CROSS_MACHINE_BACKEND_OUTAGE_SECONDS:-3}
if [[ -n "${LASO_CROSS_MACHINE_LEASE_TTL_MS:-}" ]]; then
  lease_ttl_ms=$LASO_CROSS_MACHINE_LEASE_TTL_MS
elif [[ "$scenario" == db-interruption ]]; then
  # Keep the real three-second database outage inside the normal lease window.
  lease_ttl_ms=30000
else
  lease_ttl_ms=3000
fi
[[ "$lease_ttl_ms" =~ ^[1-9][0-9]*$ ]] && ((lease_ttl_ms >= 1000 && lease_ttl_ms <= 120000)) || {
  echo "LASO_CROSS_MACHINE_LEASE_TTL_MS must be between 1000 and 120000" >&2
  exit 2
}
instance_stale_after_ms=${LASO_CROSS_MACHINE_INSTANCE_STALE_AFTER_MS:-$((lease_ttl_ms > 5000 ? lease_ttl_ms : 5000))}
[[ "$instance_stale_after_ms" =~ ^[1-9][0-9]*$ ]] &&
  ((10#$instance_stale_after_ms >= 10#$lease_ttl_ms && 10#$instance_stale_after_ms <= 86400000)) || {
    echo "LASO_CROSS_MACHINE_INSTANCE_STALE_AFTER_MS must cover the lease TTL" >&2
    exit 2
  }
remote_fixture_mode=${LASO_REMOTE_CODEX_FIXTURE_MODE:-}
[[ -n "$target" && -n "$remote_build" && -n "$owner_dsn" && -n "$remote_dsn" ]] || {
  echo "LASO_REMOTE_SSH_TARGET, LASO_REMOTE_BUILD_DIR, LASO_TEST_POSTGRES_DSN, and LASO_REMOTE_POSTGRES_DSN are required" >&2
  exit 77
}
[[ "$runs" =~ ^[1-9][0-9]*$ ]] || { echo "LASO_CROSS_MACHINE_RUNS must be a positive integer" >&2; exit 2; }
case "$artifact_backend" in
  filesystem) ;;
  s3)
    case "$s3_endpoint" in
      http://localhost:*|http://127.0.0.1:*|https://localhost:*|https://127.0.0.1:*) ;;
      *) echo "S3 acceptance requires a loopback endpoint" >&2; exit 2 ;;
    esac
    [[ -n "$s3_bucket" ]] || {
      echo "S3 acceptance requires a loopback endpoint and disposable bucket" >&2
      exit 2
    }
    if [[ -n "$s3_ca_file" && ! -r "$s3_ca_file" ]]; then
      echo "LASO_TEST_S3_CA_FILE must name a readable CA bundle" >&2
      exit 2
    fi
    [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]] || {
      echo "S3 acceptance requires test-only AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY" >&2
      exit 2
    }
    ;;
  *) echo "unsupported artifact backend: $artifact_backend" >&2; exit 2 ;;
esac
case "$remote_fixture_mode" in
  ""|success|write-workspace|quiet-over-one-minute|write-workspace-then-quiet|write-large-workspace) ;;
  *) echo "unsupported deterministic Codex fixture mode" >&2; exit 2 ;;
esac
case "$remote_artifact_mode" in
  gateway) ;;
  s3)
    [[ "$artifact_backend" == "s3" ]] || {
      echo "direct remote S3 mode requires the owner S3 backend" >&2
      exit 2
    }
    [[ "$remote_s3_endpoint" =~ ^https?://(localhost|127\.0\.0\.1):([0-9]{1,5})$ ]] || {
      echo "direct remote S3 mode requires a loopback endpoint with an explicit port" >&2
      exit 2
    }
    remote_s3_port=${BASH_REMATCH[2]}
    [[ "$remote_s3_tunnel_target" =~ ^127\.0\.0\.1:([0-9]{1,5})$ ]] || {
      echo "direct remote S3 mode requires a loopback tunnel target with an explicit port" >&2
      exit 2
    }
    remote_s3_local_port=${BASH_REMATCH[1]}
    [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]] || {
      echo "direct remote S3 mode requires synthetic test credentials" >&2
      exit 2
    }
    if [[ -n "$remote_s3_ca_file" && ! -r "$remote_s3_ca_file" ]]; then
      echo "LASO_REMOTE_TEST_S3_CA_FILE must name a readable CA bundle" >&2
      exit 2
    fi
    ;;
  *) echo "unsupported remote artifact mode: $remote_artifact_mode" >&2; exit 2 ;;
esac
for marker in "$remote_ready_path" "$s3_started_marker" "$s3_progress_marker" \
              "$s3_published_marker" "$s3_release_file" "$fault_release_file"; do
  [[ -z "$marker" || ( "$marker" =~ ^/tmp/[A-Za-z0-9._/-]+$ && "$marker" != *..* ) ]] || {
    echo "acceptance marker paths must be safe temporary paths under /tmp" >&2
    exit 2
  }
done
if [[ -n "$expected_upload_bytes" ]]; then
  [[ "$expected_upload_bytes" =~ ^[1-9][0-9]*$ ]] || {
    echo "LASO_CROSS_MACHINE_EXPECTED_UPLOAD_BYTES must be a positive integer" >&2
    exit 2
  }
fi
case "$scenario" in
  worker-death-before-publication)
    [[ "$artifact_backend" == s3 && "$remote_artifact_mode" == s3 &&
       "$remote_fixture_mode" == write-workspace-then-quiet ]] || {
      echo "pre-publication worker death requires direct S3 and the quiet workspace fixture" >&2
      exit 2
    }
    ;;
  worker-death-during-publication|worker-death-after-publication)
    [[ "$artifact_backend" == s3 && "$remote_artifact_mode" == s3 &&
       "$remote_fixture_mode" == write-large-workspace && -n "$expected_upload_bytes" &&
       -n "$s3_started_marker" ]] || {
      echo "publication worker death requires direct S3, the large workspace fixture, and expected upload size" >&2
      exit 2
    }
    if [[ "$scenario" == "worker-death-during-publication" ]]; then
      [[ -n "$s3_progress_marker" ]] || {
        echo "mid-publication worker death requires an S3 progress marker" >&2
        exit 2
      }
    else
      [[ -n "$s3_published_marker" && -n "$s3_release_file" ]] || {
        echo "post-publication worker death requires an S3 publication barrier" >&2
        exit 2
      }
    fi
    ;;
  owner-death|stale-worker)
    [[ "$artifact_backend" == s3 && "$remote_artifact_mode" == s3 ]] || {
      echo "$scenario requires direct S3 on the physical worker" >&2
      exit 2
    }
    ;;
  db-interruption)
    [[ "$artifact_backend" == s3 && "$remote_artifact_mode" == s3 && -n "$postgres_container" ]] || {
      echo "database interruption requires direct S3 and an explicit disposable PostgreSQL container" >&2
      exit 2
    }
    ;;
  s3-outage-during-transfer)
    [[ "$artifact_backend" == s3 && "$remote_artifact_mode" == s3 &&
       "$remote_fixture_mode" == write-large-workspace && -n "$expected_upload_bytes" &&
       -n "$s3_started_marker" && -n "$s3_progress_marker" && -n "$s3_test_container" ]] || {
      echo "S3 outage requires direct S3, the large workspace fixture, transfer markers, and an explicit disposable S3 container" >&2
      exit 2
    }
    ;;
esac
pipeline_file="$source_dir/tests/acceptance/distributed-m3-cross-machine-codex-pipeline.yaml"
if [[ "$scenario" == "cancel" ]]; then
  pipeline_file="$source_dir/tests/acceptance/distributed-m3-cross-machine-codex-cancel-pipeline.yaml"
  pipeline_ref="distributed-cross-machine-codex-cancel@1"
elif [[ "$scenario" == "worker-death" || "$scenario" == "worker-death-before-publication" ||
        "$scenario" == "worker-death-during-publication" ||
        "$scenario" == "worker-death-after-publication" ||
        "$scenario" == "s3-outage-during-transfer" ]]; then
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
if [[ "$scenario" == "db-interruption" || "$scenario" == "s3-outage-during-transfer" ]]; then
  [[ "$outage_seconds" =~ ^[1-9][0-9]?$ ]] && (( outage_seconds <= 30 )) || {
    echo "LASO_CROSS_MACHINE_BACKEND_OUTAGE_SECONDS must be between 1 and 30" >&2
    exit 2
  }
  [[ "$container_runtime" == podman || "$container_runtime" == docker ]] || {
    echo "LASO_TEST_CONTAINER_RUNTIME must be podman or docker" >&2
    exit 2
  }
  command -v "$container_runtime" >/dev/null || { echo "test container runtime is unavailable" >&2; exit 77; }
fi
validate_disposable_container() {
  local name=$1 kind=$2 details status image
  [[ "$name" =~ ^laso-(test|acceptance|physical-m4)-${kind}-[A-Za-z0-9._-]+$ ]] || {
    echo "container name is outside the LASO test-only naming pattern" >&2
    return 1
  }
  details=$("$container_runtime" inspect --format '{{.State.Status}}|{{.Config.Image}}' "$name") || return 1
  status=${details%%|*}
  image=${details#*|}
  [[ "$status" == running && "${image,,}" == *"$kind"* ]] || {
    echo "configured disposable $kind container is not running the expected image" >&2
    return 1
  }
}
if [[ "$scenario" == "db-interruption" ]]; then
  command -v pg_isready >/dev/null || { echo "pg_isready is required for database recovery verification" >&2; exit 77; }
  validate_disposable_container "$postgres_container" postgres || exit 77
fi
if [[ "$scenario" == "s3-outage-during-transfer" ]]; then
  validate_disposable_container "$s3_test_container" minio || exit 77
fi
wait_for_postgres() {
  for _ in $(seq 1 300); do
    PGPASSWORD="$owner_pgpassword" pg_isready -d "$owner_dsn" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  return 1
}
wait_for_s3() {
  local health_url="${s3_endpoint%/}/minio/health/live"
  for _ in $(seq 1 300); do
    if [[ "$s3_endpoint" == https://* && -n "$s3_ca_file" ]]; then
      curl -fsS --max-time 2 --cacert "$s3_ca_file" "$health_url" >/dev/null 2>&1 && return 0
    else
      curl -fsS --max-time 2 "$health_url" >/dev/null 2>&1 && return 0
    fi
    sleep 0.1
  done
  return 1
}

ssh_options=(-o BatchMode=no -o StrictHostKeyChecking=yes)
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
postgres_container_stopped=0
s3_test_container_stopped=0
artifact_tunnel_pid=
s3_tunnel_pid=
artifact_tunnel_active=0
s3_tunnel_active=0
run_root=$(mktemp -d)
owner_pid=
cleanup() {
  local status=$?
  if [[ "${postgres_container_stopped:-0}" == 1 ]]; then
    "$container_runtime" start "$postgres_container" >/dev/null 2>&1 || true
    postgres_container_stopped=0
  fi
  if [[ "${s3_test_container_stopped:-0}" == 1 ]]; then
    "$container_runtime" start "$s3_test_container" >/dev/null 2>&1 || true
    s3_test_container_stopped=0
  fi
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
  if [[ -n "${artifact_tunnel_pid:-}" ]]; then kill "$artifact_tunnel_pid" 2>/dev/null || true; wait "$artifact_tunnel_pid" 2>/dev/null || true; fi
  if [[ -n "${s3_tunnel_pid:-}" ]]; then kill "$s3_tunnel_pid" 2>/dev/null || true; wait "$s3_tunnel_pid" 2>/dev/null || true; fi
  if [[ "${artifact_tunnel_active:-0}" == 1 && -n "${LASO_SSH_CONTROL_PATH:-}" ]]; then
    ssh "${ssh_options[@]}" -O cancel -R "$artifact_forward" "$target" >/dev/null 2>&1 || true
  fi
  if [[ "${s3_tunnel_active:-0}" == 1 && -n "${LASO_SSH_CONTROL_PATH:-}" ]]; then
    ssh "${ssh_options[@]}" -O cancel -R "$s3_forward" "$target" >/dev/null 2>&1 || true
  fi
  if [[ -n "${s3_release_file:-}" ]]; then touch "$s3_release_file" 2>/dev/null || true; fi
  if [[ -n "${fault_release_file:-}" ]]; then touch "$fault_release_file" 2>/dev/null || true; fi
  for marker in "${remote_ready_path:-}" "${s3_started_marker:-}" \
                "${s3_progress_marker:-}" "${s3_published_marker:-}"; do
    [[ -z "$marker" ]] || rm -f -- "$marker" 2>/dev/null || true
  done
  if [[ -n "${fault_release_file:-}" ]]; then rm -f -- "$fault_release_file" 2>/dev/null || true; fi
  if [[ -n "${remote_b_root:-}" ]]; then remote_has "rm -rf -- '$remote_b_root'" || true; fi
  if [[ -n "${remote_root:-}" && -n "${remote_ready_path:-}" ]]; then
    remote_has "rm -f -- '$remote_ready_path' '$remote_root/provider-finished.done'" || true
  fi
  remote_has "rm -rf -- '$remote_root'" || true
  rm -rf -- "$run_root"
  return "$status"
}
trap cleanup EXIT

escape_sed() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }
remote_signal_process_tree() {
  local signal=$1 pid=${2:-}
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0
  remote_has "
    descend() {
      for child in \$(pgrep -P \"\$1\" 2>/dev/null || true); do
        descend \"\$child\"
      done
      printf '%s\\n' \"\$1\"
    }
    if ! kill -0 '$pid' 2>/dev/null; then
      printf '%s\\n' \"remote_process_tree_target_missing=1 pid=$pid\" >&2
      exit 1
    fi
    owned_pids=\$(descend '$pid' | sort -rn | uniq)
    kill -$signal \$owned_pids 2>/dev/null || true
    if [ '$signal' = KILL ]; then
      for _ in \$(seq 1 100); do
        remaining=
        for process in \$owned_pids; do
          state=\$(ps -o stat= -p \"\$process\" 2>/dev/null | awk 'NR==1 {print \$1}')
          case \"\$state\" in
            ''|Z*) ;;
            *) remaining=\"\$remaining \$process\" ;;
          esac
        done
        if [ -z \"\$remaining\" ]; then
          printf '%s\\n' \"remote_process_tree_kill_verified=1 pid=$pid pids=\$owned_pids\"
          exit 0
        fi
        sleep 0.05
      done
      printf '%s\\n' \"remote_process_tree_kill_incomplete=1 pid=$pid remaining=\$remaining\" >&2
      exit 1
    fi
    printf '%s\\n' \"remote_process_tree_signal_sent=1 pid=$pid signal=$signal pids=\$owned_pids\"
  "
}
wait_remote_file() {
  local remote_path=$1
  for _ in $(seq 1 1800); do
    remote_has "test -s '$remote_path'" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  return 1
}
wait_local_file() {
  local path=$1
  for _ in $(seq 1 1800); do
    [[ -e "$path" ]] && return 0
    sleep 0.1
  done
  return 1
}
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
    -e "s|@LEASE_TTL_MS@|$lease_ttl_ms|g" \
    -e "s|@ARTIFACT_SERVICE_PORT@|$owner_artifact_port|g" \
    -e "s|@ARTIFACT_SERVICE_URL@||g" \
    -e "s|@ARTIFACT_SERVICE_TOKEN@|$(escape_sed "$artifact_token")|g" \
    -e "s|^artifact_service_port:.*|artifact_service_port: $owner_artifact_port|" \
    "$source_dir/tests/acceptance/distributed-m3-cross-machine-worker-config.yaml.in" |
  sed '/^process_workers:/,$d' > "$run_root/owner.yaml"
if [[ "$artifact_backend" == "s3" ]]; then
  {
    printf 'artifact_backend: s3\n'
    printf 'artifact_s3_endpoint: "%s"\n' "$(escape_sed "$s3_endpoint")"
    printf 'artifact_s3_bucket: "%s"\n' "$(escape_sed "$s3_bucket")"
    printf 'artifact_s3_region: "%s"\n' "$(escape_sed "$s3_region")"
    printf 'artifact_s3_prefix: "cross-machine/%s"\n' "$schema"
    printf 'artifact_s3_path_style: true\n'
    if [[ -n "$s3_ca_file" ]]; then
      printf 'artifact_s3_ca_file: %s\n' "$(jq -Rn --arg value "$s3_ca_file" '$value')"
    fi
    if [[ "$s3_endpoint" == http://* ]]; then
      printf 'artifact_s3_allow_http: true\n'
    fi
  } >> "$run_root/owner.yaml"
fi

# The provider is installed for the remote user's login environment.  Resolve
# it through a login shell and pass the absolute path into LASO so the worker
# does not depend on a non-login SSH PATH.
if [[ -n "$remote_fixture_mode" ]]; then
  remote_codex_bin="$remote_build/bin/laso-codex-fixture"
  remote_has "test -x '$remote_codex_bin'" || {
    echo "deterministic remote Codex fixture is missing" >&2
    exit 77
  }
else
  remote_codex_bin=${LASO_REMOTE_CODEX_BIN:-$(remote_has 'bash -lc "command -v codex"')}
fi
remote_data="$remote_root/data"
remote_worker_root="$remote_data/distributed-workspaces"
remote_config="$remote_root/worker.yaml"
remote_connection_dsn="$remote_dsn"
remote_has "mkdir -p '$remote_worker_root' '$remote_data'"
if [[ -z "$remote_ready_path" ]]; then
  remote_ready_path="$remote_root/provider-output.ready"
fi
remote_done_path="$remote_root/provider-finished.done"
remote_artifact_url="http://127.0.0.1:$remote_artifact_port"
if [[ "$remote_artifact_mode" == "s3" ]]; then
  remote_artifact_url=
fi
remote_template="$run_root/worker-template.yaml"
sed -e "s|@DATA_DIR@|$(escape_sed "$remote_data")|g" \
    -e "s|@POSTGRES_DSN@|$(escape_sed "$remote_connection_dsn")|g" \
    -e "s|@POSTGRES_SCHEMA@|$(escape_sed "$schema")|g" \
    -e "s|@LEASE_TTL_MS@|$lease_ttl_ms|g" \
    -e "s|@ARTIFACT_SERVICE_URL@|$(escape_sed "$remote_artifact_url")|g" \
    -e "s|@ARTIFACT_SERVICE_TOKEN@|$(escape_sed "$artifact_token")|g" \
    -e "s|@CODEX_WORKER@|$(escape_sed "$remote_build/bin/laso-codex-worker")|g" \
    -e "s|@CODEX_BIN@|$(escape_sed "$remote_codex_bin")|g" \
    -e "s|@WORKSPACE_ROOT@|$(escape_sed "$remote_worker_root")|g" \
    "$source_dir/tests/acceptance/distributed-m3-cross-machine-worker-config.yaml.in" > "$remote_template"
remote_ca_copy=
if [[ "$remote_artifact_mode" == "s3" ]]; then
  {
    printf '\nartifact_backend: s3\n'
    printf 'artifact_s3_endpoint: "%s"\n' "$(escape_sed "$remote_s3_endpoint")"
    printf 'artifact_s3_bucket: "%s"\n' "$(escape_sed "$s3_bucket")"
    printf 'artifact_s3_region: "%s"\n' "$(escape_sed "$s3_region")"
    printf 'artifact_s3_prefix: "cross-machine/%s"\n' "$schema"
    printf 'artifact_s3_path_style: true\n'
    if [[ "$remote_s3_endpoint" == http://* ]]; then
      printf 'artifact_s3_allow_http: true\n'
    fi
    if [[ -n "$remote_s3_ca_file" ]]; then
      remote_ca_copy="$remote_root/s3-test-ca.pem"
      printf 'artifact_s3_ca_file: "%s"\n' "$remote_ca_copy"
    fi
  } >> "$remote_template"
fi
remote_env_local="$run_root/remote-worker.env"
remote_env="$remote_root/worker.env"
remote_launcher_local="$run_root/remote-launcher.sh"
remote_launcher="$remote_root/launch-worker.sh"
write_remote_env() {
  local destination=$1 variant=${2:-} sleep_ms=${3:-} ready_path=${4:-} done_path=${5:-}
  umask 077
  {
    printf 'PGPASSWORD=%q\n' "${LASO_REMOTE_PGPASSWORD:-}"
    printf 'LASO_INSTANCE_STALE_AFTER_MS=%q\n' "$instance_stale_after_ms"
    if [[ -n "$remote_fixture_mode" ]]; then
      printf 'LASO_CODEX_FIXTURE_MODE=%q\n' "$remote_fixture_mode"
      printf 'LASO_CODEX_FIXTURE_MARKER=%q\n' "${ready_path:-$remote_ready_path}"
      printf 'LASO_CODEX_FIXTURE_DONE_MARKER=%q\n' "${done_path:-$remote_done_path}"
      if [[ -n "$sleep_ms" ]]; then
        printf 'LASO_CODEX_FIXTURE_SLEEP_MS=%q\n' "$sleep_ms"
      fi
      if [[ -n "$variant" ]]; then
        printf 'LASO_CODEX_FIXTURE_ARTIFACT_CONTENT=%q\n' "$variant"
      fi
    fi
    if [[ "$remote_artifact_mode" == "s3" ]]; then
      printf 'AWS_ACCESS_KEY_ID=%q\n' "$AWS_ACCESS_KEY_ID"
      printf 'AWS_SECRET_ACCESS_KEY=%q\n' "$AWS_SECRET_ACCESS_KEY"
      printf 'AWS_EC2_METADATA_DISABLED=%q\n' true
    fi
  } > "$destination"
  chmod 600 "$destination"
}
remote_variant=${LASO_REMOTE_FIXTURE_ARTIFACT_CONTENT:-}
remote_sleep_ms=${LASO_REMOTE_FIXTURE_SLEEP_MS:-}
if [[ "$scenario" == "stale-worker" && -z "$remote_variant" ]]; then
  remote_variant=stale-attempt
fi
if [[ "$scenario" == "stale-worker" && -z "$remote_sleep_ms" ]]; then
  remote_sleep_ms=15000
fi
write_remote_env "$remote_env_local" "$remote_variant" "$remote_sleep_ms" \
  "$remote_ready_path" "$remote_done_path"
owner_env_local="$run_root/owner.env"
umask 077
{
  printf 'PGPASSWORD=%q\n' "$owner_pgpassword"
  printf 'LASO_INSTANCE_STALE_AFTER_MS=%q\n' "$instance_stale_after_ms"
} > "$owner_env_local"
chmod 600 "$owner_env_local"
cat > "$remote_launcher_local" <<'LAUNCHER'
#!/usr/bin/env bash
set -Eeuo pipefail
set -a
source "$1"
set +a
shift
exec "$@"
LAUNCHER
chmod 700 "$remote_launcher_local"
scp -q "${ssh_options[@]}" "$remote_template" "$target:$remote_config"
scp -q "${ssh_options[@]}" "$remote_env_local" "$target:$remote_env"
scp -q "${ssh_options[@]}" "$remote_launcher_local" "$target:$remote_launcher"
if [[ -n "$remote_ca_copy" ]]; then
  scp -q "${ssh_options[@]}" "$remote_s3_ca_file" "$target:$remote_ca_copy"
fi
remote_has "chmod 600 '$remote_env' && chmod 700 '$remote_launcher'"

bash "$remote_launcher_local" "$owner_env_local" "$owner_build/bin/laso-server" --config "$run_root/owner.yaml" --host 127.0.0.1 --port "$owner_port" \
  >"$run_root/owner.stdout" 2>"$run_root/owner.stderr" &
owner_pid=$!
base="http://127.0.0.1:$owner_port/api/v1"
for _ in $(seq 1 300); do
  if curl -fsS "$base/health" >/dev/null 2>&1; then break; fi
  kill -0 "$owner_pid" 2>/dev/null || { echo "owner failed during startup" >&2; exit 1; }
  sleep 0.1
done
curl -fsS "$base/health" >/dev/null || { echo "owner health check failed" >&2; exit 1; }
echo "cross_machine_owner_api_port=$owner_port artifact_gateway_port=$owner_artifact_port"
if [[ "$artifact_backend" == "s3" ]]; then
  echo "cross_machine_s3_namespace=cross-machine/$schema"
fi
artifact_probe=$(curl -sS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $artifact_token" \
  -I "http://127.0.0.1:$owner_artifact_port/api/v1/artifacts/invalid-id" 2>/dev/null || true)
echo "cross_machine_owner_artifact_gateway_probe=$artifact_probe"

# The worker's artifact URL is loopback-local on the remote host. Carry it
# over a scoped reverse SSH tunnel so PostgreSQL coordination and object
# transport remain private to the two test instances.
artifact_forward="127.0.0.1:${remote_artifact_port}:127.0.0.1:${owner_artifact_port}"
if [[ -n "${LASO_SSH_CONTROL_PATH:-}" ]]; then
  ssh "${ssh_options[@]}" -O forward -R "$artifact_forward" "$target"
  artifact_tunnel_active=1
else
  ssh -N "${ssh_options[@]}" -o ExitOnForwardFailure=yes -R "$artifact_forward" "$target" \
    >"$run_root/artifact-tunnel.stdout" 2>"$run_root/artifact-tunnel.stderr" &
  artifact_tunnel_pid=$!
fi
for _ in $(seq 1 100); do
  if [[ -n "$artifact_tunnel_pid" ]] && ! kill -0 "$artifact_tunnel_pid" 2>/dev/null; then
    echo "artifact reverse tunnel failed during startup" >&2
    cat "$run_root/artifact-tunnel.stderr" >&2 || true
    exit 1
  fi
  response_code=$(remote_has "curl -sS -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer $artifact_token' -I 'http://127.0.0.1:$remote_artifact_port/api/v1/artifacts/invalid-id'" 2>/dev/null || true)
  [[ "$response_code" == 400 ]] && break
  sleep 0.1
done
[[ "${response_code:-}" == 400 ]] || {
  echo "artifact reverse tunnel health check failed (owner HTTP $artifact_probe, worker HTTP ${response_code:-no response})" >&2
  remote_has "ss -ltn | grep ':$remote_artifact_port ' || true" >&2 || true
  exit 1
}

if [[ "$remote_artifact_mode" == "s3" ]]; then
  s3_forward="127.0.0.1:${remote_s3_port}:${remote_s3_tunnel_target}"
  if [[ -n "${LASO_SSH_CONTROL_PATH:-}" ]]; then
    ssh "${ssh_options[@]}" -O forward -R "$s3_forward" "$target"
    s3_tunnel_active=1
  else
    ssh -N "${ssh_options[@]}" -o ExitOnForwardFailure=yes -R "$s3_forward" "$target" \
      >"$run_root/s3-tunnel.stdout" 2>"$run_root/s3-tunnel.stderr" &
    s3_tunnel_pid=$!
  fi
  if [[ "$remote_s3_endpoint" == https://* && -n "$remote_ca_copy" ]]; then
    remote_s3_health="curl -fsS --cacert '$remote_ca_copy' '$remote_s3_endpoint/minio/health/live' >/dev/null"
  else
    remote_s3_health="curl -fsS '$remote_s3_endpoint/minio/health/live' >/dev/null"
  fi
  for _ in $(seq 1 100); do
    if [[ -n "$s3_tunnel_pid" ]] && ! kill -0 "$s3_tunnel_pid" 2>/dev/null; then
      echo "S3 reverse tunnel failed during startup" >&2
      cat "$run_root/s3-tunnel.stderr" >&2 || true
      exit 1
    fi
    remote_has "$remote_s3_health" >/dev/null 2>&1 && break
    sleep 0.1
  done
  remote_has "$remote_s3_health" >/dev/null || {
    echo "remote S3 reverse tunnel health check failed" >&2
    exit 1
  }
fi

start_remote_server() {
  remote_server_pid=$(ssh "${ssh_options[@]}" "$target" \
    "nohup bash '$remote_launcher' '$remote_env' '$remote_build/bin/laso-server' --config '$remote_config' --host 127.0.0.1 --port '$remote_port' >'$remote_root/worker.stdout' 2>'$remote_root/worker.stderr' </dev/null & echo \$!")
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
  remote_b_env_local="$run_root/remote-worker-b.env"
  remote_b_env="$remote_b_root/worker.env"
  remote_b_port=$((remote_port + 1))
  remote_has "mkdir -p '$remote_b_worker_root' '$remote_b_data'"
  remote_template_b="$run_root/worker-template-b.yaml"
  sed -e "s|@DATA_DIR@|$(escape_sed "$remote_b_data")|g" \
      -e "s|@POSTGRES_DSN@|$(escape_sed "$remote_dsn")|g" \
      -e "s|@POSTGRES_SCHEMA@|$(escape_sed "$schema")|g" \
      -e "s|@LEASE_TTL_MS@|$lease_ttl_ms|g" \
      -e "s|@ARTIFACT_SERVICE_URL@|$(escape_sed "$remote_artifact_url")|g" \
      -e "s|@ARTIFACT_SERVICE_TOKEN@|$(escape_sed "$artifact_token")|g" \
      -e "s|@CODEX_WORKER@|$(escape_sed "$remote_build/bin/laso-codex-worker")|g" \
      -e "s|@CODEX_BIN@|$(escape_sed "$remote_codex_bin")|g" \
      -e "s|@WORKSPACE_ROOT@|$(escape_sed "$remote_b_worker_root")|g" \
      -e "s|api_port: 0|api_port: $remote_b_port|" \
      "$source_dir/tests/acceptance/distributed-m3-cross-machine-worker-config.yaml.in" > "$remote_template_b"
  if [[ "$remote_artifact_mode" == "s3" ]]; then
    {
      printf '\nartifact_backend: s3\n'
      printf 'artifact_s3_endpoint: "%s"\n' "$(escape_sed "$remote_s3_endpoint")"
      printf 'artifact_s3_bucket: "%s"\n' "$(escape_sed "$s3_bucket")"
      printf 'artifact_s3_region: "%s"\n' "$(escape_sed "$s3_region")"
      printf 'artifact_s3_prefix: "cross-machine/%s"\n' "$schema"
      printf 'artifact_s3_path_style: true\n'
      if [[ "$remote_s3_endpoint" == http://* ]]; then
        printf 'artifact_s3_allow_http: true\n'
      fi
      if [[ -n "$remote_ca_copy" ]]; then
        printf 'artifact_s3_ca_file: "%s"\n' "$remote_ca_copy"
      fi
    } >> "$remote_template_b"
  fi
  scp -q "${ssh_options[@]}" "$remote_template_b" "$target:$remote_b_config"
  remote_b_variant=${LASO_REMOTE_FIXTURE_WINNER_CONTENT:-winning-attempt}
  remote_b_ready_path="$remote_b_root/provider-output.ready"
  remote_b_done_path="$remote_b_root/provider-finished.done"
  remote_b_sleep_ms=${LASO_REMOTE_FIXTURE_WINNER_SLEEP_MS:-0}
  write_remote_env "$remote_b_env_local" "$remote_b_variant" "$remote_b_sleep_ms" \
    "$remote_b_ready_path" "$remote_b_done_path"
  scp -q "${ssh_options[@]}" "$remote_b_env_local" "$target:$remote_b_env"
  remote_has "chmod 600 '$remote_b_env'"
  start_remote_b() {
    remote_b_server_pid=$(ssh "${ssh_options[@]}" "$target" \
      "nohup bash '$remote_launcher' '$remote_b_env' '$remote_build/bin/laso-server' --config '$remote_b_config' --host 127.0.0.1 --port '$remote_b_port' >'$remote_b_root/worker.stdout' 2>'$remote_b_root/worker.stderr' </dev/null & echo \$!")
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
  if [[ -n "$remote_ready_path" ]]; then
    remote_has "rm -f -- '$remote_ready_path'"
  fi
  for marker in "$s3_started_marker" "$s3_progress_marker" "$s3_published_marker" "$s3_release_file" "$fault_release_file"; do
    [[ -z "$marker" ]] || rm -f -- "$marker"
  done
  request=$(jq -n --argjson manifest "$manifest" \
    '{input:{request:"repair the synthetic project"},metadata:{classification:"public",workspace_manifest:$manifest}}')
  run_id=$(curl -fsS -X POST "$base/pipelines/$pipeline_ref/runs" \
    -H 'Content-Type: application/json' --data-binary "$request" | jq -r '.id')
  echo "cross_machine_run_id=$run_id scenario=$scenario"
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
  if [[ "$scenario" == "worker-death" || "$scenario" == "worker-death-before-publication" ||
        "$scenario" == "worker-death-during-publication" ||
        "$scenario" == "worker-death-after-publication" ]]; then
    worker_killed=0
    for _ in $(seq 1 900); do
      run=$(curl -fsS "$base/runs/$run_id")
      state=$(jq -r '.state' <<<"$run")
      jobs=$(curl -fsS "$base/worker-jobs?limit=100")
      active_job=$(jq -r --arg run "$run_id" '[.[] | select(.run_id == $run and (.status == "Submitting" or .status == "Running"))] | first // empty | .id // empty' <<<"$jobs")
      if [[ -n "$active_job" || "$scenario" == "worker-death-during-publication" ||
            "$scenario" == "worker-death-after-publication" ]]; then
        case "$scenario" in
          worker-death-before-publication)
            if ! remote_has "test -s '$remote_ready_path'" >/dev/null 2>&1; then
              sleep 0.1
              continue
            fi
            echo "cross_machine_fixture_output_ready=1 worker_job=$active_job"
            ;;
          worker-death-during-publication)
            [[ "$remote_artifact_mode" == "s3" && -n "$s3_progress_marker" &&
               -n "$expected_upload_bytes" ]] || {
              echo "mid-publication worker death requires direct S3 and progress evidence" >&2
              exit 2
            }
            progress_bytes=0
            if [[ -n "$s3_progress_marker" && -r "$s3_progress_marker" ]]; then
              read -r progress_bytes < "$s3_progress_marker" || progress_bytes=0
            fi
            if [[ ! "$progress_bytes" =~ ^[0-9]+$ || "$progress_bytes" == 0 ||
                  "$progress_bytes" -ge "$expected_upload_bytes" ]]; then
              sleep 0.1
              continue
            fi
            echo "cross_machine_s3_upload_active=1 bytes_sent=$progress_bytes expected_bytes=$expected_upload_bytes worker_job=${active_job:-server-process-only}"
            ;;
          worker-death-after-publication)
            [[ "$remote_artifact_mode" == "s3" && -n "$s3_published_marker" &&
               -n "$s3_release_file" ]] || {
              echo "post-publication worker death requires direct S3 publication and release markers" >&2
              exit 2
            }
            [[ -s "$s3_published_marker" ]] || { sleep 0.1; continue; }
            echo "cross_machine_s3_object_published=1 worker_job=$active_job"
            ;;
        esac
        remote_signal_process_tree KILL "$remote_server_pid"
        echo "cross_machine_worker_process_tree_killed=1 pid=$remote_server_pid worker_job=${active_job:-server-process-only}"
        if [[ -n "$fault_release_file" ]]; then
          echo "cross_machine_fault_inspection_ready=1 scenario=$scenario run_id=$run_id"
          wait_local_file "$fault_release_file" || {
            echo "fault inspection barrier timed out" >&2
            exit 1
          }
        fi
        if [[ "$scenario" == "worker-death-after-publication" ]]; then
          [[ -z "$s3_release_file" ]] || touch "$s3_release_file"
        fi
        sleep 1
        fault_run=$(curl -fsS "$base/runs/$run_id")
        fault_state=$(jq -r '.state' <<<"$fault_run")
        [[ "$fault_state" != "Completed" ]] || {
          echo "run became authoritative before worker recovery after $scenario" >&2
          exit 1
        }
        echo "cross_machine_state_after_fault=$fault_state scenario=$scenario"
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
        if [[ -n "$remote_fixture_mode" ]] &&
           ! remote_has "test -s '$remote_ready_path'" >/dev/null 2>&1; then
          sleep 0.1
          continue
        fi
        stale_worker_job_id=$active_job
        remote_signal_process_tree STOP "$remote_server_pid"
        echo "cross_machine_stale_worker_suspended=1 worker_job=$active_job"
        # Instance staleness is 5 seconds in this fixture. Leave a margin so
        # the replacement cannot race the old instance's heartbeat timeout.
        sleep 7
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
        bash "$remote_launcher_local" "$owner_env_local" "$owner_build/bin/laso-server" --config "$run_root/owner.yaml" \
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
    for _ in $(seq 1 900); do
      run=$(curl -fsS "$base/runs/$run_id")
      state=$(jq -r '.state' <<<"$run")
      jobs=$(curl -fsS "$base/worker-jobs?limit=100")
      active_job=$(jq -r --arg run "$run_id" '[.[] | select(.run_id == $run and (.status == "Submitting" or .status == "Running") and .worker_id == "codex")] | first // empty | .id // empty' <<<"$jobs")
      if [[ "$provider_observed" == 0 ]] && remote_has "pgrep -f '$remote_codex_bin' >/dev/null" >/dev/null 2>&1; then
        provider_observed=1
      fi
      if [[ "$provider_observed" == 1 && -n "$active_job" ]]; then
        postgres_container_stopped=1
        "$container_runtime" stop --time 0 "$postgres_container" >/dev/null
        db_interruption_sent=1
        echo "cross_machine_test_postgres_stopped=1 worker_job=$active_job"
        sleep "$outage_seconds"
        "$container_runtime" start "$postgres_container" >/dev/null
        postgres_container_stopped=0
        wait_for_postgres || { echo "disposable PostgreSQL did not recover" >&2; exit 1; }
        echo "cross_machine_test_postgres_recovered=1 outage_seconds=$outage_seconds"
        # PostgreSQL can accept connections before the owner API has cleared
        # transient database errors. Require sustained health across both routes
        # used to observe run state and worker recovery before polling resumes.
        api_recovery_successes=0
        for _ in $(seq 1 300); do
          if curl -fsS --max-time 2 "$base/health" >/dev/null 2>&1 &&
             run=$(curl -fsS --max-time 2 "$base/runs/$run_id" 2>/dev/null) &&
             jq -e --arg run "$run_id" '.id == $run' <<<"$run" >/dev/null 2>&1 &&
             jobs=$(curl -fsS --max-time 2 "$base/worker-jobs?limit=100" 2>/dev/null) &&
             jq -e 'type == "array"' <<<"$jobs" >/dev/null 2>&1; then
            api_recovery_successes=$((api_recovery_successes + 1))
            if [[ "$api_recovery_successes" -ge 20 ]]; then break; fi
          else
            api_recovery_successes=0
          fi
          sleep 0.2
        done
        [[ "$api_recovery_successes" -ge 20 ]] || {
          echo "owner API did not sustain run and worker-job reads after disposable PostgreSQL interruption" >&2
          exit 1
        }
        echo "cross_machine_owner_api_recovered=1 stable_api_cycles=$api_recovery_successes"
        break
      fi
      [[ "$state" == "Completed" || "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]] && break
      sleep 0.1
    done
    [[ "$provider_observed" == 1 ]] || { echo "remote provider was not observed before database interruption" >&2; exit 1; }
    [[ "$db_interruption_sent" == 1 ]] || { echo "active remote worker was not observed before database interruption" >&2; exit 1; }
  fi
  if [[ "$scenario" == "s3-outage-during-transfer" ]]; then
    s3_outage_injected=0
    for _ in $(seq 1 900); do
      run=$(curl -fsS "$base/runs/$run_id")
      state=$(jq -r '.state' <<<"$run")
      jobs=$(curl -fsS "$base/worker-jobs?limit=100")
      active_job=$(jq -r --arg run "$run_id" '[.[] | select(.run_id == $run and (.status == "Submitting" or .status == "Running"))] | first // empty | .id // empty' <<<"$jobs")
      progress_bytes=0
      if [[ -r "$s3_progress_marker" ]]; then read -r progress_bytes < "$s3_progress_marker" || progress_bytes=0; fi
      if [[ -s "$s3_started_marker" &&
            "$progress_bytes" =~ ^[0-9]+$ && "$progress_bytes" -gt 0 &&
            "$progress_bytes" -lt "$expected_upload_bytes" ]]; then
        echo "cross_machine_s3_upload_active=1 bytes_sent=$progress_bytes expected_bytes=$expected_upload_bytes worker_job=${active_job:-server-process-only}"
        s3_test_container_stopped=1
        "$container_runtime" stop --time 0 "$s3_test_container" >/dev/null
        echo "cross_machine_test_s3_stopped=1 worker_job=$active_job"
        sleep "$outage_seconds"
        "$container_runtime" start "$s3_test_container" >/dev/null
        s3_test_container_stopped=0
        wait_for_s3 || { echo "disposable S3 service did not recover" >&2; exit 1; }
        echo "cross_machine_test_s3_recovered=1 outage_seconds=$outage_seconds"
        s3_outage_injected=1
        break
      fi
      [[ "$state" == "Completed" || "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]] && break
      sleep 0.1
    done
    [[ "$s3_outage_injected" == 1 ]] || { echo "active S3 upload was not observed before outage injection" >&2; exit 1; }
  fi
  for _ in $(seq 1 1800); do
    if ! run=$(curl -fsS --max-time 2 "$base/runs/$run_id" 2>/dev/null); then
      if [[ "$scenario" == "db-interruption" ]]; then
        sleep 0.2
        continue
      fi
      echo "owner run-state API read failed" >&2
      exit 1
    fi
    state=$(jq -r '.state' <<<"$run")
    if [[ "$state" == "Completed" || "$state" == "Failed" || "$state" == "Cancelled" || "$state" == "TimedOut" ]]; then break; fi
    if [[ "$observed_remote" == 0 ]] && remote_has "pgrep -f '$remote_build/bin/laso-codex-worker' >/dev/null" >/dev/null 2>&1; then
      observed_remote=1
    fi
    sleep 0.1
  done
  if [[ "$scenario" == "stale-worker" ]]; then
    remote_signal_process_tree CONT "$remote_server_pid"
    wait_remote_file "$remote_done_path" || {
      echo "stale provider did not return after losing worker authority" >&2
      exit 1
    }
    sleep 2
    attempts=$(curl -fsS "$base/runs/$run_id/attempts?limit=100")
    codex_attempts=$(jq -r '[.[] | select(.node_id == "codex")] | length' <<<"$attempts")
    [[ "$codex_attempts" -ge 2 ]] || { echo "replacement worker did not create a distinct attempt" >&2; exit 1; }
    [[ -n "${stale_worker_job_id:-}" ]] || { echo "stale worker job identity was not retained" >&2; exit 1; }
    jobs=$(curl -fsS "$base/worker-jobs?limit=100")
    stale_job=$(jq -ce --arg id "$stale_worker_job_id" '[.[] | select(.id == $id)] | first // empty' <<<"$jobs") || {
      echo "stale worker job disappeared before rejection was recorded" >&2
      exit 1
    }
    stale_status=$(jq -r '.status' <<<"$stale_job")
    stale_error=$(jq -r '.error' <<<"$stale_job")
    [[ "$stale_status" == Failed && "$stale_error" == *"superseded or lost its node lease"* ]] || {
      echo "stale worker job was not rejected after losing authority" >&2
      jq -c '{id,status,error}' <<<"$stale_job" >&2
      exit 1
    }
    echo "STALE_RESULT_REJECTED worker_job=$stale_worker_job_id status=$stale_status codex_attempts=$codex_attempts"
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
  attempts=$(curl -fsS "$base/runs/$run_id/attempts?limit=100")
  jq -c '[.[] | {id,node_id,state,attempt,worker_job_id,started_at,finished_at,fencing_token}]' <<<"$attempts"
  if [[ "$scenario" == "stale-worker" ]]; then
    winning_attempt_id=$(jq -r '.manifest.provenance.attempt_id' "$run_root/result-manifest.json")
    winning_attempt=$(jq -ce --arg id "$winning_attempt_id" \
      '[.[] | select(.node_id == "codex" and .id == $id and .state == "Completed")] | first // empty' \
      <<<"$attempts") || {
      echo "authoritative manifest does not name a completed worker attempt" >&2
      exit 1
    }
    winning_job_id=$(jq -r '.worker_job_id' <<<"$winning_attempt")
    [[ -n "$winning_job_id" && "$winning_job_id" != "$stale_worker_job_id" ]] || {
      echo "authoritative result did not come from the replacement worker job" >&2
      exit 1
    }
    stale_attempt=$(jq -ce --arg job "$stale_worker_job_id" \
      '[.[] | select(.node_id == "codex" and .worker_job_id == $job and .state == "Failed")] | first // empty' \
      <<<"$attempts") || {
      echo "superseded worker attempt remained nonterminal in durable history" >&2
      exit 1
    }
    echo "cross_machine_stale_fencing=passed rejected_job=$stale_worker_job_id winner_job=$winning_job_id winning_attempt=$winning_attempt_id stale_attempt_state=$(jq -r '.state' <<<"$stale_attempt")"
  fi
  provenance=$(jq -c '.manifest.provenance | {run_id,node_work_id,attempt_id,worker_id,fencing_token}' \
    "$run_root/result-manifest.json")
  echo "cross_machine_artifact_provenance=$provenance"
  artifact_info=$(jq -c '.manifest.files[] | select(.path == "remote-artifact.txt") | {sha256,size,object_id}' \
    "$run_root/result-manifest.json")
  [[ -n "$artifact_info" ]] || { echo "fixture artifact was absent from the result manifest" >&2; exit 1; }
  if [[ "$scenario" == "stale-worker" ]]; then
    grep -Fxq "${LASO_REMOTE_FIXTURE_WINNER_CONTENT:-winning-attempt}" \
      "$run_root/accepted-$iteration/remote-artifact.txt" || {
      echo "authoritative workspace did not contain the replacement worker output" >&2
      exit 1
    }
  fi
  echo "cross_machine_artifact=$artifact_info"
  echo "cross_machine_run=$iteration provider=codex state=Completed remote_artifact=verified owner_build_tests=pass"
done
echo "DISTRIBUTED_M3_CROSS_MACHINE_CODEX_OK"
