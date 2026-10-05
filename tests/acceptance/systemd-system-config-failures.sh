#!/usr/bin/env bash
set -Eeuo pipefail

# Exercise failure reporting using the installed system-manager unit. This is
# deliberately opt-in and only touches the marked LASO acceptance config.
unit=laso.service
config=/etc/laso/laso.yaml
dropin_dir=/run/systemd/system/laso.service.d
dropin=$dropin_dir/90-laso-acceptance.conf

if [[ ${LASO_SYSTEMD_ALLOW_SYSTEM_MUTATION:-} != 1 || $EUID -ne 0 ]]; then
  echo "set LASO_SYSTEMD_ALLOW_SYSTEM_MUTATION=1 and run as root" >&2
  exit 2
fi
[[ "$(cat /proc/1/comm 2>/dev/null || true)" == systemd ]] || {
  echo "system manager is not PID 1" >&2
  exit 77
}
command -v systemctl >/dev/null && command -v journalctl >/dev/null &&
  command -v curl >/dev/null || {
  echo "required systemd/journal/HTTP tools are unavailable" >&2
  exit 77
}
[[ -f $config && -r $config && ! -L $config ]] || {
  echo "marked acceptance configuration is missing" >&2
  exit 77
}
id laso >/dev/null 2>&1 && getent group laso >/dev/null || {
  echo "dedicated LASO service account/group is unavailable" >&2
  exit 77
}
grep -Fqx '# LASO_SYSTEMD_ACCEPTANCE_FIXTURE' "$config" || {
  echo "refusing to modify a configuration without the acceptance marker" >&2
  exit 77
}
systemctl cat "$unit" | grep -Fq -- '--config "/etc/laso/laso.yaml"' || {
  echo "installed unit does not target the expected acceptance configuration" >&2
  exit 77
}
systemctl is-active --quiet "$unit" && {
  echo "stop the acceptance service before running config-failure checks" >&2
  exit 77
}

port=$(sed -nE 's/^api_port:[[:space:]]*([0-9]+).*$/\1/p' "$config" | head -n1)
[[ $port =~ ^[0-9]+$ ]] || {
  echo "acceptance configuration must specify api_port" >&2
  exit 77
}
host=$(sed -nE 's/^api_host:[[:space:]]*([^[:space:]]+).*$/\1/p' "$config" | head -n1)
[[ $host == 127.0.0.1 || $host == ::1 ]] || {
  echo "acceptance configuration must bind the API to loopback" >&2
  exit 77
}
base="http://127.0.0.1:$port/api/v1"
if curl -fsS --max-time 1 "$base/health" >/dev/null 2>&1; then
  echo "acceptance health endpoint is already served" >&2
  exit 77
fi

[[ ! -e $dropin ]] || {
  echo "acceptance runtime drop-in already exists; refusing to replace it" >&2
  exit 77
}
[[ ! -L $dropin_dir ]] || {
  echo "acceptance runtime drop-in directory is a symlink" >&2
  exit 77
}
tmp=$(mktemp -d /run/laso-systemd-config-check.XXXXXX)
chmod 0700 "$tmp"
cp --archive "$config" "$tmp/valid-config"
dropin_dir_existed=1
if [[ ! -d $dropin_dir ]]; then
  mkdir -m 0755 "$dropin_dir"
  dropin_dir_existed=0
fi
restore() {
  local rc=$?
  systemctl stop "$unit" >/dev/null 2>&1 || true
  install -o root -g laso -m 0640 "$tmp/valid-config" "$config" 2>/dev/null || rc=1
  rm -f -- "$dropin"
  if ((!dropin_dir_existed)); then rmdir "$dropin_dir" 2>/dev/null || true; fi
  systemctl daemon-reload >/dev/null 2>&1 || rc=1
  systemctl reset-failed "$unit" >/dev/null 2>&1 || true
  systemctl start "$unit" >/dev/null 2>&1 || rc=1
  rm -rf -- "$tmp"
  return "$rc"
}
trap restore EXIT

cat >"$tmp/override" <<'EOF'
[Service]
Restart=no
EOF
install -o root -g root -m 0644 "$tmp/override" "$dropin"
systemctl daemon-reload

case_failure() {
  local name=$1 expected=$2 mode=${3:-0640}
  systemctl stop "$unit" >/dev/null 2>&1 || true
  systemctl reset-failed "$unit" >/dev/null 2>&1 || true
  local cursor
  cursor=$(journalctl -b -u "$unit" -n 0 --show-cursor --no-pager |
    sed -n 's/^-- cursor: //p')
  install -o root -g laso -m 0640 "$tmp/candidate" "$config"
  if [[ $mode == unreadable ]]; then chmod 000 "$config"; fi
  systemctl start "$unit" >/dev/null 2>&1 || true
  for _ in $(seq 1 100); do
    [[ $(systemctl show -p ActiveState --value "$unit") == failed ]] && break
    sleep 0.1
  done
  [[ $(systemctl show -p ActiveState --value "$unit") == failed ]] || {
    echo "$name did not leave the system unit failed" >&2
    return 1
  }
  if curl -fsS --max-time 1 "$base/health" >/dev/null 2>&1; then
    echo "$name left a misleading healthy endpoint" >&2
    return 1
  fi
  journalctl -b -u "$unit" --after-cursor="$cursor" --no-pager |
    grep -Fq "$expected" || {
    echo "$name diagnostic was absent from the service journal" >&2
    return 1
  }
  echo "PASS: $name fails clearly under the system manager"
}

cat >"$tmp/candidate" <<EOF
data_dir: /var/lib/laso
api_host: 127.0.0.1
api_port: $port
storage_backend: postgres
EOF
case_failure "invalid storage configuration" "PostgreSQL DSN is required"

cat >"$tmp/candidate" <<EOF
data_dir: /var/lib/laso
api_host: 127.0.0.1
api_port: $port
plugin_dirs:
  - /etc/laso/acceptance-missing-plugin-directory
EOF
case_failure "unavailable plugin directory" "Configured plugin directory is unavailable"

cat >"$tmp/candidate" <<EOF
data_dir: /etc/laso/acceptance-denied-state
db_path: /etc/laso/acceptance-denied-state/laso.db
api_host: 127.0.0.1
api_port: $port
EOF
case_failure "unwritable state directory" "Read-only file system"

cat >"$tmp/candidate" <<'EOF'
data_dir: [malformed
EOF
case_failure "malformed configuration" "Invalid configuration YAML"

cat >"$tmp/candidate" <<EOF
data_dir: /var/lib/laso
api_host: 127.0.0.1
api_port: $port
EOF
case_failure "unreadable configuration" "Cannot open configuration file" unreadable

systemctl stop "$unit" >/dev/null 2>&1 || true
systemctl reset-failed "$unit" >/dev/null 2>&1 || true
cursor=$(journalctl -b -u "$unit" -n 0 --show-cursor --no-pager |
  sed -n 's/^-- cursor: //p')
rm -f -- "$config"
systemctl start "$unit" >/dev/null 2>&1 || true
for _ in $(seq 1 100); do
  [[ $(systemctl show -p ActiveState --value "$unit") == failed ]] && break
  sleep 0.1
done
[[ $(systemctl show -p ActiveState --value "$unit") == failed ]] || {
  echo "missing configuration did not leave the system unit failed" >&2
  exit 1
}
if curl -fsS --max-time 1 "$base/health" >/dev/null 2>&1; then
  echo "missing configuration left a misleading healthy endpoint" >&2
  exit 1
fi
journalctl -b -u "$unit" --after-cursor="$cursor" --no-pager |
  grep -Fq "Cannot open configuration file" || {
  echo "missing configuration diagnostic was absent from the service journal" >&2
  exit 1
}
echo "PASS: missing configuration fails clearly under the system manager"

echo "System-manager configuration-failure checks passed; restoring valid marked fixture."
