# Linux build and deployment

Primary targets: Ubuntu 24.04 and Debian 13 on x86-64. The source intentionally
requires Linux; there are no alternate Windows loaders or native runtime branches.
ARM64 is not validated, but no x86-specific application data representation is used.

Install the dependencies listed in the README. For a fresh validation, follow
[first Linux validation](first-linux-validation.md), including both compilers,
formatting, clang-tidy, sanitizers and process-restart tests. No dependencies are
downloaded by CMake.

## Local native execution

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build
ctest --test-dir build --output-on-failure
./build/bin/laso run start examples/hello-pipeline/pipeline.yaml
./build/bin/laso-server --config config/laso.example.yaml
```

Local data defaults to `.laso`; root is not required. Run the CLI from the same
working directory or use `LASO_DATA_DIR`. `LASO_DB_PATH` explicitly selects a
database and takes precedence over the derived data-directory location.

## systemd

Configure the install prefix before building; the generated system unit embeds
that prefix's absolute server executable path and is installed under
`<prefix>/lib/systemd/system`. The default prefix is `/usr/local`. Do not change
the install prefix only at `cmake --install --prefix` time for a systemd install,
because the generated unit would still refer to the configure-time path.

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build build --parallel 2
sudo cmake --install build
sudo groupadd --system laso
sudo useradd --system --gid laso --home-dir /var/lib/laso laso
sudo install -d -m 0750 -o root -g laso /etc/laso
if [ ! -e /etc/laso/laso.yaml ]; then
  sudo install -m 0640 -o root -g laso \
    /usr/local/share/laso/laso.systemd.example.yaml /etc/laso/laso.yaml
fi
sudo systemctl daemon-reload
sudo systemctl enable --now laso
curl -fsS http://127.0.0.1:8080/api/v1/health
journalctl -u laso --since today
```

The account-creation commands are one-time setup; skip each command if its group
or user already exists. The service runs as `laso:laso`, not root. `StateDirectory`
creates `/var/lib/laso` with mode `0700` and grants it as the service's writable
state path. Keep the root-owned configuration readable by the service group and
mode `0640`. The example config binds the unauthenticated API to loopback. Do not
add credentials to the example; use a deployment-specific protected secret source
and explicitly allowlist any environment passed to child workers.

The service uses systemd's cgroup process tracking, `KillMode=control-group`,
and a 30-second graceful-stop deadline before systemd may send SIGKILL. LASO handles
SIGTERM and SIGINT through Asio, stops listeners/schedulers, requests cooperative
cancellation, and drains runtime work; a noncooperative in-process plugin cannot
be forcibly stopped safely. `Restart=on-failure` retries abnormal exits with a
5-second delay and a five-start/60-second rate limit. Invalid configuration exits
nonzero and is rate-limited rather than allowed to restart indefinitely. Inspect
logs with `journalctl -u laso`; LASO does not log configuration contents or DSNs.

The installed unit can be checked before activation with:

```sh
systemd-analyze verify /usr/local/lib/systemd/system/laso.service
systemctl status laso
sudo systemctl restart laso
sudo systemctl stop laso
```

Use `tests/acceptance/systemd-lifecycle.sh --preflight` for non-mutating
prerequisite and unit checks. Its opt-in `--user` mode exercises a real transient
systemd user service without root, but does not validate the dedicated system
account or system-unit filesystem sandbox. Set
`LASO_SYSTEMD_ACCEPTANCE_POSTGRES_DSN` and use `--user-postgres` to run the same
recovery checks against a disposable database schema. Dedicated-account
system-unit validation uses the installed `laso.service`; it requires root
privileges and is not simulated by the user-mode harness. For controlled
configuration-failure checks on a disposable test installation, mark its
`/etc/laso/laso.yaml` with `# LASO_SYSTEMD_ACCEPTANCE_FIXTURE`, stop the service,
then run:

```sh
sudo env LASO_SYSTEMD_ALLOW_SYSTEM_MUTATION=1 \
  tests/acceptance/systemd-system-config-failures.sh
```

The harness refuses
unmarked configuration, temporarily installs a runtime-only `Restart=no`
drop-in, and restores the marked config and service behavior on exit. Review
`VALIDATION.md` for tested system-manager scope. Trusted plugin locations must
remain readable within the unit's filesystem restrictions.

## Containers and Debian validation

```sh
docker build --target validation -t laso-validation .
docker run --rm laso-validation
docker compose -f deploy/docker/compose.yaml up --build
```

The multi-stage Dockerfile runs CTest while building. The runtime image contains
native runtime dependencies and an unprivileged service user, not a compiler or
managed runtime. A named volume preserves state. The Linux host-network Compose
example makes the loopback API accessible on the host without exposing an
unauthenticated service to every interface. An alternative bridged deployment
requires deliberate binding/port publication and authentication decisions.

Package installation and Docker builds require distribution network access;
the installed framework and test suite do not require external services. The
healthcheck probes local HTTP only. Systemd acceptance results are recorded in
`VALIDATION.md`; a successful launch is not by itself a production-readiness claim.
