# Deterministic distributed example

This example runs one owner and one worker against a disposable PostgreSQL
schema. The worker uses LASO's built-in deterministic reference process, so no
provider account or proprietary executable is required. The same configuration
shape can be placed on separate machines; only the PostgreSQL connection and
explicitly staged workspace need to be reachable by both instances.

The pipeline requires the `deterministic` capability, creates a durable worker
job, stages a bounded content-addressed workspace through the artifact gateway,
has the worker create a deterministic 2 MiB result object, validates the worker
result with a deterministic validator, and downloads/verifies the returned
artifact from the owner before reaching the output boundary.

## Run it

From the repository root on a Linux host with a disposable authenticated
PostgreSQL database:

```sh
export LASO_TEST_POSTGRES_DSN='host=127.0.0.1 port=5432 dbname=laso_test user=laso_test'
cmake -S . -B build-postgres -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DLASO_ENABLE_POSTGRES=ON
cmake --build build-postgres --parallel 2
bash examples/distributed/run.sh build-postgres .
```

The DSN is read from the environment and is never written into the repository
or printed by the script. Use a dedicated schema/database and loopback-only
PostgreSQL for local testing. The script chooses a unique schema and removes
its temporary owner/worker state on exit.

The script starts both instances on loopback for a reproducible smoke test.
For a two-machine run, render `owner.yaml.in` on the owner and `worker.yaml.in`
on the worker. Keep the worker's `data_dir` and process-worker roots local to
the worker. Do not share host absolute paths as workflow state and do not
expose PostgreSQL or the artifact gateway publicly; use a private network or
authenticated tunnels. The worker's artifact URL must reach only the owner's
authenticated object gateway.

## Inspect and recover

After a run, use the owner CLI against the rendered owner configuration:

```sh
build-postgres/bin/laso --config owner.yaml run list
build-postgres/bin/laso --config owner.yaml run inspect RUN_ID
build-postgres/bin/laso --config owner.yaml node-work list --run-id RUN_ID
build-postgres/bin/laso --config owner.yaml instance list
```

To exercise recovery, stop only the worker LASO process while a longer worker
job is active, wait for the lease to expire, then start a replacement worker
with the same capability. The replacement receives a new attempt/fence; a late
completion from the old worker is rejected. Do not repair the database by hand.

For real Codex or OpenCode execution, use the opt-in M3 acceptance harness in
`tests/acceptance/` and follow the provider-specific allowed-root guidance. The
deterministic example intentionally remains credential-free.
