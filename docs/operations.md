# Operations and recovery

## What is durable

Runs, node executions, `NodeWork`, worker jobs, claims, leases, approvals,
interactions, artifacts, and migration state are stored in the configured
backend. Operator inspection should use LASO's CLI/API, not direct database
updates.

For a safe durable lineage view:

```sh
laso run list
laso run inspect RUN_ID
laso node-work list --run-id RUN_ID
laso worker-job inspect JOB_ID
laso instance list
```

The inspection commands return JSON summaries. They include IDs, states,
attempts, owner/lease/fence metadata, provider usage summaries, failures, and
artifact integrity metadata, but intentionally omit message payloads, prompts,
absolute artifact locations, and arbitrary provider metadata.

## Execution semantics

LASO permits more than one physical attempt when a worker or owner fails,
connectivity is uncertain, or a lease expires. A claim records an opaque owner,
lease expiry, and fencing generation. Renewal succeeds only for the current
claim. Every authoritative completion is checked against that fence.

This gives **at-least-once attempt semantics with one authoritative fenced
completion**. It is not exactly-once execution. A stale worker may finish
physically, but its result is rejected and may be retained only as diagnostic
provenance.

## Cancellation

Cancellation has separate durable and physical meanings:

1. cancellation is requested;
2. the worker observes the request;
3. provider termination is initiated;
4. termination is acknowledged only if confirmed;
5. the authoritative node/run reaches its terminal state.

If completion wins before cancellation commits, the completed state remains
authoritative. If cancellation wins, a later provider result cannot change it.
If the worker disappears before acknowledging termination, the state records
that confirmation was unavailable and recovery relies on lease expiry/fencing.

## Controlled worker recovery

The deterministic reference worker makes a safe local recovery drill possible:

1. create a disposable PostgreSQL database and set `LASO_TEST_POSTGRES_DSN`;
2. follow [the distributed example](../examples/distributed/README.md);
3. start the owner and worker;
4. start a run and record its ID;
5. stop only the LASO worker process, leaving PostgreSQL running;
6. wait for the configured lease to expire;
7. start a replacement worker using the same capability configuration;
8. inspect `laso node-work list --run-id RUN_ID` and `laso run inspect RUN_ID`.

The replacement claim has a new attempt/fence. No manual SQL repair is needed;
the old worker's late result cannot commit authoritatively.

## Migration and backup policy

PostgreSQL migrations are applied during storage initialization and recorded in
`laso_schema_migrations`. The current schema version is 8. Upgrade from the
previous supported version is covered by the PostgreSQL test suite. Take a
database backup before upgrading. The migration mechanism does not provide a
general rollback operation; rollback requires restoring the disposable/test
database from backup or following a future documented migration procedure.
