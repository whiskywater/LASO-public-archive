# Durable scheduling and event triggers

LASO schedules and triggers are persisted framework records. They launch the same
normal runtime path as `laso run start` and therefore retain policies, schemas,
retries, deadlines, approvals, plugins, concurrency limits, persistence,
provenance, and nested pipeline behavior.

## Schedule definitions

A schedule pins a registered immutable pipeline revision. The preferred reference
is `pipeline: name@version`; an unversioned name is resolved to the sole existing
revision when the record is created and the concrete name/version is persisted.

```json
{
  "id": "nightly-normalize",
  "name": "normalize once",
  "pipeline": "scheduled-work@1",
  "type": "one_time",
  "at": "2030-01-01T00:00:00.000Z",
  "input": {"mode": "offline"},
  "misfire_policy": "RUN_ONCE",
  "overlap_policy": "ALLOW"
}
```

Supported types are:

- `one_time`: run at the absolute UTC `at` timestamp, then disable itself.
- `interval`: use a positive bounded `interval_ms`; an optional `start_at`
  (stored as `next_due_at`) establishes the first occurrence.
- `cron`: use exactly five UTC fields: `minute hour day-of-month month
  day-of-week`. Lists, ranges, and steps are supported; Sunday is 0 or 7.

Durable timestamps use the fixed UTC form `YYYY-MM-DDTHH:MM:SS.mmmZ`. The local
wait uses a monotonic Asio timer, but every wake rechecks persisted wall time.
Host timezone and DST changes therefore do not alter cron meaning.

## Misfires, overlap, and claims

`SKIP` advances past occurrences missed while LASO was stopped. `RUN_ONCE`
launches one catch-up run and advances past all other missed occurrences; it does
not replay an unbounded backlog. For recurring schedules, `ALLOW` starts each
due occurrence subject to normal runtime capacity, `SKIP` records an overlapping
occurrence as skipped, and `QUEUE_ONE` collapses overlaps into one queued run.

Before a launch, LASO inserts a durable occurrence claim identified by
`schedule_id|due_at`. A restart can reconcile a claim with its run or retry a
claim left pending. This prevents the obvious duplicate within the storage
transaction/ownership model, but LASO makes no distributed exactly-once claim.
Capacity failures remain bounded durable pending work and are retried by the
scheduler. Failed child/runtime work uses ordinary runtime retry semantics.

Disabling prevents new launches; active runs continue. Deleting is a soft delete:
future work stops and historical runs/claims remain inspectable. Updates are
atomic and do not change the pinned pipeline unless a new pipeline reference is
explicitly supplied.

## Event triggers

```json
{
  "id": "on-artifact",
  "name": "process imported artifacts",
  "pipeline": "scheduled-work@1",
  "event": "artifact.created",
  "match": {"source": "import"}
}
```

Matching is exact on event type and optional scalar metadata keys. A trigger run
receives the source LASO event as `{"event": <event>}`. Delivery records use
`trigger_id|event_id` and are persisted before/with the launch state so replay of
persisted events after restart is deduplicated. Triggers created after an event do
not replay older events. Disabled/deleted triggers do nothing.

Generic native event-source plugins may publish those same durable Events through
the bounded host ingress API. The trigger engine does not distinguish a plugin
origin from an internal event after persistence; source plugin/component identity,
external event ID, occurrence time, ingestion time, and causal fields remain on
the Event for inspection. See [event sources](event-sources.md) for the ABI,
schema, backpressure, lifecycle, and security contract. LASO Core does not include
vendor adapters or network listeners.

Event-caused runs retain `initiation_type: event`, trigger/event IDs, the root
event ID, and causal depth. Events emitted by those runs carry the causal fields;
the configured `max_event_trigger_depth` (default 16, range 1–64) stops obvious
self-triggering loops. `max_event_trigger_deliveries` (default 1024) bounds
in-memory/recovery work. No external event source or network fetch is performed.

## API and CLI

The versioned API provides list/get/create/update/delete and enable/disable routes
under `/api/v1/schedules` and `/api/v1/triggers`. The CLI provides:

```text
laso schedule list|show ID|create FILE|enable ID|disable ID|delete ID
laso trigger  list|show ID|create FILE|enable ID|disable ID|delete ID
laso event-source list|show ID|enable ID|disable ID
```

Run inspection exposes origin fields through the normal run response. Schedule,
trigger, occurrence, delivery, and lifecycle events use the same backend-neutral
storage and event history as the rest of LASO.

## Configuration and guarantees

`max_pending_scheduler_launches`, `max_event_trigger_depth`, and
`max_event_trigger_deliveries` have bounded defaults and may be set through YAML
or `LASO_` environment variables. Each scheduler-created root run uses its own
normal per-run limits while sharing global run/node/model/tool limits. PostgreSQL
uses the existing single-service ownership lease plus transactional claims;
SQLite uses its serialized local adapter. Neither backend provides a distributed
worker scheduler or exactly-once delivery.
