# External event-source plugins

LASO provides a generic ingress boundary for native event-source plugins. Core
does not contain vendor adapters. A configured source loads from an explicit
plugin directory, starts with declarative JSON configuration, and submits events
through the C ABI host callback:

```text
external system → event-source plugin → bounded host ingress
→ validated durable Event → existing trigger engine → normal pipeline runtime
```

## Configuration

```yaml
plugin_dirs:
  - build/plugins
schema_roots:
  - examples/event-source
event_sources:
  offline:
    plugin: example-event-source
    component: example-event
    schema: event.schema.json
    enabled: false
    config:
      mode: deterministic
```

The configuration key is the stable source ID. `plugin` and optional `component`
select the registered ABI component. The source record exposes plugin/component
identity, plugin version, capabilities, schema reference, configuration hash,
health, enabled state, and bounded lifecycle counters. Resolved secret values and
the source configuration body are not persisted. Source configuration may contain
logical secret references for a future or existing secret-provider integration;
plain credentials should not be placed in YAML.

Event-source components use ABI v1's size-aware lifecycle suffix. `event_start`
receives the bounded configuration JSON and a context containing `emit_event`;
`event_stop` is called during shutdown or disable. A source may emit from its own
threads, but it owns synchronization of its internal state and must quiesce and
join those threads before returning from stop. LASO serializes lifecycle calls,
keeps the library loaded until stop and accepted ingress callbacks have drained,
and catches/translates non-conforming failures at the boundary. Native plugins
are privileged in-process code and are not sandboxed.

## Canonical event and limits

The host accepts a JSON object with a bounded type, optional stable `external_id`,
optional UTC `occurred_at`, `payload`, object `metadata`, and optional causal
fields. LASO assigns `source_id`, plugin/component provenance, ingestion time, and
trigger depth. It rejects source-identity spoofing and nonzero incoming trigger
depth. Payloads are limited to 1 MiB and metadata to 64 KiB; ingress capacity is
bounded by the configured scheduler pending-launch limit (128 by default) and 32
in-flight events per source. A full source receives `LASO_BACKPRESSURE`; it must
apply its own bounded retry/backoff policy rather than assuming unbounded queueing.

If a source declares or configuration selects an event schema, the existing local
JSON Schema validator checks `payload` before the Event is persisted. Invalid
events cannot reach triggers. Schema roots and local-reference security rules are
the same as node-boundary validation.

When `external_id` is supplied, `source_id + external_id` is a durable identity.
The first accepted delivery inserts an external-event claim and the Event in one
storage transaction. A retry returns `LASO_DUPLICATE` and the original event ID.
Without an external ID, LASO does not fabricate exactly-once semantics. SQLite
uses its serialized local adapter; PostgreSQL uses its existing single-service
ownership lease and transactional claim. Neither promises distributed exactly
once delivery.

## Trigger integration and provenance

Accepted events are ordinary durable LASO Events and are published only after
storage succeeds. Existing trigger matching, delivery deduplication, policy
evaluation, runtime limits, pipeline provenance, and event recursion/depth
protection apply unchanged. Event-caused runs retain the event and root-event IDs;
the Event itself records source plugin/component, external ID, occurrence time and
ingestion time. Event metadata is never dumped into lifecycle logs.

## Inspection and operations

Inspect sources through:

```text
laso event-source list
laso event-source show offline
laso event-source enable offline
laso event-source disable offline
GET /api/v1/event-sources
GET /api/v1/event-sources/{id}
POST /api/v1/event-sources/{id}/enable
POST /api/v1/event-sources/{id}/disable
```

Disabling prevents new source callbacks while preserving configuration and
already accepted events. Shutdown stops accepting ingress, drains accepted work,
stops sources in reverse load order, and only then allows libraries to unload.
Restart reloads configured sources and durable source/deduplication state. The
framework intentionally does not install plugins, connect to external systems,
provide a remote event API, or claim delivery exactly once.
