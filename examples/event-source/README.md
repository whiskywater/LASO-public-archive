# Offline external event-source example

This example demonstrates a generic native event-source plugin without a network,
service credential, model API, or vendor adapter. The deterministic source emits
one `example.item.created` event when enabled. LASO validates its payload,
persists the event, deduplicates its stable external ID, and delivers it through
the existing durable event trigger engine.

From the repository root after a Linux build:

```sh
laso --config examples/event-source/config.yaml \
  pipeline register examples/event-source/pipeline.yaml
laso --config examples/event-source/config.yaml \
  trigger create examples/event-source/trigger.json
laso --config examples/event-source/config.yaml \
  event-source enable offline
laso --config examples/event-source/config.yaml \
  event-source list
```

The source is intentionally disabled in configuration so startup does not emit
anything until explicitly enabled. The example uses a local SQLite data directory
and the plugin produced at `build/plugins`; use a trusted plugin directory in any
deployment.
