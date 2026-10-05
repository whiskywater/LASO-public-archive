# Plugin extension model

Core has separate provider, tool, storage, event, scheduler, identity, artifact and
node interfaces. Native application code can inject these interfaces directly.
Dynamic plugins use the versioned C SDK rather than the compiler-dependent C++ ABI.

The working plugin kinds are deterministic tools, model providers, event sources,
and external workers. Tools/providers use bounded synchronous calls. Event sources use the
same ABI with lifecycle callbacks and a host-owned event submission callback;
they never create runs directly. Other component kinds remain reserved explicitly
and are rejected as unsupported. Native plugins are trusted in-process code and
are not sandboxed.

Workers use lifecycle callbacks plus submit/status/result/cancel callbacks. A
worker submission creates a durable `WorkerJob`; worker status events return via
the same bounded event ingress path used by event-source plugins. See
[worker adapters](workers.md).

The `examples/plugin-model/config.yaml` file maps the example logical model to
the provider plugin. Run it from the repository root after building the example
plugins; it makes no network calls and requires no credentials.

See [plugin ABI](plugin-abi.md) and [SDK README](../plugin_sdk/README.md).

Event-source configuration is declarative under `event_sources` and selects a
plugin/component, optional event schema, bounded JSON configuration, and enabled
state. The host assigns the configured source identity, persists accepted events,
applies external-ID deduplication, and passes them to the existing trigger
engine. See [event sources](event-sources.md).
