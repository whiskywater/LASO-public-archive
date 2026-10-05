# LASO events

LASO events are durable internal records delivered through the in-process event
bus. Event triggers, scheduler observability, event-source plugins, and worker
adapters use this same path. Events are not an external message-broker contract.

Native event and worker plugins may submit bounded JSON events through their
host callback. LASO assigns the source identity, validates the event and optional
schema, persists it through the selected storage backend, deduplicates supplied
external IDs, and then publishes it to the existing trigger subscribers.

Worker status events use `worker.job.*` types and correlate with the durable job
ID and external job ID. A plugin cannot assign LASO source identity or trigger
depth. Terminal worker jobs ignore late status events. Event delivery is durable
within LASO's storage transaction model, but no exactly-once guarantee is made
for arbitrary external systems.
