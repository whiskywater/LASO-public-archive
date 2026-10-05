# Durable scheduling example

This example is offline and organization-neutral. Register the deterministic
pipeline first, then use the JSON files with the schedule/trigger CLI commands:

```sh
laso pipeline register examples/scheduling/pipeline.yaml
laso schedule create examples/scheduling/one-time.json
laso schedule create examples/scheduling/interval.json
laso schedule create examples/scheduling/cron.json
laso trigger create examples/scheduling/event-trigger.json
```

The timestamps are deliberately in the future for safe inspection. The cron
expression is five-field UTC cron. No network, model service, credentials, or
external event adapter is required.
