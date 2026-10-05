# ADR 0004: External workers are durable adapters

## Context

External execution systems have different APIs and optional capabilities, but
their work must remain inspectable and recoverable through LASO.

## Decision

Workers submit bounded structured requests and report status, results, artifacts,
events, cancellation, and optional normalized usage through the durable
`WorkerJob` contract. Worker plugins do not create pipeline runs directly.

## Consequences

Worker output follows normal policy, schema, retry, deadline, provenance, and
storage paths. Missing usage fields remain missing, and Core contains no vendor
pricing or billing logic.

## Supersedes

None; this records the current architecture.
