# ADR 0001: Pipelines are the root orchestration abstraction

## Context

LASO needs one durable execution model for CLI, API, scheduled, triggered, and
nested work.

## Decision

Pipelines are the root abstraction. Every launch enters the normal runtime,
including child pipelines and external-worker completion paths.

## Consequences

Policies, schemas, retries, deadlines, approvals, limits, provenance, and
storage remain shared. Integrations do not create runs through parallel engines.

## Supersedes

None; this records the current architecture.
