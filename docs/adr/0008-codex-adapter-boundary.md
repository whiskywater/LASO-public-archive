# ADR 0008: Codex is an optional external worker adapter

## Context

LASO already has a backend-neutral worker contract, a supervised local process
transport, durable interaction requests, and generic usage/budget accounting.
Codex provides a structured local app-server interface, but its protocol and
runtime are vendor-specific and are not required by LASO Core.

## Decision

Implement Codex in the optional `laso-codex-worker` executable. It speaks
Codex's structured app-server JSON-RPC protocol and the existing LASO worker
process protocol. It is built only with `LASO_BUILD_CODEX_ADAPTER=ON` and is
configured only through the existing `process_workers` boundary. Session IDs,
structured usage, project-root validation, and LASO-controlled interaction
responses are normalized at the adapter boundary.

## Consequences

Native workers, generic process workers, and existing configurations remain
unchanged. Codex can be upgraded or disabled independently of Core. The
app-server interface is experimental and must be validated per installed Codex
version. Process supervision and project-root checks are isolation boundaries,
not a complete OS/container sandbox; ambiguous in-flight turns retain
conservative recovery semantics.

## Supersedes

None; this records the first Codex-specific adapter decision.
