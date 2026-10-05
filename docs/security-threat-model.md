# Security threat model

This document describes the trust boundaries that exist in LASO RC1. It is a
deployment guide, not a claim that LASO is a sandbox or a complete security
boundary.

## Trust boundaries

- The LASO owner process is trusted to enforce pipeline policy and durable
  state transitions.
- A worker process is a constrained participant. In multi-instance mode it is
  treated as crashable and potentially stale; its completion must prove the
  current claim/fence.
- A provider executable and native plugin are deployment-trusted code. They
  can read anything available to their process and are not sandboxed by LASO.
- PostgreSQL is the coordination authority for leases, claims, migrations, and
  fenced writes. It must be private, authenticated, and dedicated to the
  deployment.
- Workspace contents and returned artifact metadata are untrusted input and are
  validated before use.

## Threats and mitigations

| Threat | Mitigation in RC1 |
|---|---|
| Stale worker commits after lease loss | Database-authoritative lease checks and fencing on every authoritative completion. |
| Duplicate completion after uncertain response | Idempotent equivalent completion; conflicting completion fails closed. |
| Late result after cancellation/timeout | Terminal-state immutability and durable reconciliation. |
| Path traversal or absolute workspace path | Relative-path validation, bounded manifests, canonicalization, and explicit staging roots. |
| Symlink/non-regular workspace entry | Rejected by manifest/staging validation. |
| Oversized manifest, file, or artifact | Count, byte, and per-item limits; artifact writes are bounded. |
| Provider process leaks | Explicit owned process groups with bounded graceful/escalated teardown. |
| Credential leakage through workers | Narrow environment allowlists; credentials are not persisted in PostgreSQL state. |
| Accidental PostgreSQL exposure | Bind PostgreSQL privately/loopback-only, authenticate, and use a dedicated database/schema. |
| Native plugin compromise | Plugins are privileged in-process code; load only explicitly trusted plugin directories. |
| Arbitrary remote side effects | Not enabled by default; remote scope is limited to supported agent/worker workloads. |

## Deployment requirements

Run LASO under a dedicated OS account with a dedicated state/workspace root,
restrict file permissions, and keep the API on loopback unless a separately
authenticated deployment is in place. Do not pass SSH keys, GitHub tokens,
provider credentials, or unrelated environment variables to workers.

LASO does not promise OS-level sandboxing, arbitrary tool isolation, exactly-once
effects, or protection from a malicious process that already has equivalent OS
privileges. Those require a separate sandbox/remote-execution design.
