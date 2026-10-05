# Capability matrix

The matrix describes the supported contract in this release candidate. A
successful local unit test does not imply support in every backend mode.

| Capability | PostgreSQL single-owner | PostgreSQL multi-instance |
|---|---|---|---|
| Deterministic pipelines | Supported | Supported |
| Real agents/providers | Local process only | Supported through eligible worker workloads |
| Durable approvals | Supported | Supported for the owner-controlled run |
| Crash/restart recovery | Local durable history | Owner and worker recovery with leases/fencing |
| Distributed `NodeWork` | Not supported | Supported with capability-filtered claims |
| Remote agent workers | Not supported | Supported for bounded worker workloads |
| Worker leasing/fencing | Not applicable | Supported; stale completion is rejected |
| Workspace manifest transport | Not supported | Supported with bounded inline or content-addressed manifests and SHA-256 validation |
| Large artifact transport | Local filesystem by default; optional S3 build | Shared filesystem, authenticated gateway, or optional S3-compatible store; objects remain bounded and content-addressed |
| Arbitrary remote tools | Not supported | Not supported |

LASO provides at-least-once attempt semantics with one authoritative fenced
completion. It does not claim exactly-once execution. Remote side effects,
general remote shell access, and cluster scheduling remain outside this release
candidate. Artifact transfer is limited to bounded, content-addressed objects
on a trusted configured store; S3 garbage collection remains unsupported and
arbitrary remote filesystem access is not enabled.
