# Configuration reference

Configuration is YAML. Values may also be supplied through `LASO_` environment
variables; environment names use uppercase field names, for example
`LASO_DATA_DIR` and `LASO_POSTGRES_DSN`. A value supplied through the explicit
CLI override takes precedence over the file and environment.

`config/laso.example.yaml` is a safe SQLite starting point. It contains no
credentials and binds the development API to loopback.

## Storage and execution

| Field | Default | Meaning |
|---|---:|---|
| `data_dir` | `.laso` | Runtime state and the default local artifact-store parent. Keep it inside a dedicated service-owned directory. |
| `db_path` | `data_dir/laso.db` | SQLite database path when SQLite is selected. |
| `artifact_root` | `data_dir/artifacts` | Content-addressed object store root. In multi-instance mode this must be a trusted shared/reachable filesystem root for every participating instance. |
| `artifact_backend` | `filesystem` | `filesystem` (default) or optional `s3`. S3 requires a build with `LASO_ENABLE_S3=ON`. |
| `artifact_s3_endpoint` | empty | Optional S3-compatible endpoint origin. Empty uses the AWS SDK's region-derived endpoint. Do not include credentials or URL query strings. |
| `artifact_s3_bucket` | empty | Required bucket when `artifact_backend: s3` is selected. LASO does not create buckets. |
| `artifact_s3_region` | `us-east-1` | Signing region used by the S3 client. |
| `artifact_s3_prefix` | `laso` | Dedicated key namespace prefix; raw workspace paths are never used as keys. |
| `artifact_s3_path_style` | `false` | Use path-style requests where required by an S3-compatible service. |
| `artifact_s3_allow_http` | `false` | Explicit loopback-only exception for isolated local testing; production endpoints must use verified HTTPS. |
| `artifact_s3_connect_timeout_ms` | `3000` | Bounded connection timeout. |
| `artifact_s3_request_timeout_ms` | `30000` | Bounded per-request timeout. |
| `artifact_s3_max_retries` | `2` | Bounded SDK retry count (maximum `5`). |
| `max_artifact_bytes` | `268435456` | Maximum size of one streamed artifact object. |
| `max_artifact_temp_bytes` | `536870912` | Maximum configured temporary upload budget; incomplete uploads are private and reaped after the cleanup grace period. |
| `artifact_cleanup_grace_seconds` | `3600` | Age before unreferenced objects or temporary uploads are eligible for cleanup. |
| `artifact_service_host` | `127.0.0.1` | Bind address for the optional authenticated streaming artifact gateway. |
| `artifact_service_port` | `0` | Gateway port on the artifact-store owner; `0` disables the gateway. |
| `artifact_service_url` | empty | `http://host:port` endpoint used by a remote worker to fetch/upload objects. |
| `artifact_service_token` | empty | Required bearer token for the gateway. Treat it as a secret and prefer `LASO_ARTIFACT_SERVICE_TOKEN`. |
| `storage_backend` | `sqlite` | `sqlite` or `postgres`. PostgreSQL requires a PostgreSQL-enabled build. |
| `postgres_dsn` | empty | PostgreSQL connection string. Treat it as a secret when it contains credentials. |
| `postgres_schema` | `public` | Dedicated schema for this LASO deployment. |
| `execution_mode` | `single` | `single` for SQLite/local use or `multi_instance` for PostgreSQL coordination. |
| `coordination.mode` | `single_owner` | Use the documented experimental multi-instance mode only with PostgreSQL. |
| `postgres_pool_min_connections` | `1` | Minimum PostgreSQL pool size. |
| `postgres_pool_max_connections` | `4` | Maximum PostgreSQL pool size. |
| `postgres_pool_acquisition_timeout_ms` | `1000` | Bounded pool acquisition wait. |

SQLite is deliberately single-instance. Do not run multiple LASO processes
against the same SQLite state. PostgreSQL multi-instance mode uses database
leases and fencing; it permits at-least-once attempts, not exactly-once work.

Artifact objects are immutable SHA-256-addressed files. The artifact store
streams files through bounded temporary files and verifies the digest and byte
count before materialization. A multi-instance deployment may either give each
instance the same trusted `artifact_root` or use the optional authenticated
object-only gateway. The gateway exposes no directory listing or arbitrary path
access and must remain loopback/private unless an authenticated deployment
explicitly permits a remote bind. Do not put credentials, provider
environments, or arbitrary host paths in manifests.

The optional S3 backend uses the AWS SDK credential-provider chain and is not a
dependency of the default build. Set credentials through the deployment's
standard SDK mechanism (prefer workload identity/roles); never place keys in
the LASO YAML file. `artifact_backend: s3` cannot be combined with the owner
artifact gateway. Each participating owner/worker must be able to reach the
same bucket directly. Remote GC is intentionally unsupported in this milestone.

## Concurrency, deadlines, and budgets

`workers`, `max_runs`, `max_nodes`, `max_nodes_per_run`, `max_worker_jobs`, and
`max_worker_jobs_per_worker` bound local concurrency. `claim_batch_size` and
`max_pending_runs` bound scheduler/recovery pressure. Node and pipeline
`timeout_ms` values are per-attempt or per-pipeline deadlines in the pipeline
document.

For PostgreSQL coordination, `coordination.lease_ttl_ms` and
`coordination.heartbeat_interval_ms` control bounded ownership. The database's
clock is authoritative for lease expiry.

The optional `max_worker_wall_time_ms`, `max_worker_tokens_per_run`, and
`max_worker_cost_units_per_run` settings limit aggregate worker usage. A zero
value disables the corresponding budget. Missing provider usage metrics are
not interpreted as zero.

## Providers and workers

`models` maps logical model names to provider/model pairs. The built-in mock
provider is offline. Process workers are explicitly configured under
`process_workers`:

```yaml
process_workers:
  reference:
    executable: /path/to/laso-example-worker-host
    args: [--mode, success]
    environment_allowlist: [PATH]
    startup_timeout_ms: 5000
    request_timeout_ms: 5000
    interaction_timeout_ms: 300000
```

The process worker starts with an empty inherited environment unless variables
are named in `environment_allowlist` or supplied as literal `environment`
overrides. Provider credentials remain in the provider environment and are not
stored in PostgreSQL state.

Optional Codex, OpenCode, and Claude adapters use the same supervised worker
boundary. Their executable paths, allowed workspace roots, and environment
allowlists should be deployment-local configuration, never committed examples.

## Security-sensitive settings

`postgres_dsn`, provider credentials, `plugin_dirs`, `schema_roots`, process
worker executable paths, and environment overrides may disclose sensitive
information. Keep them outside version control, use restrictive permissions,
and redact them from logs. `allow_network` and `allow_remote_api` default to
false. A remote API bind requires deployment-owned authentication; LASO's
development identity is intentionally local and unauthenticated.

## Cancellation and approvals

Cancellation is durable and may be observed after a restart. A provider
termination acknowledgement is reported only when the provider process or
transport confirms termination. A worker that disappears before confirmation
does not create a false acknowledgement. Late provider results cannot replace
an authoritative cancelled or otherwise terminal state.

Approval and worker-interaction state is durable. Use the approval CLI/API to
make decisions; do not edit the database manually.
