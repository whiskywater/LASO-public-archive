# API and CLI reference

The server uses JSON bodies, `/api/v1`, loopback port 8080 by default. Registration
accepts `{"yaml":"..."}`; run creation accepts `{"input":{...}}`. Approval accepts
`{"comment":"..."}`. Actor identity comes from the identity provider rather than
an arbitrary request-body actor. Local development identity is unauthenticated.

| Method | Path |
|---|---|
| GET | `/health`, `/version` |
| GET, POST | `/pipelines` |
| GET | `/pipelines/{id}` |
| POST | `/pipelines/{id}/runs` |
| GET | `/runs`, `/runs/{id}` |
| POST | `/runs/{id}/cancel`, `/runs/{id}/resume` |
| GET | `/runs/{id}/events`, `/runs/{id}/attempts`, `/runs/{id}/messages` |
| GET | `/approvals`, `/approvals/{id}` |
| POST | `/approvals/{id}/approve`, `/approvals/{id}/reject` |
| GET | `/providers`, `/tools`, `/plugins` |
| GET | `/event-sources`, `/event-sources/{id}` |
| POST | `/event-sources/{id}/enable`, `/event-sources/{id}/disable` |
| GET | `/workers`, `/workers/{id}` |
| GET | `/worker-jobs`, `/worker-jobs/{id}` |
| POST | `/worker-jobs/{id}/cancel` |
| GET | `/worker-requests`, `/worker-requests/{id}` |
| POST | `/worker-requests/{id}/respond`, `/worker-requests/{id}/answer`, `/worker-requests/{id}/deny`, `/worker-requests/{id}/cancel` |

All paths above are relative to `/api/v1`. Creation/decisions return 201/202; callers
inspect run state separately. Errors use 400 (validation), 403 (policy), 404, 409
(state conflict), 429 (capacity), or 503 (storage). Request bodies are capped at
1 MiB, headers at 16 KiB and connections at 128. One request is served per
connection, with 15-second I/O deadlines. Malformed/oversized HTTP transport input
closes the connection. List endpoints accept `?limit=50&offset=0`; limits are
1..100, default 50. Responses are capped at 4 MiB; reduce the page size for large
stored messages. The C++ storage interface also supports bounded pagination.

```text
laso [--config FILE] [--data-dir DIR] version
laso health
laso pipeline validate FILE
laso pipeline register FILE
laso pipeline list
laso pipeline show NAME
laso run start NAME_OR_FILE [--input JSON] [--actor NAME]
laso run list
laso run show ID
laso run inspect ID
laso run cancel ID
laso run resume ID
laso approval list
laso approval approve ID [--actor NAME] [--comment TEXT]
laso approval reject ID [--actor NAME] [--comment TEXT]
laso plugin list
laso provider list
laso tool list
laso event-source list
laso event-source show ID
laso event-source enable ID
laso event-source disable ID
laso worker list
laso worker show ID
laso worker-job list
laso worker-job show ID
laso worker-job inspect ID
laso worker-job cancel ID
laso node-work list [--run-id ID]
laso node-work show ID
laso artifact list [--run-id ID]
laso artifact verify
laso artifact gc [--execute] [--grace-seconds N]
laso instance list
```

Pipeline IDs may be explicit revisions such as `research@2`; `pipeline show` and
`run start` accept that identity. A run response includes its `pipeline_version`,
parent fields when nested, and a `children` array containing child run IDs,
pipeline revisions, parent node IDs and states. `run show` uses the same view as
the API. `run inspect`, `node-work`, and `worker-job inspect` are operator views:
they omit message payloads, prompts, absolute artifact locations, and arbitrary
provider metadata while retaining durable state, attempts, leases, fences,
failures, and integrity summaries.

CLI run commands wait until execution finishes or reaches a durable wait. JSON
results go to stdout; logs/errors go to stderr. Failed/timed-out runs return status
2. The CLI opens local services and requires exclusive database ownership; it must
not run against a database currently owned by a daemon. Health is a local startup
check. This avoids silently running a second executor behind an HTTP daemon.

Config precedence: defaults < YAML file < `LASO_` environment < CLI overrides.
`--config` selects a file; otherwise `LASO_CONFIG` selects it. The `.env.example`
file is documentation, not automatically loaded. Plugin directory environment
override replaces the YAML directory list. API host override still requires
`allow_remote_api` for any non-loopback binding. Configuration uses no shell or
environment interpolation in YAML payloads.
