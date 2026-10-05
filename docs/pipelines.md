# Pipeline registry and composition

LASO registers pipeline revisions in the configured backend-neutral registry. The
logical identity is `name@version`, not a YAML file path. A definition with
`name: research` and `version: 2` is registered as `research@2`.

Revisions are immutable. Registering the same source again is idempotent;
registering different source with the same name and version returns a conflict.
`research@2` and `research@3` can coexist. An unversioned lookup is accepted only
when exactly one revision exists for that name, preserving old pipelines without
an implicit “latest” policy.

## Subpipeline node

```yaml
nodes:
  research:
    type: subpipeline
    pipeline: research@2
```

The parent node passes its structured payload directly to the child pipeline. The
child's final structured payload becomes the parent node's output; no expression
language or implicit field mapping is used. Parent node `input_schema` and
`output_schema` contracts surround the invocation, while the child pipeline's
optional root `input_schema` and `output_schema` contracts run inside the child.

The child is a normal LASO run. Its record stores `parent_id`, `parent_node_id`,
`parent_message_id`, `pipeline_id`, and `pipeline_version`; the parent stores its
child run IDs, including separate IDs created by retries. Run inspection and the
API run response include a `children` summary array.

Child policies, providers, tools, approvals, retries, timeouts, cancellation,
schemas, events, persistence and recovery are not bypassed. A child approval
waits durably and pauses the parent. Approving the child through the existing API
or CLI resumes the child and then the parent. Cancelling a parent requests
cancellation of every active child, including children created by parallel
branches.

Registration rejects missing or ambiguous references and detects direct and
indirect recursive dependencies. Runtime also enforces `max_subpipeline_depth`,
which defaults to 16 and accepts values from 1 through 64. The registry is local
and durable. Optional multi-instance ownership and deterministic branch work are
documented in [distributed execution](distributed-execution.md); this feature
does not provide a package registry or distributed worker leasing.

The offline examples in `examples/composition` show a versioned two-pipeline
composition and a three-pipeline chain. Register referenced children first, then
the parent, using `laso pipeline register`; start with `laso run start process@1`.

Schedules and event triggers are separate durable records, not pipeline macros.
Create them after registering their pinned pipeline revision with the API or with
`laso schedule create FILE` / `laso trigger create FILE`. Their definitions and
the scheduler's UTC/misfire/overlap semantics are documented in
[scheduling](scheduling.md).
