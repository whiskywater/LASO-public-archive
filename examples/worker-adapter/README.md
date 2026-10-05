# Offline worker adapter

This example demonstrates the complete local flow:

```text
input -> FunctionNode -> WorkerNode -> ValidatorNode -> output
                         |       ^
                         |       |
                    durable job  status events
```

Build LASO first, then from the repository root run:

```sh
cmake --build build
./build/bin/laso --config examples/worker-adapter/config.yaml \
  pipeline register examples/worker-adapter/pipeline.yaml
./build/bin/laso --config examples/worker-adapter/config.yaml \
  run start worker-adapter@1 --input '{"value":42}'
```

The `example-worker` plugin is deterministic, offline, and requires no
credentials or network. It emits worker status events through LASO's host event
ingress callback; the result is persisted as a `WorkerJob`, validated by the
worker node and validator node, and passed to the output boundary.
