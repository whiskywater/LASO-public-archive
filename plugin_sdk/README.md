# LASO native plugin SDK

The SDK header has no C++ or third-party dependency. The supplied C examples echo
JSON and provide a deterministic offline model provider. Build them independently on Linux:

```sh
mkdir -p trusted-plugins
cc -std=c11 -Wall -Wextra -Wpedantic -fPIC -shared \
  -I plugin_sdk/include plugin_sdk/examples/echo.c \
  -o trusted-plugins/liblaso_example_tool.so
LASO_PLUGIN_DIR=trusted-plugins ./build/bin/laso plugin list
LASO_PLUGIN_DIR=trusted-plugins ./build/bin/laso run start examples/native-plugin/pipeline.yaml
```

The main CMake build also builds the tool, model-provider, offline event-source,
and offline worker examples. The worker example is placed under
`build/worker-plugins`; the other examples are under `build/plugins`. Do not mix test fixtures into a deployment plugin
directory. Query reports ABI 1; the project version and plugin's own version are
separate.

Read [the ownership and compatibility contract](../docs/plugin-abi.md) before
implementing a plugin. Tool, model-provider, and event-source registration are
and worker registration are implemented; the remaining component kinds return
`LASO_UNSUPPORTED`. Plugins are
privileged native code, loaded only from explicitly configured locations. The
samples have no network or shell behavior. Worker responses may include the
optional normalized `usage` JSON object described in
[the worker documentation](../docs/workers.md); no vendor-specific pricing or
worker implementation is required.
