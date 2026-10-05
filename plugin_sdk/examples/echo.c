#include "laso_plugin.h"
#include <stddef.h>

static const laso_plugin_descriptor descriptor = {sizeof(laso_plugin_descriptor),
                                                  LASO_PLUGIN_ABI_VERSION, "example-tool", "0.1.0",
                                                  "Harmless deterministic JSON echo"};
static int instance;
static int32_t echo(void *self, const char *input, uint64_t length,
                    const laso_call_context *context) {
  (void)self;
  if (!context || context->struct_size < sizeof(laso_call_context) ||
      context->abi_version != LASO_PLUGIN_ABI_VERSION)
    return LASO_INVALID;
  if (context->should_stop(context->host_context))
    return LASO_CANCELLED;
  return context->write_json(context->host_context, input, length);
}
const laso_plugin_descriptor *laso_plugin_query(void) {
  return &descriptor;
}
int32_t laso_plugin_init(const laso_host_api *host, laso_plugin_handle *out) {
  if (!out)
    return LASO_INVALID;
  *out = NULL;
  if (!host || host->struct_size < sizeof(laso_host_api) ||
      host->abi_version != LASO_PLUGIN_ABI_VERSION)
    return LASO_INVALID;
  laso_component component = {
      .struct_size = sizeof(laso_component),
      .kind = LASO_COMPONENT_TOOL,
      .name = "example.echo",
      .metadata_json =
          "{\"description\":\"Native JSON echo\",\"network\":false,\"timeout_ms\":1000}",
      .instance = &instance,
      .invoke = echo};
  int32_t status = host->register_component(host->host_context, &component);
  if (status != LASO_OK)
    return status;
  *out = &instance;
  return LASO_OK;
}
void laso_plugin_shutdown(laso_plugin_handle handle) {
  (void)handle;
}
