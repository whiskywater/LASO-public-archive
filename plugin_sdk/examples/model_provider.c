#include <laso_plugin.h>
#include <string.h>

static const laso_plugin_descriptor descriptor = {
    sizeof(laso_plugin_descriptor), LASO_PLUGIN_ABI_VERSION, "example-model-provider", "1.0.0",
    "Offline deterministic model provider example"};

static int32_t health(void *instance, const laso_call_context *context) {
  (void)instance;
  static const char result[] = "{\"healthy\":true,\"detail\":\"offline deterministic provider\"}";
  if (!context || !context->write_json)
    return LASO_INVALID;
  return context->write_json(context->host_context, result, sizeof(result) - 1) == LASO_OK
             ? LASO_OK
             : LASO_FAILED;
}

static int32_t generate(void *instance, const char *input, uint64_t input_length,
                        const laso_call_context *context) {
  (void)instance;
  if (!input || !context || !context->write_json ||
      (context->should_stop && context->should_stop(context->host_context)))
    return LASO_CANCELLED;
  if (input_length == 0 || input_length > 1024 * 1024)
    return LASO_INVALID;
  static const char result[] =
      "{\"ok\":true,\"provider\":\"example-model\",\"model\":\"offline-example\","
      "\"output\":{\"text\":\"Offline plugin model response\",\"reviewed\":true}}";
  return context->write_json(context->host_context, result, sizeof(result) - 1) == LASO_OK
             ? LASO_OK
             : LASO_FAILED;
}

LASO_PLUGIN_EXPORT const laso_plugin_descriptor *laso_plugin_query(void) {
  return &descriptor;
}

LASO_PLUGIN_EXPORT int32_t laso_plugin_init(const laso_host_api *host, laso_plugin_handle *out) {
  static const char metadata[] = "{\"version\":\"1.0.0\",\"network\":false,\"remote\":false,"
                                 "\"capabilities\":[\"chat-completions\",\"structured-output\"]}";
  static const laso_component component = {.struct_size = sizeof(laso_component),
                                           .kind = LASO_COMPONENT_MODEL,
                                           .name = "example-model",
                                           .metadata_json = metadata,
                                           .invoke = generate,
                                           .health = health};
  if (!host || !out || host->abi_version != LASO_PLUGIN_ABI_VERSION || !host->register_component)
    return LASO_INVALID;
  if (host->register_component(host->host_context, &component) != LASO_OK)
    return LASO_FAILED;
  *out = (laso_plugin_handle)&component;
  return LASO_OK;
}

LASO_PLUGIN_EXPORT void laso_plugin_shutdown(laso_plugin_handle handle) {
  (void)handle;
}
