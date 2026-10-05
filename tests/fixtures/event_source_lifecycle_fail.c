#include "laso_plugin.h"
#include <string.h>

static int fail_stop = 0;
static int32_t source_start(void *instance, const char *config, uint64_t length,
                            const laso_call_context *context) {
  (void)instance;
  (void)length;
  if (!context || context->struct_size < sizeof(laso_call_context))
    return LASO_INVALID;
  fail_stop = config && strstr(config, "\"stop_failure\":true") != NULL;
  return config && strstr(config, "\"start_failure\":true") != NULL ? LASO_FAILED : LASO_OK;
}
static int32_t source_stop(void *instance, const laso_call_context *context) {
  (void)instance;
  (void)context;
  return fail_stop ? LASO_FAILED : LASO_OK;
}
static int32_t source_health(void *instance, const laso_call_context *context) {
  (void)instance;
  static const char response[] = "{\"healthy\":true}";
  return context && context->write_json
             ? context->write_json(context->host_context, response, sizeof(response) - 1)
             : LASO_INVALID;
}
static const laso_plugin_descriptor descriptor = {
    sizeof(laso_plugin_descriptor), LASO_PLUGIN_ABI_VERSION, "lifecycle-failure-source", "1.0.0",
    "Test event source lifecycle failures"};
static const laso_component component = {sizeof(laso_component),
                                         LASO_COMPONENT_EVENT,
                                         "lifecycle-failure",
                                         "{\"capabilities\":[\"test\"]}",
                                         0,
                                         0,
                                         source_health,
                                         source_start,
                                         source_stop,
                                         0,
                                         0,
                                         0,
                                         0,
                                         0,
                                         0};
const laso_plugin_descriptor *laso_plugin_query(void) {
  return &descriptor;
}
int32_t laso_plugin_init(const laso_host_api *host, laso_plugin_handle *out) {
  if (!host || !out || !host->register_component)
    return LASO_INVALID;
  *out = (void *)1;
  return host->register_component(host->host_context, &component);
}
void laso_plugin_shutdown(laso_plugin_handle handle) {
  (void)handle;
}
