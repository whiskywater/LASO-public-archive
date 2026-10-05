#include "laso_plugin.h"
#include <string.h>

static laso_event_emit_fn emit_event;
static void *emit_context;

static int32_t source_start(void *instance, const char *config_json, uint64_t config_length,
                            const laso_call_context *context) {
  (void)instance;
  (void)config_json;
  (void)config_length;
  if (!context || context->struct_size < sizeof(laso_call_context) || !context->emit_event)
    return LASO_INVALID;
  emit_event = context->emit_event;
  emit_context = context->host_context;
  static const char event_json[] =
      "{\"type\":\"example.item.created\",\"external_id\":\"example-1\","
      "\"occurred_at\":\"2030-01-01T00:00:00.000Z\","
      "\"payload\":{\"value\":42},\"metadata\":{\"source\":\"offline-example\"}}";
  const int32_t status = emit_event(emit_context, event_json, (uint64_t)strlen(event_json));
  return status == LASO_OK || status == LASO_DUPLICATE ? LASO_OK : LASO_FAILED;
}

static int32_t source_stop(void *instance, const laso_call_context *context) {
  (void)instance;
  (void)context;
  emit_event = 0;
  emit_context = 0;
  return LASO_OK;
}

static int32_t source_health(void *instance, const laso_call_context *context) {
  (void)instance;
  static const char health[] = "{\"healthy\":true,\"detail\":\"offline example source\"}";
  if (!context || !context->write_json)
    return LASO_INVALID;
  return context->write_json(context->host_context, health, (uint64_t)strlen(health));
}

static const laso_plugin_descriptor descriptor = {
    sizeof(laso_plugin_descriptor), LASO_PLUGIN_ABI_VERSION, "example-event-source", "1.0.0",
    "Offline deterministic LASO event source"};
static const laso_component component = {sizeof(laso_component),
                                         LASO_COMPONENT_EVENT,
                                         "example-event",
                                         "{\"capabilities\":[\"deterministic\"]}",
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
  if (!host || !out || host->struct_size < sizeof(laso_host_api) || !host->register_component)
    return LASO_INVALID;
  *out = (void *)1;
  if (host->register_component(host->host_context, &component) != LASO_OK) {
    *out = 0;
    return LASO_INVALID;
  }
  return LASO_OK;
}
void laso_plugin_shutdown(laso_plugin_handle handle) {
  (void)handle;
}
