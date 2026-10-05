#include "laso_plugin.h"
#include <stdio.h>
#include <string.h>

static int extract_job_id(const char *request, uint64_t length, char *out, size_t capacity) {
  static const char key[] = "\"job_id\":\"";
  if (!request || !out || capacity == 0)
    return 0;
  for (uint64_t i = 0; i + sizeof(key) - 1 < length; ++i) {
    if (memcmp(request + i, key, sizeof(key) - 1) != 0)
      continue;
    i += sizeof(key) - 1;
    size_t written = 0;
    while (i < length && request[i] != '"') {
      if (written + 1 >= capacity)
        return 0;
      out[written++] = request[i++];
    }
    if (i >= length)
      return 0;
    out[written] = '\0';
    return written != 0;
  }
  return 0;
}

static int32_t worker_start(void *instance, const char *config, uint64_t length,
                            const laso_call_context *context) {
  (void)instance;
  (void)config;
  (void)length;
  return context && context->abi_version == LASO_PLUGIN_ABI_VERSION ? LASO_OK : LASO_INVALID;
}

static int32_t worker_stop(void *instance, const laso_call_context *context) {
  (void)instance;
  (void)context;
  return LASO_OK;
}

static int32_t worker_health(void *instance, const laso_call_context *context) {
  (void)instance;
  static const char response[] = "{\"healthy\":true,\"detail\":\"offline example worker\"}";
  return context && context->write_json
             ? context->write_json(context->host_context, response, sizeof(response) - 1)
             : LASO_INVALID;
}

static int32_t worker_submit(void *instance, const char *request, uint64_t length,
                             const laso_call_context *context) {
  (void)instance;
  char job_id[129];
  if (!context || !context->write_json || !context->emit_event ||
      !extract_job_id(request, length, job_id, sizeof(job_id)))
    return LASO_INVALID;
  char external_id[160];
  (void)snprintf(external_id, sizeof(external_id), "example-%s", job_id);
  char event_id[192];
  char event[1024];
  (void)snprintf(event_id, sizeof(event_id), "%s:started", external_id);
  int written = snprintf(event, sizeof(event),
                         "{\"type\":\"worker.job.started\",\"external_id\":\"%s\","
                         "\"payload\":{\"job_id\":\"%s\",\"external_job_id\":\"%s\"}}",
                         event_id, job_id, external_id);
  if (written < 0 || (size_t)written >= sizeof(event) ||
      context->emit_event(context->host_context, event, (uint64_t)written) != LASO_OK)
    return LASO_FAILED;
  (void)snprintf(event_id, sizeof(event_id), "%s:completed", external_id);
  written = snprintf(event, sizeof(event),
                     "{\"type\":\"worker.job.completed\",\"external_id\":\"%s\","
                     "\"payload\":{\"job_id\":\"%s\",\"external_job_id\":\"%s\","
                     "\"result\":{\"ok\":true,\"worker\":\"offline-example\"},"
                     "\"metadata\":{\"deterministic\":true}}}",
                     event_id, job_id, external_id);
  if (written < 0 || (size_t)written >= sizeof(event) ||
      context->emit_event(context->host_context, event, (uint64_t)written) != LASO_OK)
    return LASO_FAILED;
  char response[256];
  written = snprintf(response, sizeof(response),
                     "{\"external_job_id\":\"%s\",\"status\":\"Queued\"}", external_id);
  return written < 0 || (size_t)written >= sizeof(response)
             ? LASO_BUFFER_LIMIT
             : context->write_json(context->host_context, response, (uint64_t)written);
}

static int32_t worker_status(void *instance, const char *external_id, uint64_t length,
                             const laso_call_context *context) {
  (void)instance;
  (void)external_id;
  (void)length;
  static const char response[] =
      "{\"status\":\"Completed\",\"result\":{\"ok\":true,\"worker\":\"offline-example\"}}";
  return context && context->write_json
             ? context->write_json(context->host_context, response, sizeof(response) - 1)
             : LASO_INVALID;
}

static int32_t worker_cancel(void *instance, const char *external_id, uint64_t length,
                             const laso_call_context *context) {
  (void)instance;
  (void)external_id;
  (void)length;
  (void)context;
  return LASO_OK;
}

static int32_t worker_result(void *instance, const char *external_id, uint64_t length,
                             const laso_call_context *context) {
  return worker_status(instance, external_id, length, context);
}

static const laso_plugin_descriptor descriptor = {sizeof(laso_plugin_descriptor),
                                                  LASO_PLUGIN_ABI_VERSION, "example-worker",
                                                  "1.0.0", "Offline deterministic LASO worker"};
static const laso_component component = {
    sizeof(laso_component),
    LASO_COMPONENT_WORKER,
    "example-worker",
    "{\"capabilities\":[\"offline\",\"structured_output\"],\"local\":true,"
    "\"supports_recovery\":true,\"supports_cancellation\":true}",
    0,
    0,
    worker_health,
    0,
    0,
    worker_start,
    worker_stop,
    worker_submit,
    worker_status,
    worker_cancel,
    worker_result};

const laso_plugin_descriptor *laso_plugin_query(void) {
  return &descriptor;
}

int32_t laso_plugin_init(const laso_host_api *host, laso_plugin_handle *out) {
  if (!host || !out || host->struct_size < sizeof(laso_host_api) ||
      host->abi_version != LASO_PLUGIN_ABI_VERSION || !host->register_component)
    return LASO_INVALID;
  *out = (void *)1;
  return host->register_component(host->host_context, &component);
}

void laso_plugin_shutdown(laso_plugin_handle handle) {
  (void)handle;
}
