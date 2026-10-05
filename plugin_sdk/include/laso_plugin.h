#ifndef LASO_PLUGIN_H
#define LASO_PLUGIN_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
#define LASO_PLUGIN_ABI_VERSION 1u
#define LASO_PLUGIN_EXPORT __attribute__((visibility("default")))
typedef enum laso_status {
  LASO_OK = 0,
  LASO_INVALID = 1,
  LASO_FAILED = 2,
  LASO_CANCELLED = 3,
  LASO_UNSUPPORTED = 4,
  LASO_BUFFER_LIMIT = 5,
  LASO_DUPLICATE = 6,
  LASO_BACKPRESSURE = 7,
  LASO_STOPPED = 8,
  LASO_UNAVAILABLE = 9
} laso_status;
typedef enum laso_component_kind {
  LASO_COMPONENT_TOOL = 1,
  LASO_COMPONENT_MODEL = 2,
  LASO_COMPONENT_STORAGE = 3,
  LASO_COMPONENT_EVENT = 4,
  LASO_COMPONENT_IDENTITY = 5,
  LASO_COMPONENT_SCHEDULER = 6,
  LASO_COMPONENT_ARTIFACT = 7,
  LASO_COMPONENT_TELEMETRY = 8,
  LASO_COMPONENT_NODE = 9,
  LASO_COMPONENT_WORKER = 10
} laso_component_kind;
typedef int32_t (*laso_event_emit_fn)(void *host_context, const char *event_json,
                                      uint64_t event_length);
/* Strings are UTF-8. Input and context pointers are borrowed for one call only.
 * Plugins return JSON through host-owned write callbacks; no allocation crosses ABI.
 * No exceptions may escape any callback. Calls must cooperate with cancellation.
 */
typedef struct laso_call_context {
  uint32_t struct_size;
  uint32_t abi_version;
  void *host_context;
  int32_t (*should_stop)(void *host_context);
  int32_t (*write_json)(void *host_context, const char *bytes, uint64_t length);
  /* Optional ABI-v1 suffix. Event sources may call this from their own
   * threads. The callback is valid until the event source stop callback
   * returns and never starts a pipeline directly. */
  int32_t (*emit_event)(void *host_context, const char *event_json, uint64_t event_length);
} laso_call_context;
typedef int32_t (*laso_invoke_fn)(void *instance, const char *input_json, uint64_t input_length,
                                  const laso_call_context *context);
/* Optional for MODEL components. The host accepts the original v1 component
 * size, so existing tool plugins remain ABI-compatible. Health output is a
 * bounded JSON object: {"healthy":bool,"detail":string}. */
typedef int32_t (*laso_health_fn)(void *instance, const laso_call_context *context);
typedef int32_t (*laso_event_start_fn)(void *instance, const char *config_json,
                                       uint64_t config_length, const laso_call_context *context);
typedef int32_t (*laso_event_stop_fn)(void *instance, const laso_call_context *context);
typedef int32_t (*laso_worker_submit_fn)(void *instance, const char *request_json,
                                         uint64_t request_length, const laso_call_context *context);
typedef int32_t (*laso_worker_status_fn)(void *instance, const char *external_job_id,
                                         uint64_t external_job_id_length,
                                         const laso_call_context *context);
typedef int32_t (*laso_worker_cancel_fn)(void *instance, const char *external_job_id,
                                         uint64_t external_job_id_length,
                                         const laso_call_context *context);
typedef int32_t (*laso_worker_result_fn)(void *instance, const char *external_job_id,
                                         uint64_t external_job_id_length,
                                         const laso_call_context *context);
typedef struct laso_component {
  uint32_t struct_size;
  uint32_t kind;
  const char *name;
  const char *metadata_json;
  void *instance;
  laso_invoke_fn invoke;
  laso_health_fn health;
  /* Required for LASO_COMPONENT_EVENT. This size-aware suffix preserves the
   * original tool/model component layout and ABI-v1 plugin behavior. */
  laso_event_start_fn event_start;
  laso_event_stop_fn event_stop;
  /* Required for LASO_COMPONENT_WORKER. These callbacks are short and may
   * use emit_event asynchronously until worker_stop returns. */
  laso_event_start_fn worker_start;
  laso_event_stop_fn worker_stop;
  laso_worker_submit_fn worker_submit;
  laso_worker_status_fn worker_status;
  laso_worker_cancel_fn worker_cancel;
  laso_worker_result_fn worker_result;
} laso_component;
typedef struct laso_host_api {
  uint32_t struct_size;
  uint32_t abi_version;
  void *host_context;
  /* Registration is only valid during init. Host copies strings and callbacks.
   * TOOL, MODEL, and EVENT kinds are supported by the v1 host. Existing TOOL
   * and MODEL components may use the original smaller v1 struct size and omit
   * `health` and the event lifecycle suffix.
   */
  int32_t (*register_component)(void *host_context, const laso_component *component);
} laso_host_api;
typedef struct laso_plugin_descriptor {
  uint32_t struct_size;
  uint32_t abi_version;
  const char *name;
  const char *version;
  const char *description;
} laso_plugin_descriptor;
typedef void *laso_plugin_handle;
/* Descriptor and its strings must live until dlclose. Host checks ABI before init. */
LASO_PLUGIN_EXPORT const laso_plugin_descriptor *laso_plugin_query(void);
/* On failure, init must release everything it allocated and leave *out null.
 * On success, shutdown owns releasing the handle and component instances.
 * The host pointer itself is borrowed only during init.
 */
LASO_PLUGIN_EXPORT int32_t laso_plugin_init(const laso_host_api *host, laso_plugin_handle *out);
LASO_PLUGIN_EXPORT void laso_plugin_shutdown(laso_plugin_handle handle);
#ifdef __cplusplus
}
#endif
#endif
