#pragma once
#include <cstdint>
#include <filesystem>
#include <laso/policies/policy.hpp>
#include <laso/providers/provider.hpp>

namespace laso {
struct EventSourceConfig {
  std::string plugin, component, schema;
  bool enabled = false;
  Json config = Json::object();
};
struct WorkerConfig {
  std::string plugin, component, event_schema;
  bool enabled = true;
  Json config = Json::object();
};
struct ProcessWorkerConfig {
  std::string executable;
  std::vector<std::string> args;
  // The child starts with no inherited environment by default. Only these
  // explicitly named parent variables and literal overrides are passed.
  std::vector<std::string> environment_allowlist;
  std::map<std::string, std::string> environment;
  std::uint64_t startup_timeout_ms = 5000, request_timeout_ms = 5000,
                interaction_timeout_ms = 300000;
};
struct Config {
  std::filesystem::path data_dir = ".laso", db_path, artifact_root;
  std::string storage_backend = "sqlite";
  std::string postgres_dsn, postgres_schema = "public";
  std::string artifact_backend = "filesystem";
  std::string artifact_s3_endpoint, artifact_s3_bucket, artifact_s3_region = "us-east-1",
                                                        artifact_s3_prefix = "laso";
  std::filesystem::path artifact_s3_ca_file;
  std::string artifact_service_url, artifact_service_token;
  std::string execution_mode = "single";
  std::string coordination_mode = "single_owner";
  std::vector<std::filesystem::path> plugin_dirs;
  std::vector<std::filesystem::path> schema_roots;
  std::string api_host = "127.0.0.1", artifact_service_host = "127.0.0.1", log_level = "info",
              local_openai_endpoint;
  unsigned api_port = 8080, artifact_service_port = 0, workers = 2, max_runs = 16, max_nodes = 32,
           max_session_sse_streams = 32, max_nodes_per_run = 8, max_models = 4, max_tools = 8,
           max_subpipeline_depth = 16, max_pending_scheduler_launches = 128,
           max_event_trigger_depth = 16, max_event_trigger_deliveries = 1024, max_worker_jobs = 32,
           max_worker_jobs_per_worker = 16, claim_batch_size = 8, max_pending_runs = 1024,
           artifact_s3_max_retries = 2;
  // Zero disables a budget. Token and cost budgets accumulate per run.
  std::uint64_t max_worker_wall_time_ms = 0, max_worker_tokens_per_run = 0;
  std::uint64_t max_artifact_bytes = std::uint64_t{256} * 1024 * 1024,
                max_artifact_temp_bytes = std::uint64_t{512} * 1024 * 1024,
                artifact_cleanup_grace_seconds = 3600;
  std::uint64_t postgres_pool_acquisition_timeout_ms = 1000, coordination_lease_ttl_ms = 30000,
                coordination_heartbeat_interval_ms = 10000, instance_stale_after_ms = 90000;
  std::uint64_t artifact_s3_connect_timeout_ms = 3000, artifact_s3_request_timeout_ms = 30000;
  unsigned postgres_pool_min_connections = 1, postgres_pool_max_connections = 4;
  double max_worker_cost_units_per_run = 0.0;
  bool json_logs = false, allow_network = false, allow_remote_api = false,
       artifact_s3_path_style = false, artifact_s3_allow_http = false;
  std::map<std::string, EventSourceConfig> event_sources;
  std::map<std::string, WorkerConfig> worker_plugins;
  std::map<std::string, ProcessWorkerConfig> process_workers;
  std::map<std::string, ModelBinding> models{{"research", {"mock", "mock-v1"}},
                                             {"reviewer", {"mock", "mock-v1"}}};
  std::vector<PolicyRule> rules;
  void validate();
};
Config load_config(const std::filesystem::path &file = {},
                   const std::map<std::string, std::string> &overrides = {});
void configure_logging(const Config &);
void log_event(const Event &event);
void log_diagnostic(const std::string &event, const Json &safe_metadata = Json::object());
} // namespace laso
